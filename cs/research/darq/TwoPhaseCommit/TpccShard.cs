using System.Collections.Concurrent;
using System.Collections.Concurrent.Extended;
using System.Diagnostics;
using System.Transactions;
using FASTER.common;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.Hosting;
using protobuf;

namespace TwoPhaseCommit;

public class TpccShardSettings
{
    public FasterLogSettings logSettings;
    public Dictionary<int, string> clusterMap;
    public bool speculative;
}

public class TpccShard : StateObject
{
    public TpccShardSettings settings;
    public Dictionary<int, GrpcChannel> channels = new();
    public FasterLog log;
    public LongValueAttachment timestamp = new();

    // Item table is replicated across all shards
    public ConcurrentDictionary<int, Item> items = new();
    public ConcurrentDictionary<byte, Warehouse> warehouses = new();
    public ConcurrentDictionary<DistrictKey, District> districts = new();
    public ConcurrentDictionary<CustomerKey, Customer> customers = new();
    public ConcurrentSortedDictionary<OrderKey, Order> orders = new();
    public ConcurrentSortedDictionary<OrderLineKey, OrderLine> orderLines = new();
    public ConcurrentDictionary<StockKey, Stock> stocks = new();

    public SimpleObjectPool<TransactionContext> objectPool = new(() => new TransactionContext());
    public ConcurrentDictionary<long, TransactionContext> activeTransactions = new();
    public ConcurrentDictionary<long, ConcurrentQueue<TransactionContext>> recentlyCommittedTransactions = new();

    public TpccShard(TpccShardSettings settings, IVersionScheme versionScheme, DprWorkerOptions options) : base(
        versionScheme, options)
    {
        this.settings = settings;
        log = new FasterLog(settings.logSettings);
        AddAttachment(timestamp);
        foreach (var e in settings.clusterMap)
            channels[e.Key] = GrpcChannel.ForAddress(e.Value);
    }

    // Assuming we have smaller than 255 warehouses, guarantees unique txnId across warehouses and that transactions from different warehouses can interleave in order
    public long GetNextTransactionId() => Interlocked.Increment(ref timestamp.value) << 56 | Me().guid;

    public TransactionContext StartTransaction(long txnId)
    {
        var txn = objectPool.Checkout().Reset(txnId);
        activeTransactions[txnId] = txn;
        return txn;
    }

    public async ValueTask Commit(TransactionContext txn)
    {
        if (!txn.ReadOnly())
        {
            await log.EnqueueAsync(LogRecords.CreateCommitRecord(txn.Id()));
            recentlyCommittedTransactions.GetOrAdd(Version(), _ => new ConcurrentQueue<TransactionContext>())
                .Enqueue(txn);
        }

        txn.ReleaseLocks();
        activeTransactions.TryRemove(txn.Id(), out _);
    }

    public async ValueTask Prepare(TransactionContext txn)
    {
        if (!txn.ReadOnly())
            await log.EnqueueAsync(LogRecords.CreateCommitRecord(txn.Id()));

        txn.MarkPrepared();
    }

    public async ValueTask StartTwoPC(TransactionContext txn)
    {
        await log.EnqueueAsync(LogRecords.CreateTwoPCStartRecord(txn.Id()));
        txn.MarkTwoPC();
    }

    public async ValueTask Abort(TransactionContext txn, bool yieldRecord = true)
    {
        if (!txn.ReadOnly() && yieldRecord)
            await log.EnqueueAsync(LogRecords.CreateAbortRecord(txn.Id()));
        txn.Undo();
        txn.ReleaseLocks();
        activeTransactions.TryRemove(txn.Id(), out _);
        objectPool.Return(txn);
    }

    public override void PerformCheckpoint(long version, ReadOnlySpan<byte> metadata, Action onPersist)
    {
        log.CommitStrongly(out _, out _, false, metadata.ToArray(), version, onPersist);
    }

    private unsafe void ReplayLog()
    {
        var it = log.Scan(0, log.TailAddress);
        while (it.UnsafeGetNext(out var b, out _, out _, out _))
        {
            LogRecords.ApplyLogRecord(b, this);
            it.UnsafeRelease();
        }

        var toRemove = new List<long>();
        // Any uncommitted/prepared active transaction at this point must have been lost, so we can abort
        foreach (var e in activeTransactions)
        {
            if (e.Value.Prepared()) continue;
            Abort(e.Value, false);
            toRemove.Add(e.Key);
        }

        foreach (var e in toRemove)
            activeTransactions.TryRemove(e, out _);
    }

    public override void RestoreCheckpoint(long version, out ReadOnlySpan<byte> metadata)
    {
        log = new FasterLog(settings.logSettings);
        log.Recover(version);
        metadata = log.RecoveredCookie;
        // Use timestamp field to determine whether we are rolling back or recovering from a crash
        if (timestamp.value == 0)
        {
            ReplayLog();
        }
        else
        {
            // Can rollback by simply undoing all the active transactions and speculatively committed transactions
            foreach (var txn in activeTransactions.Values)
                Abort(txn, false);

            foreach (var e in recentlyCommittedTransactions)
            {
                Debug.Assert(e.Key > version);
                while (e.Value.TryDequeue(out var txn))
                {
                    txn.Undo();
                    objectPool.Return(txn);
                }
            }

            recentlyCommittedTransactions.Clear();
        }
    }

    public override void PruneVersion(long version)
    {
        if (recentlyCommittedTransactions.TryRemove(version, out var txns))
            while (txns.TryDequeue(out var txn))
                objectPool.Return(txn);
        settings.logSettings.LogCommitManager.RemoveCommit(version);
    }

    public override IEnumerable<Memory<byte>> GetUnprunedVersions()
    {
        var commits = settings.logSettings.LogCommitManager.ListCommits().ToList();
        return commits.Select(commitNum =>
        {
            var newLog = new FasterLog(settings.logSettings);
            newLog.Recover(commitNum);
            var commitCookie = newLog.RecoveredCookie;
            newLog.Dispose();
            return new Memory<byte>(commitCookie);
        });
    }

    public override void Dispose()
    {
        log.Dispose();
    }
}

public class TpccShardBackgroundService : BackgroundService
{
    public TpccShard so;

    public TpccShardBackgroundService(TpccShard so)
    {
        this.so = so;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        so.ConnectToCluster(out _);
        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
    }
}

public class TpccShardServiceImpl : TpccShardService.TpccShardServiceBase
{
    private TpccShardBackgroundService bg;

    public TpccShardServiceImpl(TpccShardBackgroundService bg)
    {
        this.bg = bg;
    }

    private async ValueTask AbortWrapper(TransactionContext txn)
    {
        var v = bg.so.Abort(txn);
        if (v.IsCompleted) return;
        var s = bg.so.DetachFromWorkerAndPauseAction();
        await v;
        if (!await bg.so.TryMergeAndStartActionAsync(s))
            throw new DprSessionRolledBackException(bg.so.WorldLine());
    }

    private async ValueTask CommitWrapper(TransactionContext txn)
    {
        var v = bg.so.Commit(txn);
        if (v.IsCompleted) return;
        var s = bg.so.DetachFromWorkerAndPauseAction();
        await v;
        if (!await bg.so.TryMergeAndStartActionAsync(s))
            throw new DprSessionRolledBackException(bg.so.WorldLine());
    }

    private async ValueTask PrepareWrapper(TransactionContext txn)
    {
        var v = bg.so.Prepare(txn);
        if (v.IsCompleted) return;
        var s = bg.so.DetachFromWorkerAndPauseAction();
        await v;
        if (!await bg.so.TryMergeAndStartActionAsync(s))
            throw new DprSessionRolledBackException(bg.so.WorldLine());
    }

    private async ValueTask StartTwoPCWrapper(TransactionContext txn)
    {
        var v = bg.so.StartTwoPC(txn);
        if (v.IsCompleted) return;
        var s = bg.so.DetachFromWorkerAndPauseAction();
        await v;
        if (!await bg.so.TryMergeAndStartActionAsync(s))
            throw new DprSessionRolledBackException(bg.so.WorldLine());
    }

    private async ValueTask<bool> TryAccessWriteWrapper(TransactionContext txn, RowRecord record)
    {
        var v = txn.TryAccessWrite(record);
        if (v.IsCompleted)
        {
            if (!v.Result)
                await AbortWrapper(txn);
            return v.Result;
        }

        var s = bg.so.DetachFromWorkerAndPauseAction();
        var result = await v;
        if (!await bg.so.TryMergeAndStartActionAsync(s))
            throw new DprSessionRolledBackException(bg.so.WorldLine());
        return result;
    }

    private async ValueTask<bool> TryAccessReadWrapper(TransactionContext txn, RowRecord record)
    {
        var v = txn.TryAccessRead(record);
        if (v.IsCompleted)
        {
            if (!v.Result)
                await AbortWrapper(txn);
            return v.Result;
        }

        var s = bg.so.DetachFromWorkerAndPauseAction();
        var result = await v;
        if (!await bg.so.TryMergeAndStartActionAsync(s))
            throw new DprSessionRolledBackException(bg.so.WorldLine());
        return result;
    }

    private async ValueTask EnqueueWrapper<T>(T e) where T : ILogEnqueueEntry
    {
        var v = bg.so.log.EnqueueAsync(e);
        if (v.IsCompleted) return;
        var s = bg.so.DetachFromWorkerAndPauseAction();
        await v;
        if (!await bg.so.TryMergeAndStartActionAsync(s))
            throw new DprSessionRolledBackException(bg.so.WorldLine());
    }

    private async ValueTask<bool> UpdateStock(TransactionContext txn, protobuf.OrderLine ol)
    {
        // Read-only table, no concurrency control required
        var item = bg.so.items[ol.ItemId];
        var s = bg.so.stocks[new StockKey((byte)ol.WSupplyingId, ol.ItemId)];
        if (!await TryAccessWriteWrapper(txn, s))
            return false;

        var oldQuantity = s.sQuantity;
        txn.AddUndoAction(() => s.sQuantity = oldQuantity);
        var newQuantity = s.sQuantity > ol.Quantity + 10
            ? s.sQuantity - ol.Quantity
            : s.sQuantity - ol.Quantity + 91;
        await EnqueueWrapper(
            LogRecords.CreateUpdateStockRecord(txn.Id(), (byte)ol.WSupplyingId, ol.ItemId, newQuantity));
        s.sQuantity = newQuantity;
        return true;
    }
    
    private GrpcChannel GetChannelForWarehouse(byte wId) => bg.so.channels[wId % bg.so.channels.Count];

    public override async Task<NewOrderResponse> NewOrder(NewOrderRequest request, ServerCallContext context)
    {
        var txnId = bg.so.GetNextTransactionId();
        for (var i = 0; i < TpccConstants.TXN_MAX_RETRY; i++)
        {
            if (await NewOrderInternal(request, txnId))
                return new NewOrderResponse
                {
                    Success = true
                };
            await Task.Yield();
        }

        return new NewOrderResponse
        {
            Success = false
        };
    }

    public async Task<bool> NewOrderInternal(NewOrderRequest request, long txnId)
    {
        var txn = bg.so.StartTransaction(txnId);
        var w = bg.so.warehouses[(byte)request.WId];
        if (!await TryAccessReadWrapper(txn, w)) return false;

        var c = bg.so.customers[new CustomerKey((byte)request.WId, (byte)request.DId, request.CId)];
        if (!await TryAccessReadWrapper(txn, c)) return false;

        var d = bg.so.districts[new DistrictKey((byte)request.WId, (byte)request.DId)];
        if (!await TryAccessWriteWrapper(txn, d)) return false;

        await bg.so.log.EnqueueAsync(
            LogRecords.CreateUpdateNextOrderIdRecord(txn.Id(), (byte)request.WId, (byte)request.DId));
        var oId = d.dNextOrderId++;
        txn.AddUndoAction(() => d.dNextOrderId = oId);

        var newOrderKey = new OrderKey((byte)request.WId, (byte)request.DId, request.CId, oId);
        var newOrder = new Order
        {
            oId = oId,
            oDId = (byte)request.DId,
            oWId = (byte)request.WId,
            oCId = request.CId,
            oEntryD = DateTime.Now,
            oCarrierId = 0,
            oOLCnt = request.Items.Count,
            oAllLocal = 0,
        };
        
        // Should always succeed synchronously as it is a new row
        var acquireResult = txn.TryAccessWrite(newOrder);
        Debug.Assert(acquireResult.IsCompleted && acquireResult.Result);
        
        await EnqueueWrapper(LogRecords.CreateInsertOrderRecord(txn.Id(), newOrder));
        txn.AddUndoAction(() => bg.so.orders.TryRemove(newOrderKey));
        bg.so.orders.TryAdd(new OrderKey((byte)request.WId, (byte)request.DId, request.CId, oId), newOrder);

        var requestsToShards = new List<RemoteOrderRequest>();
        for (var i = 0; i < bg.so.channels.Count; i++)
            requestsToShards.Add(new RemoteOrderRequest
            {
                TxnId = txn.Id()
            });
        var remote = false;

        for (var i = 0; i < request.Items.Count; i++)
        {
            var ol = request.Items[i];
            // Read-only table, no concurrency control required
            var item = bg.so.items[ol.ItemId];

            // Only perform local updates to stock
            if (!bg.so.warehouses.ContainsKey((byte)ol.WSupplyingId))
            {
                remote = true;
                var r = requestsToShards[i % bg.so.channels.Count];
                r.Items.Add(ol);
            }
            else if (!await UpdateStock(txn, ol))
                return false;

            var olKey = new OrderLineKey((byte)request.WId, (byte)request.DId, oId, (byte)i);
            var newOrderLine = new OrderLine
            {
                olOId = oId,
                olDId = (byte)request.DId,
                olWId = (byte)request.WId,
                olNumber = (byte)i,
                olIId = ol.ItemId,
                olSupplyWId = (byte)ol.WSupplyingId,
                olDeliveryD = default,
                olQuantity = ol.Quantity,
                olAmount = ol.Quantity * item.iPrice
            };
            acquireResult = txn.TryAccessWrite(newOrderLine);
            Debug.Assert(acquireResult.IsCompleted && acquireResult.Result);

            await EnqueueWrapper(
                LogRecords.CreateInsertOrderLineRecord(txn.Id(), newOrderLine));
            txn.AddUndoAction(() => bg.so.orderLines.TryRemove(olKey));
            bg.so.orderLines.TryAdd(olKey, newOrderLine);
        }

        if (!remote)
        {
            await CommitWrapper(txn);
            return true;
        }

        await StartTwoPCWrapper(txn);
        var s = bg.so.DetachFromWorkerAndPauseAction();
        if (!bg.so.settings.speculative)
            await s.SpeculationBarrier(bg.so.GetDprFinder());
        var tasksToWait = new List<(TpccShardService.TpccShardServiceClient, Task<RemoteOrderResponse>)>();
        for (var i = 0; i < requestsToShards.Count; i++)
        {
            if (requestsToShards[i].Items.Count == 0) continue;
            var channel = bg.so.channels[i];
            var client = bg.so.settings.speculative
                ? new TpccShardService.TpccShardServiceClient(
                    channel.Intercept(new DprClientInterceptor(s)))
                : new TpccShardService.TpccShardServiceClient(channel);
            tasksToWait.Add((client, client.RemoteOrderAsync(requestsToShards[i]).ResponseAsync));
        }

        await Task.WhenAll(tasksToWait.Select(t => t.Item2));
        var success = tasksToWait.Select(t => t.Item2.Result).All(r => r.Success);

        if (!await bg.so.TryMergeAndStartActionAsync(s))
            throw new DprSessionRolledBackException(s.WorldLine);

        if (success)
            await CommitWrapper(txn);
        else
            await AbortWrapper(txn);

        s = bg.so.DetachFromWorkerAndPauseAction();
        // Asynchronously notify the remote warehouse of the outcome
        Task.Run(async () =>
        {
            if (!bg.so.settings.speculative)
                await s.SpeculationBarrier(bg.so.GetDprFinder());
            foreach (var v in tasksToWait)
            {
                if (success)
                    await v.Item1.CommitRemoteParticipantAsync(new CommitRemoteParticipantRequest
                    {
                        // Use local variable because we could re-use TransactionContext object when this goes out
                        TxnId = txnId
                    });
                else
                    await v.Item1.AbortRemoteParticipantAsync(new AbortRemoteParticipantRequest
                    {
                        // Use local variable because we could re-use TransactionContext object when this goes out
                        TxnId = txnId
                    });
            }
        });
        return success;
    }

    private async ValueTask<bool> UpdateCustomer(TransactionContext txn, Customer c, double amount)
    {
        if (!await TryAccessWriteWrapper(txn, c))
            return false;

        await EnqueueWrapper(LogRecords.CreateUpdateCustomerRecord(txn.Id(), c.cWId,
            c.cDId, c.cID, amount));
        var oldBalance = c.cBalance;
        var oldYtdPayment = c.cYtdPayment;
        var oldPaymentCnt = c.cPaymentCnt;
        txn.AddUndoAction(() =>
        {
            c.cBalance = oldBalance;
            c.cYtdPayment = oldYtdPayment;
            c.cPaymentCnt = oldPaymentCnt;
        });
        c.cBalance -= amount;
        c.cYtdPayment += amount;
        c.cPaymentCnt++;
        return true;
    }


    public override async Task<PaymentResponse> Payment(PaymentRequest request, ServerCallContext context)
    {
        var txnId = bg.so.GetNextTransactionId();
        for (var i = 0; i < TpccConstants.TXN_MAX_RETRY; i++)
        {
            if (await PaymentInternal(request, txnId))
                return new PaymentResponse
                {
                    Success = true
                };
            await Task.Yield();
        }

        return new PaymentResponse
        {
            Success = false
        };
    }

    
    public async Task<bool> PaymentInternal(PaymentRequest request, long txnId)
    {
        var txn = bg.so.StartTransaction(txnId);

        var w = bg.so.warehouses[(byte)request.WId];
        if (!await TryAccessWriteWrapper(txn, w)) return false;

        await EnqueueWrapper(
            LogRecords.CreateUpdateWarehouseYtdRecord(txn.Id(), (byte)request.WId, request.Amount));
        var wYtd = w.wYtd;
        txn.AddUndoAction(() => w.wYtd = wYtd);
        w.wYtd += request.Amount;

        var d = bg.so.districts[new DistrictKey((byte)request.WId, (byte)request.DId)];
        if (!await TryAccessWriteWrapper(txn, d)) return false;

        await EnqueueWrapper(
            LogRecords.CreateUpdateDistrictYtdRecord(txn.Id(), (byte)request.WId, (byte)request.DId, request.Amount));
        var dYtd = d.dYtd;
        txn.AddUndoAction(() => d.dYtd = dYtd);
        d.dYtd += request.Amount;

        if (bg.so.warehouses.ContainsKey((byte)request.CwId))
        {
            // This is a local transaction
            var c = bg.so.customers[new CustomerKey((byte)request.CwId, (byte)request.CdId, request.CId)];
            if (!await UpdateCustomer(txn, c, request.Amount)) return false;

            // Skip the insert into history because it's never read in the workload

            await CommitWrapper(txn);
            return true;
        }

        // Start 2pc code path
        await StartTwoPCWrapper(txn);

        var s = bg.so.DetachFromWorkerAndPauseAction();
        if (!bg.so.settings.speculative)
            await s.SpeculationBarrier(bg.so.GetDprFinder());
        var channel = GetChannelForWarehouse((byte)request.CwId);
        var client = bg.so.settings.speculative
            ? new TpccShardService.TpccShardServiceClient(
                channel.Intercept(new DprClientInterceptor(s)))
            : new TpccShardService.TpccShardServiceClient(channel);

        var vote = await client.RemotePaymentAsync(new RemotePaymentRequest
        {
            TxnId = txn.Id(),
            CId = request.CId,
            CwId = request.CwId,
            CdId = request.CdId,
            Amount = request.Amount
        });

        if (!await bg.so.TryMergeAndStartActionAsync(s))
            throw new DprSessionRolledBackException(s.WorldLine);

        if (vote.Success)
            await CommitWrapper(txn);
        else
            await AbortWrapper(txn);

        s = bg.so.DetachFromWorkerAndPauseAction();
        // Asynchronously notify the remote warehouse of the outcome
        Task.Run(async () =>
        {
            if (!bg.so.settings.speculative)
                await s.SpeculationBarrier(bg.so.GetDprFinder());
            if (vote.Success)
                await client.CommitRemoteParticipantAsync(new CommitRemoteParticipantRequest
                {
                    // Use local copy because we may free and reuse the txn object locally
                    TxnId = txnId
                });
            else
                await client.AbortRemoteParticipantAsync(new AbortRemoteParticipantRequest
                {
                    // Use local copy because we may free and reuse the txn object locally
                    TxnId = txnId
                });
        });
        return vote.Success;
    }
    
    public override async Task<OrderStatusResponse> OrderStatus(OrderStatusRequest request, ServerCallContext context)
    {
        var txnId = bg.so.GetNextTransactionId();
        for (var i = 0; i < TpccConstants.TXN_MAX_RETRY; i++)
        {
            var result = await OrderStatusInternal(request, txnId);
            if (result.Success)
                return result;
            await Task.Yield();
        }

        return new OrderStatusResponse
        {
            Success = false
        };
    }

    public async Task<OrderStatusResponse> OrderStatusInternal(OrderStatusRequest request, long transactionId)
    {
        var txn = bg.so.StartTransaction(transactionId);

        var scanKey = new OrderKey((byte)request.WId, (byte)request.DId, request.CId, int.MaxValue);
        var max = bg.so.orders.StartingWith(scanKey, true).Select(e => e.Value).FirstOrDefault((Order) null);
        // If null, the order table is empty. Otherwise, the customer has no orders 
        if (max == null || max.oCId != request.CId || max.oDId != request.DId || max.oWId != request.WId)
            return new OrderStatusResponse
            {
                Success = true,
                CId = request.CId,
                OId = -1,
                WId = -1
            };
        
        if (!await TryAccessReadWrapper(txn, max))
        {
            return new OrderStatusResponse
            {
                Success = false
            };
        }

        var id = max.oId;

        var response = new OrderStatusResponse
        {
            Success = false,
            CId = max.oCId,
            OId = max.oId,
            WId = max.oWId,
        };

        foreach (var v in bg.so.orderLines.Range(
                     new OrderLineKey((byte)request.WId, (byte)request.DId, id, 0),
                     new OrderLineKey((byte)request.WId, (byte)request.DId, id, 16)))
        {
            if (!await TryAccessReadWrapper(txn, v.Value))
            {
                return response;
            }

            response.OrderLines.Add(new protobuf.OrderLine
            {
                ItemId = v.Value.olIId,
                WSupplyingId = v.Value.olSupplyWId,
                Quantity = v.Value.olQuantity
            });
        }

        await CommitWrapper(txn);
        response.Success = true;
        return response;
    }

    public override async Task<RemoteOrderResponse> RemoteOrder(RemoteOrderRequest request, ServerCallContext context)
    {
        // We shouldn't get duplicate requests with presumed aborts
        Debug.Assert(!bg.so.activeTransactions.TryGetValue(request.TxnId, out _));

        var txn = bg.so.StartTransaction(request.TxnId);
        ;
        foreach (var t in request.Items)
            await UpdateStock(txn, t);

        // Implicitly assume that the coordinator asks us to prepare and vote yes
        await PrepareWrapper(txn);

        return new RemoteOrderResponse
        {
            Success = true
        };
    }

    public override async Task<RemotePaymentResponse> RemotePayment(RemotePaymentRequest request,
        ServerCallContext context)
    {
        // We shouldn't get duplicate requests with presumed aborts
        Debug.Assert(!bg.so.activeTransactions.TryGetValue(request.TxnId, out _));

        var txn = bg.so.StartTransaction(request.TxnId);
        ;
        var c = bg.so.customers[new CustomerKey((byte)request.CwId, (byte)request.CdId, request.CId)];
        if (!await UpdateCustomer(txn, c, request.Amount))
            return new RemotePaymentResponse
            {
                Success = false
            };

        // Implicitly assume that the coordinator asks us to prepare and vote yes
        await PrepareWrapper(txn);

        return new RemotePaymentResponse
        {
            Success = true
        };
    }

    public override async Task<CommitRemoteParticipantResponse> CommitRemoteParticipant(
        CommitRemoteParticipantRequest request, ServerCallContext context)
    {
        if (!bg.so.activeTransactions.TryGetValue(request.TxnId, out var txn))
            return new CommitRemoteParticipantResponse();
        Debug.Assert(txn.Prepared());
        await CommitWrapper(txn);
        return new CommitRemoteParticipantResponse();
    }

    public override async Task<AbortRemoteParticipantResponse> AbortRemoteParticipant(
        AbortRemoteParticipantRequest request, ServerCallContext context)
    {
        if (!bg.so.activeTransactions.TryGetValue(request.TxnId, out var txn))
            return new AbortRemoteParticipantResponse();
        await AbortWrapper(txn);
        return new AbortRemoteParticipantResponse();
    }

    public override Task<DeliveryResponse> Delivery(DeliveryRequest request, ServerCallContext context)
    {
        throw new NotImplementedException();
    }

    public override Task<StockLevelResponse> StockLevel(StockLevelRequest request, ServerCallContext context)
    {
        throw new NotImplementedException();
    }

    public override Task<LoadDataResponse> LoadData(LoadDataRequest request, ServerCallContext context)
    {
        TpccWorkloadGenerator.GenerateShardData(bg.so, request);
        return Task.FromResult(new LoadDataResponse
        {
            Success = true
        });
    }
}