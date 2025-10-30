using System.Collections.Concurrent;
using System.Collections.Concurrent.Extended;
using System.Diagnostics;
using System.Transactions;
using FASTER.common;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Hosting;
using protobuf;

namespace TwoPhaseCommit;

public class TransactionContext
{
    private long txnId = -1;
    private List<RowRecord> sharedLocks = new(), exclusiveLocks = new();
    private List<Action> undoActions = new();
    
    public TransactionContext Reset(long transactionId)
    {
        sharedLocks.Clear();
        exclusiveLocks.Clear();
        undoActions.Clear();
        txnId = transactionId;
        return this;
    }
    
    public long Id() => txnId;
    
    public bool ReadOnly() => undoActions.Count == 0;

    public async ValueTask<bool> TryAccessRead(RowRecord record)
    {
        if (!await record.TryAcquireShared(txnId)) return false;
        sharedLocks.Add(record);
        return true;
    }
    
    public async ValueTask<bool> TryAccessWrite(RowRecord record)
    {
        if (!await record.TryAcquireExclusive(txnId)) return false;
        exclusiveLocks.Add(record);
        return true;
    }
    
    public void AddUndoAction(Action action)
    {
        undoActions.Add(action);
    }
    
    public void Undo()
    {
        foreach (var u in undoActions) u();

    }

    public void ReleaseLocks()
    {
        foreach (var l in sharedLocks) l.ReleaseShared(txnId);
        foreach (var l in exclusiveLocks) l.ReleaseExclusive(txnId);
    }
}


public class TpccShardSettings
{
    public FasterLogSettings logSettings;
    public Dictionary<int, (int, string)> clusterMap;
}

public class TpccShard : StateObject
{
    private TpccShardSettings settings;
    public FasterLog log;
    public LongValueAttachment timestamp = new();
    
    // Item table is replicated across all shards
    public ConcurrentDictionary<int, Item> items = new();
    public ConcurrentDictionary<byte, Warehouse> warehouses;
    public ConcurrentDictionary<DistrictKey, District> districts = new();
    public ConcurrentDictionary<CustomerKey, Customer> customers = new();
    public ConcurrentSortedDictionary<OrderKey, Order> orders = new();
    public ConcurrentSortedDictionary<OrderLineKey, OrderLine> orderLines = new();
    public ConcurrentDictionary<StockKey, Stock> stocks = new();
    
    public SimpleObjectPool<TransactionContext> objectPool = new(() => new TransactionContext());
    public ConcurrentDictionary<long, TransactionContext> activeTransactions = new();
    public ConcurrentDictionary<long, TransactionContext> preparedTransactions = new();
    public ConcurrentDictionary<long, ConcurrentQueue<TransactionContext>> recentlyCommittedTransactions = new();

    public TpccShard(TpccShardSettings settings, IVersionScheme versionScheme, DprWorkerOptions options) : base(versionScheme, options)
    {
        this.settings = settings;
        log = new FasterLog(settings.logSettings);
        AddAttachment(timestamp);
    }

    public TransactionContext StartTransaction()
    {
        // Assuming we have smaller than 255 warehouses, guarantees unique txnId across warehouses and that transactions from different warehouses can interleave in order
        var txnId = Interlocked.Increment(ref timestamp.value) << 56 | Me().guid;
        var txn = objectPool.Checkout().Reset(txnId);
        activeTransactions[txnId] = txn;
        return txn;
    }

    public async ValueTask Commit(TransactionContext txn)
    {
        if (!txn.ReadOnly())
        {
            await log.EnqueueAsync(LogRecords.CreateCommitRecord(txn.Id()));
            recentlyCommittedTransactions.GetOrAdd(Version(), _ => new ConcurrentQueue<TransactionContext>()).Enqueue(txn);;
        }
        txn.ReleaseLocks();
        activeTransactions.TryRemove(txn.Id(), out _);
    }
    
    public async ValueTask Prepare(TransactionContext txn)
    {
        if (!txn.ReadOnly())
        {
            // TODO(Tianyu): Change to a PREPARED record
            await log.EnqueueAsync(LogRecords.CreateCommitRecord(txn.Id()));
            preparedTransactions.TryAdd(txn.Id(), txn);;
        }
        txn.ReleaseLocks();
        activeTransactions.TryRemove(txn.Id(), out _);
    }
    
    public async ValueTask CommitPrepared(TransactionContext txn)
    {
        if (!txn.ReadOnly())
        {
            await log.EnqueueAsync(LogRecords.CreateCommitRecord(txn.Id()));
            recentlyCommittedTransactions.GetOrAdd(Version(), _ => new ConcurrentQueue<TransactionContext>()).Enqueue(txn);;
        }
        txn.ReleaseLocks();
        preparedTransactions.TryRemove(txn.Id(), out _);
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
        // Any uncommitted active transaction at this point must have been lost, so we can abort
        foreach (var txn in activeTransactions.Values)
            Abort(txn, false);
        activeTransactions.Clear();
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
            while (txns.TryDequeue(out var txn)) objectPool.Return(txn);
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

    
    private async ValueTask CommitPreparedWrapper(TransactionContext txn)
    {
        var v = bg.so.CommitPrepared(txn);
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

    private async ValueTask EnqueueWrapper<T>(ILogEnqueueEntry e)
    {       
        var v = bg.so.log.EnqueueAsync(e);
        if (v.IsCompleted) return;
        var s = bg.so.DetachFromWorkerAndPauseAction();
        await v;
        if (!await bg.so.TryMergeAndStartActionAsync(s))
            throw new DprSessionRolledBackException(bg.so.WorldLine());
    }
    
    
    public override async Task<NewOrderResponse> NewOrder(NewOrderRequest request, ServerCallContext context)
    {
        // TODO(Tianyu): Check for remote warehouses outside of transaction protection and start a distributed transaction
        var txn = bg.so.StartTransaction();
        var w = bg.so.warehouses[(byte) request.WId];
        if (!await TryAccessReadWrapper(txn, w))
        {
            return new NewOrderResponse
            {
                Success = false
            };
        }
        
        var c = bg.so.customers[new CustomerKey((byte) request.WId, (byte) request.DId, request.CId)];
        if (!await TryAccessReadWrapper(txn, c))
        {
            return new NewOrderResponse
            {
                Success = false
            };
        }

        var d = bg.so.districts[new DistrictKey((byte) request.WId, (byte) request.DId)];
        if (!await TryAccessWriteWrapper(txn, d))
        {
            return new NewOrderResponse
            {
                Success = false
            };
        }

        await bg.so.log.EnqueueAsync(LogRecords.CreateUpdateNextOrderIdRecord(txn.Id(), (byte) request.WId, (byte) request.DId));
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
        await EnqueueWrapper<InsertOrderLogRecord>(LogRecords.CreateInsertOrderRecord(txn.Id(), newOrder));
        txn.AddUndoAction(() => bg.so.orders.TryRemove(newOrderKey));
        bg.so.orders.TryAdd(new OrderKey((byte) request.WId, (byte) request.DId, request.CId, oId), newOrder);;

        for (var i = 0; i < request.Items.Count; i++)
        {
            var ol = request.Items[i];
            // Read-only table, no concurrency control required
            var item = bg.so.items[ol.ItemId];
            var s = bg.so.stocks[new StockKey((byte)ol.WSupplyingId, ol.ItemId)];
            if (!await TryAccessWriteWrapper(txn, s))
            {
                return new NewOrderResponse
                {
                    Success = false
                };
            }
            var oldQuantity = s.sQuantity;
            txn.AddUndoAction(() => s.sQuantity = oldQuantity);
            var newQuantity = s.sQuantity > ol.Quantity + 10 ? s.sQuantity - ol.Quantity : s.sQuantity - ol.Quantity + 91;
            await EnqueueWrapper<UpdateStockLogRecord>(LogRecords.CreateUpdateStockRecord(txn.Id(), (byte) ol.WSupplyingId, ol.ItemId, newQuantity));
            s.sQuantity = newQuantity;
            
            var olKey = new OrderLineKey((byte) request.WId, (byte) request.DId, oId, (byte) i);
            var newOrderLine = new OrderLine
            {
                olOId = oId,
                olDId = (byte) request.DId,
                olWId = (byte) request.WId,
                olNumber = (byte) i,
                olIId = ol.ItemId,
                olSupplyWId = (byte) ol.WSupplyingId,
                olDeliveryD = default,
                olQuantity = ol.Quantity,
                olAmount = ol.Quantity * item.iPrice
            };
            await EnqueueWrapper<InsertOrderLineLogRecord>(
                LogRecords.CreateInsertOrderLineRecord(txn.Id(), newOrderLine));
            txn.AddUndoAction(() => bg.so.orderLines.TryRemove(olKey));
            bg.so.orderLines.TryAdd(olKey, newOrderLine);
        }

        // TODO(Tianyu): Use 2pc if necessary
        await CommitWrapper(txn); 
        return new NewOrderResponse
        {
            Success = true
        };
    }

    public override async Task<PaymentResponse> Payment(PaymentRequest request, ServerCallContext context)
    {
        var txn = bg.so.StartTransaction();
        
        var w = bg.so.warehouses[(byte) request.WId];
        if (!await TryAccessWriteWrapper(txn, w))
        {
            return new PaymentResponse
            {
                Success = false
            };
        }

        await EnqueueWrapper<UpdateWarehouseYtdLogRecord>(
            LogRecords.CreateUpdateWarehouseYtdRecord(txn.Id(), (byte)request.WId, request.Amount));
        var wYtd = w.wYtd;
        txn.AddUndoAction(() => w.wYtd = wYtd);
        w.wYtd += request.Amount;
        
        var d = bg.so.districts[new DistrictKey((byte) request.WId, (byte) request.DId)];;
        if (!await TryAccessWriteWrapper(txn, d))
        {
            return new PaymentResponse
            {
                Success = false
            };
        }

        await EnqueueWrapper<UpdateDistrictYtdLogRecord>(
            LogRecords.CreateUpdateDistrictYtdRecord(txn.Id(), (byte)request.WId, (byte)request.DId, request.Amount));
        var dYtd = d.dYtd;
        txn.AddUndoAction(() => d.dYtd = dYtd);
        d.dYtd += request.Amount;
        
        var c = bg.so.customers[new CustomerKey((byte) request.WId, (byte) request.DId, request.CId)];
        if (!await TryAccessWriteWrapper(txn, c))
        {
            return new PaymentResponse
            {
                Success = false
            };
        }

        await EnqueueWrapper<UpdateCustomerLogRecord>(LogRecords.CreateUpdateCustomerRecord(txn.Id(), (byte)request.WId,
            (byte)request.DId, request.CId, request.Amount));
        var oldBalance = c.cBalance;
        var oldYtdPayment = c.cYtdPayment;
        var oldPaymentCnt = c.cPaymentCnt;
        txn.AddUndoAction(() =>
        {
            c.cBalance = oldBalance;
            c.cYtdPayment = oldYtdPayment;
            c.cPaymentCnt = oldPaymentCnt;
        });
        c.cBalance -= request.Amount;
        c.cYtdPayment += request.Amount;
        c.cPaymentCnt++;
        
        // Skip the insert into history because it's never read in the workload
        
        await CommitWrapper(txn);
        return new PaymentResponse
        {
            Success = true
        };
    }

    public override async Task<OrderStatusResponse> OrderStatus(OrderStatusRequest request, ServerCallContext context)
    {
        var txn = bg.so.StartTransaction();
        
        var scanKey = new OrderKey((byte)request.WId, (byte)request.DId, request.CId, int.MaxValue);
        var max = bg.so.orders.StartingWith(scanKey, true).First().Value;
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

    public override Task<RemoteOrderResponse> RemoteOrder(RemoteOrderRequest request, ServerCallContext context)
    {
        return base.RemoteOrder(request, context);
    }

    public override Task<RemotePaymentResponse> RemotePayment(RemotePaymentRequest request, ServerCallContext context)
    {
        return base.RemotePayment(request, context);
    }

    public override Task<CommitRemoteParticipantResponse> CommitRemoteParticipant(CommitRemoteParticipantRequest request, ServerCallContext context)
    {
        bg.so.activeTransactions.
    }

    public override Task<AbortRemoteParticipantResponse> AbortRemoteParticipant(AbortRemoteParticipantRequest request, ServerCallContext context)
    {
        return base.AbortRemoteParticipant(request, context);
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