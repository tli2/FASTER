using FASTER.core;
using FASTER.libdpr;
using Microsoft.Extensions.Logging.Abstractions;
using protobuf;
using TwoPhaseCommit;

public class TpccShardServiceLocalTests : IDisposable
{
    private readonly DprWorkerId worker;
    private readonly TpccShard shard;
    private readonly TpccShardServiceImpl service;
    private readonly TpccShardBackgroundService bgService;
    private readonly TpccShardSettings settings;
    private readonly StateObjectRefreshBackgroundService refresher ;
    private readonly CancellationTokenSource cts = new CancellationTokenSource();

    public TpccShardServiceLocalTests()
    {
        var logSettings = new FasterLogSettings { LogDevice = new NullDevice() };
        settings = new TpccShardSettings
        {
            logSettings = logSettings,
            clusterMap = new Dictionary<int, string>(), // No remotes
            speculative = false
        };

        var workerOptions = new DprWorkerOptions
        {
            Me = new DprWorkerId(0),
            DprFinder = new LocalStubDprFinder(),
            CheckpointPeriodMilli = 5,
            RefreshPeriodMilli = 1
        };
        
        shard = new TpccShard(settings, new RwLatchVersionScheme(), workerOptions);
        shard.ConnectToCluster(out _);
        service = new TpccShardServiceImpl(new TpccShardBackgroundService(shard));
        refresher = new StateObjectRefreshBackgroundService(new NullLogger<StateObjectRefreshBackgroundService>(), shard);
        Task.Run(() => refresher.StartAsync(cts.Token));

        shard.warehouses.TryAdd(1, new Warehouse
        {
            wId = 1,
            wYtd = 0
        });

        shard.districts.TryAdd(new DistrictKey(1, 1), new District
        {
            dId = 1,
            dWId = 1,
            dYtd = 0,
            dNextOrderId = 0
        });

        shard.customers.TryAdd(new CustomerKey(1, 1, 1), new Customer
        {
            cID = 1,
            cDId = 1,
            cWId = 1,
            cBalance = 0,
            cYtdPayment = 0,
            cPaymentCnt = 0
        });
        
        shard.customers.TryAdd(new CustomerKey(1, 1, 2), new Customer
        {
            cID = 2,
            cDId = 1,
            cWId = 1,
            cBalance = 0,
            cYtdPayment = 0,
            cPaymentCnt = 0
        });

        shard.items.TryAdd(1, new TwoPhaseCommit.Item
        {
            iId = 1,
            iPrice = 10
        });
        
        shard.items.TryAdd(2, new TwoPhaseCommit.Item
        {
            iId = 2,
            iPrice = 10
        });

        shard.stocks.TryAdd(new StockKey(1, 1), new Stock
        {
            sIId = 1,
            sWId = 1,
            sQuantity = 100000
        });
        
        shard.stocks.TryAdd(new StockKey(1, 2), new Stock
        {
            sIId = 2,
            sWId = 1,
            sQuantity = 100000
        });
    }

    public void Dispose()
    {
        cts.Cancel();
        refresher.Dispose();
        shard.Dispose();
    }
    
    [Fact]
    public async Task TestPaymentInternal_Local_Success()
    {
        // Get initial state from the shard's real tables
        var warehouse = shard.warehouses[1];
        var district = shard.districts[new DistrictKey(1, 1)];
        var customer = shard.customers[new CustomerKey(1, 1, 1)];

        double expectedWYtd = warehouse.wYtd + 100.0;
        double expectedDYtd = district.dYtd + 100.0;
        double expectedCBalance = customer.cBalance - 100.0;
        double expectedCYtd = customer.cYtdPayment + 100.0;
        int expectedCPaymentCnt = customer.cPaymentCnt + 1;
        
        long logTailBefore = shard.log.TailAddress;

        var request = new PaymentRequest
        {
            WId = 1,
            DId = 1,
            CId = 1,
            CwId = 1, // Local transaction
            CdId = 1,
            Amount = 100.0
        };
        
        shard.StartLocalAction();
        var success = await service.PaymentInternal(request, 42);
        shard.EndAction();
        success.Should().BeTrue();
        
        // Verify data was modified in the shard's tables
        warehouse.wYtd.Should().Be(expectedWYtd);
        district.dYtd.Should().Be(expectedDYtd);
        customer.cBalance.Should().Be(expectedCBalance);
        customer.cYtdPayment.Should().Be(expectedCYtd);
        customer.cPaymentCnt.Should().Be(expectedCPaymentCnt);
        
        // Verify transaction was committed and cleaned up
        shard.activeTransactions.Should().BeEmpty();
        shard.recentlyCommittedTransactions.Should().NotBeEmpty();
        
        // Verify log was written to
        shard.log.TailAddress.Should().BeGreaterThan(logTailBefore);
    }

    [Fact]
    public async Task TestNewOrderInternal_Local_Success()
    {
        // Get initial state
        var district = shard.districts[new DistrictKey(1, 1)];
        var stock1 = shard.stocks[new StockKey(1, 1)];
        var stock2 = shard.stocks[new StockKey(1, 2)];

        int expectedOrderId = district.dNextOrderId;
        int expectedStock1Qty = stock1.sQuantity - 5; // Request asks for 5
        int expectedStock2Qty = stock2.sQuantity - 8; // Request asks for 8

        var request = new NewOrderRequest
        {
            WId = 1,
            DId = 1,
            CId = 1,
        };
        // Add two local items
        request.Items.Add(new protobuf.OrderLine { ItemId = 1, WSupplyingId = 1, Quantity = 5 });
        request.Items.Add(new protobuf.OrderLine { ItemId = 2, WSupplyingId = 1, Quantity = 8 });

        shard.StartLocalAction();
        bool success = await service.NewOrderInternal(request, 42);
        shard.EndAction();

        success.Should().BeTrue();
        
        // Verify district D_NEXT_O_ID was incremented
        district.dNextOrderId.Should().Be(expectedOrderId + 1);
        
        // Verify stock was decremented
        stock1.sQuantity.Should().Be(expectedStock1Qty);
        stock2.sQuantity.Should().Be(expectedStock2Qty);
        
        // Verify new Order and OrderLines were created
        var orderKey = new OrderKey(1, 1, 1, expectedOrderId);
        shard.orders.ContainsKey(orderKey).Should().BeTrue();
        shard.orders[orderKey].oOLCnt.Should().Be(2);

        var olKey1 = new OrderLineKey(1, 1, expectedOrderId, 0);
        var olKey2 = new OrderLineKey(1, 1, expectedOrderId, 1);
        shard.orderLines.ContainsKey(olKey1).Should().BeTrue();
        shard.orderLines.ContainsKey(olKey2).Should().BeTrue();
        shard.orderLines[olKey1].olIId.Should().Be(1);
        shard.orderLines[olKey2].olIId.Should().Be(2);

        // Verify transaction was committed and cleaned up
        shard.activeTransactions.Should().BeEmpty();
    }

    [Fact]
    public async Task TestOrderStatusInternal_Local_Success()
    {
        
        var r1 = new NewOrderRequest
        {
            WId = 1,
            DId = 1,
            CId = 1,
        };
        // Add two local items
        r1.Items.Add(new protobuf.OrderLine { ItemId = 1, WSupplyingId = 1, Quantity = 5 });
        r1.Items.Add(new protobuf.OrderLine { ItemId = 2, WSupplyingId = 1, Quantity = 8 });

        shard.StartLocalAction();
        await service.NewOrderInternal(r1, 42);
        shard.EndAction();
        
        var r2 = new NewOrderRequest
        {
            WId = 1,
            DId = 1,
            CId = 1,
        };
        // Add two local items
        r2.Items.Add(new protobuf.OrderLine { ItemId = 1, WSupplyingId = 1, Quantity = 1 });
        r2.Items.Add(new protobuf.OrderLine { ItemId = 2, WSupplyingId = 1, Quantity = 2 });

        shard.StartLocalAction();
        await service.NewOrderInternal(r2, 43);
        shard.EndAction();
        
        var r3 = new NewOrderRequest
        {
            WId = 1,
            DId = 1,
            CId = 1,
        };
        // Add two local items
        r3.Items.Add(new protobuf.OrderLine { ItemId = 1, WSupplyingId = 1, Quantity = 4 });
        shard.StartLocalAction();
        await service.NewOrderInternal(r3, 44);
        shard.EndAction();
        
        var request = new OrderStatusRequest
        {
            WId = 1,
            DId = 1,
            CId = 1
        };
        
        shard.StartLocalAction();
        var response = await service.OrderStatusInternal(request, 42);
        shard.EndAction();
        response.Success.Should().BeTrue();
        response.OId.Should().Be(2);
        response.CId.Should().Be(1);
        
        // Verify the order lines were loaded
        response.OrderLines.Should().HaveCount(1);
        response.OrderLines[0].ItemId.Should().Be(1);
        response.OrderLines[0].Quantity.Should().Be(4);
        
        // Verify read-only transaction was cleaned up
        shard.activeTransactions.Should().BeEmpty();
    }
    
        [Fact]
    public async Task TestConcurrentWorkload_Invariants()
    {
        const int NUM_WORKER_THREADS = 8;
        const int OPS_PER_THREAD = 1000; // 20 workers * 25 ops = 500 total txns

        // --- Get initial state for final audit ---
        var w = shard.warehouses[1];
        var d = shard.districts[new DistrictKey(1, 1)];
        var c1 = shard.customers[new CustomerKey(1, 1, 1)];
        var c2 = shard.customers[new CustomerKey(1, 1, 2)];
        var s1 = shard.stocks[new StockKey(1, 1)];
        var s2 = shard.stocks[new StockKey(1, 2)];

        // Payment Invariants
        double initial_w_ytd = w.wYtd;
        double initial_d_ytd = d.dYtd;
        double initial_c1_ytd = c1.cYtdPayment;
        double initial_c2_ytd = c2.cYtdPayment;
        double initial_c1_bal = c1.cBalance;
        double initial_c2_bal = c2.cBalance;
        
        // NewOrder Invariants
        int initial_d_nextoid = d.dNextOrderId;
        var initial_orders_count = shard.orders.Count;
        var initial_ol_count = shard.orderLines.Count;
        int initial_s1_qty = s1.sQuantity;
        int initial_s2_qty = s2.sQuantity;

        // --- Invariants for concurrent snapshot reader ---
        double const_WD_YTD = initial_w_ytd - initial_d_ytd;
        double const_DC_YTD = initial_d_ytd - (initial_c1_ytd + initial_c2_ytd);
        
        var workerTasks = new List<Task>();
        var cts = new CancellationTokenSource();

        var readerTask = Task.Run(async () =>
        {
            var txn = new TransactionContext();
            while (!cts.IsCancellationRequested)
            {
                // Use a unique (but high) txnId
                long txnId = Interlocked.Increment(ref shard.timestamp.value) << 56 | shard.Me().guid;
                txn.Reset(txnId);

                // Try to get a snapshot of all payment-related tables
                if (await txn.TryAccessRead(w) &&
                    await txn.TryAccessRead(d) &&
                    await txn.TryAccessRead(c1) &&
                    await txn.TryAccessRead(c2))
                {
                    // --- CONCURRENT INVARIANT CHECK ---
                    double current_w_ytd = w.wYtd;
                    double current_d_ytd = d.dYtd;
                    double current_c1_ytd = c1.cYtdPayment;
                    double current_c2_ytd = c2.cYtdPayment;
                    
                    // The difference between warehouse YTD and district YTD should be constant
                    (current_w_ytd - current_d_ytd).Should().BeApproximately(const_WD_YTD, 0.001);
                    
                    // The difference between district YTD and the sum of its customers' YTD
                    // should be constant
                    (current_d_ytd - (current_c1_ytd + current_c2_ytd)).Should().BeApproximately(const_DC_YTD, 0.001);
                }
                
                txn.ReleaseLocks();
                await Task.Delay(5); // Poll lightly
            }
        });

        // --- 3. ACT (Worker Threads) ---
        for (int i = 0; i < NUM_WORKER_THREADS; i++)
        {
            workerTasks.Add(Task.Run(async () =>
            {
                for (int j = 0; j < OPS_PER_THREAD; j++)
                {
                    // Use the public gRPC methods, which include retry logic
                    double rand = Random.Shared.NextDouble();
                    
                    shard.StartLocalAction();
                    if (rand < 0.45) // 45% Payment
                    {
                        var req = new PaymentRequest
                        {
                            WId = 1, DId = 1, CwId = 1, CdId = 1,
                            CId = (j % 2 == 0) ? 1 : 2, // Alternate customers
                            Amount = 10.0
                        };
                        await service.Payment(req, null);
                    }
                    else if (rand < 0.90) // 45% NewOrder
                    {
                        var req = new NewOrderRequest { WId = 1, DId = 1, CId = 1 };
                        req.Items.Add(new protobuf.OrderLine { ItemId = 1, WSupplyingId = 1, Quantity = 1 });
                        req.Items.Add(new protobuf.OrderLine { ItemId = 2, WSupplyingId = 1, Quantity = 1 });
                        await service.NewOrder(req, null);
                    }
                    else // 10% OrderStatus
                    {
                        var req = new OrderStatusRequest { WId = 1, DId = 1, CId = 1 };
                        await service.OrderStatus(req, null);
                    }
                    shard.EndAction();
                }
            }));
        }

        // --- 4. WAIT & STOP ---
        await Task.WhenAll(workerTasks);
        cts.Cancel(); // Stop the reader
        await readerTask;
        
        // --- Payment Invariant Audit ---
        double final_w_ytd = w.wYtd;
        double final_d_ytd = d.dYtd;
        double final_c1_ytd = c1.cYtdPayment;
        double final_c2_ytd = c2.cYtdPayment;
        double final_c1_bal = c1.cBalance;
        double final_c2_bal = c2.cBalance;
        
        double delta_w_ytd = final_w_ytd - initial_w_ytd;
        double delta_d_ytd = final_d_ytd - initial_d_ytd;
        double delta_c_ytd_sum = (final_c1_ytd - initial_c1_ytd) + (final_c2_ytd - initial_c2_ytd);
        double delta_c_bal_sum = (initial_c1_bal - final_c1_bal) + (initial_c2_bal - final_c2_bal);
        
        // All YTD/Balance deltas must be consistent
        delta_w_ytd.Should().BeGreaterThan(0, "Payment work should have been done");
        delta_w_ytd.Should().BeApproximately(delta_d_ytd, 0.001);
        delta_w_ytd.Should().BeApproximately(delta_c_ytd_sum, 0.001);
        delta_w_ytd.Should().BeApproximately(delta_c_bal_sum, 0.001);
        
        // --- NewOrder Invariant Audit ---
        int final_d_nextoid = d.dNextOrderId;
        var final_orders_count = shard.orders.Count;
        var final_ol_count = shard.orderLines.Count;
        int final_s1_qty = s1.sQuantity;
        int final_s2_qty = s2.sQuantity;

        var total_orders_created = final_orders_count - initial_orders_count;
        int delta_d_nextoid = final_d_nextoid - initial_d_nextoid;
        
        // Number of orders created must match the order ID increment
        total_orders_created.Should().BeGreaterThan(0, "NewOrder work should have been done");
        total_orders_created.Should().Be(delta_d_nextoid);

        // Sum of all quantities from all new order_lines
        int total_quantity_ordered = 0;
        foreach (var ol in shard.orderLines.Values.Where(ol => ol.olOId >= initial_d_nextoid))
        {
            if (ol.olIId == 1) total_quantity_ordered += ol.olQuantity;
            if (ol.olIId == 2) total_quantity_ordered += ol.olQuantity;
        }
        
        // Total stock quantity change
        int delta_s_qty = (initial_s1_qty - final_s1_qty) + (initial_s2_qty - final_s2_qty);
        
        // Stock change must match the sum of quantities from *all* created order lines
        delta_s_qty.Should().Be(total_quantity_ordered);
    }
}