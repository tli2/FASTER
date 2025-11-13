using System.Net;
using FASTER.core;
using FASTER.libdpr;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using protobuf;

namespace TwoPhaseCommit.Tests;

public class TpccShardServiceDistributedTests : IAsyncLifetime, IDisposable
{
    private readonly List<IHost> hosts = new();
    private readonly List<TpccShard> shards = new();
    private readonly List<string> addresses = new();
    private readonly Dictionary<int, string> clusterMap = new();
    private readonly LocalStubDprFinder dprFinder;
    private readonly IVersionScheme versionScheme;

    private const int NUM_SHARDS = 2;
    private const byte SHARD_1_WID = 1;
    private const byte SHARD_2_WID = 2;

    private TpccShardService.TpccShardServiceClient clientShard1;
    private GrpcChannel channelShard1;

    public TpccShardServiceDistributedTests()
    {
        dprFinder = new LocalStubDprFinder();
        versionScheme = new RwLatchVersionScheme();
    }

    private async Task<(IHost, TpccShard, string)> StartShardHostAsync(byte workerId,
        Dictionary<int, string> clusterMap)
    {
        var logSettings = new FasterLogSettings { LogDevice = new NullDevice() };
        var settings = new TpccShardSettings
        {
            logSettings = logSettings,
            clusterMap = clusterMap,
            speculative = false
        };

        var workerOptions = new DprWorkerOptions
        {
            Me = new DprWorkerId(workerId),
            DprFinder = dprFinder,
            CheckpointPeriodMilli = 10,
            RefreshPeriodMilli = 5
        };

        var shard = new TpccShard(settings, versionScheme, workerOptions);
        var bgService = new TpccShardBackgroundService(shard);
        var serviceImpl = new TpccShardServiceImpl(bgService);

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders(); // Disable logging for clean test output
        builder.Services.AddGrpc();
        builder.Services.AddSingleton(bgService);
        builder.Services.AddSingleton(serviceImpl);
        
        var port = 15721 + workerId;
        string address = "http://127.0.0.1:" + port;

        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(IPAddress.Loopback, port, listenOptions =>
            {
                // Explicitly enable HTTP/2 on this unencrypted port
                listenOptions.Protocols = HttpProtocols.Http2;
            });
        });
        var app = builder.Build();
        app.MapGrpcService<TpccShardServiceImpl>();
        await app.StartAsync();

        shards.Add(shard);
        return (app, shard, address);
    }

    public async Task InitializeAsync()
    {
        // 1. Start all shard hosts
        for (byte i = 0; i < NUM_SHARDS; i++)
        {
            // Start host with an empty cluster map for now
            var (host, shard, address) = await StartShardHostAsync(i, new Dictionary<int, string>());
            hosts.Add(host);
            addresses.Add(address);
            clusterMap[i] = address;
        }

        // 2. Now that all addresses are known, update each shard's clusterMap
        // and create their gRPC channels
        for (int i = 0; i < NUM_SHARDS; i++)
        {
            shards[i].settings.clusterMap = clusterMap;
            foreach (var e in clusterMap)
            {
                // Create gRPC channels (Http/2 only)
                var channel = GrpcChannel.ForAddress(e.Value);
                shards[i].channels[e.Key] = channel;
            }
        }

        // 3. Load Sharded Data
        LoadShardedData(shards[0], SHARD_1_WID);
        LoadShardedData(shards[1], SHARD_2_WID);

        // 4. Create a test client connection to Shard 1
        channelShard1 = GrpcChannel.ForAddress(addresses[0], new GrpcChannelOptions
        {
            Credentials = ChannelCredentials.Insecure, // Http/2 only
        });
        clientShard1 = new TpccShardService.TpccShardServiceClient(channelShard1);
    }

    private void LoadShardedData(TpccShard shard, byte wId)
    {
        // Load common data (Items table is replicated)
        shard.items.TryAdd(1, new Item { iId = 1, iPrice = 10.0 });
        shard.items.TryAdd(2, new Item { iId = 2, iPrice = 20.0 });

        // Load warehouse-specific data
        shard.warehouses.TryAdd(wId, new Warehouse { wId = wId, wYtd = 1000.0 });
        shard.districts.TryAdd(new DistrictKey(wId, 1),
            new District { dWId = wId, dId = 1, dYtd = 500.0, dNextOrderId = 3001 });
        shard.customers.TryAdd(new CustomerKey(wId, 1, 1),
            new Customer { cWId = wId, cDId = 1, cID = 1, cBalance = 5000.0, cYtdPayment = 200.0, cPaymentCnt = 2 });
        shard.stocks.TryAdd(new StockKey(wId, 1), new Stock { sWId = wId, sIId = 1, sQuantity = 100 });
        shard.stocks.TryAdd(new StockKey(wId, 2), new Stock { sWId = wId, sIId = 2, sQuantity = 100 });
    }

    public async Task DisposeAsync()
    {
        channelShard1.Dispose();
        foreach (var shard in shards)
        {
            foreach (var channel in shard.channels.Values)
                channel.Dispose();
            shard.Dispose();
        }

        foreach (var host in hosts)
        {
            await host.StopAsync();
            host.Dispose();
        }
    }

    public void Dispose()
    {
        // Nothing synchronous to dispose
    }

    // --- PHASE 1 TESTS ---

    [Fact]
    public async Task TestPayment_Remote_2PC_Commit()
    {
        // --- 1. ARRANGE ---
        double paymentAmount = 100.0;
        var shard1 = shards[0];
        var shard2 = shards[1];

        // Get initial state
        var w1 = shard1.warehouses[SHARD_1_WID];
        var d1 = shard1.districts[new DistrictKey(SHARD_1_WID, 1)];
        var c2 = shard2.customers[new CustomerKey(SHARD_2_WID, 1, 1)];

        double w1_ytd_expected = w1.wYtd + paymentAmount;
        double d1_ytd_expected = d1.dYtd + paymentAmount;
        double c2_bal_expected = c2.cBalance - paymentAmount;
        double c2_ytd_expected = c2.cYtdPayment + paymentAmount;

        var request = new PaymentRequest
        {
            WId = SHARD_1_WID, // Coordinator
            DId = 1,
            CId = 1,
            CwId = SHARD_2_WID, // Participant
            CdId = 1,
            Amount = paymentAmount
        };

        // --- 2. ACT ---
        var response = await clientShard1.PaymentAsync(request);

        // --- 3. ASSERT ---
        response.Success.Should().BeTrue();

        // Verify state on Shard 1 (Coordinator)
        w1.wYtd.Should().Be(w1_ytd_expected);
        d1.dYtd.Should().Be(d1_ytd_expected);

        // Verify state on Shard 2 (Participant)
        c2.cBalance.Should().Be(c2_bal_expected);
        c2.cYtdPayment.Should().Be(c2_ytd_expected);

        // Verify transactions are cleaned up on both shards
        shard1.activeTransactions.Should().BeEmpty();
        shard2.activeTransactions.Should().BeEmpty();
    }

    [Fact]
    public async Task TestNewOrder_Remote_2PC_Commit()
    {
        // --- 1. ARRANGE ---
        var shard1 = shards[0];
        var shard2 = shards[1];

        // Get initial state
        var s1 = shard1.stocks[new StockKey(SHARD_1_WID, 1)]; // Local item
        var s2 = shard2.stocks[new StockKey(SHARD_2_WID, 2)]; // Remote item
        var d1 = shard1.districts[new DistrictKey(SHARD_1_WID, 1)];

        int s1_qty_expected = s1.sQuantity - 5;
        int s2_qty_expected = s2.sQuantity - 8;
        int d1_oid_expected = d1.dNextOrderId;

        var request = new NewOrderRequest
        {
            WId = SHARD_1_WID,
            DId = 1,
            CId = 1,
        };
        request.Items.Add(new protobuf.OrderLine { ItemId = 1, WSupplyingId = SHARD_1_WID, Quantity = 5 }); // Local
        request.Items.Add(new protobuf.OrderLine { ItemId = 2, WSupplyingId = SHARD_2_WID, Quantity = 8 }); // Remote

        // --- 2. ACT ---
        var response = await clientShard1.NewOrderAsync(request);

        // --- 3. ASSERT ---
        response.Success.Should().BeTrue();

        // Verify state on Shard 1 (Coordinator)
        d1.dNextOrderId.Should().Be(d1_oid_expected + 1);
        s1.sQuantity.Should().Be(s1_qty_expected);
        shard1.orders.ContainsKey(new OrderKey(SHARD_1_WID, 1, 1, d1_oid_expected)).Should().BeTrue();

        // Verify state on Shard 2 (Participant)
        s2.sQuantity.Should().Be(s2_qty_expected);

        // Verify transactions are cleaned up on both shards
        shard1.activeTransactions.Should().BeEmpty();
        shard2.activeTransactions.Should().BeEmpty();
    }

    [Fact]
    public async Task TestPayment_Remote_2PC_ParticipantAbort()
    {
        // --- 1. ARRANGE ---
        double paymentAmount = 100.0;
        var shard1 = shards[0];
        var shard2 = shards[1];

        // Get initial state
        var w1 = shard1.warehouses[SHARD_1_WID];
        var d1 = shard1.districts[new DistrictKey(SHARD_1_WID, 1)];
        var c2 = shard2.customers[new CustomerKey(SHARD_2_WID, 1, 1)];

        double w1_ytd_initial = w1.wYtd;
        double d1_ytd_initial = d1.dYtd;
        double c2_bal_initial = c2.cBalance;

        // Manually acquire conflicting lock on Shard 2
        long conflictingTxnId = 10; // Older
        var conflictingTxn = shard2.StartTransaction(conflictingTxnId);
        (await conflictingTxn.TryAccessWrite(c2)).Should().BeTrue();

        var request = new PaymentRequest
        {
            WId = SHARD_1_WID, // Coordinator (TxnId will be > 10)
            DId = 1,
            CId = 1,
            CwId = SHARD_2_WID, // Participant
            CdId = 1,
            Amount = paymentAmount
        };

        // --- 2. ACT ---
        // This gRPC call will initiate Txn > 10.
        // It will call RemotePayment, which will fail TryAccessWrite(c2)
        // due to WAIT-DIE, returning Success=false.
        var response = await clientShard1.PaymentAsync(request);

        // --- 3. ASSERT ---
        response.Success.Should().BeFalse();

        // Verify state on Shard 1 (Coordinator) was rolled back
        w1.wYtd.Should().Be(w1_ytd_initial);
        d1.dYtd.Should().Be(d1_ytd_initial);

        // Verify state on Shard 2 (Participant) was not touched by test txn
        c2.cBalance.Should().Be(c2_bal_initial);

        // Verify test transaction is cleaned up on both shards
        shard1.activeTransactions.Should().BeEmpty();
        shard2.activeTransactions.ContainsKey(conflictingTxnId).Should().BeTrue(); // Conflicting lock still held
        // We can't easily check the test txn ID on shard2 as it's aborted immediately
    }

    // --- PHASE 2 TESTS ---

    [Fact]
    public async Task TestConcurrentRemotePayments_InvariantCheck()
    {
        // --- 1. ARRANGE ---
        const int NUM_THREADS = 10;
        const int OPS_PER_THREAD = 10;
        const double PAYMENT_AMOUNT = 10.0;

        var shard1 = shards[0];
        var shard2 = shards[1];

        // Get initial state
        var w1 = shard1.warehouses[SHARD_1_WID];
        var d1 = shard1.districts[new DistrictKey(SHARD_1_WID, 1)];
        var c2 = shard2.customers[new CustomerKey(SHARD_2_WID, 1, 1)];

        double w1_ytd_initial = w1.wYtd;
        double d1_ytd_initial = d1.dYtd;
        double c2_ytd_initial = c2.cYtdPayment;
        double c2_bal_initial = c2.cBalance;

        var tasks = new List<Task<PaymentResponse>>();

        // --- 2. ACT ---
        for (int i = 0; i < NUM_THREADS; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                PaymentResponse lastResponse = null;
                for (int j = 0; j < OPS_PER_THREAD; j++)
                {
                    var request = new PaymentRequest
                    {
                        WId = SHARD_1_WID, // Coordinator
                        DId = 1,
                        CId = 1,
                        CwId = SHARD_2_WID, // Participant
                        CdId = 1,
                        Amount = PAYMENT_AMOUNT
                    };
                    lastResponse = await clientShard1.PaymentAsync(request);
                }

                return lastResponse;
            }));
        }

        await Task.WhenAll(tasks);

        // --- 3. ASSERT (Distributed Invariant) ---
        // All transactions *should* have succeeded due to retry logic.
        int totalOps = NUM_THREADS * OPS_PER_THREAD;
        double expectedTotalChange = totalOps * PAYMENT_AMOUNT;

        // Get final state
        double w1_ytd_final = w1.wYtd;
        double d1_ytd_final = d1.dYtd;
        double c2_ytd_final = c2.cYtdPayment;
        double c2_bal_final = c2.cBalance;

        // Calculate deltas
        double delta_w1 = w1_ytd_final - w1_ytd_initial;
        double delta_d1 = d1_ytd_final - d1_ytd_initial;
        double delta_c2_ytd = c2_ytd_final - c2_ytd_initial;
        double delta_c2_bal = c2_bal_initial - c2_bal_final; // Inverted

        // Check that work was done
        delta_w1.Should().Be(expectedTotalChange);

        // Check that all other deltas match *exactly*
        delta_d1.Should().Be(expectedTotalChange);
        delta_c2_ytd.Should().Be(expectedTotalChange);
        delta_c2_bal.Should().BeApproximately(expectedTotalChange, 0.001);
    }
}