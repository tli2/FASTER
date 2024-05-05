using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using FASTER.client;
using System.Diagnostics;
using Grpc.Net.Client;
using darq;
using darq.client;
using System.Collections.Concurrent;

namespace DB {

public class DarqTransactionProcessorService : TransactionProcessor.TransactionProcessorBase, IDarqProcessor {
    private ShardedTransactionManager txnManager;
    private ConcurrentDictionary<(long, long), long> externalToInternalTxnId = new ConcurrentDictionary<(long, long), long>();
    private ConcurrentDictionary<long, TransactionContext> txnIdToTxnCtx = new ConcurrentDictionary<long, TransactionContext>();
    private DarqWal wal;
    private long partitionId;
    // from darqProcessor
    private Darq backend;
    private readonly DarqBackgroundTask _backgroundTask;
    private readonly DarqBackgroundWorkerPool workerPool;
    private readonly ManualResetEventSlim terminationStart, terminationComplete;
    private Thread refreshThread, processingThread;
    private ColocatedDarqProcessorClient processorClient;

    private IDarqProcessorClientCapabilities capabilities;

    private SimpleObjectPool<StepRequest> stepRequestPool = new(() => new StepRequest());
    private int nextWorker = 0;
    private StepRequest reusableRequest = new();
    Dictionary<int, ShardedTable> tables;
    Dictionary<DarqId, GrpcChannel> clusterMap;
    private TpccBenchmark tpccBenchmark;
    protected ILogger logger;
    public DarqTransactionProcessorService(
        long partitionId,
        Dictionary<int, ShardedTable> tables,
        ShardedTransactionManager txnManager,
        DarqWal wal,
        Darq darq,
        DarqBackgroundWorkerPool workerPool,
        Dictionary<DarqId, GrpcChannel> clusterMap,
        ILogger logger = null
    ) {
        this.tables = tables;
        this.logger = logger;
        this.txnManager = txnManager;
        this.partitionId = partitionId;
        this.wal = wal;
        this.clusterMap = clusterMap;

        int PartitionsPerThread = 2;
        int ThreadCount = 2;
        int MachineCount = 2;

        BenchmarkConfig ycsbCfg = new BenchmarkConfig(
            ratio: 0.2,
            attrCount: 10,
            threadCount: ThreadCount,
            insertThreadCount: 12,
            iterationCount: 1,
            nCommitterThreads: 5
            // perThreadDataCount: 1000000
        );
        TpccConfig tpccConfig = new TpccConfig(
            numWh: PartitionsPerThread * ThreadCount * MachineCount,
            partitionsPerThread: PartitionsPerThread
            // newOrderCrossPartitionProbability: 0,
            // paymentCrossPartitionProbability: 0
            // numCustomer: 10,
            // numDistrict: 10,
            // numItem: 10,
            // numOrder: 10,
            // numStock: 10
        );
        
        tpccBenchmark = new TpccBenchmark((int)partitionId, tpccConfig, ycsbCfg, tables, txnManager);

        backend = darq;
        _backgroundTask = new DarqBackgroundTask(backend, workerPool, session => new TransactionProcessorProducerWrapper(clusterMap, session));
        terminationStart = new ManualResetEventSlim();
        terminationComplete = new ManualResetEventSlim();
        this.workerPool = workerPool;
        backend.ConnectToCluster(out _);

        
        
        _backgroundTask.BeginProcessing();

        refreshThread = new Thread(() =>
        {
            while (!terminationStart.IsSet)
                backend.Refresh();
            terminationComplete.Set();
        });
        refreshThread.Start();

        processorClient = new ColocatedDarqProcessorClient(backend);
        processingThread = new Thread(() =>
        {
            processorClient.StartProcessing(this);
        });
        processingThread.Start();
        // TODO(Tianyu): Hacky
        // spin until we are sure that we have started 
        while (capabilities == null) {}
    }
    public override Task<ReadReply> Read(ReadRequest request, ServerCallContext context)
    {
        long internalTid = GetOrRegisterTid(request.PartitionId, request.Tid);
        Table table = tables[request.Key.Table];
        TransactionContext ctx = txnIdToTxnCtx[internalTid];
        PrimaryKey tupleId = new PrimaryKey(request.Key.Table, request.Key.Keys.ToArray()[0], request.Key.Keys.ToArray()[1], request.Key.Keys.ToArray()[2], request.Key.Keys.ToArray()[3], request.Key.Keys.ToArray()[4], request.Key.Keys.ToArray()[5]);
        TupleDesc[] tupleDescs = table.GetSchema();
        ReadReply reply = new ReadReply{ Value = ByteString.CopyFrom(table.Read(tupleId, tupleDescs, ctx))};
        return Task.FromResult(reply);
    }

    public override Task<ReadSecondaryReply> ReadSecondary(ReadSecondaryRequest request, ServerCallContext context)
    {
        PrintDebug($"Reading secondary from rpc service");
        long internalTid = GetOrRegisterTid(request.PartitionId, request.Tid);
        Table table = tables[request.Table];
        TransactionContext ctx = txnIdToTxnCtx[internalTid];
        var (value, pk) = table.ReadSecondary(request.Key.ToArray(), table.GetSchema(), ctx);
        ReadSecondaryReply reply = new ReadSecondaryReply{ Value = ByteString.CopyFrom(value), Key = new PbPrimaryKey{ Keys = {pk.Key1, pk.Key2, pk.Key3, pk.Key4, pk.Key5, pk.Key6}, Table = pk.Table}};
        return Task.FromResult(reply);
    }

    public override Task<PopulateTablesReply> PopulateTables(PopulateTablesRequest request, ServerCallContext context)
    {
        PrintDebug($"Populating tables from rpc service");
        BenchmarkConfig cfg = new BenchmarkConfig(
            seed: request.Seed,
            ratio: request.Ratio,
            threadCount: request.ThreadCount,
            attrCount: request.AttrCount,
            perThreadDataCount: request.PerThreadDataCount,
            iterationCount: request.IterationCount,
            perTransactionCount: request.PerTransactionCount,
            nCommitterThreads: request.NCommitterThreads
        );

        TpccConfig tpccCfg = new TpccConfig(
            numWh: request.NumWh,
            numDistrict: request.NumDistrict,
            numCustomer: request.NumCustomer,
            numItem: request.NumItem,
            numOrder: request.NumOrder,
            numStock: request.NumStock,
            newOrderCrossPartitionProbability: request.NewOrderCrossPartitionProbability,
            paymentCrossPartitionProbability: request.PaymentCrossPartitionProbability,
            partitionsPerThread: request.PartitionsPerThread
        );
        TpccBenchmark tpccBenchmark = new TpccBenchmark((int)partitionId, tpccCfg, cfg, tables, txnManager);
        txnManager.Run();
        tpccBenchmark.PopulateTables();
        txnManager.Terminate();
        PopulateTablesReply reply = new PopulateTablesReply{ Success = true};
        return Task.FromResult(reply);
    }

    public override Task<EnqueueWorkloadReply> EnqueueWorkload(EnqueueWorkloadRequest request, ServerCallContext context)
    {
        switch (request.Workload) {
            case "ycsb_single":
                // only uses single table
                // TableBenchmark ycsb_single = new FixedLenTableBenchmark("ycsb_local", ycsbCfg, wal);
                // ycsb_single.RunTransactions();
                break;
            case "ycsb":
                // only uses single table
                // TableBenchmark b = new ShardedBenchmark("2pc", ycsbCfg, txnManager, tables[0], wal);
                // b.RunTransactions();
                break;
            case "tpcc":
                tpccBenchmark.RunTransactions();
                // tpccBenchmark.GenerateTables();
                break;
            case "tpcc-populate":
                tpccBenchmark.PopulateTables();
                break;
            default:
                throw new NotImplementedException();
        }


        // Table table = tables[0];
        // txnManager.Run();
        // var ctx = txnManager.Begin();
        // Console.WriteLine("Should go to own");
        // var own = table.Read(new PrimaryKey(table.GetId(), 0), new TupleDesc[]{new TupleDesc(12345, 8, 0)}, ctx);
        // Console.WriteLine(own.ToString());
        // foreach (var b in own.ToArray()){
        //     Console.WriteLine(b);
        // }
        // Console.WriteLine("Should RPC:");
        // var other = table.Read(new PrimaryKey(table.GetId(), 1), new TupleDesc[]{new TupleDesc(12345, 8, 0)}, ctx);
        // Console.WriteLine(other.ToString());
        // foreach (var b in other.ToArray()){
        //     Console.WriteLine(b);
        // }
        // Console.WriteLine("Starting commit");
        // txnManager.Commit(ctx);
        // txnManager.Terminate();
        EnqueueWorkloadReply enqueueWorkloadReply = new EnqueueWorkloadReply{Success = true};
        return Task.FromResult(enqueueWorkloadReply);
    }

    // typically used for Prepare() and Commit() 
    public override async Task<WalReply> WriteWalEntry(WalRequest request, ServerCallContext context)
    {
        // PrintDebug($"Writing to WAL from {request.PartitionId}");
        LogEntry entry = LogEntry.FromBytes(request.Message.ToArray());

        if (entry.type == LogType.Prepare || entry.type == LogType.Commit)
        {
            long internalTid = GetOrRegisterTid(request.PartitionId, request.Tid);
            entry.lsn = internalTid; // TODO: HACKY reuse, we keep tid to be original tid
            entry.prevLsn = request.PartitionId; // TODO: hacky place to put sender id
            PrintDebug($"Stepping prepare/commit {entry.lsn}");
        } else {
            PrintDebug($"Stepping ok/ack {entry.tid}");
        }
        
        var stepRequest = stepRequestPool.Checkout();
        var requestBuilder = new StepRequestBuilder(stepRequest);
        // TODO: do we need to step messages consumed, self, and out messages 
        requestBuilder.AddSelfMessage(entry.ToBytes());
        await capabilities.Step(requestBuilder.FinishStep());
        stepRequestPool.Return(stepRequest);
        backend.EndAction();
        return new WalReply{Success = true};
    }

    private long GetOrRegisterTid(long partitionId, long tid) {
        PrintDebug($"Getting or registering tid: ({partitionId}, {tid})");
        if (externalToInternalTxnId.ContainsKey((partitionId, tid))) 
            return externalToInternalTxnId[(partitionId, tid)];

        var ctx = txnManager.Begin();
        long internalTid = ctx.tid;
        PrintDebug("Registering new tid: " + internalTid);
        externalToInternalTxnId[(partitionId, tid)] = internalTid;
        txnIdToTxnCtx[internalTid] = ctx;
        return internalTid;
    }

    public void Dispose(){
        foreach (var table in tables.Values) {
            table.Dispose();
        }
        txnManager.Terminate();
        terminationStart.Set();
        // TODO(Tianyu): this shutdown process is unsafe and may leave things unsent/unprocessed in the queue
        backend.ForceCheckpoint();
        Thread.Sleep(1000);
        _backgroundTask.StopProcessing();
        _backgroundTask.Dispose();
        processorClient.StopProcessingAsync().GetAwaiter().GetResult();
        processorClient.Dispose();
        terminationComplete.Wait();
        refreshThread.Join();
        processingThread.Join();
    }

    public Darq GetBackend() => backend;

    public bool ProcessMessage(DarqMessage m){
        PrintDebug($"Processing message");
        bool recoveryMode = false;
        switch (m.GetMessageType()){
            case DarqMessageType.IN:
            {
                unsafe
                {
                    fixed (byte* b = m.GetMessageBody())
                    {
                        int signal = *(int*)b;
                        // This is a special termination signal
                        if (signal == -1)
                        {
                            m.Dispose();
                            // Return false to signal that there are no more messages to process and the processing
                            // loop can exit
                            return false;
                        }
                    }
                }

                LogEntry entry = LogEntry.FromBytes(m.GetMessageBody().ToArray());
                var requestBuilder = new StepRequestBuilder(reusableRequest);
                // requestBuilder.AddRecoveryMessage(m.GetMessageBody());
                switch (entry.type)
                {
                    // Coordinator side
                    case LogType.Ok:
                    {
                        PrintDebug($"Got OK log entry: {entry}");
                        txnManager.MarkAcked(entry.tid, TransactionStatus.Validated, m.GetLsn(), entry.prevLsn);
                        m.Dispose();
                        return true;
                    }
                    case LogType.Ack:
                    {
                        PrintDebug($"Got ACK log entry: {entry}");
                        // can ignore in DARQ since we know out commit message is sent
                        m.Dispose();
                        return true;
                    }
                    // Worker side
                    case LogType.Prepare:
                    {
                        PrintDebug($"Got prepare log entry: {entry}");
                        requestBuilder.MarkMessageConsumed(m.GetLsn());
                        long sender = entry.prevLsn; // hacky
                        long internalTid = entry.lsn; // ""

                        // add each write to context before validating
                        TransactionContext ctx = txnIdToTxnCtx[internalTid];
                        for (int i = 0; i < entry.pks.Length; i++)
                        {
                            PrimaryKey pk = entry.pks[i];
                            ctx.AddWriteSet(ref pk, entry.tupleDescs[i], entry.vals[i]);
                        }
                        bool success = txnManager.Validate(ctx);
                        PrintDebug($"Validated at node {partitionId}: {success}; now sending OK to {sender}");
                        if (success) {
                            LogEntry okEntry = new LogEntry(partitionId, entry.tid, LogType.Ok);
                            requestBuilder.AddOutMessage(new DarqId(sender), okEntry.ToBytes());
                        }
                        break;
                    }
                    case LogType.Commit:
                    {
                        PrintDebug($"Got commit log entry: {entry}");
                        requestBuilder.MarkMessageConsumed(m.GetLsn());
                        long sender = entry.prevLsn; // hacky
                        long internalTid = entry.lsn; // ""
                        
                        txnManager.Write(txnIdToTxnCtx[internalTid], (tid, type) => wal.Finish(tid, type));

                        PrintDebug($"Committed at node {partitionId}; now sending ACK to {sender}");
                        LogEntry ackEntry = new LogEntry(partitionId, entry.tid, LogType.Ack);
                        requestBuilder.AddOutMessage(new DarqId(sender), ackEntry.ToBytes());
                        break;
                    }
                    default:
                        throw new NotImplementedException();
                }

                
                m.Dispose();
                var v = capabilities.Step(requestBuilder.FinishStep());
                Debug.Assert(v.GetAwaiter().GetResult() == StepStatus.SUCCESS);
                return true;
            }
            case DarqMessageType.RECOVERY: // this is on recovery; TODO: do we need to double pass?
                PrintDebug($"Recovering?, got log");
                if (recoveryMode) {
                    LogEntry entry = LogEntry.FromBytes(m.GetMessageBody().ToArray());
                    
                    PrintDebug($"Recovering, got log entry: {entry}");

                }
                m.Dispose();
                return true;
            default:
                throw new NotImplementedException();
        }
    }

    public void OnRestart(IDarqProcessorClientCapabilities capabilities) {
        this.capabilities = capabilities;
        this.wal.SetCapabilities(capabilities);
    }

    void PrintDebug(string msg, TransactionContext ctx = null){
        if (logger != null) logger.LogInformation($"[TPS {partitionId} TID {(ctx != null ? ctx.tid : -1)}]: {msg}");
    }
}


public class TransactionProcessorProducerWrapper : IDarqProducer
{
    private Dictionary<DarqId, GrpcChannel> clusterMap;
    private ConcurrentDictionary<DarqId, TransactionProcessor.TransactionProcessorClient> clients = new();
    private DprSession session;

    public TransactionProcessorProducerWrapper(Dictionary<DarqId, GrpcChannel> clusterMap, DprSession session)
    {
        this.clusterMap = clusterMap;
        this.session = session;
    }
    
    public void Dispose() {}

    public void EnqueueMessageWithCallback(DarqId darqId, ReadOnlySpan<byte> message, Action<bool> callback, long producerId, long lsn)
    {
        LogEntry entry = LogEntry.FromBytes(message.ToArray());
        var client = clients.GetOrAdd(darqId,
            _ => new TransactionProcessor.TransactionProcessorClient(clusterMap[darqId]));
        var walRequest = new WalRequest
        {
            Message = ByteString.CopyFrom(message),
            Tid = entry.tid,
            PartitionId = producerId,
            Lsn = lsn,
        };
        Task.Run(async () =>
        {
            try
            {
                await client.WriteWalEntryAsync(walRequest);
                callback(true);
            }
            catch
            {
                callback(false);
                throw;
            }
        });
    }

    public void ForceFlush()
    {
        // TODO(Tianyu): Not implemented for now
    }
}
}