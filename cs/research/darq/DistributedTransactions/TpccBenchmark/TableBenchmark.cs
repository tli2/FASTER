using System;
using System.Threading;
using System.Diagnostics;
using System.Collections;

namespace DB {

public struct BenchmarkConfig {
    public int seed;
    public double ratio;
    public int threadCount;
    public int insertThreadCount;
    public int attrCount;
    public int perThreadDataCount; // enough to be larger than l3 cache
    public int iterationCount;
    public int datasetSize;
    public int perTransactionCount;
    public int nCommitterThreads;

    public BenchmarkConfig(
        int seed = 12345,
        double ratio = 0.2,
        int threadCount = 16,
        int insertThreadCount = -1,
        int attrCount = 2,
        int perThreadDataCount = 100000,
        int iterationCount = 10,
        int perTransactionCount = 1,
        int nCommitterThreads = 7){

        this.seed = seed;
        this.ratio = ratio;
        this.threadCount = threadCount;
        this.insertThreadCount = insertThreadCount == -1 ? threadCount : insertThreadCount;
        this.attrCount = attrCount;
        this.perThreadDataCount = perThreadDataCount;
        this.iterationCount = iterationCount;
        this.perTransactionCount = perTransactionCount;
        this.datasetSize = perThreadDataCount * threadCount;
        this.nCommitterThreads = nCommitterThreads;
    }

    public override readonly string ToString(){
        return $"seed: {seed}, ratio: {ratio}, threadCount: {threadCount}, attrCount: {attrCount}, perThreadDataCount: {perThreadDataCount}, iterationCount: {iterationCount}, perTransactionCount: {perTransactionCount}, datasetSize: {datasetSize}, nCommitterThreads: {nCommitterThreads}";
    }
}

public abstract class TableBenchmark
{
    protected internal BenchmarkConfig cfg;   
    protected internal PrimaryKey[] keys;
    protected internal byte[][] values;
    protected internal BitArray isWrite;
    // TODO: in the future support multiple tables, make this list
    protected internal TupleDesc[] td; // TODO: remove, this is the same as schema 
    protected internal Thread[] workers;
    protected internal BenchmarkStatistics? stats;
    protected internal IWriteAheadLog? wal;

    public TableBenchmark(BenchmarkConfig cfg, IWriteAheadLog? wal = null){
        this.cfg = cfg;
        this.wal = wal;
        td = new TupleDesc[cfg.attrCount];
        workers = new Thread[Math.Max(cfg.insertThreadCount, cfg.threadCount)];

        keys = new PrimaryKey[cfg.datasetSize];
        values = new byte[cfg.datasetSize][];
        isWrite = new BitArray(cfg.datasetSize);
    }

    virtual protected internal int InsertSingleThreadedTransactions(Table tbl, TransactionManager txnManager, int thread_idx){
        int abortCount = 0;
        int c = 0;
        Debug.Assert(cfg.threadCount == cfg.insertThreadCount, "Insert thread count must be equal to thread count");
        for (int i = 0; i < cfg.perThreadDataCount; i += cfg.perTransactionCount){
            TransactionContext t = txnManager.Begin();
            for (int j = 0; j < cfg.perTransactionCount; j++) {
                int loc = i + j + (cfg.perThreadDataCount * thread_idx);
                PrimaryKey tupleId = tbl.Insert(values[loc], t);
                keys[loc] = tupleId;
            }
            var success = txnManager.Commit(t);
            if (!success){
                abortCount++;
            } else {
                c++;
            }
        }
        return abortCount;
    }

    protected internal int InsertMultiThreadedTransactions(Table tbl, TransactionManager txnManager)
    {
        int totalAborts = 0;
        for (int thread = 0; thread < cfg.insertThreadCount; thread++) {
            int t = thread;
            workers[thread] = new Thread(() => {
                int aborts = InsertSingleThreadedTransactions(tbl, txnManager, t);
                Interlocked.Add(ref totalAborts, aborts);
            });
            workers[thread].Start();
        }
        for (int thread = 0; thread < cfg.insertThreadCount; thread++) {
            workers[thread].Join();
        }
        return totalAborts;
    }

    virtual protected internal int WorkloadSingleThreadedTransactions(Table tbl, TransactionManager txnManager, int thread_idx, double ratio){
        int abortCount = 0;
        for (int i = 0; i < cfg.perThreadDataCount; i += cfg.perTransactionCount){
            TransactionContext t = txnManager.Begin();
            for (int j = 0; j < cfg.perTransactionCount; j++){
                int loc = i + j + (cfg.perThreadDataCount * thread_idx);
                PrimaryKey key = keys[loc];
                // uncomment to make workload only insert one attribute instead of all
                // long attr = schema[loc%cfg.attrCount].Item1;
                // TupleDesc[] td = new TupleDesc[]{new TupleDesc(attr, tbl.metadata[attr].Item1)};

                if (isWrite[loc]) {
                    // shift value by thread_idx to write new value
                    int newValueIndex = loc + thread_idx < values.Length ?  loc + thread_idx : values.Length - 1;
                    // Span<byte> val = new Span<byte>(values[newValueIndex]).Slice(0, sizeof(long));
                    byte[] val = values[newValueIndex];
                    tbl.Update(ref key, td, val, t);
                } else {
                    tbl.Read(key, td, t);
                }
            }
            var success = txnManager.Commit(t);
            if (!success){
                abortCount++;
            }
        }
        return abortCount;
    }

    protected internal int WorkloadMultiThreadedTransactions(Table tbl, TransactionManager txnManager, double ratio)
    {
        int totalAborts = 0;
        for (int thread = 0; thread < cfg.threadCount; thread++) {
            int t = thread;
            workers[thread] = new Thread(() => {
                int aborts = WorkloadSingleThreadedTransactions(tbl, txnManager, t, ratio);
                Interlocked.Add(ref totalAborts, aborts);
            });
            workers[thread].Start();
        }
        for (int thread = 0; thread < cfg.threadCount; thread++) {
            workers[thread].Join();
        }
        return totalAborts;
    }

    virtual public void RunTransactions(){
        for (int i = 0; i < cfg.iterationCount; i++){
            (long, int)[] schema = new (long, int)[cfg.attrCount];
            for (int j = 0; j < td.Length; j++){
                schema[j] = (td[j].Attr, td[j].Size);
            }
            using (Table tbl = new Table(0, schema)) {
                Dictionary<int, Table> tables = new Dictionary<int, Table>();
                tables.Add(tbl.GetId(), tbl);
                TransactionManager txnManager = new TransactionManager(cfg.nCommitterThreads, tables, wal);
                txnManager.Run();
                var insertSw = Stopwatch.StartNew();
                int insertAborts = InsertMultiThreadedTransactions(tbl, txnManager); // setup
                insertSw.Stop();
                System.Console.WriteLine("done inserting");
                long insertMs = insertSw.ElapsedMilliseconds;
                var opSw = Stopwatch.StartNew();
                int txnAborts = WorkloadMultiThreadedTransactions(tbl, txnManager, cfg.ratio);
                opSw.Stop();
                long opMs = opSw.ElapsedMilliseconds;
                stats?.AddTransactionalResult((insertMs, opMs, insertAborts, txnAborts));
                txnManager.Terminate();
            }
        }
        stats?.ShowAllStats();
        stats?.SaveStatsToFile();
    }
}

public class BenchmarkStatistics {
    internal readonly List<long> insMsPerRun = new List<long>();
    internal readonly List<long> opsMsPerRun = new List<long>();
    internal readonly List<int> insAbortsPerRun = new List<int>();
    internal readonly List<int> txnAbortsPerRun = new List<int>();
    internal string name;
    internal BenchmarkConfig cfg;
    internal TpccConfig? tpcCfg;
    internal int inserts;
    internal int operations;

    internal BenchmarkStatistics(string name, BenchmarkConfig cfg, int inserts, int operations, TpccConfig? tpcCfg = null)
    {
        this.cfg = cfg;
        this.tpcCfg = tpcCfg;
        this.name = name;
        this.inserts = inserts;
        this.operations = operations;
    }

    internal void AddResult((long ims, long oms) result)
    {
        insMsPerRun.Add(result.ims);
        opsMsPerRun.Add(result.oms);
    }

    internal void AddTransactionalResult((long ims, long oms, int insAborts, int txnAborts) result)
    {
        insMsPerRun.Add(result.ims);
        opsMsPerRun.Add(result.oms);
        insAbortsPerRun.Add(result.insAborts);
        txnAbortsPerRun.Add(result.txnAborts);
    }

    internal string[] GetStats() {
        string[] data = new string[]{
            $"Benchmark {name}",
            "-----BENCHMARK CONFIG-----",
            cfg.ToString(),
            "-----TPCC CONFIG-----",
            tpcCfg?.ToString(),
            "-----STATS-----",
            GetInsDataString(operations, insMsPerRun),
            GetOpsDataString(inserts, operations-inserts, opsMsPerRun, txnAbortsPerRun)
        };

        if (insAbortsPerRun.Count != 0) {
            data = data.Concat(new string[]{
                GetInsAbortDataString(insAbortsPerRun),
                GetTxnAbortDataString(txnAbortsPerRun)
            }).ToArray();
        }

        return data;
    }

    internal void ShowAllStats(){
        foreach (string line in GetStats()){
            Console.WriteLine(line);
        }
    }

    internal async void SaveStatsToFile(){
        string now = DateTime.Now.ToString("yyyyMMdd-HHmmss"); 
        await File.WriteAllLinesAsync($"benchmark/benchmarkResults/{name}-{now}.txt", GetStats());
    }

    internal string GetOpsDataString(int inserts, int reads, List<long> opsMsPerRun, List<int> txnAborts) => $"{(inserts+reads-txnAborts.Average())/opsMsPerRun.Average()} successful operations/ms {(inserts+reads)/opsMsPerRun.Average()} operations/ms ({inserts+reads} operations ({inserts} inserts, {reads} reads) in {opsMsPerRun.Average()} ms)";
    internal string GetInsDataString(int inserts, List<long> insMsPerRun) => $"{inserts/insMsPerRun.Average()} inserts/ms ({inserts} inserts in {insMsPerRun.Average()} ms)";
    internal string GetTxnAbortDataString(List<int> txnAborts) => $"Operations: Average {txnAborts.Average()} aborts out of {cfg.datasetSize/cfg.perTransactionCount} transactions ({txnAborts.Average()/(cfg.datasetSize/cfg.perTransactionCount)*100}% abort rate)";
    internal string GetInsAbortDataString(List<int> insAborts) => $"Insertions: Average {insAborts.Average()} aborts out of {cfg.datasetSize/cfg.perTransactionCount} transactions ({insAborts.Average()/(cfg.datasetSize/cfg.perTransactionCount)*100}% abort rate)";

    // internal static string GetLoadingTimeLine(double insertsPerSec, long elapsedMs)
    //     => $"##00; {InsPerSec}: {insertsPerSec:N2}; sec: {(double)elapsedMs / 1000:N3}";

    // internal static string GetAddressesLine(AddressLineNum lineNum, long begin, long head, long rdonly, long tail)
    //     => $"##{(int)lineNum:00}; begin: {begin}; head: {head}; readonly: {rdonly}; tail: {tail}";

    // internal static string GetStatsLine(StatsLineNum lineNum, string opsPerSecTag, double opsPerSec)
    //     => $"##{(int)lineNum:00}; {opsPerSecTag}: {opsPerSec:N2}; {OptionsString}";

    // internal static string GetStatsLine(StatsLineNum lineNum, string meanTag, double mean, double stdev, double stdevpct)
    //     => $"##{(int)lineNum:00}; {meanTag}: {mean:N2}; stdev: {stdev:N1}; stdev%: {stdevpct:N1}; {OptionsString}";
}

}