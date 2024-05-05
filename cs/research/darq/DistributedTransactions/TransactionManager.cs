using System.Collections;
using System.Collections.Concurrent;
using System.Threading;
using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;
using System.Runtime.InteropServices;

namespace DB {
public class TransactionManager {
    private static readonly int MAX_QUEUE_SIZE = 4;
    internal BlockingCollection<TransactionContext> txnQueue;
    internal static int pastTnumCircularBufferSize = 1 << 14;
    internal TransactionContext[] tnumToCtx = new TransactionContext[pastTnumCircularBufferSize]; // write protected by spinlock, atomic with txnc increment
    internal int txnc = 0;
    internal int tid = 0;
    internal Thread[] committer;
    internal SimpleObjectPool<TransactionContext> ctxPool;
    internal List<TransactionContext> active = new List<TransactionContext>(); // list of active transaction contexts, protected by spinlock
    internal SpinLock sl = new SpinLock();
    private IWriteAheadLog? wal;
    protected Dictionary<int, Table> tables;
    protected ILogger logger;
    public TransactionManager(int numThreads, Dictionary<int, Table> tables, IWriteAheadLog? wal = null, ILogger logger = null){
        this.wal = wal;
        this.logger = logger;
        this.tables = tables;
        ctxPool = new SimpleObjectPool<TransactionContext>(() => new TransactionContext(tables));
        committer = new Thread[numThreads];
        txnQueue = new BlockingCollection<TransactionContext>(MAX_QUEUE_SIZE);

        for (int i = 0; i < committer.Length; i++) {
            committer[i] = new Thread(() => {
                try {
                    while (true) {
                        TransactionContext ctx = txnQueue.Take();
                        ValidateAndWrite(ctx);
                    }
                } catch (ThreadInterruptedException){
                    System.Console.WriteLine("Terminated");
                }
            });
        }
    }

    /// <summary>
    /// Create a new transaction context 
    /// </summary>
    /// <returns>Newly created transaction context</returns>
    public TransactionContext Begin(){
        var ctx = ctxPool.Checkout();
        ctx.Init(startTxn: txnc, NewTransactionId());
        if (wal != null) {
            wal.Begin(ctx.tid);            
        }
        return ctx;
    }

    /// <summary>
    /// Submit a transaction context to be committed. Blocks until commit is completed
    /// </summary>
    /// <param name="ctx">Context to commit</param>
    /// <returns>True if the transaction committed, false otherwise</returns>
    public bool Commit(TransactionContext ctx){
        PrintDebug($"adding ctx to queue for commit", ctx);
        ctx.status = TransactionStatus.Pending;
        txnQueue.Add(ctx);        
        while (!Util.IsTerminalStatus(ctx.status)){
            Thread.Yield();
        }
        if (ctx.status == TransactionStatus.Aborted){
            return false;
        } else if (ctx.status == TransactionStatus.Committed) {
            return true;
        }
        return false;
    }

    public void CommitWithCallback(TransactionContext ctx, Action<bool> callback){
        PrintDebug($"adding ctx to queue for commit", ctx);
        ctx.status = TransactionStatus.Pending;
        ctx.callback = callback;
        txnQueue.Add(ctx);
    }

    /// <summary>
    /// Spawns a thread that continuously polls the queue to 
    /// validate and commit a transaction context
    /// </summary>
    public void Run(){
        for (int i = 0; i < committer.Length; i++) {
            committer[i].Start();
        }
    }

    public void Reset() {
        txnQueue.CompleteAdding();
        txnQueue = new BlockingCollection<TransactionContext>(MAX_QUEUE_SIZE);
        active = new List<TransactionContext>();
        sl = new SpinLock();
        txnc = 0;
        tid = 0;
        tnumToCtx = new TransactionContext[pastTnumCircularBufferSize];
        ctxPool = new SimpleObjectPool<TransactionContext>(() => new TransactionContext(tables));
    }

    public void Terminate(){
        for (int i = 0; i < committer.Length; i++) {
            committer[i]?.Interrupt();
        }
        if (wal != null) wal.Terminate();
    }

    /// <summary>
    /// Mutates "active", sets fields to mark active transaction
    /// </summary>
    /// <param name="ctx"></param>
    /// <returns></returns>
    public bool Validate(TransactionContext ctx){
        PrintDebug($"Validating own keys", ctx);
        bool lockTaken = false; // signals if this thread was able to acquire lock
        int finishTxn;
        List<TransactionContext> finish_active;
        try {
            sl.Enter(ref lockTaken);
            finishTxn = txnc;
            finish_active = new List<TransactionContext>(active);
            active.Add(ctx);
        } finally {
            if (lockTaken) sl.Exit();
            lockTaken = false;
        }
        // PrintDebug($"Committing {ctx.startTxn} to {finishTxn}", ctx);

        // validate
        for (int i = ctx.startTxnNum + 1; i <= finishTxn; i++){
            // Console.WriteLine((i & (pastTnumCircularBufferSize - 1)) + " readset: " + ctx.GetReadset().Count + "; writeset:" + ctx.GetWriteset().Count);
            // foreach (var x in tnumToCtx[i % pastTnumCircularBufferSize].GetWriteset()){
            //     Console.Write($"{x}, ");
            // }
            foreach (ref var tupleId in CollectionsMarshal.AsSpan(ctx.GetReadsetKeys())){
                // Console.WriteLine($"scanning for {keyAttr}");
                // TODO: rename keyattr since tupleid is redundant
                if (tnumToCtx[i & (pastTnumCircularBufferSize - 1)].InWriteSet(ref tupleId)){
                    // Console.WriteLine($"1 ABORT for {ctx.tid} because conflict: {tupleId} in {tnumToCtx[i & (pastTnumCircularBufferSize - 1)].tid}");
                    return false;
                }
            }
        }
        
        foreach (TransactionContext pastTxn in finish_active){
            foreach (var item in pastTxn.GetWritesetKeys()){
                PrimaryKey tupleId = item;
                if (ctx.InReadSet(ref tupleId) || ctx.InWriteSet(ref tupleId)){
                    // Console.WriteLine($"2 ABORT for {ctx.tid} because conflict: {tupleId} in {pastTxn.tid}");
                    return false;
                }
            }
        }
        return true;
    }

    virtual public void Write(TransactionContext ctx, Action<long, LogType> commit){
        PrintDebug("Write phase", ctx);
        bool lockTaken = false; // signals if this thread was able to acquire lock
        List<PrimaryKey> writesetKeys = ctx.GetWritesetKeys();
        for(int i = 0; i < writesetKeys.Count; i++){
            PrimaryKey tupleId = writesetKeys[i];
            var item = ctx.GetFromWriteset(i);
            // TODO: should not throw exception here, but if it does, abort. 
            // failure here means crashed before commit. would need to rollback
            tables[tupleId.Table].Write(ref tupleId, item.Item1, item.Item2);
        }
        // TODO: verify that should be logged before removing from active
        if (wal != null){
            commit(ctx.tid, LogType.Commit);
            // wal.Finish(new LogEntry(prevLsn, ctx.tid, LogType.Commit));
        }
        ctx.callback?.Invoke(true);
        // assign num 
        int finalTxnNum;
        try {
            sl.Enter(ref lockTaken);
            txnc += 1; // TODO: deal with int overflow
            finalTxnNum = txnc;
            active.Remove(ctx);
            if (tnumToCtx[finalTxnNum & (pastTnumCircularBufferSize - 1)] != null){ 
                ctxPool.Return(tnumToCtx[finalTxnNum & (pastTnumCircularBufferSize - 1)]);
            }
            tnumToCtx[finalTxnNum & (pastTnumCircularBufferSize - 1)] = ctx;
        } finally {
            if (lockTaken) sl.Exit();
            lockTaken = false;
        }
        // PrintDebug("Write phase done", ctx);
    }

    public void Abort(TransactionContext ctx, Action<bool> callback = null){
        PrintDebug($"Aborting tid {ctx.tid}");
        bool lockTaken = false; // signals if this thread was able to acquire lock
        // TODO: verify that should be logged before removing from active
        if (wal != null){
            wal.Finish(ctx.tid, LogType.Abort);
        }
        if (ctx.callback == null){
            ctx.callback = callback;
        }
        ctx.callback?.Invoke(false);
        try {
            sl.Enter(ref lockTaken);
            active.Remove(ctx);
        } finally {
            if (lockTaken) sl.Exit();
            lockTaken = false;
        }
    }

    virtual protected void ValidateAndWrite(TransactionContext ctx) {
        bool valid = Validate(ctx);

        if (valid) {
            ctx.status = TransactionStatus.Validated;
            Write(ctx, (tid, type) => wal.Finish(tid, type));
            ctx.status = TransactionStatus.Committed;
        } else {
            Abort(ctx);
            ctx.status = TransactionStatus.Aborted;
        }
    }

    virtual public void PrintDebug(string msg, TransactionContext ctx = null){
        if (logger != null) logger.LogInformation($"[TM TID {(ctx != null ? ctx.tid : -1)}]: {msg}");
    }

    private long NewTransactionId(){
        return Interlocked.Increment(ref tid);
    }
}

public class ShardedTransactionManager : TransactionManager {
    private RpcClient rpcClient;
    private IWriteAheadLog? wal;
    private ConcurrentDictionary<long, List<(long, long)>> txnIdToOKDarqLsns = new ConcurrentDictionary<long, List<(long, long)>>(); // tid to num shards waiting on
    public ShardedTransactionManager(int numThreads, RpcClient rpcClient, Dictionary<int, ShardedTable> tables, IWriteAheadLog? wal = null, ILogger logger = null) : base(numThreads, tables.ToDictionary(kv => kv.Key, kv => (Table)kv.Value), wal, logger){
        this.rpcClient = rpcClient;
        this.wal = wal;
    }

    public void MarkAcked(long tid, TransactionStatus status, long darqLsn, long shard){
        TransactionContext? ctx = active.Find(ctx => ctx.tid == tid);
        if (ctx == null) return; // already aborted, ignore
        if (status == TransactionStatus.Aborted){
            Abort(ctx);
            ctx.status = TransactionStatus.Aborted;
            // TODO: should consume all existing OKs 
            return;
        } else if (status != TransactionStatus.Validated){
            throw new Exception($"Invalid status {status} for tid {tid}");
        }

        txnIdToOKDarqLsns[tid].Add((darqLsn, shard));        
        wal.RecordOk(tid, shard);

        PrintDebug($"Marked acked", ctx);

        if (txnIdToOKDarqLsns[tid].Count == rpcClient.GetNumServers() - 1){
            PrintDebug($"done w validation", ctx);
            ctx.status = TransactionStatus.Validated;
            Write(ctx, (tid, type) => wal.Finish2pc(tid, type, txnIdToOKDarqLsns[tid]));
            ctx.status = TransactionStatus.Committed;
        }
    }

    override protected void ValidateAndWrite(TransactionContext ctx){
        ctx.status = TransactionStatus.Pending;
        // validate own, need a map to track which has responded 
        bool valid = Validate(ctx);

        if (valid) {
            // split writeset into shards
            Dictionary<long, List<(PrimaryKey, TupleDesc[], byte[])>> shardToWriteset = new Dictionary<long, List<(PrimaryKey, TupleDesc[], byte[])>>();
            List<PrimaryKey> writesetKeys = ctx.GetWritesetKeys();
            for (int i = 0; i < writesetKeys.Count; i++){
                PrimaryKey tupleId = writesetKeys[i];
                (TupleDesc[] td, byte[] val) = ctx.GetFromWriteset(i);
                long shardDest = rpcClient.HashKeyToDarqId(tupleId);
                if (!rpcClient.IsLocalKey(tupleId)){
                    if (!shardToWriteset.ContainsKey(shardDest)){
                        shardToWriteset[shardDest] = new List<(PrimaryKey, TupleDesc[], byte[])>();
                    }
                    shardToWriteset[shardDest].Add((tupleId, td, val));
                }
            }
            if (shardToWriteset.Count > 0) {

                if (txnIdToOKDarqLsns.ContainsKey(ctx.tid)) throw new Exception($"Ctx TID {ctx.tid} already started validating?");
                PrintDebug($"Created waiting for OK list", ctx);
                txnIdToOKDarqLsns[ctx.tid] = new List<(long, long)>();
                for (int shard = 0; shard < rpcClient.GetNumServers(); shard++){
                    if (shard == rpcClient.GetId() || shardToWriteset.ContainsKey(shard)) continue;
                    txnIdToOKDarqLsns[ctx.tid].Add((-1, shard)); // hacky way to indicate that we don't need to wait for this shard
                }
                // send out prepare messages and wait; the commit is finished by calls to MarkAcked
                wal.Prepare(shardToWriteset, ctx.tid);
            } else {
                PrintDebug($"Commit on local, no waiting needed", ctx);
                Write(ctx, (tid, type) => wal.Finish(tid, type));
                ctx.status = TransactionStatus.Committed;
            }
        } else {
            Abort(ctx);
            ctx.status = TransactionStatus.Aborted;
        }

    }

    override public void Write(TransactionContext ctx, Action<long, LogType> commit){
        PrintDebug("Write phase", ctx);
        bool lockTaken = false; // signals if this thread was able to acquire lock
        List<PrimaryKey> writesetKeys = ctx.GetWritesetKeys();
        for(int i = 0; i < writesetKeys.Count; i++){
            PrimaryKey tupleId = writesetKeys[i];
            if (!rpcClient.IsLocalKey(tupleId)) continue;
            var item = ctx.GetFromWriteset(i);
            // TODO: should not throw exception here, but if it does, abort. 
            // failure here means crashed before commit. would need to rollback
            tables[tupleId.Table].Write(ref tupleId, item.Item1, item.Item2);
        }
        // TODO: verify that should be logged before removing from active
        if (wal != null){
            commit(ctx.tid, LogType.Commit);
            // wal.Finish(new LogEntry(prevLsn, ctx.tid, LogType.Commit));
        }
        ctx.callback?.Invoke(true);
        // assign num 
        int finalTxnNum;
        try {
            sl.Enter(ref lockTaken);
            txnc += 1; // TODO: deal with int overflow
            finalTxnNum = txnc;
            active.Remove(ctx);
            if (tnumToCtx[finalTxnNum & (pastTnumCircularBufferSize - 1)] != null){ 
                ctxPool.Return(tnumToCtx[finalTxnNum & (pastTnumCircularBufferSize - 1)]);
            }
            tnumToCtx[finalTxnNum & (pastTnumCircularBufferSize - 1)] = ctx;
        } finally {
            if (lockTaken) sl.Exit();
            lockTaken = false;
        }
        // PrintDebug("Write phase done", ctx);
    }

    public RpcClient GetRpcClient(){
        return rpcClient;
    }

    void PrintDebug(string msg, TransactionContext ctx = null){
        if (logger != null) logger.LogInformation($"[STM {rpcClient.GetId()} TID {(ctx != null ? ctx.tid : -1)}]: {msg}");
    }
    
}
}