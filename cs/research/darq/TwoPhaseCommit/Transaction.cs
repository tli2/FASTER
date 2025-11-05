namespace TwoPhaseCommit;

public class TransactionContext
{
    private long txnId = -1;
    private List<RowRecord> sharedLocks = new(), exclusiveLocks = new();
    private List<Action> undoActions = new();
    private bool prepared = false;
    private bool twoPC = false;

    public TransactionContext Reset(long transactionId)
    {
        sharedLocks.Clear();
        exclusiveLocks.Clear();
        undoActions.Clear();
        txnId = transactionId;
        prepared = false;
        twoPC = false;
        return this;
    }

    public bool Prepared() => prepared;

    public void MarkPrepared() => prepared = true;
    
    public bool TwoPC() => twoPC;
    
    public void MarkTwoPC() => twoPC = true;

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