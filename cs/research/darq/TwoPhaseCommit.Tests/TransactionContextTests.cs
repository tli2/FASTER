using System.Collections.Concurrent;
using TwoPhaseCommit;

public class TestRowRecord : RowRecord
{
    public int value;
}

public class TransactionContextTests
{
    /// <summary>
    /// This test walks a single TransactionContext through all its
    /// key lifecycle states: Reset, Acquire, AddUndo, Undo, Release,
    /// and final Reset, verifying external state at each step.
    /// </summary>
    [Fact]
    public async Task TestBasicTransactionLifecycle()
    {
        var txnContext = new TransactionContext();

        // Test Initial State & Reset
        long txnId = 10;
        txnContext.Reset(txnId);

        txnContext.Id().Should().Be(txnId);
        txnContext.ReadOnly().Should().BeTrue();
        txnContext.Prepared().Should().BeFalse();
        txnContext.TwoPC().Should().BeFalse();

        // Setup records and data for tx
        var recordS = new TestRowRecord();
        var recordX = new TestRowRecord { value = 0 }; // TestData is merged here

        // Test Lock Acquisition
        var r1 = await txnContext.TryAccessRead(recordS);
        var r2 = await txnContext.TryAccessWrite(recordX);
        r1.Should().BeTrue();
        r2.Should().BeTrue();

        // Test Undo Actions
        txnContext.ReadOnly().Should().BeTrue(); // Still read-only

        txnContext.AddUndoAction(() => recordX.value = 0); // Add undo action
        recordX.value = 100; // Modify the record directly
        txnContext.ReadOnly().Should().BeFalse(); // Now it's not read-only
        recordX.value.Should().Be(100);

        // Test Flags
        txnContext.MarkPrepared();
        txnContext.MarkTwoPC();
        txnContext.Prepared().Should().BeTrue();
        txnContext.TwoPC().Should().BeTrue();

        // Test Undo
        txnContext.Undo();
        recordX.value.Should().Be(0); // Value should be reverted

        // 7. Test Lock Release (External Behavior)
        // We test this by having another (older) txn try to acquire
        // the locks. It should wait, then succeed after release.
        var otherTxn = new TransactionContext().Reset(5);
        var sLockTask = otherTxn.TryAccessWrite(recordS); // S -> X
        var xLockTask = otherTxn.TryAccessRead(recordX); // X -> S

        // Give a moment for tasks to enter wait queue
        await Task.Delay(10);
        sLockTask.IsCompleted.Should().BeFalse();
        xLockTask.IsCompleted.Should().BeFalse();

        // Now, release the locks
        txnContext.ReleaseLocks();

        (await sLockTask).Should().BeTrue();
        (await xLockTask).Should().BeTrue();

        // Test Reset for Pooling
        // The context should be "dirty" here (flags are set)
        txnContext.Prepared().Should().BeTrue();
        txnContext.ReadOnly().Should().BeFalse();

        // Call Reset again
        txnContext.Reset(11);
        // Verify it's clean (externally)
        txnContext.Id().Should().Be(11);
        txnContext.ReadOnly().Should().BeTrue();
        txnContext.Prepared().Should().BeFalse();
        txnContext.TwoPC().Should().BeFalse();
    }

    private static async Task<bool> TryCheckInvariant(TransactionContext txn, List<TestRowRecord> records)
    {
        var sum = 0;
        // Acquire S-Lock on *all* records
        foreach (var rec in records)
        {
            if (!await txn.TryAccessRead(rec))
            {
                txn.ReleaseLocks();
                return false;
            }

            sum += rec.value;
        }

        sum.Should().Be(0);
        txn.ReleaseLocks();
        return true;
    }

    private static async Task<bool> TryPerformUpdate(TransactionContext txn, List<TestRowRecord> records)
    {
        // Pick two different random records
        int idxA = Random.Shared.Next(records.Count);
        int idxB = Random.Shared.Next(records.Count);
        if (idxA == idxB) return false; // Should retry
        
        var rec1 = records[idxA];
        var rec2 = records[idxB];

        if (!await txn.TryAccessWrite(rec1)) return false;
        
        int original1 = rec1.value;
        txn.AddUndoAction(() => rec1.value = original1);
        rec1.value--;

        if (!await txn.TryAccessWrite(rec2))
        {
            txn.Undo();
            txn.ReleaseLocks();
            return false;
        }
        
        int original2 = rec2.value;
        txn.AddUndoAction(() => rec2.value = original2);
        rec2.value++;
        txn.ReleaseLocks();
        return true;
    }

    [Fact]
    public async Task TestConcurrentTransactionLogic()
    {
        const int NUM_RECORDS = 32;
        const int NUM_WORKER_THREADS = 10;
        const int OPS_PER_THREAD = 100;
        const int MAX_TXN_RETRIES = 10;
        const double READ_PROBABILITY = 0.1;

        // Setup shared records
        var records = new List<TestRowRecord>();
        for (int i = 0; i < NUM_RECORDS; i++)
            records.Add(new TestRowRecord { value = 0 });

        long globalTxnId = 0;
        var workerTasks = new List<Task>(); // Renamed from writerTasks

        // --- 2. Start Worker Tasks ---
        for (int i = 0; i < NUM_WORKER_THREADS; i++)
        {
            workerTasks.Add(Task.Run(async () =>
            {
                var txn = new TransactionContext();
                for (int j = 0; j < OPS_PER_THREAD; j++)
                {
                    long txnId = Interlocked.Increment(ref globalTxnId);

                    if (Random.Shared.NextDouble() < READ_PROBABILITY)
                    {
                        for (int retry = 0; retry < MAX_TXN_RETRIES; retry++)
                        {
                            txn.Reset(txnId);
                            if (await TryCheckInvariant(txn, records)) break;
                            await Task.Yield();
                        }
                    }
                    else
                    {
                        for (int retry = 0; retry < MAX_TXN_RETRIES; retry++)
                        {
                            txn.Reset(txnId);
                            if (await TryPerformUpdate(txn, records)) break;
                            await Task.Yield();
                        }
                    }
                }
            }));
        }

        while (!workerTasks.All(w => w.IsCompleted))
        {
            await Task.Delay(100);
        }
        // await Task.WhenAll(workerTasks);
        // After all workers are done and all locks are released,
        // the sum must be 0.
        int finalSum = records.Sum(r => r.value);
        finalSum.Should().Be(0);
    }
}