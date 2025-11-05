using FluentAssertions;
using TwoPhaseCommit;
using Xunit;

namespace TwoPhaseCommit.Tests;

public class RowRecordTests
{
    [Fact]
    public async Task TestBasicLocking()
    {
        var record = new TestRowRecord();

        // --- Basic S-S-S acquire ---
        var result1 = await record.TryAcquireShared(11);
        var result2 = await record.TryAcquireShared(12);
        var result3 = await record.TryAcquireShared(13);

        result1.Should().BeTrue();
        result2.Should().BeTrue();
        result3.Should().BeTrue();

        // --- S -> X block and wait-die ---
        // WAIT-DIE should disallow younger transactions
        var result4 = await record.TryAcquireExclusive(14); // Die (younger)
        var result5 = await record.TryAcquireExclusive(15); // Die (younger)
        var result6 = record.TryAcquireExclusive(10); // Wait (older)

        result4.Should().BeFalse();
        result5.Should().BeFalse();
        result6.IsCompleted.Should().BeFalse();

        // --- S release -> grant waiting X ---
        record.ReleaseShared(11);
        result6.IsCompleted.Should().BeFalse();
        record.ReleaseShared(12);
        result6.IsCompleted.Should().BeFalse();
        record.ReleaseShared(13);

        // Waiting X lock (result6) should now be granted
        result6.IsCompleted.Should().BeTrue();
        (await result6).Should().BeTrue();

        // --- X -> S/X block and wait-die ---
        // Record is now held by 10 (Exclusive)

        // WAIT-DIE should disallow younger transactions
        var result7 = await record.TryAcquireExclusive(16); // Die (younger)
        var result8 = await record.TryAcquireShared(17); // Die (younger)
        result7.Should().BeFalse();
        result8.Should().BeFalse();

        var result9_S = record.TryAcquireShared(8);
        var result11_S = record.TryAcquireShared(9);
        var result10_X = record.TryAcquireExclusive(7);
        var result12_S = record.TryAcquireShared(6);


        // Verify all are waiting
        result9_S.IsCompleted.Should().BeFalse(); 
        (await result11_S).Should().BeFalse(); // Not allowed to wait on older transactions
        result10_X.IsCompleted.Should().BeFalse();
        result12_S.IsCompleted.Should().BeFalse();

        // Release the exclusive lock (10)
        record.ReleaseExclusive(10);
        
        // Should only grant in order of wait 
        (await result9_S).Should().BeTrue();
        result12_S.IsCompleted.Should().BeFalse();
        result10_X.IsCompleted.Should().BeFalse();

        record.ReleaseShared(8);
        result10_X.IsCompleted.Should().BeTrue();
        (await result10_X).Should().BeTrue();
        result12_S.IsCompleted.Should().BeFalse();

        // On the last shared release, the exclusive lock should be granted
        record.ReleaseExclusive(7);
        (await result12_S).Should().BeTrue();
    }

    [Fact]
    public async Task TestConcurrencyStress()
    {
        var record = new TestRowRecord();
        var sharedHolders = 0;
        var exclusivelyHeld = 0;
        
        int numTasks = 5000;
        int operationsPerTask = 20;
        var tasks = new List<Task>();

        for (int i = 0; i < numTasks; i++)
        {
            // Give each task a unique, non-overlapping txnId
            int txnId = i + 1;

            tasks.Add(Task.Run(async () =>
            {
                for (int j = 0; j < operationsPerTask; j++)
                {
                    // Check invariants *before* every operation
                    CheckModelInvariants(sharedHolders, exclusivelyHeld);

                    // 80% chance of shared, 20% chance of exclusive
                    if (Random.Shared.NextDouble() < 0.8)
                    {
                        var acquired = await record.TryAcquireShared(txnId);

                        if (acquired)
                            Interlocked.Increment(ref sharedHolders);

                        // Check invariants *after* acquire attempt
                        CheckModelInvariants(sharedHolders, exclusivelyHeld);;

                        if (acquired)
                        {
                            await Task.Delay(Random.Shared.Next(1, 5)); // Simulate "work"
                            Interlocked.Decrement(ref sharedHolders);
                            record.ReleaseShared(txnId);

                            // Check invariants *after* release
                            CheckModelInvariants(sharedHolders, exclusivelyHeld);;
                        }
                    }
                    else
                    {
                        var acquired = await record.TryAcquireExclusive(txnId);

                        if (acquired)
                        {
                            // Set our model to "held" and fail if it was *already* held
                            var val = Interlocked.Increment(ref exclusivelyHeld);
                            val.Should().Be(1, "model should not have been exclusively held");
                        }

                        // Check invariants *after* acquire attempt
                        CheckModelInvariants(sharedHolders, exclusivelyHeld);;

                        if (acquired)
                        {
                            await Task.Delay(Random.Shared.Next(1, 5)); // Simulate "work"
                            // Set our model to "not held"
                            Interlocked.Exchange(ref exclusivelyHeld, 0);
                            record.ReleaseExclusive(txnId);
                            // Check invariants *after* release
                            CheckModelInvariants(sharedHolders, exclusivelyHeld);;
                        }
                    }
                }
            }));
        }

        // Wait for all tasks to complete.
        await Task.WhenAll(tasks);
        
        // Final state of our model should be empty
        sharedHolders.Should().Be(0);
        exclusivelyHeld.Should().Be(0);

        var finalLock = await record.TryAcquireExclusive(999);
        finalLock.Should().BeTrue("The lock should be available after all tasks complete.");
        record.ReleaseExclusive(999);
    }

    /// <summary>
    /// Simple test implementation of RowRecord for testing purposes
    /// </summary>
    private class TestRowRecord : RowRecord
    {
    }
    
    /// <summary>
    /// Asserts that the test-side "shadow model" is in a consistent state.
    /// This method uses NO reflection.
    /// </summary>
    private void CheckModelInvariants(int shared, int exclusive)
    {
        // A lock can be held by 0..N shared holders OR 1 exclusive holder, but not both.
        
        // Check for impossible states:
        shared.Should().BeGreaterOrEqualTo(0, "model shared count cannot be negative");
        exclusive.Should().BeLessOrEqualTo(1, "model exclusive held must be 0 or 1");

        if (shared > 0)
            exclusive.Should().Be(0, "cannot have shared holders AND an exclusive holder");

        if (exclusive > 0)
            shared.Should().Be(0, "cannot have an exclusive holder AND shared holders");
    }
}