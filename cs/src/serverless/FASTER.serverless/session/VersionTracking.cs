﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using FASTER.core;

namespace FASTER.serverless
{
    public struct WorkerVersion
    {
        public Worker Worker { get; set; }
        public long Version { get; set; }

        public WorkerVersion(Worker worker, long version)
        {
            Worker = worker;
            Version = version;
        }

        public WorkerVersion(long worker, long version) : this(new Worker(worker), version)
        {
        }

        public bool Equals(WorkerVersion other)
        {
            return Worker.Equals(other.Worker) && Version == other.Version;
        }

        public override bool Equals(object obj)
        {
            return obj is WorkerVersion other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Worker.GetHashCode() * 397) ^ Version.GetHashCode();
            }
        }
    }

    public partial class FasterServerlessSession<Key, Value, Input, Output, Functions> : IDisposable
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Empty>
    {

        private long FindUntilSerialNum(long localSafeVersion)
        {
            while (sequentialVersionList.Count != 0)
            {
                var entry = sequentialVersionList.Peek();
                // Some version is not yet committed, and we should stop here. 
                if (entry.Item2 > localSafeVersion) return entry.Item1 - 1;
                sequentialVersionList.Dequeue();
            }

            // If control flow reaches here, all segments tracked in sequentialVersionList are committed. We
            // over approximate and use the current maximum as the until serial num
            return serialNum - 1;
        }

        // We may be able to rewrite the computed commit point in a more concise way, e.g. if the computed commit point
        // is until op 9, with exception of op 7, 8, 9, we can simplify this to until 6.
        private void AdjustCommitPoint(ref CommitPoint computed)
        {
            for (var i = computed.ExcludedSerialNos.Count - 1; i >= 0; i--)
            {
                if (computed.ExcludedSerialNos[i] != computed.UntilSerialNo - 1) return;
                computed.UntilSerialNo--;
                computed.ExcludedSerialNos.RemoveAt(i);
            }
        }

        public void MaterializeExceptionList()
        {
            var dprTable = AttachedWorker.DprManager.ReadSnapshot();
            var localSafeVersion = dprTable.SafeVersion(AttachedWorker.Me());
            currentCommitPoint.UntilSerialNo = FindUntilSerialNum(localSafeVersion);
            
            currentCommitPoint.ExcludedSerialNos.Clear();
            foreach (var (s, _) in exceptionList)
            {
                if (s >= currentCommitPoint.UntilSerialNo) break;
                currentCommitPoint.ExcludedSerialNos.Add(s);
            }

            foreach (var i in currentPendingOps)
            {
                var pendingContext = reusablePendingContexts[i];
                var s = pendingContext.op.header.serialNum;
                if (s >= currentCommitPoint.UntilSerialNo) break;
                currentCommitPoint.ExcludedSerialNos.Add(s);
            }

            // Merge with local session commit point to account for exceptions.
            // No need to consider the difference between two until serial numbers, as the only exceptions the serverless
            // session will fail to capture are the operations that went pending locally and did not complete, and that
            // information is never stored in until serial no
            AdjustCommitPoint(ref currentCommitPoint);
            var until = currentCommitPoint.UntilSerialNo;
            // List of operations that are pending locally, tracked by FASTER.
            currentCommitPoint.ExcludedSerialNos.AddRange(
                localSession.CommitPoint().ExcludedSerialNos.Where(ex => ex <= until));
            AdjustCommitPoint(ref currentCommitPoint);
        }
        

        internal void TruncateVersionsOnRollback(ref CommitPoint recoveredProgress)
        {
            // Ok to allocate new objects as this code path is rare and never called in a tight loop
            var newVersionList = new Queue<(long, long)>();
            while (sequentialVersionList.Count != 0)
            {
                var entry = sequentialVersionList.Dequeue();
                if (entry.Item1 < recoveredProgress.UntilSerialNo)
                    newVersionList.Enqueue(entry);
                break;
            }
            sequentialVersionList = newVersionList;

            exceptionList.DropRolledbackExceptions(ref recoveredProgress);
        }

        internal List<Worker> RelevantWorkers()
        {
            var result = new HashSet<Worker>();
            foreach (var (_, workerVersion) in exceptionList)
                if (!workerVersion.Worker.Equals(Worker.INVALID) && !workerVersion.Worker.Equals(AttachedWorker.Me()))
                    result.Add(workerVersion.Worker);
            return new List<Worker>(result);
        }
    }
}