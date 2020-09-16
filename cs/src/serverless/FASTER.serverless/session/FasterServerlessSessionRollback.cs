﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
 using System.Diagnostics;
 using System.Threading;
using FASTER.core;

namespace FASTER.serverless
{
    public class FasterServerlessRollbackException : FasterException
    {
        public readonly CommitPoint recoveredProgress;

        public FasterServerlessRollbackException(CommitPoint recoveredProgress)
        {
            this.recoveredProgress = recoveredProgress;
        }
    }
    
    public partial class FasterServerlessSession<Key, Value, Input, Output, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        
        // Ok to use slow dictionary because this is uncommon code path
        internal ConcurrentDictionary<int, Message<Key, Value, Input, Output>> recoveryResults = new ConcurrentDictionary<int, Message<Key, Value, Input, Output>>();
        internal ConcurrentDictionary<int, ManualResetEventSlim> recoveryProgress = new ConcurrentDictionary<int, ManualResetEventSlim>();
        private IEnumerable<long> GatherRecoveryInfo(long worldLine)
        {
            var workersToContact = RelevantWorkers();
            if (workersToContact.Count == 0)
                return new List<long>();
            
            for (var i = 0; i < workersToContact.Count; i++)
                recoveryProgress.TryAdd(i, new ManualResetEventSlim());
            
            for (var i = 0; i < workersToContact.Count; i++)
            {
                var worker = workersToContact[i];
                // Ok to bypass pending context and allocate new objects for exceptional circumstance
                var requestBatch = new ParsedMessageBatch<Key, Value, Input, Output>();
                requestBatch.header.sessionId = Id;
                requestBatch.header.recipient = worker.guid;
                requestBatch.header.sender = AttachedWorker.Me().guid;
                requestBatch.header.numDeps = 0;
                requestBatch.header.numMessages = 0;
                requestBatch.header.replyOnly = false;
                requestBatch.AddMessage().InitializeRecoveryStatusCheck(i, worldLine);
                
                AttachedWorker.MessageManager.Send(this, requestBatch, AttachedWorker.serializer);
            }
            Stopwatch sw = new Stopwatch();
            sw.Start();
            
            var result = new List<long>();
            foreach (var query in recoveryProgress)
            {
                // TODO(Tianyu): Blocking on remote operation just for the exceptional case should be fine?
                query.Value.Wait();
                recoveryResults.TryRemove(query.Key, out var recoveryResult);
                result.Add(recoveryResult.recoveredUntil);
            }
            sw.Stop();
            recoveryProgress.Clear();
            Console.WriteLine($"session {Id} gathered recovery info in {sw.ElapsedMilliseconds} ms");
            return result;
        }

        private static CommitPoint JoinRecoveryPoint(IEnumerable<long> recoveredProgress, CommitPoint localCommitPoint)
        {
            var result = new CommitPoint
            {
                ExcludedSerialNos = localCommitPoint.ExcludedSerialNos,
                UntilSerialNo = localCommitPoint.UntilSerialNo
            };
            foreach (var n in recoveredProgress)
                result.UntilSerialNo = Math.Max(result.UntilSerialNo, n);
            return result;
        }

        private void HandlePendingQueueOnRollback(ref CommitPoint recoveredProgress)
        {
            // Replacement queue for recovered pending operations
            while (currentPendingOps.Count != 0)
            {
                var pendingContext = reusablePendingContexts[currentPendingOps.Dequeue()];
                if (pendingContext.completion.Wait(TimeSpan.Zero))
                {
                    UnboxRemoteExecutionResult(pendingContext);
                    UpdateDependenciesOnPendingOperationResolution(pendingContext);
                }
            }
        }

        private void HandleLocalRollback()
        {
            var worldLine = AttachedWorker.workerWorldLine;
            var localCommitPoint = localSession.CommitPoint();
            var recoveredProgress = JoinRecoveryPoint(GatherRecoveryInfo(worldLine), localCommitPoint);
            // Because this thread has already advanced past THROW there is no need to suspend thread, it will not
            // block others from making progress.
            // HandlePendingQueueOnRollback(ref recoveredProgress);
            exceptionList.ResolveExceptions(AttachedWorker.DprManager.ReadSnapshot(), opCommitTick, stopwatch.ElapsedTicks);
            TruncateVersionsOnRollback(ref recoveredProgress);
            sessionWorldLine = worldLine;
            // TODO(Tianyu): Sometimes a client may be unaffected. Maybe it will be nice to check that and not throw
            // an exception in that case.
            throw new FasterServerlessRollbackException(recoveredProgress);
        }
    }
}