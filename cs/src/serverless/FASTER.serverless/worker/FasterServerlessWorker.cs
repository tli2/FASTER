using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using Nito.AsyncEx;

namespace FASTER.serverless
{
    /* Worker API for dist-CPR implementation */
    public partial class FasterServerless<Key, Value, Input, Output, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        public List<long> checkpointLatencies = new List<long>();
        internal ConcurrentQueue<OutstandingLocalVersion> toReport = new ConcurrentQueue<OutstandingLocalVersion>();
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long CurrentVersion() => liveLocalVersion.Version();

        public void RefreshDprTable()
        {
            if (!clientOnly)
            {
                while (toReport.TryDequeue(out var local))
                {
                    DprManager.ReportNewPersistentVersion(new WorkerVersion(MessageManager.Me(), local.Version()),
                        local.GetDependenciesSlow());
                }
            }

            DprManager.Refresh();
            dprViewNumber++;
            if (!clientOnly)
            {
                var newLocalSafeVersion = DprManager.SafeVersion(MessageManager.Me());

                // Remove all local versions that are committed, as we will no longer need to track them for potential
                // rollbacks.
                while (true)
                {
                    if (!outstandingVersions.TryPeek(out var v)) break;
                    // Keep the safe version in the queue for potential rollbacks
                    if (v.Version() >= newLocalSafeVersion) break;
                    outstandingVersions.TryDequeue(out _);
                }
            }

            // TODO(Tianyu): Hacky solution for sequential world-line shifts. Because in the prototype we use the
            // DPR manager as communication mechanism for failures, there will not be individual messages for 
            // each failure.
            for (var i = workerWorldLine; i < DprManager.SystemWorldLine(); i++)
                Rollback(i + 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitiateVersionBump(long targetVersion)
        {
            if (clientOnly) throw new Exception("unsupported operation");
            
            if (targetVersion <= inProgressBump ||
                !Utility.MonotonicUpdate(ref inProgressBump, targetVersion, out _))
                return;
            // Ok if not actually started, thread refreshes will continuously try again.
            localFaster._fasterKV.BumpVersion(out _, targetVersion, out _);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryBumpToVersion(long targetVersion, ClientSession<Key, Value, Input, Output, Empty, Functions> session, int maxRetry = 5)
        {
            if (clientOnly) throw new Exception("unsupported operation");

            if (session.Version() >= targetVersion) return true;
            InitiateVersionBump(targetVersion);
            for (var i = 0; i < maxRetry; i++)
            {
                session.Refresh();
                if (session.Version() >= targetVersion) return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WaitUntilVersion(long targetVersion,
            ClientSession<Key, Value, Input, Output, Empty, Functions> session, int maxRetry = 5)
        {
            if (clientOnly) throw new Exception("unsupported operation");

            // Try bump again in case the caller didn't. Can't hurt.
            if (TryBumpToVersion(targetVersion, session, maxRetry)) return;
            do
            {
                // Need to periodically refresh and check for version, in case the task blocking is the
                // only thread alive and refreshing in the system.
                session.UnsafeSuspendThread();
                Thread.Yield();
                session.UnsafeResumeThread();
            } while (session.Version() < targetVersion);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<Guid> PerformCheckpoint()
        {
            if (clientOnly) throw new Exception("unsupported operation");

            Guid token;
            while (!localFaster.TakeHybridLogCheckpoint(out token, CheckpointType.FoldOver, DprManager.GlobalMaxVersion()))
                await Task.Delay(10);
            await localFaster._fasterKV.stateMachineCompletion.Task;
            return token;
        }
        
        // TODO(Tianyu) For now, the worker always rolls back to the guaranteed version, which is overly conservative. 
        public void Rollback(long targetWorldLine)
        {
            // World-line shifting happens sequentially
            // TODO(Tianyu): In this simplistic implementation, we don't need this guarantee
            // Rollbacks are idempotent because we aggressively revert to the DPR guarantee, and we temporarily
            // pause the computation for new guarantees when workers are in the process of recovery.
            while (workerWorldLine != targetWorldLine - 1)
                Thread.Sleep(10);
            
            var targetVersion = DprManager.SafeVersion(MessageManager.Me());
            var targetVersionObject = outstandingVersions.FirstOrDefault(outstandingVersion => outstandingVersion.Version() == targetVersion);
            long offset = -1;
            var progress = new ConcurrentDictionary<string, CommitPoint>();
            if (targetVersionObject != default)
            {
                offset = targetVersionObject.FuzzyVersionStartLogOffset();
                progress = targetVersionObject.checkpointSessionProgress;
            }
            
            TaskCompletionSource<object> tcs;
            while (!localFaster._fasterKV.Rollback(targetVersion, offset, progress, out tcs))
            {
                // Wait for previous state machine to complete
                localFaster.CompleteCheckpointAsync().AsTask().Wait();
            }

            localFaster.CompleteCheckpointAsync().AsTask().Wait();
            // No need to clear local queue of outstanding operations, as that will be cleared in the next commit
        }

        public void ReportVersionDependencies(long versionNum, LightDependencySet deps)
        {
            if (!deps.MaybeNotEmpty()) return;
            
            var version = liveLocalVersion;
            if (version.Version() != versionNum)
            {
                version = stableLocalVersion;
                // The epoch protection framework should guarantee that no active session is more behind that stable.
                Debug.Assert(version.Version() == versionNum);
            }

            for (var i = 0; i < deps.DependentVersions.Length; i++)
            {
                if (MessageManager.Me().guid == i) continue;
                version.AddDependency(new Worker(i), deps.DependentVersions[i]);
            }
        }

        public void ReportVersionDependencies(long versionNum, ref MessageBatchRaw batch)
        {
            if (batch.header.numDeps == 0) return;
            
            var version = liveLocalVersion;
            if (version.Version() != versionNum)
            {
                version = stableLocalVersion;
                // The epoch protection framework should guarantee that no active session is more behind that stable.
                Debug.Assert(version.Version() == versionNum);
            }

            for (var i = 0; i < batch.header.numDeps; i++)
            {
                ref var dep = ref batch.GetDep(i);
                if (!dep.Worker.Equals(MessageManager.Me()))
                    version.AddDependency(dep.Worker, dep.Version);
            }
        }
        
        public void ReportVersionDependencies(long versionNum, ParsedMessageBatch<Key, Value, Input, Output> batch)
        {
            if (batch.header.numDeps == 0) return;
            
            var version = liveLocalVersion;
            if (version.Version() != versionNum)
            {
                version = stableLocalVersion;
                // The epoch protection framework should guarantee that no active session is more behind that stable.
                Debug.Assert(version.Version() == versionNum);
            }

            for (var i = 0; i < batch.header.numDeps; i++)
            {
                ref var dep = ref batch.deps[i];
                if (!dep.Worker.Equals(MessageManager.Me()))
                    version.AddDependency(dep.Worker, dep.Version);
            }
        }
    }
}