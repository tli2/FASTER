﻿using System.Collections.Generic;
 using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FASTER.serverless
{
    // TODO(Tianyu): Add ownership change back in
    // public class
    //     MetadataStoreUpdateTask<Key, Value, Input, Output, Context, Functions> : ISynchronizationTask
    //     where Key : new()
    //     where Value : new()
    //     where Functions : IFunctions<Key, Value, Input, Output, Context>
    // {
    //     private FasterServerless<Key, Value, Input, Output, Context, Functions> worker;
    //
    //     public MetadataStoreUpdateTask(
    //         FasterServerless<Key, Value, Input, Output, Context, Functions> worker)
    //     {
    //         this.worker = worker;
    //     }
    //
    //     public void GlobalBeforeEnteringState<Key, Value, Input, Output, Context, Functions>(SystemState next,
    //         FasterKV<Key, Value, Input, Output, Context, Functions> faster) where Key : new()
    //         where Value : new()
    //         where Functions : IFunctions<Key, Value, Input, Output, Context>
    //     {
    //         switch (next.phase)
    //         {
    //             case Phase.IN_PROGRESS:
    //                 // TODO(Tianyu): Is this version the same as nextVersion before?
    //                 worker.MetadataStore.OnCheckpointVersionChange(next.version);
    //                 break;
    //             case Phase.WAIT_FLUSH:
    //                 worker.MetadataStore.OnCheckpointFinish();
    //                 break;
    //         }
    //     }
    //
    //     public void GlobalAfterEnteringState<Key, Value, Input, Output, Context, Functions>(SystemState next,
    //         FasterKV<Key, Value, Input, Output, Context, Functions> faster) where Key : new()
    //         where Value : new()
    //         where Functions : IFunctions<Key, Value, Input, Output, Context>
    //     {
    //     }
    //
    //     public ValueTask OnThreadState<Key, Value, Input, Output, Context, Functions>(SystemState current,
    //         SystemState prev,
    //         FasterKV<Key, Value, Input, Output, Context, Functions> faster,
    //         FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
    //         ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool async = true,
    //         CancellationToken token = default) where Key : new()
    //         where Value : new()
    //         where Functions : IFunctions<Key, Value, Input, Output, Context>
    //     {
    //         return default;
    //     }
    // }

    public class VersionBoundaryCaptureTask<Key, Value, Input, Output, Functions> : ISynchronizationTask
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        private FasterServerless<Key, Value, Input, Output, Functions> worker;

        public VersionBoundaryCaptureTask(FasterServerless<Key, Value, Input, Output, Functions> worker)
        {
            this.worker = worker;
        }

        public void GlobalBeforeEnteringState<Key, Value>(
            SystemState next,
            FasterKV<Key, Value> faster)
        {
            switch (next.phase)
            {
                case Phase.IN_PROGRESS:
                case Phase.ROLLBACK_THROW:
                    worker.stableLocalVersion = worker.liveLocalVersion;
                    var newVersion = new OutstandingLocalVersion(next.version, faster.Log.TailAddress);
                    // Atomically update the live version to be the new version. Any readers after this point will
                    // check for the version number and observe a mismatch, and thus retrying on the stable version.
                    worker.liveLocalVersion = newVersion;
                    worker.outstandingVersions.Enqueue(newVersion);
                    break;
                case Phase.WAIT_FLUSH:
                    // Capture the associated metadata information with the checkpoint, so any rollbacks can complete
                    // without disk I/O
                    worker.stableLocalVersion.checkpointSessionProgress = faster._hybridLogCheckpoint.info.checkpointTokens;
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    Interlocked.Increment(ref worker.numCheckpointPerformed);
                    worker.toReport.Enqueue(worker.stableLocalVersion);
                    break;
            }
        }

        public void GlobalAfterEnteringState<Key, Value>(
            SystemState next,
            FasterKV<Key, Value> faster)
        {
        }

        public void OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev,
            FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where FasterSession: IFasterSession
        {
        }
    }
    
    public class WorldLineShiftTask<Key, Value, Input, Output, Functions> : ISynchronizationTask
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        private FasterServerless<Key, Value, Input, Output, Functions> worker;

        public WorldLineShiftTask(FasterServerless<Key, Value, Input, Output, Functions> worker)
        {
            this.worker = worker;
        }

        public void GlobalBeforeEnteringState<Key, Value>(
            SystemState next,
            FasterKV<Key, Value> faster)
        {
            // When the state machine enters rollback throw, subsequent operation can start to go to new
            // world-line, so we will need to start rejecting future operation from the old world-line. 
            // Threads that already read the old world-line will be in the old version until at least the 
            // next refresh, at which point they can either re-check the world-line or be informed of lost
            // operations by an exception.
            if (next.phase != Phase.ROLLBACK_THROW) return;
            // Guaranteed to be single-threaded by state machine semantics.
            worker.workerWorldLine++;
            worker.DprManager.ReportRecovery(worker.workerWorldLine, new WorkerVersion(worker.MessageManager.Me(), worker.DprManager.SafeVersion(worker.Me())));
        }

        public void GlobalAfterEnteringState<Key, Value>(
            SystemState next,
            FasterKV<Key, Value> faster)
        {
        }

        public void OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev,
            FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where FasterSession: IFasterSession
        {
        }
    }
}