﻿using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public class FasterRollbackException : FasterException
    {
        // TODO(Tianyu): As it stands, this class is not very useful to anyone but FASTER-serverless.
        // The reason is because we want to be able to rollback past the local committed point (FASTER-serverless
        // needs this), but the current session implementation does not store any information of past commits.
        public long fromVersion, toVersion;

        public FasterRollbackException(long fromVersion, long toVersion)
        {
            this.fromVersion = fromVersion;
            this.toVersion = toVersion;
        }
    }

    internal class RollbackTask : ISynchronizationTask
    {
        private bool completed = false;
        private long rollbackVersionStart, logScanStart;
        private ConcurrentDictionary<string, CommitPoint> sessionProgress;

        public RollbackTask(long rollbackVersionStart, long logScanStart, ConcurrentDictionary<string, CommitPoint> sessionProgress)
        {
            this.rollbackVersionStart = rollbackVersionStart;
            this.logScanStart = logScanStart;
            this.sessionProgress = sessionProgress;
        }

        public void GlobalBeforeEnteringState<Key, Value>(SystemState next,
            FasterKV<Key, Value> faster)
        {

            // TODO(Tianyu): Signal error when rolling back with snapshot checkpoints
            switch (next.phase)
            {
                case Phase.ROLLBACK_THROW:
                    faster.excludedVersionStart = rollbackVersionStart;
                    break;
                case Phase.ROLLBACK_PURGE:
                    // compute exception to be thrown for all dormant sessions so they don't miss the rollback
                    // when they wake up. Because this happens in a critical section during session state transition,
                    // the sessions will stay dormant during this process.
                    lock (faster._activeSessions)
                    {
                        foreach (var session in
                            faster._activeSessions.Where(session => session.Value.Version() < next.version))
                        {
                            session.Value.SetCannedException(
                                new FasterRollbackException(rollbackVersionStart, next.version));
                            // Reset session progress
                            if (!sessionProgress.TryGetValue(session.Key, out var preservedProgress))
                                preservedProgress = new CommitPoint {UntilSerialNo = -1, ExcludedSerialNos = new List<long>()};
                            session.Value.CommitPoint() = preservedProgress;
                        }
                    }


                    if (logScanStart == -1)
                    {
                        completed = true;
                    }
                    else
                    {
                        // Start the background tasks. The recovery is not done until these tasks finish, but the system
                        // is allowed to go ahead with operations while they are underway
                        Task.Run(() =>
                        {
                            // Actual rollback
                            faster.RollbackLogVersions(logScanStart,
                                faster.hlog.GetTailAddress(),
                                faster.excludedVersionStart, next.version);
                            completed = true;
                        });
                    }
                    break;
                case Phase.REST:
                    faster.excludedVersionStart = long.MaxValue;
                    break;
            }
        }

        public void GlobalAfterEnteringState<Key, Value>(SystemState next,
            FasterKV<Key, Value> faster)
        {
        }

        /// <inheritdoc />
        public void OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev, FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where FasterSession : IFasterSession
        {
            switch (current.phase)
            {
                case Phase.ROLLBACK_THROW:
                    if (ctx != null)
                    {
                        // Need to be very careful here as threadCtx is changing
                        var _ctx = prev.phase == Phase.ROLLBACK_THROW ? ctx.prevCtx : ctx;

                        var tokens = faster._hybridLogCheckpoint.info.checkpointTokens;
                        if (!faster.SameCycle(current) || tokens == null)
                            return;
                        if (!_ctx.markers[EpochPhaseIdx.RollbackInProg])
                        {
                            faster.AtomicSwitch(ctx, ctx.prevCtx, _ctx.version, tokens);
                            faster.InitContext(ctx, ctx.prevCtx.guid, ctx.prevCtx.serialNum,
                                rollbackVersionStart);
                            ctx.excludedVersionStart = faster.excludedVersionStart;
                            ctx.excludedVersionEnd = ctx.version;

                            // Has to be prevCtx, not ctx
                            ctx.prevCtx.markers[EpochPhaseIdx.RollbackInProg] = true;

                            faster.epoch.Mark(EpochPhaseIdx.RollbackInProg, current.version);
                            // Reset session progress
                            if (fasterSession != null)
                            {
                                if (!sessionProgress.TryGetValue(fasterSession.Id(), out var preservedProgress))
                                    preservedProgress = new CommitPoint {UntilSerialNo = -1, ExcludedSerialNos = new List<long>()};
                                fasterSession.CommitPoint() = preservedProgress;
                            }
                            
                            // Need advance the version now, because this handler will abort execution by throwing
                            // an exception now. 
                            ctx.phase = current.phase;
                            ctx.version = current.version;
                            // Throw exception to signal a rollback. Aborts the current operation.
                            throw new FasterRollbackException(faster.excludedVersionStart, ctx.version);
                        }
                    }

                    if (faster.epoch.CheckIsComplete(EpochPhaseIdx.RollbackInProg, current.version))
                        faster.GlobalStateMachineStep(current);
                    break;
                case Phase.ROLLBACK_PURGE:
                    if (completed)
                        faster.GlobalStateMachineStep(current);
                    break;
            }
        }
    }

    internal class RollbackStateMachine : SynchronizationStateMachineBase
    {
        public RollbackStateMachine(long rollbackPoint, long logScanStart, ConcurrentDictionary<string, CommitPoint> sessionProgress) 
            : base(new RollbackTask(rollbackPoint + 1, logScanStart, sessionProgress)) {}

        public override SystemState NextState(SystemState start)
        {
            var result = SystemState.Copy(ref start);
            switch (start.phase)
            {
                case Phase.REST:
                    result.phase = Phase.ROLLBACK_THROW;
                    result.version = start.version + 1;
                    break;
                case Phase.ROLLBACK_THROW:
                    result.phase = Phase.ROLLBACK_PURGE;
                    break;
                case Phase.ROLLBACK_PURGE:
                    result.phase = Phase.REST;
                    break;
                default:
                    throw new FasterException();
            }

            return result;
        }
    }
}