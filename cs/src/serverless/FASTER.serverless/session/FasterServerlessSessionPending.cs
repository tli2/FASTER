using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FASTER.serverless
{
    internal class ServerlessPendingContext<Key, Value, Input, Output>
        where Key : new()
        where Value : new()
    {
        internal Message<Key, Value, Input, Output> op =
            new Message<Key, Value, Input, Output>();

        internal long bucket;

        // This signals that a local worker is able to make immediate progress on the operation. This can be because
        // the operation has been verified for local execution, or because remote execution is complete.
        internal Message<Key, Value, Input, Output> result;

        internal WorkerVersion workerVersion;

        // Whether we need to reissue this operation through ownership validation, version synchronization and such.
        internal bool reissue;

        // optional field
        internal ReadResult<Output> readResult;

        // When a batch returns, it writes the actual dependencies that batch took on to the first request sent.
        internal WorkerVersion[] deps = new WorkerVersion[1 << LightDependencySet.MaxSizeBits];
        internal int numDeps = 0;
        internal ManualResetEventSlim completion = new ManualResetEventSlim();

        public void Reinitialize(long bucket, ReadResult<Output> readResult = null)
        {
            this.bucket = bucket;
            workerVersion.Worker = Worker.INVALID;
            workerVersion.Version = -1;
            reissue = false;
            this.readResult = readResult;
            numDeps = 0;
            completion.Reset();
        }
    }

    public partial class FasterServerlessSession<Key, Value, Input, Output, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Task LookupOwnerInBackground(
            ServerlessPendingContext<Key, Value, Input, Output> pendingContext)
        {
            return Task.Run(async () =>
            {
                var owner = await AttachedWorker.MetadataStore.ObtainOwnershipAsync(pendingContext.bucket,
                    Worker.INVALID);
                pendingContext.reissue = true;
                pendingContext.completion.Set();
            });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HandleRemoteExecutionError(ServerlessPendingContext<Key, Value, Input, Output> pendingContext)
        {
            ref var reply = ref pendingContext.result;
            switch (reply.header.ret)
            {
                case FasterServerlessReturnCode.NotOwner:
                    // The locally stored ownership information is outdated, retry operation
                    AttachedWorker.MetadataStore.InvalidateCachedEntry(pendingContext.bucket);
                    LookupOwnerInBackground(pendingContext);
                    return true;
                case FasterServerlessReturnCode.WorldLineShift:
                    // Operation paused because of a remote worker has seen a failure this client session has not
                    // This must be the first occurrence, because otherwise as part of a local rollback the pending
                    // operation would be removed from the queue
                    Debug.Assert(sessionWorldLine == reply.header.worldLine);
                    return true;
                default:
                    return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status ReturnCodeToStatus(FasterServerlessReturnCode ret)
        {
            switch (ret)
            {
                case FasterServerlessReturnCode.OK:
                    return Status.OK;
                case FasterServerlessReturnCode.NotFound:
                    return Status.NOTFOUND;
                case FasterServerlessReturnCode.Error:
                    return Status.ERROR;
                default:
                    throw new FasterException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UnboxRemoteExecutionResult(ServerlessPendingContext<Key, Value, Input, Output> pendingContext)
        {
            ref var reply = ref pendingContext.result;
            if (!HandleRemoteExecutionError(pendingContext))
            {
                if (reply.header.type == FasterServerlessMessageType.ReadResult)
                {
                    pendingContext.readResult.status = ReturnCodeToStatus(reply.header.ret);
                    pendingContext.readResult.output = reply.output;
                }

                pendingContext.workerVersion.Version = reply.header.version;
                if (trackForCommit)
                    exceptionList.Add(pendingContext.op.header.serialNum, pendingContext.workerVersion);
                Utility.MonotonicUpdate(ref sessionVersion, reply.header.version, out _);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateDependenciesOnPendingOperationResolution(
            ServerlessPendingContext<Key, Value, Input, Output> pendingContext)
        {
            // Again, due to transitivity in dependency we can drop all the pending operations' dependencies, but some
            // dependencies might persist as it was added after the operation went pending
            for (var i = 0; i < pendingContext.numDeps; i++)
            {
                ref var dep = ref pendingContext.deps[i];
                if (dep.Worker.Equals(AttachedWorker.MessageManager.Me()) &&
                    dep.Version >= previousOperationLocalVersion)
                    previousOperationLocalVersion = -1;
                else
                    predecessors.UnsafeRemove(dep.Worker, dep.Version);
            }

            predecessors.Update(pendingContext.workerVersion.Worker, pendingContext.workerVersion.Version);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CompleteLocalPending()
        {
            var result = localSession.CompletePending();
            var versionExecuted = localSession.Version();
            // If the corresponding local session number has advanced, the serverless session should also do so. This
            // could be because of a local pending operation being cleared
            if (Utility.MonotonicUpdate(ref sessionVersion, versionExecuted, out _))
                sequentialVersionList.Enqueue(ValueTuple.Create(serialNum, localSession.Version()));
            return result;
        }

        public bool CompletePending(TimeSpan waitTimeout, bool spinWait = false)
        {
            do
            {
                try
                {
                    var invoked = false;
                    // Clear pending operations due to local I/O / retries
                    var result = true;

                    // Clear pending operations due to potential remote operations / version mismatches. We need to be
                    // careful about dequeuing because we want the loop to at least exit and refresh once if clearing
                    // some pending operations yield more pending operations, otherwise this loop may block forever.
                    int numEntries = currentPendingOps.Count;
                    for (var i = 0; i < numEntries; i++)
                    {
                        var index = currentPendingOps.Peek();
                        var pendingContext = reusablePendingContexts[index];
                        lock (pendingContext)
                        {
                            // if we are not supposed to spin wait, exit immediately if unable to make progress. Need to do
                            // this to preserve order in the queue so we can use a fast circular buffer for reusable contexts.
                            if (!pendingContext.completion.Wait(TimeSpan.Zero))
                            {
                                localSession.UnsafeSuspendThread();
                                var acquired = pendingContext.completion.Wait(waitTimeout);

                                localSession.UnsafeResumeThread();


                                if (!acquired)
                                {
                                    result = false;
                                    break;
                                }
                            }
                        }

                        invoked = true;
                        currentPendingOps.Dequeue();
                        // Check for local operations that are pending because ownership was obtained for this worker only
                        // after it went pending. 
                        if (pendingContext.reissue)
                        {
                            LocalExecution(pendingContext);
                        }
                        else
                        {
                            UnboxRemoteExecutionResult(pendingContext);
                            UpdateDependenciesOnPendingOperationResolution(pendingContext);
                        }
                    }

                    result &= currentPendingOps.Count == 0;
                    // Local execution may have generated some new pending operations tracked by local sessions
                    if (!CompleteLocalPending()) result = false;

                    if (!invoked)
                    {
                        // flush batched requests when waiting on pending operations if no change to the queue is made, in order
                        // to avoid blocking indefinitely on waiting for requests that were not sent.
                        foreach (var batch in batcher.outstandingBatches)
                            if (!batch.Empty())
                            {
                                AttachedWorker.MessageManager.Send(this, batch, AttachedWorker.serializer);
                                batch.Clear();
                            }
                    }

                    if (result) return true;
                    if (spinWait)
                    {
                        Refresh();
                        Thread.Yield();
                    }
                }
                catch (FasterRollbackException)
                {
                    HandleLocalRollback();
                    // always throws
                }
            } while (spinWait);

            return false;
        }

        public int NumRemotePendingOps()
        {
            return currentPendingOps.Count;
        }

        public long NextSerialNum()
        {
            return serialNum;
        }

        internal unsafe void ProcessReplies(byte[] buf, int offset)
        {
            fixed (void* b = &buf[offset])
            {
                ref var batch = ref Unsafe.AsRef<MessageBatchRaw>(b);
                batch.ResetReader();
                Message<Key, Value, Input, Output> m = default;

                Debug.Assert(batch.header.replyOnly);
                var first = true;
                while (batch.NextMessage(ref m, AttachedWorker.serializer))
                {
                    if (m.header.type == FasterServerlessMessageType.RecoveryResult)
                    {
                        recoveryResults.TryAdd(m.header.writeLocation, m);
                        recoveryProgress[m.header.writeLocation].Set();
                        continue;
                    }

                    // Otherwise, cannot rely on the pending operation still being reserved for this operation.
                    // if (m.header.worldLine != sessionWorldLine) continue;

                    ref var ctx = ref reusablePendingContexts[m.header.writeLocation];
                    lock (ctx)
                    {
                        if (ctx.op.header.serialNum != m.header.serialNum) continue;
                        Debug.Assert(ctx.op.header.serialNum == m.header.serialNum);
                        ctx.result = m;
                        // If the first reply in a reply batch, also write down the list of dependencies
                        // the batch shipped with for predecessor maintenance 
                        if (first)
                        {
                            ctx.numDeps = batch.header.numDeps;
                            for (var i = 0; i < ctx.numDeps; i++)
                                ctx.deps[i] = batch.GetDep(i);
                            first = false;
                        }

                        ctx.completion.Set();
                    }

                    if (latencyMeasurements)
                        opEndTick[m.header.serialNum] = stopwatch.ElapsedTicks;
                }
            }
        }
    }
}