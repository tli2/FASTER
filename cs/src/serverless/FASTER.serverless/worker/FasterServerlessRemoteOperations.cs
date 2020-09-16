﻿using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using FASTER.core;

namespace FASTER.serverless
{
    public partial class FasterServerless<Key, Value, Input, Output, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        public long numRemote, numBackground;

        public CommitPoint GetSessionRecoveryProgress(Guid sessionId, long worldLine)
        {
            
            // At this point the world line could be ahead of what the requested world-line is, but in that case.
            // said worker will eventually need to come ask us again, and it doesn't matter what answer we give now.
            if (cachedLocalSessions.TryGetValue(sessionId, out var localSession))
            {
                try
                {
                    while (workerWorldLine < worldLine)
                    {
                        localSession.Item1.Refresh();
                    }
                
                    localSession.Item1.Refresh();
                } catch (FasterRollbackException) {}

                // In this case the session's progress is stored in its commit point. It suffices to read that out.
                var result = localSession.Item1.CommitPoint();
                return result;
            }

            throw new NotImplementedException();
            // Otherwise, maybe this worker crashed and we need to resume process from a checkpoint
            // try
            // {
            //     var recoveredSession = localFaster.ResumeSession(sessionId.ToString(), out var commitPoint, true);
            //     // Immediately suspend thread because it comes back affinitized, and we will not be operating on the
            //     // session here.
            //     recoveredSession.UnsafeSuspendThread();
            //     cachedLocalSessions.TryAdd(sessionId, ValueTuple.Create(recoveredSession, new SemaphoreSlim(1, 1)));
            //     return commitPoint;
            // }
            // catch (FasterException)
            // {
            //     // An exception is thrown here if there are no corresponding recovered version, which could be because
            //     // the request message got lost. Simply return a trivial commit point suffices.
            //     return new CommitPoint();
            // }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Worker Me() => MessageManager.Me();

        internal unsafe void ProcessBatch(byte[] buf, int offset,
            Socket socket,
            FasterServerlessBackgroundThreadPool<Key, Value, Input, Output, Functions> threadPool)
        {
            fixed (void* b = &buf[offset])
            {
                ref var batch = ref Unsafe.AsRef<MessageBatchRaw>(b);
                batch.ResetReader();
                Message<Key, Value, Input, Output> m = default;
                Interlocked.Add(ref numRemote, batch.header.numMessages);
                var localSession = GetLocalSession(batch.header.sessionId, out var latch);

                var processed = 0;
                var firstRead = false;
                if (latch.Wait(TimeSpan.Zero))
                {
                    var replies = messagePool.Checkout();
                    replies.obj.ReinitializeForReply(ref batch);

                    var worldLine = workerWorldLine;
                    try
                    {
                        localSession.UnsafeResumeThread();
                    }
                    catch (FasterRollbackException) {}

                    
                    while (batch.NextMessage(ref m, serializer))
                    {
                        firstRead = true;
                        if (!TryHandleMessage(ref batch, ref m, replies.obj, localSession, processed == 0, worldLine))
                        {
                            break;
                        }

                        processed++;
                        if (processed % 256 == 0)
                        {
                            try
                            {
                                worldLine = workerWorldLine;
                                localSession.Refresh();
                            }
                            catch (FasterRollbackException) {}
                            if (CurrentVersion() < inProgressBump)
                                localFaster._fasterKV.BumpVersion(out _, inProgressBump, out _);
                        }
                    }

                    try
                    {
                        localSession.CompletePending(true);
                    } catch (FasterRollbackException) {}

                    // We have processed all requests that we can, can send replies now
                    localSession.UnsafeSuspendThread();
                    latch.Release();
                    if (replies.obj.header.numMessages != 0)
                        MessageManager.Send(socket, replies.obj, serializer);
                    replies.Dispose();
                }

                if (processed != batch.header.numMessages)
                {
                    // This request cannot be executed immediately. Shoot it over to the background thread pool
                    // for execution, but mark all records up until this point 
                    var requestsForLater = messagePool.Checkout();
                    requestsForLater.obj.ReinitializeForBackgroundProcessing(ref batch, socket);
                    // Add this and all later unprocessed messages in the batch for later
                    if (firstRead) requestsForLater.obj.AddMessage() = m;
                    while (batch.NextMessage(ref m, serializer))
                        requestsForLater.obj.AddMessage() = m;
                    Interlocked.Add(ref numBackground, requestsForLater.obj.header.numMessages);
                    threadPool.SubmitRequest(requestsForLater);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        // Returns true if the message is executed immediately, false if the message must be retried later
        // offset points to the start of header
        private bool TryHandleMessage(ref MessageBatchRaw batch,
            ref Message<Key, Value, Input, Output> m,
            ParsedMessageBatch<Key, Value, Input, Output> replies,
            ClientSession<Key, Value, Input, Output, Empty, Functions> localSession,
            bool addDeps, long worldLineView)
        {
            switch (m.header.type)
            {
                case FasterServerlessMessageType.ReadRequest:
                case FasterServerlessMessageType.UpsertRequest:
                case FasterServerlessMessageType.RmwRequest:
                case FasterServerlessMessageType.DeleteRequest:
                    return TryExecutePeerRequest(ref batch, ref m, ref replies, localSession, addDeps, worldLineView);
                case FasterServerlessMessageType.RecoveryStatusCheck:
                    var recoveredCommitPoint = GetSessionRecoveryProgress(batch.header.sessionId, m.header.worldLine);
                    replies.AddMessage().ReplyRecoveryResult(ref m, recoveredCommitPoint);
                    return true;
                // TODO(Tianyu): Add ownership transfer functionality back in at some point
                // case FasterServerlessMessageType.OwnershipDropped:
                //     var droppedMessage = (FasterServerlessOwnershipDroppedMessage) message;
                //     MetadataStore.CompleteDropTask(droppedMessage.BucketDropped);
                //     break;
                // case FasterServerlessMessageType.TransferOwnership:
                //     var dropMessage = (FasterServerlessTransferOwnershipMessage) message;
                //     MetadataStore.RenounceOwnership(dropMessage.BucketToDrop, dropMessage.Sender());
                //     break;
                default:
                    throw new Exception("Unexpected message type");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CheckWorkerFailure(ref BatchHeader batch, ref Message<Key, Value, Input, Output> m,
            ParsedMessageBatch<Key, Value, Input, Output> replies, long worldLineView)
        {
            if (m.header.worldLine == worldLineView)
                return false;

            ref var reply = ref replies.AddMessage();
            if (m.header.type == FasterServerlessMessageType.ReadRequest)
                reply.ReplyReadFailureWorldLine(ref m, worldLineView);
            else
                reply.ReplyWriteFailureWorldLine(ref m, worldLineView);
            return true;
        }

        private bool CheckOwnershipMismatch(ref Message<Key, Value, Input, Output> m, long bucket,
            ClientSession<Key, Value, Input, Output, Empty, Functions> localSession,
            ParsedMessageBatch<Key, Value, Input, Output> replies)
        {
            if (MetadataStore.ValidateLocalOwnership(bucket, localSession)) return false;

            ref var reply = ref replies.AddMessage();
            if (m.header.type == FasterServerlessMessageType.ReadRequest)
                reply.ReplyReadFailureNotOwner(ref m);
            else
                reply.ReplyWriteFailureNotOwner(ref m);
            return true;
        }

        private void ExecuteOperation(ref Message<Key, Value, Input, Output> m,
            ParsedMessageBatch<Key, Value, Input, Output> replies,
            ClientSession<Key, Value, Input, Output, Empty, Functions> localSession)
        {
            ref var reply = ref replies.AddMessage();
            switch (m.header.type)
            {
                case FasterServerlessMessageType.ReadRequest:
                {
                    var status = localSession.Read(ref m.key,
                        ref m.input, ref reply.output, Empty.Default,
                        m.header.serialNum);
                    if (status == Status.PENDING)
                        throw new NotImplementedException();

                    reply.ReplyReadSuccess(ref m, localSession.Version(), status);
                    break;
                }
                case FasterServerlessMessageType.UpsertRequest:
                {
                    var status = localSession.Upsert(ref m.key,
                        ref m.value, Empty.Default, m.header.serialNum);
                    if (status == Status.PENDING)
                        // We will blockingly clear pending later
                        status = Status.OK;

                    reply.ReplyWriteSuccess(ref m, localSession.Version(), status);
                    break;
                }
                case FasterServerlessMessageType.RmwRequest:
                {
                    var status = localSession.RMW(ref m.key,
                        ref m.input, Empty.Default,
                        m.header.serialNum);
                    if (status == Status.PENDING)
                        throw new NotImplementedException();

                    reply.ReplyWriteSuccess(ref m, localSession.Version(), status);
                    break;
                }
                case FasterServerlessMessageType.DeleteRequest:
                {
                    var status = localSession.Delete(ref m.key, Empty.Default,
                        m.header.serialNum);
                    if (status == Status.PENDING)
                        throw new NotImplementedException();

                    reply.ReplyWriteSuccess(ref m, localSession.Version(), status);
                    break;
                }
                default:
                    throw new FasterException();
            }
        }

        private bool TryExecutePeerRequest(ref MessageBatchRaw batch,
            ref Message<Key, Value, Input, Output> m,
            ref ParsedMessageBatch<Key, Value, Input, Output> replies,
            ClientSession<Key, Value, Input, Output, Empty, Functions> localSession,
            bool addDeps, long worldLineView)
        {
            // If the worker is behind on world line, sit on this request until the worker recovers.
            if (worldLineView < m.header.worldLine) return false;
            // Now, if the worker world line is ahead of the client request, the client needs to be notified
            // of a failure the did not observe. Only need to do this when there is no local session on this worker
            // yet, otherwise we can rely on the exception from FASTER itself to achieve this.
            if (CheckWorkerFailure(ref batch.header, ref m, replies, worldLineView)) return true;

            var bucket = BucketingScheme.GetBucket(m.key);

            // Begin operation in a epoch-protected region

            // If the request is sent to this worker by mistake, reply with a rejection so the sender retries
            if (CheckOwnershipMismatch(ref m, bucket, localSession, replies)) return true;

            // Update local version if session has higher version. Otherwise, cannot perform operation, signal for
            // requeue to try later.
            if (!TryBumpToVersion(m.header.version, localSession, 0)) return false;

            Debug.Assert(localSession.Version() >= m.header.version);
            if (addDeps) ReportVersionDependencies(localSession.Version(), ref batch);
            ExecuteOperation(ref m, replies, localSession);
            return true;
        }

        internal void RetryBatch(ParsedMessageBatch<Key, Value, Input, Output> batch)
        {
            var replies = messagePool.Checkout();
            replies.obj.ReinitializeForReply(batch);
            var localSession = GetLocalSession(batch.header.sessionId, out var latch);

            latch.Wait();
            var worldLine = 0L;
            try
            {
                worldLine = workerWorldLine;
                localSession.UnsafeResumeThread();
            }
            catch (FasterRollbackException) {}

            for (var i = 0; i < batch.header.numMessages; i++)
            {
                switch (batch.messages[i].header.type)
                {
                    case FasterServerlessMessageType.ReadRequest:
                    case FasterServerlessMessageType.UpsertRequest:
                    case FasterServerlessMessageType.RmwRequest:
                    case FasterServerlessMessageType.DeleteRequest:
                        ExecutePeerRequestBlocking(batch, i, replies.obj, localSession, worldLine);
                        break;
                    default:
                        throw new Exception("Unexpected message type");
                }

                if (i % 256 == 0)
                {
                    try
                    {
                        worldLine = workerWorldLine;
                        localSession.Refresh();
                    }
                    catch (FasterRollbackException) {}
                }
            }

            try
            {
                localSession.CompletePending(true);
            } catch (FasterRollbackException) {}
            localSession.UnsafeSuspendThread();
            latch.Release();

            MessageManager.Send(batch.socket, replies.obj, serializer);
            replies.Dispose();
        }

        private void ExecutePeerRequestBlocking(ParsedMessageBatch<Key, Value, Input, Output> batch,
            int messageNum,
            ParsedMessageBatch<Key, Value, Input, Output> replies,
            ClientSession<Key, Value, Input, Output, Empty, Functions> localSession, long worldLineView)
        {
            // If the worker is behind on world line, sit on this request until the worker recovers.
            while (worldLineView < batch.messages[messageNum].header.worldLine)
            {
                worldLineView = workerWorldLine;
                try
                {
                    localSession.Refresh();
                } catch (FasterRollbackException) {}
            }
            // Now, if the worker world line is ahead of the client request, the client needs to be notified
            // of a failure the did not observe. Only need to do this when there is no local session on this worker
            // yet, otherwise we can rely on the exception from FASTER itself to achieve this.
            if (CheckWorkerFailure(ref batch.header, ref batch.messages[messageNum], replies, worldLineView)) return;

            var bucket = BucketingScheme.GetBucket(batch.messages[messageNum].key);
            // Begin operation in a epoch-protected region
            // If the request is sent to this worker by mistake, reply with a rejection so the sender retries
            if (CheckOwnershipMismatch(ref batch.messages[messageNum], bucket, localSession, replies)) return;

            // TODO(Tianyu): We are currently not guarding against out-of-order or concurrent execution of requests
            // within a session because the server layer prohibits that. That may change in the future.

            // Update local version if session has higher version. Otherwise, cannot perform operation, signal for
            // requeue to try later.
            WaitUntilVersion(batch.messages[messageNum].header.version, localSession);
            if (CheckWorkerFailure(ref batch.header, ref batch.messages[messageNum], replies, worldLineView)) return;


            Debug.Assert(localSession.Version() >= batch.messages[messageNum].header.version);
            // Only need to report dependency for the first message. Ok to report again even if the dependencies
            // were reported before retrying just for simplicity.
            if (messageNum == 0)
                ReportVersionDependencies(localSession.Version(), batch);
            ExecuteOperation(ref batch.messages[messageNum], replies, localSession);
        }
    }
}