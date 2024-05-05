using System.Collections.Concurrent;
using System.Diagnostics;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;

namespace FASTER.darq
{
    /// <summary>
    /// Status of a step 
    /// </summary>
    public enum StepStatus
    {
        /// <summary>
        /// Step is not yet completed
        /// </summary>
        INCOMPLETE,

        /// <summary>
        /// Step is successfully completed
        /// </summary>
        SUCCESS,

        /// <summary>
        ///  Step cannot be completed because it is either ill-formed or because it is trying to consume
        ///  consumed messages
        /// </summary>
        INVALID,

        /// <summary>
        /// The step cannot be completed because it originated from a processor that is no longer allowed to
        /// update DARQ state (possibly due to another, newer processor taking over)
        /// </summary>
        REINCARNATED
    }

    internal class StepRequestHandle
    {
        internal volatile StepStatus status;
        internal long incarnation;
        internal IReadOnlySpanBatch stepMessages;
        internal ManualResetEventSlim done = new();

        internal void Reset(long incarnation, IReadOnlySpanBatch stepMessages)
        {
            this.incarnation = incarnation;
            status = StepStatus.INCOMPLETE;
            this.stepMessages = stepMessages;
            done.Reset();
        }
    }

    public class LongValueAttachment : IStateObjectAttachment
    {
        public long value;

        public int SerializedSize() => sizeof(long);

        public void SerializeTo(Span<byte> buffer)
        {
            BitConverter.TryWriteBytes(buffer, value);
        }

        public void RecoverFrom(ReadOnlySpan<byte> serialized)
        {
            unsafe
            {
                fixed (byte* b = serialized)
                    value = *(long*)b;
            }
        }
    }

    /// <summary>
    /// DARQ data structure 
    /// </summary>
    public class Darq : StateObject, IDisposable
    {
        internal DarqSettings settings;
        internal FasterLog log;
        internal ConcurrentDictionary<long, byte> incompleteMessages = new();
        private FasterLogSettings logSetting;
        
        private readonly DeduplicationVector dvc;
        private readonly LongValueAttachment incarnation, largestSteppedLsn;
        private WorkQueueLIFO<StepRequestHandle> stepQueue;
        private ThreadLocalObjectPool<StepRequestHandle> stepRequestPool;

        public long BytesWritten => bytesWritten;
        private long bytesWritten = 0, lastCommitted = 0;

        /// <summary>
        /// Initialize DARQ with the given identity and parameters
        /// </summary>
        /// <param name="me">unique identity for this DARQ</param>
        /// <param name="darqSettings">parameters for DARQ</param>
        public Darq(DarqSettings settings, IVersionScheme versionScheme) : base(versionScheme, new DprWorkerOptions
            {
                Me = settings.MyDpr == DprWorkerId.INVALID ? new DprWorkerId(settings.Me.guid) : settings.MyDpr,
                DprFinder = settings.DprFinder,
                CheckpointPeriodMilli = settings.CheckpointPeriodMilli,
                RefreshPeriodMilli = settings.RefreshPeriodMilli
            })
        {
            this.settings = settings;
            if (settings.LogDevice == null)
                throw new FasterException("Cannot initialize DARQ as no underlying device is specified. " +
                                          "Please supply DARQ with a device under DarqSettings.LogDevice");

            if (settings.LogCommitManager == null)
            {
                settings.LogCommitManager = new DeviceLogCommitCheckpointManager
                (new LocalStorageNamedDeviceFactory(),
                    new DefaultCheckpointNamingScheme(
                        settings.LogCommitDir ??
                        new FileInfo(settings.LogDevice.FileName).Directory.FullName));
            }
            
            if (this.settings.CleanStart)
            {
                settings.LogCommitManager.RemoveAllCommits();
            }

            logSetting = new FasterLogSettings
            {
                LogDevice = settings.LogDevice,
                PageSize = settings.PageSize,
                MemorySize = settings.MemorySize,
                SegmentSize = settings.SegmentSize,
                LogCommitManager = settings.LogCommitManager,
                LogCommitDir = settings.LogCommitDir,
                GetMemory = _ =>
                    throw new FasterException(
                        "DARQ should never do anything through a code path that needs to materialize into external mem buffer"),
                LogChecksum = settings.LogChecksum,
                MutableFraction = settings.MutableFraction,
                ReadOnlyMode = false,
                FastCommitMode = settings.FastCommitMode,
                RemoveOutdatedCommits = false,
                LogCommitPolicy = null,
                TryRecoverLatest = false,
                AutoRefreshSafeTailAddress = true,
                AutoCommit = false,
                TolerateDeviceFailure = false,
            };
            
            log = new FasterLog(logSetting);
            dvc = new DeduplicationVector();
            incarnation = new LongValueAttachment();
            largestSteppedLsn = new LongValueAttachment();
            AddAttachment(dvc);
            AddAttachment(incarnation);
            AddAttachment(largestSteppedLsn);

            stepQueue = new WorkQueueLIFO<StepRequestHandle>(StepSequential);
            stepRequestPool = new ThreadLocalObjectPool<StepRequestHandle>(() => new StepRequestHandle());
        }

        /// <summary>
        /// Return the tail address that this DARQ will need to replay to upon failure recovery
        /// </summary>
        public long ReplayEnd => largestSteppedLsn.value;

        public long Tail => log.TailAddress;

        /// <inheritdoc/>
        public override void Dispose()
        {
            if (settings.DeleteOnClose)
                settings.LogCommitManager.RemoveAllCommits();
            log.Dispose();
            settings.LogDevice.Dispose();
            settings.LogCommitManager.Dispose();
        }

        private void EnqueueCallbackBatch(IReadOnlySpanBatch m, int idx, long addr)
        {
            incompleteMessages.TryAdd(addr, 0);
        }
        
        private void EnqueueCallback<T>(T entry, long addr) where T : ILogEnqueueEntry
        {
            incompleteMessages.TryAdd(addr, 0);
        }
        

        /// <summary>
        /// Enqueue given entries into DARQ, optionally deduplicated using the supplied producer ID and sequence number. 
        /// </summary>
        /// <param name="entries">
        /// Entries to enqueue. must already be well-formed on a byte level with message types, etc.
        /// </param>
        /// <param name="producerId"> Unique id of the producer for deduplication, or -1 if not required</param>
        /// <param name="sequenceNum">
        /// sequence number for deduplication. DARQ will only accept enqueue requests with monotonically increasing
        /// sequence numbers from the same producer
        /// </param>
        /// <returns> whether enqueue is successful </returns>
        public bool Enqueue(IReadOnlySpanBatch entries, long producerId, long sequenceNum)
        {
            // Check that we are not executing duplicates and update dvc accordingly
            if (producerId != -1 && !dvc.Process(producerId, sequenceNum))
                return false;

            log.Enqueue(entries, EnqueueCallbackBatch);
            return true;
        }
        
        public bool Enqueue<T>(IEnumerable<T> entries, long producerId, long sequenceNum) where T : ILogEnqueueEntry
        {
            // Check that we are not executing duplicates and update dvc accordingly
            if (producerId != -1 && !dvc.Process(producerId, sequenceNum))
                return false;
            foreach (var e in entries)
                log.Enqueue(e, EnqueueCallback);
            return true;
        }
        
        private void StepCallback(IReadOnlySpanBatch ms, int idx, long addr)
        {
            var entry = ms.Get(idx);
            // Get first byte for type
            if ((DarqMessageType)entry[0] == DarqMessageType.RECOVERY ||
                (DarqMessageType)entry[0] == DarqMessageType.IN)
                incompleteMessages.TryAdd(addr, 0);

            largestSteppedLsn.value = addr;
        }

        private unsafe void StepSequential(StepRequestHandle stepRequestHandle)
        {
            // Maintain incarnation number
            if (stepRequestHandle.incarnation != incarnation.value)
            {
                stepRequestHandle.status = StepStatus.REINCARNATED;
                stepRequestHandle.done.Set();
                return;
            }

            Debug.Assert(incarnation.value == stepRequestHandle.incarnation);

            // Validation of input batch
            var numTotalEntries = stepRequestHandle.stepMessages.TotalEntries();
            // Validate if The last entry of the step is a completion record that steps some previous message
            var lastEntry = stepRequestHandle.stepMessages.Get(numTotalEntries - 1);
            fixed (byte* h = lastEntry)
            {
                var end = h + lastEntry.Length;
                var messageType = (DarqMessageType)(*h);
                if (messageType == DarqMessageType.COMPLETION)
                {
                    Debug.Assert(lastEntry.Length % sizeof(long) == 1);
                    for (var head = h + sizeof(DarqMessageType); head < end; head += sizeof(long))
                    {
                        var completedLsn = *(long*)head;
                        if (!incompleteMessages.TryRemove(completedLsn, out _))
                        {
                            // This means we are trying to step something twice. Roll back all previous steps before
                            // failing this step
                            for (var rollbackHead = h + sizeof(DarqMessageType);
                                 rollbackHead < head;
                                 rollbackHead += sizeof(long))
                                incompleteMessages.TryAdd(*(long*)rollbackHead, 0);
                            stepRequestHandle.status = StepStatus.INVALID;
                            stepRequestHandle.done.Set();
                            Console.WriteLine($"step failed on lsn {completedLsn}");
                            return;
                        }
                    }
                }
            }
            log.Enqueue(stepRequestHandle.stepMessages, StepCallback);
            stepRequestHandle.done.Set();
            stepRequestHandle.status = StepStatus.SUCCESS;
        }

        /// <summary>
        /// Step the DARQ with given incarnation number and step content
        /// </summary>
        /// <param name="incarnation"> incarnation number of the originating processor </param>
        /// <param name="stepMessages">
        /// Step content. must already be well-formed on a byte level with message
        /// types, etc. with the last entry being a completion record
        /// </param>
        /// <returns>step result</returns>
        public StepStatus Step(long incarnation, IReadOnlySpanBatch stepMessages)
        {
            var request = stepRequestPool.Checkout();
            request.Reset(incarnation, stepMessages);
            stepQueue.EnqueueAndTryWork(request, false);
            while (request.status == StepStatus.INCOMPLETE)
                request.done.Wait();
            var result = request.status;
            stepRequestPool.Return(request);
            return result;
        }

        /// <summary>
        /// Truncate DARQ until the given lsn
        /// </summary>
        /// <param name="lsn">truncation point</param>
        public void TruncateUntil(long lsn)
        {
            log.TruncateUntil(lsn);
        }

        /// <summary>
        /// Registers a new processor the submit steps to this DARQ.
        /// </summary>
        /// <returns>the unique incarnation number assigned to this processor</returns>
        public long RegisterNewProcessor()
        {
            return RegisterNewProcessorAsync().GetAwaiter().GetResult();
        }

        public Task<long> RegisterNewProcessorAsync()
        {
            var result = Interlocked.Increment(ref incarnation.value);
            // TODO(Tianyu): Must use some sort of epoch to force all threads to synchronize and recognize the new
            // incarnation? On the other hand, maybe the sequential processing of steps is sufficient guarantee...
            // ForceCheckpoint(spin: true);
            return Task.FromResult(result);
        }

        /// <summary>
        /// Scans the DARQ with an iterator 
        /// </summary>
        /// <returns></returns>
        public DarqScanIterator StartScan(bool speculative) => new(log, largestSteppedLsn.value, speculative);
        
        public DarqScanIterator StartBackgroundScan(bool speculative) => new(log, 0, speculative, false);
        

        public override void PerformCheckpoint(long version, ReadOnlySpan<byte> metadata, Action onPersist)
        {
            var commitCookie = metadata.ToArray();
            log.CommitStrongly(out var tail, out _, false, commitCookie, version, onPersist);            
            bytesWritten += tail - Math.Max(lastCommitted, log.BeginAddress);
            lastCommitted = tail;
        }

        public override void RestoreCheckpoint(long version, out ReadOnlySpan<byte> metadata)
        {
            Console.WriteLine($"Restoring checkpoint {version}");
            incompleteMessages.Clear();

            // TODO(Tianyu): can presumably be more efficient through some type of in-mem truncation here
            log = new FasterLog(logSetting);
            log.Recover(version);
            metadata = log.RecoveredCookie;

            Console.WriteLine($"Log recovered, now restoring in-memory DARQ data structures");
            // Scan the log on recovery to repopulate in-memory auxiliary data structures
            unsafe
            {
                using var it = log.Scan(0, long.MaxValue);
                while (it.UnsafeGetNext(out byte* entry, out var len, out var lsn, out _))
                {
                    switch ((DarqMessageType)(*entry))
                    {
                        case DarqMessageType.IN:
                        case DarqMessageType.RECOVERY:
                            incompleteMessages.TryAdd(lsn, 0);
                            break;
                        case DarqMessageType.COMPLETION:
                            var completed = (long*)(entry + sizeof(DarqMessageType));
                            while (completed < entry + len)
                                incompleteMessages.TryRemove(*completed++, out _);
                            break;
                        case DarqMessageType.OUT:
                            break;
                        default:
                            throw new NotImplementedException();
                    }

                    it.UnsafeRelease();
                }
            }

            Console.WriteLine($"Recovery Finished");
        }

        public override void PruneVersion(long version)
        {
            settings.LogCommitManager.RemoveCommit(version);
        }

        public override IEnumerable<Memory<byte>> GetUnprunedVersions()
        {
            var commits = settings.LogCommitManager.ListCommits().ToList();
            return commits.Select(commitNum =>
            {
                // TODO(Tianyu): hacky
                var newLog = new FasterLog(logSetting);
                newLog.Recover(commitNum);
                var commitCookie = newLog.RecoveredCookie;
                newLog.Dispose();
                return new Memory<byte>(commitCookie);
            });
        }
    }
}