using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using FASTER.core;

namespace FASTER.serverless
{
    public class ReadResult<Output>
    {
        public Status status;
        public Output output;
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="WorkerType"></typeparam>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    /// <typeparam name="Functions"></typeparam>
    public partial class FasterServerlessSession<Key, Value, Input, Output, Functions> : IDisposable
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        public Guid Id { get; }
        public FasterServerless<Key, Value, Input, Output, Functions> AttachedWorker { get; set; }

        private long serialNum;
        private long sessionVersion;
        private long sessionWorldLine = 0;

        // For statistics only
        private bool latencyMeasurements = false;
        public long[] opStartTick, opEndTick, opCommitTick;
        private Stopwatch stopwatch = new Stopwatch();
        
        // Sequence number, version
        // In the general case, where there are no pending operations and we have strict monotonicity in version
        // issuance. We track "segments" of versions, meaning that each entry in the sorted list corresponds to 
        // the start of a bunch of operations that operated in that version (until the next entry)
        private Queue<(long, long)> sequentialVersionList = new Queue<(long, long)>();
        // In the pending case, the version of an operation is not immediately known until the pending status
        // is cleared. These operations can resolve later at a higher version and violate monotonicity (although
        // not causality, later operations cannot depend on the result of pending operations).
        private readonly VersionExceptionList exceptionList = new VersionExceptionList();

        // On refresh, check with the attached worker for any changes in CPR view number. If present, trigger the
        // session to obtain a read snapshot of the cpr table and update commit point.
        private long lastSeenDprViewNumber = 0;
        private ClientSession<Key, Value, Input, Output, Empty, Functions> localSession;
        private CommitPoint currentCommitPoint;

        // TODO(Tianyu): No need for a queue any more?
        private Queue<int> currentPendingOps;
        internal ServerlessPendingContext<Key, Value, Input, Output>[] reusablePendingContexts;
        private LightRequestBatcher<Key, Value, Input, Output> batcher;
        private int maxBatchSize;
        private int nextFree = 0;

        // for optimization in the common case, so we don't access the predecessor set in the fast path
        private long previousOperationLocalVersion = -1;
        private readonly LightDependencySet predecessors = new LightDependencySet();

        // Turn off commit tracking if the system runs without checkpoints, otherwise the operation log can grow
        // unboundedly 
        private bool trackForCommit;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="version"></param>
        public FasterServerlessSession(
            FasterServerless<Key, Value, Input, Output, Functions> attachedWorker, long version, int pendingWindowSize = 4096, int maxBatchSize = 1024,
            bool trackForCommit = true, bool latencyMeasurements = false)
        {
            AttachedWorker = attachedWorker;
            Id = Guid.NewGuid();
            sessionVersion = version;
            currentCommitPoint = new CommitPoint {UntilSerialNo = -1, ExcludedSerialNos = new List<long>()};
            sessionWorldLine = AttachedWorker.workerWorldLine;
            this.trackForCommit = trackForCommit;
            reusablePendingContexts = new ServerlessPendingContext<Key, Value, Input, Output>[pendingWindowSize];
            currentPendingOps = new Queue<int>();
            batcher = new LightRequestBatcher<Key, Value, Input, Output>(attachedWorker.Me(), Id);
            this.maxBatchSize = maxBatchSize;
            if (maxBatchSize > ParsedMessageBatch<Key, Value, Input, Output>.MaxBatchSize) throw new FasterException("batch size too large");
            // TODO(Tianyu): Right now the system always assumes local operation and will eagerly create a local session
            localSession = AttachedWorker.GetLocalSession(Id, out _);
            // pin the session to local thread
            localSession.UnsafeResumeThread();

            this.latencyMeasurements = latencyMeasurements;
            if (latencyMeasurements)
            {
                opStartTick = new long[50000000];
                opEndTick = new long[50000000];
                opCommitTick = new long[50000000];
            }

            stopwatch.Start();
        }

        public void Dispose()
        {
            AttachedWorker.attachedSessions.TryRemove(Id, out _);
            AttachedWorker.cachedLocalSessions.TryRemove(Id, out _);
            localSession.Dispose();
            stopwatch.Stop();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetVersionNum()
        {
            return sessionVersion;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetWorldLine()
        {
            return sessionWorldLine;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref CommitPoint GetCommitPoint()
        {
            return ref currentCommitPoint;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Refresh()
        {
            try
            {
                // Refresh will throw an exception if the worker has rolled back due to a failure. Although the
                // worker could have already found out about the failure from a remote worker, we choose to use
                // this local refresh as the spring-off point for error handling always. This means that a worker
                // will refresh until the local worker also sees the failure if it saw a remote failure.
                localSession.Refresh();
                // compute a new commit point if dpr table has potential new information
                if (trackForCommit &&
                    Utility.MonotonicUpdate(ref lastSeenDprViewNumber, AttachedWorker.dprViewNumber, out _))
                {
                    var dprTable = AttachedWorker.DprManager.ReadSnapshot();
                    var localSafeVersion = dprTable.SafeVersion(AttachedWorker.Me());
                    
                    // This is the number we have to search up to for the pending operations
                    currentCommitPoint.UntilSerialNo = FindUntilSerialNum(localSafeVersion);
                    exceptionList.ResolveExceptions(dprTable, opCommitTick, stopwatch.ElapsedTicks);
                }
                
                // TODO(Tianyu): Maybe add a time-based check here to send stale batches?
            }
            catch (FasterRollbackException)
            {
                HandleLocalRollback();
            }
        }

        private int GetFreeContext()
        {
            if (currentPendingOps.Count == reusablePendingContexts.Length)
                throw new FasterException("Issuing too many pending requests");
            var result = nextFree;
            if (reusablePendingContexts[result] == null)
                reusablePendingContexts[result] = new ServerlessPendingContext<Key, Value, Input, Output>();
            ++nextFree;
            if (nextFree == reusablePendingContexts.Length) nextFree = 0;
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateDependenciesLocalOperation(long version)
        {
            if (predecessors.MaybeNotEmpty())
                AttachedWorker.ReportVersionDependencies(version, predecessors);

            // Later operations will implicitly depend on everything in the predecessor by transitivity. Can
            // truncate the predecessor set to just this local version
            predecessors.UnsafeClear();
            previousOperationLocalVersion = version;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ComputePendingOperationDependencies(
            ServerlessPendingContext<Key, Value, Input, Output> pendingContext,
            ClientRequestBatch<Key, Value, Input, Output> batch)
        {
            // if previous operation was executed locally, the worker version is not present in the dependencies set
            // as a performance optimization. Need to add this manually to the set of dependencies for a pending operation.
            // If the pending operation went local, the worker will simply ignore this entry when building the dependency
            // set for the worker
            if (previousOperationLocalVersion != -1)
                batch.dependencySet.Update(AttachedWorker.MessageManager.Me(), previousOperationLocalVersion);

            if (!predecessors.MaybeNotEmpty()) return;
            
            for (var i = 0; i < predecessors.DependentVersions.Length; i++)
            {
                var entry = predecessors.DependentVersions[i];
                if (entry == LightDependencySet.NoDependency) continue;
                batch.dependencySet.Update(new Worker(i), entry);
            }
        }

        public Status Read(ref Key key, ref Input input, ReadResult<Output> readResult, out long opSerialNum, long suppliedSerialNum = -1)
        {
            opSerialNum = suppliedSerialNum == -1 ? serialNum++ : suppliedSerialNum;
            if (suppliedSerialNum == -1 && latencyMeasurements)
                opStartTick[opSerialNum] = stopwatch.ElapsedTicks;
            var bucket = AttachedWorker.BucketingScheme.GetBucket(key);
            // Quickly try to validate local ownership using the session for concurrent read into local metadata store
            var isLocalKey = AttachedWorker.MetadataStore.ValidateLocalOwnership(bucket, localSession);

            // If the key validates as a locally owned key, it will stay so until we next refresh due to epoch
            // protection. Before execution, must make sure that the worker is at least at the version of the session.
            if (isLocalKey && localSession.Version() >= sessionVersion)
            {
                // Update session version if the local session has moved onto a new version due to checkpoints.
                if (Utility.MonotonicUpdate(ref sessionVersion, localSession.Version(), out _))
                    sequentialVersionList.Enqueue(ValueTuple.Create(opSerialNum, localSession.Version()));
                UpdateDependenciesLocalOperation(localSession.Version());

                readResult.status =
                    localSession.Read(ref key, ref input, ref readResult.output, Empty.Default, opSerialNum);

                // TODO(Tianyu): This is buggy when ownership is changing. The local pending operation may execute
                // in a different, future version when the key is no longer owned by the local worker. We cannot
                // easily fix this because we cannot peek into the pending operations data structure of the local
                // FASTER. One fix is to add a locally pending operation to the list of outstanding operation here
                // as well and remember to re-validate ownership when they are cleared, but it's unclear how to 
                // do so at a per-operation granularity since local FASTER does not provide that.
                if (latencyMeasurements)
                    opEndTick[opSerialNum] = stopwatch.ElapsedTicks;
                return readResult.status;
            }

            // Otherwise, the operation cannot be executed immediately, and we need to construct an object that
            // represents the request, to be either executed remotely or stored for later execution.
            var index = GetFreeContext();
            var pendingContext = reusablePendingContexts[index];
            pendingContext.Reinitialize(bucket, readResult);
            pendingContext.op.InitializeUpsertRequest(opSerialNum, GetVersionNum(), sessionWorldLine, index, key, input);
            HandlePendingOperation(index, pendingContext, isLocalKey);

            return Status.PENDING;
        }

        public Status Upsert(ref Key key, ref Value desiredValue, out long opSerialNum, long suppliedSerialNum = -1)
        {
            opSerialNum = suppliedSerialNum == -1 ? serialNum++ : suppliedSerialNum;
            if (suppliedSerialNum == -1 && latencyMeasurements)
                opStartTick[opSerialNum] = stopwatch.ElapsedTicks;
            var bucket = AttachedWorker.BucketingScheme.GetBucket(key);
            var isLocalKey = AttachedWorker.MetadataStore.ValidateLocalOwnership(bucket, localSession);

            if (isLocalKey && localSession.Version() >= sessionVersion)
            {
                if (Utility.MonotonicUpdate(ref sessionVersion, localSession.Version(), out _))
                    sequentialVersionList.Enqueue(ValueTuple.Create(opSerialNum, localSession.Version()));
                UpdateDependenciesLocalOperation(localSession.Version());
                var result = localSession.Upsert(ref key, ref desiredValue, Empty.Default, opSerialNum);
                if (latencyMeasurements)
                    opEndTick[opSerialNum] = stopwatch.ElapsedTicks;
                return result;
            }

            var index = GetFreeContext();
            var pendingContext = reusablePendingContexts[index];
            pendingContext.Reinitialize(bucket);
            pendingContext.op.InitializeUpsertRequest(opSerialNum, GetVersionNum(), sessionWorldLine, index, key, desiredValue);
            HandlePendingOperation(index, pendingContext, isLocalKey);
            
            return Status.PENDING;
        }

        public Status RMW(ref Key key, ref Input input, out long opSerialNum,
            long suppliedSerialNum = -1)
        {
            opSerialNum = suppliedSerialNum == -1 ? serialNum++ : suppliedSerialNum;
            if (suppliedSerialNum == -1 && latencyMeasurements)
                opStartTick[opSerialNum] = stopwatch.ElapsedTicks;
            var bucket = AttachedWorker.BucketingScheme.GetBucket(key);
            var isLocalKey = AttachedWorker.MetadataStore.ValidateLocalOwnership(bucket, localSession);

            if (isLocalKey && localSession.Version() >= sessionVersion)
            {
                if (Utility.MonotonicUpdate(ref sessionVersion, localSession.Version(), out _))
                    sequentialVersionList.Enqueue(ValueTuple.Create(opSerialNum, localSession.Version()));
                UpdateDependenciesLocalOperation(localSession.Version());
                var result = localSession.RMW(ref key, ref input, Empty.Default, opSerialNum);
                if (latencyMeasurements)
                    opEndTick[opSerialNum] = stopwatch.ElapsedTicks;
                return result;
            }
            
            var index = GetFreeContext();
            var pendingContext = reusablePendingContexts[index];            
            pendingContext.Reinitialize(bucket);
            pendingContext.op.InitializeRmwRequest(opSerialNum, GetVersionNum(), sessionWorldLine, index, key, input);
            HandlePendingOperation(index, pendingContext, isLocalKey);

            return Status.PENDING;
        }

        public Status Delete(ref Key key, out long opSerialNum, long suppliedSerialNum = -1)
        {
            opSerialNum = suppliedSerialNum == -1 ? serialNum++ : suppliedSerialNum;
            if (suppliedSerialNum == -1 && latencyMeasurements)
                opStartTick[opSerialNum] = stopwatch.ElapsedTicks;
            var bucket = AttachedWorker.BucketingScheme.GetBucket(key);
            var isLocalKey = AttachedWorker.MetadataStore.ValidateLocalOwnership(bucket, localSession);

            if (isLocalKey && localSession.Version() >= sessionVersion)
            {
                if (Utility.MonotonicUpdate(ref sessionVersion, localSession.Version(), out _))
                    sequentialVersionList.Enqueue(ValueTuple.Create(opSerialNum, localSession.Version()));
                UpdateDependenciesLocalOperation(localSession.Version());
                var result = localSession.Delete(ref key, Empty.Default, opSerialNum);
                if (latencyMeasurements)
                    opEndTick[opSerialNum] = stopwatch.ElapsedTicks;
                return result;
            }

            var index = GetFreeContext();
            var pendingContext = reusablePendingContexts[index];
            pendingContext.Reinitialize(bucket);
            pendingContext.op.InitializeDeleteRequst(opSerialNum, GetVersionNum(), sessionWorldLine, index, key);
            HandlePendingOperation(index, pendingContext, isLocalKey);
            return Status.PENDING;
        }
        
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void HandlePendingOperation(int index, ServerlessPendingContext<Key, Value, Input, Output> pendingContext, bool localKey)
        {
            currentPendingOps.Enqueue(index);
            if (localKey)
            {
                // If so, the operation went pending because we cannot immediately execute it due to version problems.
                // Mark it to be retried later.
                pendingContext.reissue = true;
                pendingContext.completion.Set();
                AttachedWorker.InitiateVersionBump(sessionVersion);
                return;
            }

            var cachedOwner = AttachedWorker.MetadataStore.CachedOwner(pendingContext.bucket);
            if (!cachedOwner.Equals(Worker.INVALID))
            {
                Debug.Assert(!cachedOwner.Equals(AttachedWorker.Me()));
                pendingContext.workerVersion.Worker = cachedOwner;
                var batch = batcher.Submit(cachedOwner, pendingContext);
                ComputePendingOperationDependencies(pendingContext, batch);
                if (batch.Size() >= maxBatchSize)
                {
                    AttachedWorker.MessageManager.Send(this, batch, AttachedWorker.serializer);
                    batch.Clear();
                }
            }
            else
            {
                LookupOwnerInBackground(pendingContext);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        // Take a pending operation and execute it locally, which will re-validate key ownership and check for version
        private void LocalExecution(ServerlessPendingContext<Key, Value, Input, Output> pendingContext)
        {
            var originalCount = currentPendingOps.Count;
            switch (pendingContext.op.header.type)
            {
                case FasterServerlessMessageType.ReadRequest:
                    Read(ref pendingContext.op.key, ref pendingContext.op.input, pendingContext.readResult, out _,
                        pendingContext.op.header.serialNum);
                    break;
                case FasterServerlessMessageType.UpsertRequest:
                    Upsert(ref pendingContext.op.key, ref pendingContext.op.value, out _, pendingContext.op.header.serialNum);
                    break;
                case FasterServerlessMessageType.RmwRequest:
                    RMW(ref pendingContext.op.key, ref pendingContext.op.input, out _, pendingContext.op.header.serialNum);
                    break;
                case FasterServerlessMessageType.DeleteRequest:
                    Delete(ref pendingContext.op.key, out _, pendingContext.op.header.serialNum);
                    break;
                default:
                    throw new FasterException();
            }

            if (currentPendingOps.Count == originalCount)
            {
                // Operation was executed locally. Can update information accordingly. This version information can
                // potentially be wrong for local pending operations, as a pending operation can be executed in a future
                // version, but because the local sessions state machine maintains that, we are ok.
                pendingContext.workerVersion.Worker = AttachedWorker.MessageManager.Me();
                pendingContext.workerVersion.Version = localSession.Version();
            }
        }
    }
}