using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using FASTER.common;

namespace FASTER.libdpr
{
    public abstract class PrecomputedSyncResponseBase
    {
        public ReaderWriterLockSlim rwLatch = new ReaderWriterLockSlim();

        public abstract void ResetClusterState(ClusterState clusterState);

        public abstract void UpdateCut(Dictionary<DprWorkerId, long> newCut);
    }

    /// <summary>
    ///     Persistent state about the DPR cluster
    /// </summary>
    public class ClusterState
    {
        /// <summary>
        ///     Latest recorded world-line of the cluster
        /// </summary>
        public long currentWorldLine;

        /// <summary>
        ///     common prefix of the current world-line with previous ones. In other words, this is the cut workers had
        ///     to recover to before entering the current world-line. This also serves as point of truth of cluster
        ///     membership, and a worker is recognized as part of the cluster iff they have an entry in this dictionary.
        ///     Workers that did not participate in the previous world-lines have 0 in the cut.
        /// </summary>
        public Dictionary<DprWorkerId, long> worldLinePrefix;

        /// <summary>
        ///     Creates a new ClusterState object for an empty cluster
        /// </summary>
        public ClusterState()
        {
            currentWorldLine = 1;
            worldLinePrefix = new Dictionary<DprWorkerId, long>();
        }

        /// <summary>
        ///     Creates a ClusterState from serialized bytes
        /// </summary>
        /// <param name="buf"> byte buffer that holds serialized cluster state</param>
        /// <param name="offset"> offset to start scanning </param>
        /// <returns> end of the serialized ClusterState on the buffer</returns>
        public int PopulateFromBuffer(byte[] buf, int offset)
        {
            worldLinePrefix.Clear();
            currentWorldLine = BitConverter.ToInt64(buf, offset);
            return RespUtil.ReadDictionaryFromBytes(buf, offset + sizeof(long), worldLinePrefix);
        }

        public byte[] SerializeToBytes()
        {
            // Reserve space for world-line + prefix as a minimum
            var result = new byte[sizeof(long) + RespUtil.DictionarySerializedSize(worldLinePrefix)];
            BitConverter.TryWriteBytes(new Span<byte>(result, 0, sizeof(long)), currentWorldLine);
            RespUtil.SerializeDictionary(worldLinePrefix, result, sizeof(long));
            return result;
        }
    }

    /// <summary>
    ///     Backend logic for the RespGraphDprFinderServer.
    ///     The implementation relies on state objects to persist dependencies and avoids incurring additional storage
    ///     round-trips on the commit critical path.
    /// </summary>
    public class GraphDprFinderBackend
    {
        // Used to send add/delete worker requests to processing thread
        private readonly ConcurrentQueue<(DprWorkerId, Action<(long, long)>)> addQueue =
            new ConcurrentQueue<(DprWorkerId, Action<(long, long)>)>();

        private readonly ConcurrentQueue<(DprWorkerId, Action)> deleteQueue =
            new ConcurrentQueue<(DprWorkerId, Action)>();

        private readonly ConcurrentDictionary<WorkerVersion, List<WorkerVersion>> precedenceGraph =
            new ConcurrentDictionary<WorkerVersion, List<WorkerVersion>>();

        private readonly SimpleObjectPool<List<WorkerVersion>> objectPool =
            new SimpleObjectPool<List<WorkerVersion>>(() => new List<WorkerVersion>());

        private readonly PingPongDevice persistentStorage;

        private readonly ReaderWriterLockSlim clusterChangeLatch = new ReaderWriterLockSlim();
        private readonly Dictionary<DprWorkerId, long> currentCut = new Dictionary<DprWorkerId, long>();
        private readonly ClusterState volatileClusterState;

        private bool cutChanged;
        private readonly Queue<WorkerVersion> frontier = new Queue<WorkerVersion>();
        private readonly ConcurrentQueue<WorkerVersion> outstandingWvs = new ConcurrentQueue<WorkerVersion>();
        private readonly HashSet<WorkerVersion> visited = new HashSet<WorkerVersion>();

        // Only used during DprFinder recovery
        private readonly RecoveryState recoveryState;


        private List<PrecomputedSyncResponseBase> precomputedResponses;


        /// <summary>
        ///     Create a new EnhancedDprFinderBackend backed by the given storage. If the storage holds a valid persisted
        ///     EnhancedDprFinderBackend state, the constructor will attempt to recover from it.
        /// </summary>
        /// <param name="persistentStorage"> persistent storage backing this dpr finder </param>
        // TODO(Tianyu): Change to explicitly depend on a log instead of blob storage?
        public GraphDprFinderBackend(PingPongDevice persistentStorage)
        {
            this.persistentStorage = persistentStorage;
            // see if a previously persisted state is available
            var buf = persistentStorage.ReadLatestCompleteWrite();
            volatileClusterState = new ClusterState();
            if (buf != null)
                volatileClusterState.PopulateFromBuffer(buf, 0);

            foreach (var worker in volatileClusterState.worldLinePrefix.Keys)
                currentCut.Add(worker, 0);
            recoveryState = new RecoveryState(this);
            precomputedResponses = new List<PrecomputedSyncResponseBase>();
        }

        // Note: Only safe to call when starting the backend before processing starts.
        public void AddResponseObjectToPrecompute(PrecomputedSyncResponseBase obj)
        {
            obj.ResetClusterState(volatileClusterState);
            precomputedResponses.Add(obj);
        }


        // Try to commit a single worker version by chasing through its dependencies
        // The worker version supplied in the argument must already be reported as persistent
        private bool TryCommitWorkerVersion(WorkerVersion wv)
        {
            // Because wv is already persistent, if it's not in the graph that means it was pruned as part of a commit.
            // Ok to return as committed, but no need to mark the cut as changed
            if (!precedenceGraph.ContainsKey(wv)) return true;


            // If version is in the graph but somehow already committed, remove it and reclaim associated resources
            if (wv.Version <= currentCut.GetValueOrDefault(wv.DprWorkerId, 0))
            {
                // already committed. Remove but do not signal changes to the cut
                if (precedenceGraph.TryRemove(wv, out var list))
                    objectPool.Return(list);
                return true;
            }

            // Prepare traversal data structures
            visited.Clear();
            frontier.Clear();
            frontier.Enqueue(wv);

            // Breadth first search to find all dependencies
            while (frontier.Count != 0)
            {
                var node = frontier.Dequeue();
                if (visited.Contains(node)) continue;
                // If node is committed as determined by the cut, ok to continue
                if (currentCut.GetValueOrDefault(node.DprWorkerId, 0) >= node.Version) continue;
                // Otherwise, need to check if it is persistent (and therefore present in the graph)
                if (!precedenceGraph.TryGetValue(node, out var val)) return false;

                visited.Add(node);
                foreach (var dep in val)
                {
                    // No need to add self-dependencies
                    if (dep.DprWorkerId != node.DprWorkerId)
                        frontier.Enqueue(dep);
                }
            }

            // If all dependencies are present, we should commit them all
            // This will appear atomic without special protection as we serialize out the cut for sync calls in
            // the same thread later on. Other calls reading the cut involve cluster changes and cannot
            // interleave with this code
            foreach (var committed in visited)
            {
                // Mark cut as changed so we know to serialize the new cut later on
                cutChanged = true;
                var version = currentCut.GetValueOrDefault(committed.DprWorkerId, 0);
                // Update cut if necessary
                if (version < committed.Version)
                    currentCut[committed.DprWorkerId] = committed.Version;
                if (precedenceGraph.TryRemove(committed, out var list))
                    objectPool.Return(list);
            }

            return true;
        }

        /// <summary>
        ///     Performs work to evolve cluster state and find DPR cuts. Must be called repeatedly for the DprFinder to
        ///     make progress. Process() should only be invoked sequentially, but may be concurrent with other public methods.
        /// </summary>
        public void Process()
        {
            // Unable to make commit progress until we rebuild precedence graph from worker's persistent storage
            if (!recoveryState.RecoveryComplete()) return;

            // Process any cluster change requests
            if (!addQueue.IsEmpty || !deleteQueue.IsEmpty)
            {
                // Because this code-path is rare, ok to allocate new data structures here
                var callbacks = new List<Action>();

                // Need to grab an exclusive lock to ensure that if a rollback is triggered, there are no concurrent
                // NewCheckpoint calls that can pollute the graph with rolled back versions
                clusterChangeLatch.EnterWriteLock();

                // Process cluster change requests
                while (addQueue.TryDequeue(out var entry))
                {
                    var result = ProcessAddWorker(entry.Item1);
                    callbacks.Add(() => entry.Item2?.Invoke(result));
                }

                while (deleteQueue.TryDequeue(out var entry))
                {
                    ProcessDeleteWorker(entry.Item1);
                    callbacks.Add(() => entry.Item2());
                }

                // serialize new cluster state and persist
                var newState = volatileClusterState.SerializeToBytes();
                persistentStorage.WriteReliably(newState, 0, newState.Length);
                foreach (var response in precomputedResponses)
                {
                    response.ResetClusterState(volatileClusterState);
                    response.UpdateCut(currentCut);
                }

                clusterChangeLatch.ExitWriteLock();

                foreach (var callback in callbacks)
                    callback();
            }

            // Traverse the graph to try and commit versions
            TryCommitVersions();
        }

        private void TryCommitVersions(bool tryCommitAll = false)
        {
            // Perform graph traversal in mutual exclusion with cluster change, but it is ok for traversal to be 
            // concurrent with adding of new versions to the graph. 
            clusterChangeLatch.EnterReadLock();

            // Go through the unprocessed wvs and traverse the graph, unless instructed otherwise, give up after a while
            // to return control to the calling thread 
            var threshold = tryCommitAll ? outstandingWvs.Count : 100;
            for (var i = 0; i < threshold; i++)
            {
                if (!outstandingWvs.TryDequeue(out var wv)) break;
                if (!TryCommitWorkerVersion(wv))
                    outstandingWvs.Enqueue(wv);
            }

            if (cutChanged)
            {
                // Compute a new syncResponse for consumption
                // No need to protect against concurrent changes to the cluster because this method is either called
                // on the process thread or during recovery. No cluster change can interleave.
                // TODO(Tianyu): Maybe call latches here instead of inside UpdateCut method
                foreach (var response in precomputedResponses)
                {
                    response.UpdateCut(currentCut);
                }

                cutChanged = false;
            }

            clusterChangeLatch.ExitReadLock();
        }

        /// <summary>
        ///     Adds a new checkpoint to the precedence graph with the given dependencies
        /// </summary>
        /// <param name="worldLine"> world-line of the checkpoint </param>
        /// <param name="wv"> worker version checkpointed </param>
        /// <param name="deps"> dependencies of the checkpoint </param>
        public void NewCheckpoint(long worldLine, WorkerVersion wv, IEnumerable<WorkerVersion> deps)
        {
            // The DprFinder should be the most up-to-date w.r.t. world-lines and we should not ever receive
            // a request from the future. 
            Debug.Assert(worldLine <= volatileClusterState.currentWorldLine);
            Debug.Assert(currentCut.ContainsKey(wv.DprWorkerId));
            try
            {
                // Cannot interleave NewCheckpoint calls with cluster changes --- cluster changes may change the 
                // current world-line and remove worker-versions. A concurrent NewCheckpoint may not see that
                // and enter a worker-version that should have been removed after cluster change has finished. 
                clusterChangeLatch.EnterReadLock();
                // Unless the reported versions are in the current world-line (or belong to the common prefix), we should
                // not allow this write to go through
                if (worldLine != volatileClusterState.currentWorldLine
                    && wv.Version > volatileClusterState.worldLinePrefix[wv.DprWorkerId]) return;

                // This may be a duplicate
                if (currentCut[wv.DprWorkerId] >= wv.Version) return;

                var list = objectPool.Checkout();
                list.Clear();
                list.AddRange(deps);
                if (!precedenceGraph.TryAdd(wv, list))
                    objectPool.Return(list);
                else
                    outstandingWvs.Enqueue(wv);
            }
            finally
            {
                clusterChangeLatch.ExitReadLock();
            }
        }

        public void MarkWorkerAccountedFor(DprWorkerId dprWorkerId)
        {
            // Should only be invoked if recovery is underway. However, a new worker may send a blind graph resend. 
            if (recoveryState.RecoveryComplete()) return;
            // Lock here because can be accessed from multiple threads. No need to lock once all workers are accounted
            // for as then only the graph traversal thread will update current cut
            lock (currentCut)
            {
                Debug.Assert(currentCut.ContainsKey(dprWorkerId));
            }

            recoveryState.MarkWorkerAccountedFor(dprWorkerId);
        }

        /// <summary>
        ///     Add the worker to the cluster. If the worker is already part of the cluster the DprFinder considers that
        ///     a failure recovery and triggers necessary next steps. Given callback is invoked when the effect of this
        ///     call is recoverable on storage
        /// </summary>
        /// <param name="dprWorkerId"> worker to add to the cluster </param>
        /// <param name="callback"> callback to invoke when worker addition is persistent </param>
        public void AddWorker(DprWorkerId dprWorkerId, Action<(long, long)> callback = null)
        {
            addQueue.Enqueue(ValueTuple.Create(dprWorkerId, callback));
        }

        private (long, long) ProcessAddWorker(DprWorkerId dprWorkerId)
        {
            // Before adding a worker, make sure all workers have already reported all (if any) locally outstanding 
            // checkpoints. We require this to be able to process the request.
            if (!recoveryState.RecoveryComplete()) throw new InvalidOperationException();
            (long, long) result;
            if (volatileClusterState.worldLinePrefix.TryAdd(dprWorkerId, 0))
            {
                // First time we have seen this worker --- start them at current world-line
                result = (volatileClusterState.currentWorldLine, 0);
                currentCut.Add(dprWorkerId, 0);
                cutChanged = true;
            }
            else
            {
                // Otherwise, this worker thinks it's booting up for a second time, which means there was a restart.
                // We count this as a failure. Advance the cluster world-line
                volatileClusterState.currentWorldLine++;
                // TODO(Tianyu): This is slightly more aggressive than needed, but no worse than original DPR. Can
                // implement more precise rollback later.
                foreach (var entry in currentCut)
                    volatileClusterState.worldLinePrefix[entry.Key] = entry.Value;
                // Anything in the precedence graph is rolled back and we can just remove them 
                foreach (var list in precedenceGraph.Values)
                    objectPool.Return(list);
                precedenceGraph.Clear();
                outstandingWvs.Clear();
                var survivingVersion = currentCut[dprWorkerId];
                result = (volatileClusterState.currentWorldLine, survivingVersion);
            }

            return result;
        }

        /// <summary>
        ///     Delete a worker from the cluster. It is up to the caller to ensure that the worker is not participating in
        ///     any future operations or have outstanding unpersisted operations others may depend on. Until callback is
        ///     invoked, the worker is still considered part of the system and must be recovered if it crashes.
        /// </summary>
        /// <param name="dprWorkerId"> the worker to delete from the cluster </param>
        /// <param name="callback"> callback to invoke when worker removal is persistent on storage </param>
        public void DeleteWorker(DprWorkerId dprWorkerId, Action callback = null)
        {
            deleteQueue.Enqueue(ValueTuple.Create(dprWorkerId, callback));
        }

        private void ProcessDeleteWorker(DprWorkerId dprWorkerId)
        {
            // Before adding a worker, make sure all workers have already reported all (if any) locally outstanding 
            // checkpoints. We require this to be able to process the request.
            if (!recoveryState.RecoveryComplete()) throw new InvalidOperationException();

            currentCut.Remove(dprWorkerId);
            cutChanged = true;
            volatileClusterState.worldLinePrefix.Remove(dprWorkerId);
            volatileClusterState.worldLinePrefix.Remove(dprWorkerId);
        }

        // Recovery state is the information the backend needs to keep when restarting (presumably because
        // of failure) from previous on-disk state.
        private class RecoveryState
        {
            private readonly GraphDprFinderBackend backend;
            private readonly CountdownEvent countdown;
            private bool recoveryComplete;

            private readonly ConcurrentDictionary<DprWorkerId, byte> workersUnaccontedFor =
                new ConcurrentDictionary<DprWorkerId, byte>();

            internal RecoveryState(GraphDprFinderBackend backend)
            {
                this.backend = backend;
                // Check if the cluster is empty
                if (backend.volatileClusterState.worldLinePrefix.Count == 0)
                {
                    // If so, we do not need to recover anything, simply mark
                    // this backend as fully recovered for future operations
                    recoveryComplete = true;
                    return;
                }

                // Otherwise, we need to first rebuild an in-memory precedence graph from information persisted
                // at each state object. 
                recoveryComplete = false;
                // Mark all previously known worker as unaccounted for --- we cannot make any statements about the
                // current state of the cluster until we are sure we have up-to-date information from all of them
                foreach (var w in backend.volatileClusterState.worldLinePrefix.Keys)
                    workersUnaccontedFor.TryAdd(w, 0);
                countdown = new CountdownEvent(workersUnaccontedFor.Count);
            }

            internal bool RecoveryComplete()
            {
                return recoveryComplete;
            }

            // Called when the backend has received all precedence graph information from a worker
            internal void MarkWorkerAccountedFor(DprWorkerId dprWorkerId)
            {
                // A worker may repeatedly check-in due to crashes or other reason, we need to make sure each
                // worker decrements the count exactly once
                if (!workersUnaccontedFor.TryRemove(dprWorkerId, out _)) return;

                if (!countdown.Signal()) return;
                // At this point, we have all information that is at least as up-to-date as when we crashed. We can
                // traverse the graph and be sure to reach a conclusion that's at least as up-to-date as the guarantees
                // we may have given out before we crashed.
                backend.cutChanged = true;
                backend.TryCommitVersions(true);

                // Only mark recovery complete after we have reached that conclusion
                recoveryComplete = true;
            }
        }
    }
}