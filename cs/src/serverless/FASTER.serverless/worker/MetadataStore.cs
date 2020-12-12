using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FASTER.serverless
{
    public class MetadataStore
    {
        private readonly IOwnershipMapping ownershipMapping;
        private readonly ServerfulMessageManager messageManager;
        // TODO(Tianyu): Should probably eventually migrate some of the DprManager functionality into this class

        private ConcurrentDictionary<long, bool> stableLocalKeys, liveLocalKeys;
        private long liveVersionNum;
        // TODO(Tianyu): Add ownership change back in
        // private List<(string, Worker)> bucketsToRemove, backlog;

        // TODO(Tianyu): Maybe limit size of this cache.
        // TODO(Tianyu): Add prefetch?
        private readonly ConcurrentDictionary<long, Worker> cachedRemoteKeys;
        private readonly ConcurrentDictionary<long, TaskCompletionSource<object>> outstandingDropRequests;
        
        public MetadataStore(IOwnershipMapping ownershipMapping, ServerfulMessageManager messageManager)
        {
            this.ownershipMapping = ownershipMapping;
            this.messageManager = messageManager;
            stableLocalKeys = new ConcurrentDictionary<long, bool>();
            liveLocalKeys = new ConcurrentDictionary<long, bool>();
            liveVersionNum = long.MaxValue;
            cachedRemoteKeys = new ConcurrentDictionary<long, Worker>();
            
            // TODO(Tianyu): Add ownership change back in
            // bucketsToRemove = new List<(string, Worker)>();
            // backlog = new List<(string, Worker)>();

            outstandingDropRequests = new ConcurrentDictionary<long, TaskCompletionSource<object>>();
        }

        public bool ValidateLocalOwnership<Key, Value, Input, Output, Functions>(
            long bucket,
            ClientSession<Key, Value, Input, Output, Empty, Functions> session)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Empty>
        {
            var localKeys = session.Version() < liveVersionNum ? stableLocalKeys : liveLocalKeys;
            return localKeys.ContainsKey(bucket);
        }
        
        public void InvalidateCachedEntry(long bucket)
        {
            cachedRemoteKeys.TryRemove(bucket, out _);
        }
        
        public Worker CachedOwner(long bucket)
        {
            return cachedRemoteKeys.TryGetValue(bucket, out var result) ? result : Worker.INVALID;
        }

        public async ValueTask<Worker> LookupAsync(long bucket)
        {
            if (!cachedRemoteKeys.TryGetValue(bucket, out var result))
            {
                result = await ownershipMapping.LookupAsync(bucket);
                if (result.Equals(messageManager.Me()))
                {
                    liveLocalKeys.AddOrUpdate(bucket, true, (b, o) => true);
                    stableLocalKeys.AddOrUpdate(bucket, true, (b, o) => true);
                    InvalidateCachedEntry(bucket);
                }
                else
                {
                    cachedRemoteKeys.AddOrUpdate(bucket, result, (b, o) => result);
                }
            }

            return result;
        }

        public async ValueTask<Worker> ObtainOwnershipAsync(long bucket, Worker expectedOwner)
        {
            var owner = await ownershipMapping.ObtainOwnershipAsync(bucket, messageManager.Me(), expectedOwner);
            if (!owner.Equals(messageManager.Me()))
            {
                cachedRemoteKeys.AddOrUpdate(bucket, owner, (b, o) => owner);
            }
            else
            {
                // No need to wait for other worker to give up ownership if nobody owns it
                liveLocalKeys.AddOrUpdate(bucket, true, (b, o) => true);
                stableLocalKeys.AddOrUpdate(bucket, true, (b, o) => true);
                InvalidateCachedEntry(bucket);
            }
            
            return owner;
        }
        
        // TODO(Tianyu): Add ownership change back in
        // public void RenounceOwnership(string bucket, Worker nextOwner)
        // {
        //     lock (bucketsToRemove)
        //     {
        //         bucketsToRemove.Add(ValueTuple.Create(bucket, nextOwner));
        //     }
        // }
        // public void OnCheckpointVersionChange(long nextVersion)
        // {
        //     lock (bucketsToRemove)
        //     {
        //         lock (backlog)
        //         {
        //             liveVersionNum = nextVersion;
        //             // So there cannot be the following interleaving:
        //             //    worker A                  checkpointing
        //             // read bucketsToRemove
        //             //                                  swap
        //             //                                  apply  
        //             // add to bucketsToRemove <-- missed
        //             backlog = Interlocked.Exchange(ref bucketsToRemove, backlog);
        //         }
        //     }
        //
        //     // No concurrent threads can be adding to the list due to CPR state machine
        //     foreach (var (bucket, _) in backlog)
        //         // Safe because the state machine guarantees no concurrent read into metadata store
        //         liveLocalKeys.TryRemove(bucket, out _);
        // }
        // public void OnCheckpointFinish()
        // {
        //     // No concurrent threads can be adding to the list due to CPR state machine
        //     foreach (var (bucket, _) in backlog)
        //         // Safe because the state machine guarantees no concurrent read into metadata store
        //         stableLocalKeys.TryRemove(bucket, out _);
        //     Task.Run(() =>
        //     {
        //         lock (backlog)
        //         {
        //             // Stop others from swapping and adding into the queue as we send responses 
        //             foreach (var (bucket, nextOwner) in backlog)
        //             {
        //                 if (nextOwner.Equals(Worker.INVALID)) continue;
        //                 messageManager.Send(
        //                     new FasterServerlessOwnershipDroppedMessage(messageManager.Me(), nextOwner,
        //                         bucket));
        //             }
        //
        //             backlog.Clear();
        //         }
        //     });
        //     // Lack of atomicity here is fine because at this point they should all have the same keys removed
        //     liveLocalKeys = Interlocked.Exchange(ref stableLocalKeys, liveLocalKeys);
        //     // New threads will switch to look at stable after this point
        //     liveVersionNum = long.MaxValue;
        // }
        //
        // public void CompleteDropTask(string bucket)
        // {
        //     if (outstandingDropRequests.TryRemove(bucket, out var source))
        //     {
        //         source.SetResult(null);
        //     }
        //
        //     throw new Exception();
        // }
        // public async ValueTask<Worker> TransferOwnershipAsync(string bucket, Worker from)
        // {
            
            // var owner = await ownershipMapping.ObtainOwnershipAsync(bucket, messageManager.Me(), from);
            // if (!owner.Equals(messageManager.Me())) return owner;
            //
            // var dropRequest = new FasterServerlessTransferOwnershipMessage(messageManager.Me(), from, bucket);
            // var dropTask = new TaskCompletionSource<object>();
            // outstandingDropRequests.TryAdd(bucket, dropTask);
            // messageManager.Send(dropRequest);
            // await dropTask.Task;
            //
            // liveLocalKeys.AddOrUpdate(bucket, true, (b, o) => true);
            // stableLocalKeys.AddOrUpdate(bucket, true, (b, o) => true);
            // InvalidateCachedEntry(bucket);
            // return owner;
        // }
    }
}