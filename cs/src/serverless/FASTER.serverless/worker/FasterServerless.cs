﻿using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using Nito.AsyncEx;

namespace FASTER.serverless
{
    /* Server-management API */ 
    public partial class FasterServerless<Key, Value, Input, Output, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        /* System components */
        internal readonly bool clientOnly;
        internal readonly MetadataStore MetadataStore;
        internal readonly ServerfulMessageManager MessageManager;
        internal readonly IDprManager DprManager;
        // This value is changed when the DprManager refreshes, and used to prompt sessions to update their local
        // commit points 
        internal long dprViewNumber = 0;
        internal readonly IBucketingScheme<Key> BucketingScheme;
        // The underlying faster instance for this worker
        // TODO(Tianyu): public for benchmarking
        public readonly FasterKV<Key, Value, Input, Output, Empty, Functions> localFaster;

        /* Worker-side DPR data structures*/
        // The worker keeps some information about uncommitted versions in memory. This is useful for allowing clients
        // to wait on them, or for rolling them back. 
        internal ConcurrentQueue<OutstandingLocalVersion> outstandingVersions;
        // Sessions will only concurrently access the latest two versions that is currently live / stable. Storing
        // them as separate fields eliminate the need for workers to acquire the latching when updating dependencies
        internal OutstandingLocalVersion liveLocalVersion, stableLocalVersion;
        // To avoid all threads hitting the checkpointing logic at the same time due to a version mismatch, 
        // they each contend on this counter to try and increment to the version they are responsible for bumping
        // up to. 
        public long inProgressBump = 0;
        internal long workerWorldLine = 0;

        /* Session-related data structures */
        // The worker needs to know about which sessions are attached to it to send updates and invoke callbacks, even
        // though logically a session does not need to be "resident" on a worker.
        internal readonly ConcurrentDictionary<Guid,
            FasterServerlessSession<Key, Value, Input, Output, Functions>> attachedSessions;
        // Each serverless session has one corresponding local session on each worker. Said local session is not
        // necessarily initialized if a serverless session never performs operations on that worker.
        internal readonly ConcurrentDictionary<Guid, (ClientSession<Key, Value, Input, Output, Empty, Functions>, SemaphoreSlim)> cachedLocalSessions;


        /* For messaging and remote execution */
        // TODO(Tianyu): Responsiblility assignment-wise this is pretty wrong
        internal SimpleObjectPool<ParsedMessageBatch<Key, Value, Input, Output>> messagePool =
            new SimpleObjectPool<ParsedMessageBatch<Key, Value, Input, Output>>(() => new ParsedMessageBatch<Key, Value, Input, Output>());
        internal IParameterSerializer<Key, Value, Input, Output> serializer;

        /// <summary>
        /// Construct a new Faster-Serverless instance with the given parameters.
        /// </summary>
        /// <param name="metadataStore"></param>
        /// <param name="messageManager"></param>
        /// <param name="dprManager"></param>
        /// <param name="size"></param>
        /// <param name="functions"></param>
        /// <param name="logSettings"></param>
        /// <param name="checkpointSettings"></param>
        /// <param name="serializerSettings"></param>
        /// <param name="comparer"></param>
        /// <param name="variableLengthStructSettings"></param>
        /// <param name="bucketingScheme"></param>
        public FasterServerless(
            MetadataStore metadataStore,
            ServerfulMessageManager messageManager,
            IDprManager dprManager,
            long size, Functions functions, LogSettings logSettings,
            IParameterSerializer<Key, Value, Input, Output> serializer,
            CheckpointSettings checkpointSettings = null, SerializerSettings<Key, Value> serializerSettings = null,
            IFasterEqualityComparer<Key> comparer = null,
            VariableLengthStructSettings<Key, Value> variableLengthStructSettings = null,
            IBucketingScheme<Key> bucketingScheme = null,
            bool clientOnly = false)
        {
            localFaster = new FasterKV<Key, Value, Input, Output, Empty, Functions>(size,
                functions,
                logSettings,
                checkpointSettings,
                serializerSettings,
                comparer,
                variableLengthStructSettings);

            attachedSessions =
                new ConcurrentDictionary<Guid,
                    FasterServerlessSession<Key, Value, Input, Output, Functions>>();
            cachedLocalSessions =
                new ConcurrentDictionary<Guid, (ClientSession<Key, Value, Input, Output, Empty, Functions>, SemaphoreSlim)>();
            MetadataStore = metadataStore;
            MessageManager = messageManager;
            DprManager = dprManager;

            BucketingScheme = bucketingScheme ?? new DefaultBucketingScheme<Key>();
            
            // Need to add the first version manually. Subsequent versions will be added via checkpoints
            liveLocalVersion = new OutstandingLocalVersion(1, localFaster.Log.TailAddress);
            stableLocalVersion = liveLocalVersion;
            outstandingVersions = new ConcurrentQueue<OutstandingLocalVersion>();
            outstandingVersions.Enqueue(liveLocalVersion);

            localFaster._fasterKV.UnsafeRegisterAdditionalAction(
                new VersionBoundaryCaptureTask<Key, Value, Input, Output, Functions>(this));
            // TODO(Tianyu): Add ownership change back in
            // localFaster.UnsafeRegisterAdditionalAction(
            // new MetadataStoreUpdateTask<Key, Value, Input, Output, Functions>(this));
            localFaster._fasterKV.UnsafeRegisterAdditionalAction(new WorldLineShiftTask<Key, Value, Input, Output, Functions>(this));

            this.serializer = serializer;
            this.clientOnly = clientOnly;
        }

        public FasterServerless(
            IOwnershipMapping ownershipMapping,
            ServerfulMessageManager messageManager,
            IDprManager dprManager,
            long size, Functions functions, LogSettings logSettings,
            IParameterSerializer<Key, Value, Input, Output> serializer,
            CheckpointSettings checkpointSettings = null, SerializerSettings<Key, Value> serializerSettings = null,
            IFasterEqualityComparer<Key> comparer = null,
            VariableLengthStructSettings<Key, Value> variableLengthStructSettings = null,
            IBucketingScheme<Key> bucketingScheme = null,
            bool clientOnly = false)
            : this(
                new MetadataStore(ownershipMapping, messageManager),
                messageManager, dprManager, size, functions, logSettings, serializer, checkpointSettings, serializerSettings,
                comparer, variableLengthStructSettings, bucketingScheme, clientOnly)
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public FasterServerlessSession<Key, Value, Input, Output, Functions>
            NewServerlessSession(int windowSize = 4096, int maxBatchSize = 1024, bool trackCommits = true, bool collectLatency = false)
        {
            // TODO(Tianyu): Ensure that the proposed id is unique, and optionally give a GUID
            var session =
                new FasterServerlessSession<Key, Value, Input, Output, Functions>(this, 0, windowSize, maxBatchSize, trackCommits, collectLatency);
            var addResult = attachedSessions.TryAdd(session.Id, session);
            Debug.Assert(addResult, "should not attach a session twice");
            return session;
        }
        
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public ClientSession<Key, Value, Input, Output, Empty, Functions> GetLocalSession(Guid id, out SemaphoreSlim latch)
        {
            var (socket, outlatch) =  cachedLocalSessions.GetOrAdd(id, k =>
            {
                var result = localFaster.NewSession(id.ToString(), true);
                // Because we created using thread affinitized, the session will always be pinned to a thread when it
                // returns. Need to suspend it immediately.
                result.UnsafeSuspendThread();
                return ValueTuple.Create(result, new SemaphoreSlim(1, 1));
            });
            latch = outlatch;
            return socket;
        }
        

        // TODO(Tianyu): Also figure out what needs to happen when sessions end or detaches. Presumably a message needs
        // to be sent out to all participants to throw away the cached local sessions. Alternatively, one can throw away
        // local sessions periodically that have no more pending updates.
    }
}