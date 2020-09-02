﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.serverless
{
internal class ClientRequestBatch<Key, Value, Input, Output> : IMessageBatch<Key, Value, Input, Output>
        where Key : new()
        where Value : new()
    {
        internal BatchHeader header;
        internal LightDependencySet dependencySet; 
        internal List<ServerlessPendingContext<Key, Value, Input, Output>> messages;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref BatchHeader GetHeader() => ref header;
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ClientRequestBatch(Worker sender, Worker recipient, Guid sessionId)
        {
            header.replyOnly = false;
            header.recipient = recipient.guid;
            header.sender = sender.guid;
            header.sessionId = sessionId;
            header.numMessages = 0;
            dependencySet = new LightDependencySet();
            messages = new List<ServerlessPendingContext<Key, Value, Input, Output>>();
            Clear();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Empty() => messages.Count == 0;
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Size() => messages.Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(ServerlessPendingContext<Key, Value, Input, Output> pendingContext)
        {
            header.numMessages++;
            messages.Add(pendingContext);
            Debug.Assert(header.numMessages == messages.Count);
        }

        public void Clear()
        {
            header.numMessages = 0;
            dependencySet.UnsafeClear();
            messages.Clear();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int WriteTo(byte[] buffer, int offset, IParameterSerializer<Key, Value, Input, Output> serializer)
        {
            fixed (void *d = &buffer[offset])
            {
                // Skip ahead and write all the dependencies first, which also computes the number of dependencies
                var p = new IntPtr(d) + BatchHeader.Size;
                header.numDeps = 0;
                for (long i = 0; i < dependencySet.DependentVersions.Length; i++)
                {
                    var dep = dependencySet.DependentVersions[i];
                    if (dep == LightDependencySet.NoDependency) continue;

                    header.numDeps++;
                    Unsafe.Copy(p.ToPointer(), ref i);
                    p += sizeof(long);
                    Unsafe.Copy(p.ToPointer(), ref dep);
                    p += sizeof(long);
                }
                // Now that the number of dependencies is fixed, write the header
                var headerEnd = header.WriteTo(new IntPtr(d));
                Debug.Assert(headerEnd.ToInt64() == (new IntPtr(d) + BatchHeader.Size).ToInt64());
                
                // Write each message in sequence
                foreach (var ctx in messages)
                    p = ctx.op.WriteTo(p, serializer);
                
                // Compute the size actually written and return
                return (int) (p.ToInt64() - new IntPtr(d).ToInt64());
            }
        } 
    }
      
    // TODO(Tianyu): Cannot support more than a handful of dependencies. Need to change for larger cluster size
    internal class LightRequestBatcher<Key, Value, Input, Output>
        where Key : new()
        where Value : new()
    {
        internal ClientRequestBatch<Key, Value, Input, Output>[] outstandingBatches;

        public LightRequestBatcher(Worker me, Guid sessionId)
        {
            // Accomodate the same number of nodes as light dependency set
            outstandingBatches = new ClientRequestBatch<Key, Value, Input, Output>[1 << LightDependencySet.MaxSizeBits];
            for (var i = 0; i < outstandingBatches.Length; i++)
            {
                outstandingBatches[i] = new ClientRequestBatch<Key, Value, Input, Output>(me, new Worker(i), sessionId);
            }
        }

        public ClientRequestBatch<Key, Value, Input, Output> Submit(Worker recipient, ServerlessPendingContext<Key, Value, Input, Output> m)
        {
            var result = outstandingBatches[recipient.guid];
            result.Add(m);
            return result;
        }
    }
}