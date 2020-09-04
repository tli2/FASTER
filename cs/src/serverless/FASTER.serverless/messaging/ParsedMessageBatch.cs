﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;

namespace FASTER.serverless
{
    public class ParsedMessageBatch<Key, Value, Input, Output> : IMessageBatch<Key, Value, Input, Output>
        where Key : new()
        where Value : new()
    {
        public const int MaxBatchSize = 4096;
        internal BatchHeader header;
        internal Message<Key, Value, Input, Output>[] messages;
        internal WorkerVersion[] deps;
        internal Socket socket;

        public ParsedMessageBatch()
        {
            messages = new Message<Key, Value, Input, Output>[MaxBatchSize];
            deps = new WorkerVersion[1 << LightDependencySet.MaxSizeBits];
        }

        public ref BatchHeader GetHeader() => ref header;
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReinitializeForReply(ref MessageBatchRaw request)
        {
            header.recipient = request.header.sender;
            header.sender =  request.header.recipient;
            header.sessionId = request.header.sessionId;
            header.numDeps = request.header.numDeps;
            for (var i = 0; i < header.numDeps; i++)
                deps[i] = request.GetDep(i);
            header.replyOnly = true;
            header.numMessages = 0;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReinitializeForReply(ParsedMessageBatch<Key, Value, Input, Output> request)
        {
            header.recipient = request.header.sender;
            header.sender =  request.header.recipient;
            header.sessionId = request.header.sessionId;
            header.numDeps = request.header.numDeps;
            for (var i = 0; i < header.numDeps; i++)
                deps[i] = request.deps[i];
            header.replyOnly = true;
            header.numMessages = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReinitializeForBackgroundProcessing(ref MessageBatchRaw request, Socket socket)
        {
            this.socket = socket;
            header = request.header;
            for (var i = 0; i < header.numDeps; i++)
                deps[i] = request.GetDep(i);
            header.replyOnly = false;
            header.numMessages = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref Message<Key, Value, Input, Output> AddMessage() => ref messages[header.numMessages++];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int WriteTo(byte[] buffer, int offset, IParameterSerializer<Key, Value, Input, Output> serializer)
        {
            fixed (void *d = &buffer[offset])
            {
                var p = header.WriteTo(new IntPtr(d));
                for (long i = 0; i < header.numDeps; i++)
                {
                    var worker = deps[i].Worker.guid;
                    Unsafe.Copy(p.ToPointer(), ref worker);
                    p += sizeof(long);
                    var version = deps[i].Version;
                    Unsafe.Copy(p.ToPointer(), ref version);
                    p += sizeof(long);
                }
                
                // Write each message in sequence
                for (var i = 0; i < header.numMessages; i++)
                     p = messages[i].WriteTo(p, serializer);
                // Compute the size actually written and return
                return (int) (p.ToInt64() - new IntPtr(d).ToInt64());
            }
        } 
    }
}