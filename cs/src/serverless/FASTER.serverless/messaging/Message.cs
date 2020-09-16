﻿using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.core;

namespace FASTER.serverless
{
    [StructLayout(LayoutKind.Explicit, Size = 48)]
    public unsafe struct BatchHeader
    {
        public const int Size = 48;
        [FieldOffset(0)]
        public fixed byte data[Size];
        [FieldOffset(0)]
        public Guid sessionId;
        [FieldOffset(16)]
        public long sender;
        [FieldOffset(24)]
        public long recipient;
        [FieldOffset(32)]
        public ushort numDeps;
        [FieldOffset(34)]
        public ushort numMessages;
        // Some free space to help with traversal on the receiver side, because why not
        [FieldOffset(36)]
        public int readHead;
        [FieldOffset(40)]
        public int numRead;
        [FieldOffset(44)]
        public bool replyOnly;
        
        public Worker Recipient => new Worker(recipient);
        
        public Worker Sender => new Worker(sender);
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IntPtr WriteTo(IntPtr dst)
        {
            fixed (byte *s = data)
                Buffer.MemoryCopy(s, dst.ToPointer(), Size, Size);
            return dst + Size;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IntPtr PopulateFrom(IntPtr src)
        {
            fixed (byte *d = data)
                Buffer.MemoryCopy(src.ToPointer(), d, Size, Size);
            return src + Size;
        }
    }
    
    [StructLayout(LayoutKind.Explicit, Size = 32)]
    public unsafe struct MessageHeader
    {
        public const int Size = 32;
        [FieldOffset(0)]
        public fixed byte data[Size];
        [FieldOffset(0)]
        public long version;
        [FieldOffset(8)]
        public long worldLine;
        [FieldOffset(16)]
        public long serialNum;
        [FieldOffset(24)]
        public int writeLocation;
        [FieldOffset(28)]
        public FasterServerlessMessageType type;
        [FieldOffset(29)]
        public FasterServerlessReturnCode ret;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IntPtr WriteTo(IntPtr dst)
        {
            fixed (byte *s = data)
                Buffer.MemoryCopy(s, dst.ToPointer(), Size, Size);
            return dst + Size;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IntPtr PopulateFrom(IntPtr src)
        {
            fixed (byte *d = data)
                Buffer.MemoryCopy(src.ToPointer(), d, Size, Size);
            return src + Size;
        }
    }

    [StructLayout(LayoutKind.Explicit, Size = 48)]
    public unsafe struct MessageBatchRaw
    {
        public const int Size = 48;
        [FieldOffset(0)]
        public BatchHeader header;
        // Used to index into the rest of the buffer.
        [FieldOffset(BatchHeader.Size)]
        public fixed byte data[1];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref WorkerVersion GetDep(int i)
        {
            fixed (void *start = &data[i * sizeof(WorkerVersion)])
                return ref Unsafe.AsRef<WorkerVersion>(start);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ResetReader()
        {
            header.numRead = 0;
            header.readHead = header.numDeps * sizeof(WorkerVersion);
            return header.readHead;
        } 
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool NextMessage<Key, Value, Input, Output>(ref Message<Key, Value, Input, Output> dst, IParameterSerializer<Key, Value, Input, Output> serializer)
            where Key : new()
            where Value : new()
        {
            if (header.numRead >= header.numMessages) return false;
            
            fixed (void* d = &data[header.readHead])
            {
                var ptr = new IntPtr(d);
                header.readHead += (int) (dst.PopulateFrom(ptr, serializer).ToInt64() - ptr.ToInt64());
                header.numRead++;
            }

            return true;
        }
    }

    public struct Message<Key, Value, Input, Output>
        where Key : new()
        where Value : new()
    {
        public MessageHeader header;
        // Provision enough on the struct to hold any message body type, even if no message uses all of it.
        public Key key;
        public Input input;
        public Value value;
        public Output output;
        public long recoveredUntil;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe IntPtr WriteTo(IntPtr dst, IParameterSerializer<Key, Value, Input, Output> serializer)
        {
            var result = dst;
            result = header.WriteTo(result);
            switch (header.type)
            {
                case FasterServerlessMessageType.ReadRequest:
                case FasterServerlessMessageType.RmwRequest:
                    result = serializer.WriteKey(result, ref key);
                    result = serializer.WriteInput(result, ref input);
                    break;
                case FasterServerlessMessageType.UpsertRequest:
                    result = serializer.WriteKey(result, ref key);
                    result = serializer.WriteValue(result, ref value);
                    break;
                case FasterServerlessMessageType.DeleteRequest:
                    result = serializer.WriteKey(result, ref key);
                    break;
                case FasterServerlessMessageType.ReadResult:
                    result = serializer.WriteOutput(result, ref output);
                    break;
                case FasterServerlessMessageType.RecoveryResult:
                    Unsafe.Copy(result.ToPointer(), ref recoveredUntil);
                    result += sizeof(long);
                    break;
            }
            return result;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe IntPtr PopulateFrom(IntPtr src, IParameterSerializer<Key, Value, Input, Output> serializer)
        {
            var result = src;
            result = header.PopulateFrom(result);
            switch (header.type)
            {
                case FasterServerlessMessageType.ReadRequest:
                case FasterServerlessMessageType.RmwRequest:
                    result = serializer.ReadKey(result, out key);
                    result = serializer.ReadInput(result, out input);
                    break;
                case FasterServerlessMessageType.UpsertRequest:
                    result = serializer.ReadKey(result, out key);
                    result = serializer.ReadValue(result, out value);
                    break;
                case FasterServerlessMessageType.DeleteRequest:
                    result = serializer.ReadKey(result, out key);
                    break;
                case FasterServerlessMessageType.ReadResult:
                    result = serializer.ReadOutput(result, out output);
                    break;
                case FasterServerlessMessageType.RecoveryResult:
                    Unsafe.Copy(ref recoveredUntil, result.ToPointer());
                    result += sizeof(long);
                    break;
            }
            return result;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InitializeHeader(FasterServerlessMessageType type, long serialNum, long version, long worldLine)
        {
            header.version = version;
            header.serialNum = serialNum;
            header.worldLine = worldLine;
            header.type = type;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static FasterServerlessReturnCode ConvertToReturnCode(Status status)
        {
            switch (status)
            {
                case Status.OK:
                    return FasterServerlessReturnCode.OK;
                case Status.NOTFOUND:
                    return FasterServerlessReturnCode.NotFound;
                case Status.ERROR:
                    return FasterServerlessReturnCode.Error;
                default:
                    // Should not have pending
                    throw new FasterException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeUpsertRequest(long serialNum, long version, long worldLine, int writeLocation, Key key, Input input)
        {
            InitializeHeader(FasterServerlessMessageType.ReadRequest, serialNum, version, worldLine);
            header.writeLocation = writeLocation;
            this.key = key;
            this.input = input;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReplyReadSuccess(ref Message<Key, Value, Input, Output> request, long version, Status status)
        {
            header = request.header;
            header.type = FasterServerlessMessageType.ReadResult;
            header.version = version;
            header.ret = ConvertToReturnCode(status);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReplyReadFailureNotOwner(ref Message<Key, Value, Input, Output> request)
        {
            header = request.header;
            header.type = FasterServerlessMessageType.ReadResult;
            header.ret = FasterServerlessReturnCode.NotOwner;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReplyReadFailureWorldLine(ref Message<Key, Value, Input, Output> request, long newWorldLine)
        {
            header = request.header;
            header.type = FasterServerlessMessageType.ReadResult;
            header.ret = FasterServerlessReturnCode.WorldLineShift;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReplyWriteSuccess(ref Message<Key, Value, Input, Output> request, long version,Status status)
        {
            header = request.header;
            header.type = FasterServerlessMessageType.RequestComplete;
            header.version = version;
            header.ret = ConvertToReturnCode(status);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReplyWriteFailureNotOwner(ref Message<Key, Value, Input, Output> request)
        {
            header = request.header;
            header.type = FasterServerlessMessageType.RequestComplete;
            header.ret = FasterServerlessReturnCode.NotOwner;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReplyWriteFailureWorldLine(ref Message<Key, Value, Input, Output> request, long newWorldLine)
        {
            header = request.header;
            header.type = FasterServerlessMessageType.RequestComplete;
            header.ret = FasterServerlessReturnCode.WorldLineShift;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeUpsertRequest(long serialNum, long version, long worldLine, int writeLocation, Key key, Value value)
        {
            InitializeHeader(FasterServerlessMessageType.UpsertRequest, serialNum,
                version, worldLine);
            header.writeLocation = writeLocation;
            this.key = key;
            this.value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeRmwRequest(long serialNum, long version, long worldLine, int writeLocation, Key key, Input input)
        {
            InitializeHeader(FasterServerlessMessageType.RmwRequest, serialNum,
                version, worldLine);
            header.writeLocation = writeLocation;
            this.key = key;
            this.input = input;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeDeleteRequst(long serialNum, long version, long worldLine, int writeLocation, Key key)
        {
            InitializeHeader(FasterServerlessMessageType.DeleteRequest, serialNum,
                version, worldLine);
            header.writeLocation = writeLocation;
            this.key = key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeRecoveryStatusCheck(int writeLocation,
            long worldLine)
        {
            InitializeHeader(FasterServerlessMessageType.RecoveryStatusCheck, -1, -1, worldLine);
            header.writeLocation = writeLocation;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReplyRecoveryResult(ref Message<Key, Value, Input, Output> m, CommitPoint recoveredProgress)
        {
            header = m.header;
            header.type = FasterServerlessMessageType.RecoveryResult;
            recoveredUntil = recoveredProgress.UntilSerialNo;
        }
    }
}