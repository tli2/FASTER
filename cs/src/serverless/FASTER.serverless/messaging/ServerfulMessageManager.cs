using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FASTER.serverless
{
    [Serializable]
    public struct Worker
    {
        public static Worker INVALID = new Worker(-1);
        public readonly long guid;

        public Worker(long guid)
        {
            this.guid = guid;
        }
        
        public bool Equals(Worker other)
        {
            return guid == other.guid;
        }

        public override bool Equals(object obj)
        {
            return obj is Worker other && Equals(other);
        }

        public override int GetHashCode()
        {
            return guid.GetHashCode();
        }
    }

    // TODO(Tianyu): Move this class out of this file
    public static class SocketIoUtil
    {
        public static bool ReceiveFully(this Socket clientSocket, byte[] buffer, int numBytes,
            SocketFlags flags = SocketFlags.None)
        {
            Debug.Assert(numBytes <= buffer.Length);
            var totalReceived = 0;
            do
            {
                var received = clientSocket.Receive(buffer, totalReceived, numBytes - totalReceived, flags);
                if (received == 0) return false;
                totalReceived += received;
            } while (totalReceived < numBytes);

            return true;
        }

        public static void SendFully(this Socket clientSocket, byte[] buffer, int offset, int size,
            SocketFlags flags = SocketFlags.None)
        {
            Debug.Assert(offset >= 0 && offset < buffer.Length && offset + size <= buffer.Length);
            var totalSent = 0;
            do
            {
                var sent = clientSocket.Send(buffer, offset + totalSent, size - totalSent, flags);
                totalSent += sent;
            } while (totalSent < size);
        }
    }
    
    internal class ClientConnectionState<Key, Value, Input, Output, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        internal Socket socket;
        private FasterServerlessSession<Key, Value, Input, Output, Functions> session;
        private int bytesRead;
        private int readHead;

        public ClientConnectionState(Socket socket,
            FasterServerlessSession<Key, Value, Input, Output, Functions> session)
        {
            this.socket = socket;
            this.session = session;
            bytesRead = 0;
            readHead = 0;
        }

        internal void AddBytesRead(int bytesRead) => this.bytesRead += bytesRead;
        
        internal int TryConsumeMessages(byte[] buf)
        {
            while (TryReadMessages(buf, out var offset))
                session.ProcessReplies(buf, offset);

            // The bytes left in the current buffer not consumed by previous operations
            var bytesLeft = bytesRead - readHead;
            if (bytesLeft != bytesRead)
            {
                // Shift them to the head of the array so we can reset the buffer to a consistent state
                Array.Copy(buf, readHead, buf, 0, bytesLeft);
                bytesRead = bytesLeft;
                readHead = 0;
            }

            return bytesRead;
        }

        private bool TryReadMessages(byte[] buf, out int offset)
        {
            offset = default;

            var bytesAvailable = bytesRead - readHead;
            // Need to at least have read off of size field on the message
            if (bytesAvailable < sizeof(int)) return false;

            var size = BitConverter.ToInt32(buf, readHead);
            // Not all of the message has arrived
            if (bytesAvailable < size + sizeof(int)) return false;
            offset = readHead + sizeof(int);

            // Consume this message and the header
            readHead += size + sizeof(int);
            return true;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var connState = (ClientConnectionState<Key, Value, Input, Output, Functions>) e.UserToken;
            if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
            {
                connState.socket.Dispose();
                e.Dispose();
                return false;
            }

            connState.AddBytesRead(e.BytesTransferred);
            var newHead = connState.TryConsumeMessages(e.Buffer);
            e.SetBuffer(newHead, e.Buffer.Length - newHead);
            return true;
        }

        public static void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            var connState = (ClientConnectionState<Key, Value, Input, Output, Functions>) e.UserToken;
            do
            {
                // No more things to receive
                if (!HandleReceiveCompletion(e)) break;
            } while (!connState.socket.ReceiveAsync(e));
        }
    }

    internal class ServerConnectionState<Key, Value, Input, Output, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        internal Socket socket;
        private FasterServerless<Key, Value, Input, Output, Functions> worker;
        private FasterServerlessBackgroundThreadPool<Key, Value, Input, Output, Functions> threadPool;
        private int bytesRead;
        private int readHead;

        public ServerConnectionState(Socket socket,
            FasterServerless<Key, Value, Input, Output, Functions> worker,
            FasterServerlessBackgroundThreadPool<Key, Value, Input, Output, Functions> threadPool)
        {
            this.socket = socket;
            this.worker = worker;
            this.threadPool = threadPool;
            bytesRead = 0;
            readHead = 0;
        }

        internal void AddBytesRead(int bytesRead) => this.bytesRead += bytesRead;
        
        internal int TryConsumeMessages(byte[] buf)
        {
            while (TryReadMessages(buf, out var offset))
                worker.ProcessBatch(buf, offset, socket, threadPool);

            // The bytes left in the current buffer not consumed by previous operations
            var bytesLeft = bytesRead - readHead;
            if (bytesLeft != bytesRead)
            {
                // Shift them to the head of the array so we can reset the buffer to a consistent state
                Array.Copy(buf, readHead, buf, 0, bytesLeft);
                bytesRead = bytesLeft;
                readHead = 0;
            }

            return bytesRead;
        }

        private bool TryReadMessages(byte[] buf, out int offset)
        {
            offset = default;

            var bytesAvailable = bytesRead - readHead;
            // Need to at least have read off of size field on the message
            if (bytesAvailable < sizeof(int)) return false;

            var size = BitConverter.ToInt32(buf, readHead);
            // Not all of the message has arrived
            if (bytesAvailable < size + sizeof(int)) return false;
            offset = readHead + sizeof(int);

            // Consume this message and the header
            readHead += size + sizeof(int);
            return true;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var connState = (ServerConnectionState<Key, Value, Input, Output, Functions>) e.UserToken;
            if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
            {
                connState.socket.Dispose();
                e.Dispose();
                return false;
            }

            connState.AddBytesRead(e.BytesTransferred);
            var newHead = connState.TryConsumeMessages(e.Buffer);
            e.SetBuffer(newHead, e.Buffer.Length - newHead);
            return true;
        }

        public static void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            var connState = (ServerConnectionState<Key, Value, Input, Output, Functions>) e.UserToken;
            do
            {
                // No more things to receive
                if (!HandleReceiveCompletion(e)) break;
            } while (!connState.socket.ReceiveAsync(e));
        }
    }

    /// <summary>
    /// A MessageManager that operates on serverful workers
    /// </summary>
    public class ServerfulMessageManager : IDisposable
    {
        // Should be large enough for most batches
        // TODO(Tianyu): Need to actually either enforce batch size or handle cases where batches don't fit
        internal const int batchMaxSize = 1 << 20;
        private readonly Worker me;
        private readonly ConcurrentDictionary<Worker, ServerfulWorkerInfo> routingTable;

        private ThreadLocal<Dictionary<Worker, Socket>> cachedConnections;
        private SimpleObjectPool<SocketAsyncEventArgs> reusableSendArgs;
        private Socket servSocket;

        public ServerfulMessageManager(Worker me, ConcurrentDictionary<Worker, ServerfulWorkerInfo> routingTable)
        {
            this.me = me;
            this.routingTable = routingTable;
            cachedConnections =
                new ThreadLocal<Dictionary<Worker, Socket>>(() => new Dictionary<Worker, Socket>(), true);
            reusableSendArgs = new SimpleObjectPool<SocketAsyncEventArgs>(() =>
            {
                var result = new SocketAsyncEventArgs();
                result.SetBuffer(new byte[batchMaxSize], 0, batchMaxSize);
                result.Completed += SendEventArg_Completed;
                return result;
            }, e => e.Dispose());
        }

        public void Dispose()
        {
            servSocket.Dispose();
            foreach (var dict in cachedConnections.Values)
            {
                foreach (var entry in dict)
                    entry.Value.Dispose();
            }

            reusableSendArgs.Dispose();
        }

        public Worker Me()
        {
            return me;
        }

        private Socket GetSendSocket<Key, Value, Input, Output, Functions>(
            FasterServerlessSession<Key, Value, Input, Output, Functions> session, Worker recipient)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Empty>
        {
            if (!cachedConnections.Value.TryGetValue(recipient, out var socket))
            {
                var info = routingTable[recipient];
                var ip = IPAddress.Parse(info.GetAddress());
                var endPoint = new IPEndPoint(ip, info.GetPort());
                socket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                socket.Connect(endPoint);
                cachedConnections.Value.Add(recipient, socket);

                // Ok to create new event args on accept because we assume a connection to be long-running
                var receiveEventArgs = new SocketAsyncEventArgs();
                receiveEventArgs.SetBuffer(new byte[batchMaxSize], 0, batchMaxSize);
                receiveEventArgs.UserToken =
                    new ClientConnectionState<Key, Value, Input, Output, Functions>(socket, session);
                receiveEventArgs.Completed += ClientConnectionState<Key, Value, Input, Output, Functions>.RecvEventArg_Completed;
                var response = socket.ReceiveAsync(receiveEventArgs);
                Debug.Assert(response);
            }

            return socket;
        }

        private void SendEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            ((ReusableObject<SocketAsyncEventArgs>) e.UserToken).Dispose();
        }
        
        public unsafe void Send<Key, Value, Input, Output>(
            Socket socket,
            IMessageBatch<Key, Value, Input, Output> batch,
            IParameterSerializer<Key, Value, Input, Output> serializer)
            where Key : new()
            where Value : new()
        {
            var sendArg = reusableSendArgs.Checkout();
            // Leave 4 bytes in the front for size
            var size = batch.WriteTo(sendArg.obj.Buffer, sizeof(int), serializer);
            // Write size field after it is known
            Marshal.Copy(new IntPtr(&size), sendArg.obj.Buffer, 0, sizeof(int));

            // Reset send buffer
            sendArg.obj.SetBuffer(0, sizeof(int) + size);
            // Set user context to reusable object handle for disposal when send is done
            sendArg.obj.UserToken = sendArg;
            if (!socket.SendAsync(sendArg.obj))
                SendEventArg_Completed(null, sendArg.obj);
        }

        public void Send<Key, Value, Input, Output, Functions>(
            FasterServerlessSession<Key, Value, Input, Output, Functions> session,
            IMessageBatch<Key, Value, Input, Output> batch,
            IParameterSerializer<Key, Value, Input, Output> serializer)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Empty>
        {
            var socket = GetSendSocket(session, batch.GetHeader().Recipient);
            Send(socket, batch, serializer);
        }

        private bool HandleNewConnection<Key, Value, Input, Output, Functions>(SocketAsyncEventArgs e)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Empty>
        {
            if (e.SocketError != SocketError.Success)
            {
                e.Dispose();
                return false;
            }

            var (worker, threadPool) =
                (ValueTuple<FasterServerless<Key, Value, Input, Output, Functions>,
                    FasterServerlessBackgroundThreadPool<Key, Value, Input, Output, Functions>>) e.UserToken;

            // Ok to create new event args on accept because we assume a connection to be long-running
            var receiveEventArgs = new SocketAsyncEventArgs();
            receiveEventArgs.SetBuffer(new byte[batchMaxSize], 0, batchMaxSize);
            receiveEventArgs.UserToken =
                new ServerConnectionState<Key, Value, Input, Output, Functions>(e.AcceptSocket, worker, threadPool);
            receiveEventArgs.Completed += ServerConnectionState<Key, Value, Input, Output, Functions>.RecvEventArg_Completed;

            // If the client already have packets, avoid handling it here on the handler so we don't block future accepts.
            if (!e.AcceptSocket.ReceiveAsync(receiveEventArgs))
                Task.Run(() => ServerConnectionState<Key, Value, Input, Output, Functions>.RecvEventArg_Completed(null, receiveEventArgs));
            return true;
        }

        private void AcceptEventArg_Completed<Key, Value, Input, Output, Functions>(object sender,
            SocketAsyncEventArgs e)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Empty>
        {
            do
            {
                if (!HandleNewConnection<Key, Value, Input, Output, Functions>(e)) break;
                e.AcceptSocket = null;
            } while (!servSocket.AcceptAsync(e));
        }

        public void StartServer<Key, Value, Input, Output, Functions>(
            FasterServerless<Key, Value, Input, Output, Functions> worker,
            FasterServerlessBackgroundThreadPool<Key, Value, Input, Output, Functions> threadPool)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Empty>
        {
            var info = routingTable[me];
            var ip = IPAddress.Parse(info.GetAddress());
            var endPoint = new IPEndPoint(ip, info.GetPort());
            servSocket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            servSocket.Bind(endPoint);
            servSocket.Listen(512);

            var acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.UserToken = ValueTuple.Create(worker, threadPool);
            acceptEventArg.Completed += AcceptEventArg_Completed<Key, Value, Input, Output, Functions>;
            if (!servSocket.AcceptAsync(acceptEventArg))
                AcceptEventArg_Completed<Key, Value, Input, Output, Functions>(null, acceptEventArg);
        }
    }
}