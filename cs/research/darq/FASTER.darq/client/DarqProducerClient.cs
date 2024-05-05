using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using darq.client;
using FASTER.common;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.libdpr.proto;

namespace FASTER.client
{
    /// <summary>
    /// Encodes information about a DARQ Cluster
    /// </summary>
    public interface IDarqClusterInfo
    {
        /// <summary></summary>
        /// <returns>Create a new DprFinder instance for the cluster, or null if none is configured</returns>
        IDprFinder GetNewDprFinder();

        /// <summary></summary>
        /// <returns> workers present in the cluster and human-readable descriptions</returns>
        IEnumerable<(DarqId, string)> GetMembers();

        /// <summary></summary>
        /// <returns>number of workers present in the cluster</returns>
        int GetClusterSize();

        /// <summary></summary>
        /// <param name="worker"> DarqId of interest </param>
        /// <returns> Return the IP address and port number the given worker is reachable at </returns>
        (string, int) GetMemberAddress(DarqId worker);
    }

    [Serializable]
    public class HardCodedClusterInfo : IDarqClusterInfo
    {
        private Dictionary<DarqId, (string, string, int)> memberMap;
        private string dprFinderIp;
        private int dprFinderPort = -1;

        public HardCodedClusterInfo()
        {
            memberMap = new Dictionary<DarqId, (string, string, int)>();
        }

        public HardCodedClusterInfo AddWorker(DarqId worker, string description, string ip, int port)
        {
            memberMap.Add(worker, (description, ip, port));
            return this;
        }

        public HardCodedClusterInfo SetDprFinder(string ip, int port)
        {
            dprFinderIp = ip;
            dprFinderPort = port;
            return this;
        }

        public (string, int) GetDprFinderInfo() => (dprFinderIp, dprFinderPort);

        public IDprFinder GetNewDprFinder()
        {
            if (dprFinderPort == -1 || dprFinderIp == null)
                throw new FasterException("DprFinder location not set!");
            return new RespGraphDprFinder(dprFinderIp, dprFinderPort);
        }

        public (string, int) GetMemberAddress(DarqId worker)
        {
            var (_, ip, port) = memberMap[worker];
            return (ip, port);
        }

        public IEnumerable<(DarqId, string)> GetMembers() =>
            memberMap.Select(e => (e.Key, e.Value.Item1));

        public int GetClusterSize() => memberMap.Count;
    }

    /// <summary>
    /// Producer client to add entries to DARQ. Should be invoked single-threaded. 
    /// </summary>
    public class DarqProducerClient : IDarqProducer
    {
        private IDarqClusterInfo darqClusterInfo;
        private Dictionary<DarqId, SingleDarqProducerClient> clients;
        private DprSession dprSession;
        private long serialNum = 0;

        /// <summary>
        /// Creates a new DarqProducerClient
        /// </summary>
        /// <param name="darqClusterInfo"> Cluster information </param>
        /// <param name="session"> The DprClientSession to use for speculative return (default if want return only after commit) </param>
        public DarqProducerClient(IDarqClusterInfo darqClusterInfo, DprSession session = null)
        {
            this.darqClusterInfo = darqClusterInfo;
            clients = new Dictionary<DarqId, SingleDarqProducerClient>();
            // TODO(Tianyu): Do something about session to set up SU correctly
            dprSession = session ?? new DprSession();
        }

        public void Dispose()
        {
            foreach (var client in clients.Values)
                client.Dispose();
        }

        /// <summary>
        /// Enqueues a sprint into the DARQ. Task will complete when DARQ has acked the enqueue, or when the enqueue is
        /// committed and recoverable if waitCommit is true.
        /// </summary>
        /// <param name="darqId">ID of the DARQ to enqueue onto</param>
        /// <param name="message"> body of the sprint </param>
        /// <param name="producerId"> producer ID to use (for deduplication purposes), or -1 if none </param>
        /// <param name="lsn">lsn to use (for deduplication purposes), should be monotonically increasing in every producer</param>
        /// <param name="forceFlush">
        /// whether to force flush buffer and send all requests. If false, requests are buffered
        /// until a set number has been accumulated or until forced to flush
        /// </param>
        /// <returns></returns>
        public Task EnqueueMessageAsync(DarqId darqId, ReadOnlySpan<byte> message, long producerId = -1,
            long lsn = -1,
            bool forceFlush = true)
        {
            if (!clients.TryGetValue(darqId, out var singleClient))
            {
                var (ip, port) = darqClusterInfo.GetMemberAddress(darqId);
                singleClient = new SingleDarqProducerClient(dprSession, ip, port);
            }

            var task = singleClient.EnqueueMessageAsync(message, producerId, lsn);

            if (forceFlush)
            {
                foreach (var client in clients.Values)
                    client.Flush();
            }

            return task;
        }

        // TODO(Tianyu): Handle socket-related anomalies?
        public void EnqueueMessageWithCallback(DarqId darqId, ReadOnlySpan<byte> message, Action<bool> callback,
            long producerId = -1, long lsn = -1)
        {
            if (!clients.TryGetValue(darqId, out var singleClient))
            {
                var (ip, port) = darqClusterInfo.GetMemberAddress(darqId);
                singleClient = new SingleDarqProducerClient(dprSession, ip, port);
            }

            singleClient.EnqueueMessageWithCallback(message, producerId, lsn, callback);
        }

        
        public void ForceFlush()
        {
            foreach (var client in clients.Values)
                client.Flush();
        }
    }

    internal class SingleDarqProducerClient : IDisposable, INetworkMessageConsumer
    {
        private readonly INetworkSender networkSender;

        // TODO(Tianyu): Change to something else for DARQ
        private readonly MaxSizeSettings maxSizeSettings;
        readonly int bufferSize;
        private bool disposed;
        private int offset;
        private int numMessages;
        private const int reservedDprHeaderSpace = 160;

        private DprSession dprSession;
        private ElasticCircularBuffer<Action<bool>> callbackQueue;

        public SingleDarqProducerClient(DprSession dprSession, string address, int port)
        {
            this.dprSession = dprSession;
            maxSizeSettings = new MaxSizeSettings();
            bufferSize = BufferSizeUtils.ClientBufferSize(maxSizeSettings);

            networkSender = new TcpNetworkSender(GetSendSocket(address, port), maxSizeSettings);
            networkSender.GetResponseObject();
            offset = 2 * sizeof(int) + reservedDprHeaderSpace + BatchHeader.Size;
            numMessages = 0;

            callbackQueue = new ElasticCircularBuffer<Action<bool>>();
        }

        public void Dispose()
        {
            disposed = true;
            networkSender.Dispose();
        }

        internal unsafe void Flush()
        {
            try
            {
                if (offset > 2 * sizeof(int) + reservedDprHeaderSpace + BatchHeader.Size)
                {
                    var head = networkSender.GetResponseObjectHead();
                    // Set packet size in header
                    *(int*)head = -(offset - sizeof(int));
                    head += sizeof(int);

                    ((BatchHeader*)head)->SetNumMessagesProtocol(numMessages,
                        (WireFormat)DarqProtocolType.DarqProducer);
                    head += sizeof(BatchHeader);

                    // Set DprHeader size
                    *(int*)head = reservedDprHeaderSpace;
                    head += sizeof(int);

                    // populate DPR header
                    var headerBytes = new Span<byte>(head, reservedDprHeaderSpace);
                    if (dprSession.TagMessage(headerBytes) < 0)
                        // TODO(Tianyu): Handle size mismatch by probably copying into a new array and up-ing reserved space in the future
                        throw new NotImplementedException();
                    if (!networkSender.SendResponse(0, offset))
                        throw new ObjectDisposedException("socket closed");

                    networkSender.GetResponseObject();
                    offset = 2 * sizeof(int) + reservedDprHeaderSpace + BatchHeader.Size;
                    numMessages = 0;
                }
            }
            catch (DprSessionRolledBackException)
            {
                // Ensure that callback queue is drained only on a single-thread. This is not a scalability issue
                // because except in the event of a rollback, callback queue is not concurrently accessed
                lock (callbackQueue)
                {
                    // TODO(Tianyu): Eagerly clearing the callback queue here may result in an overapproximation of
                    // things that are rolled back, but is the expedient approach here. Maybe fix later
                    while (!callbackQueue.IsEmpty())
                        callbackQueue.Dequeue()(false);
                }

                throw;
            }
        }

        public Task EnqueueMessageAsync(ReadOnlySpan<byte> message, long producerId, long lsn)
        {
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            EnqueueMessageWithCallback(message, producerId, lsn, success =>
            {
                if (success)
                    tcs.SetResult(null);
                else
                    tcs.SetCanceled();
            });
            return tcs.Task;
        }

        public void EnqueueMessageWithCallback(ReadOnlySpan<byte> message, long producerId, long lsn,
            Action<bool> callback)
        {
            EnqueueMessageInternal(message, producerId, lsn, callback);
        }

        internal unsafe void EnqueueMessageInternal(ReadOnlySpan<byte> message, long producerId, long lsn,
            Action<bool> action)
        {
            byte* curr, end;
            var entryBatchSize = SerializedDarqEntryBatch.ComputeSerializedSize(message);
            while (true)
            {
                end = networkSender.GetResponseObjectHead() + bufferSize;
                curr = networkSender.GetResponseObjectHead() + offset;
                var serializedSize = sizeof(byte) + sizeof(long) * 2 + entryBatchSize;
                if (end - curr >= serializedSize) break;
                Flush();
            }

            *curr = (byte)DarqCommandType.DarqEnqueue;
            curr += sizeof(byte);

            *(long*)curr = producerId;
            curr += sizeof(long);
            *(long*)curr = lsn;
            curr += sizeof(long);

            var batch = new SerializedDarqEntryBatch(curr);
            batch.SetContent(message);
            curr += entryBatchSize;
            offset = (int)(curr - networkSender.GetResponseObjectHead());
            numMessages++;
            callbackQueue.Enqueue(action);
        }

        unsafe void INetworkMessageConsumer.ProcessReplies(byte[] buf, int startOffset, int size)
        {
            fixed (byte* b = buf)
            {
                var src = b + startOffset;

                var count = ((BatchHeader*)src)->NumMessages;
                src += BatchHeader.Size;

                var dprHeader = new ReadOnlySpan<byte>(src, DprMessageHeader.FixedLenSize);
                src += DprMessageHeader.FixedLenSize;

                // Ensure that callback queue is drained only on a single-thread. This is not a scalability issue
                // because except in the event of a rollback, callback queue is not concurrently accessed
                lock (callbackQueue)
                {
                    try
                    {
                        if (!dprSession.Receive(dprHeader)) return;
                        for (int i = 0; i < count; i++)
                        {
                            var messageType = (DarqCommandType)(*src++);
                            switch (messageType)
                            {
                                case DarqCommandType.DarqEnqueue:
                                    callbackQueue.Dequeue()(true);
                                    break;
                                // Even though the server could return DarqCommandType.INVALID, this should not get
                                // past the Receive() call which triggers the rollback codepath
                                default:
                                    throw new FasterException("Unexpected return type");
                            }
                        }
                    }
                    catch (DprSessionRolledBackException)
                    {
                        // TODO(Tianyu): Eagerly clearing the callback queue here may result in an overapproximation of
                        // things that are rolled back, but is the expedient approach here. Maybe fix later
                        while (!callbackQueue.IsEmpty())
                            callbackQueue.Dequeue()(false);
                    }
                }
            }
        }

        private Socket GetSendSocket(string address, int port, int millisecondsTimeout = -2)
        {
            var ip = IPAddress.Parse(address);
            var endPoint = new IPEndPoint(ip, port);
            var socket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true
            };

            if (millisecondsTimeout != -2)
            {
                IAsyncResult result = socket.BeginConnect(endPoint, null, null);
                result.AsyncWaitHandle.WaitOne(millisecondsTimeout, true);
                if (socket.Connected)
                    socket.EndConnect(result);
                else
                {
                    socket.Close();
                    throw new Exception("Failed to connect server.");
                }
            }
            else
            {
                socket.Connect(endPoint);
            }

            // Ok to create new event args on accept because we assume a connection to be long-running
            var receiveEventArgs = new SocketAsyncEventArgs();
            var bufferSize = BufferSizeUtils.ServerBufferSize(maxSizeSettings);
            receiveEventArgs.SetBuffer(new byte[bufferSize], 0, bufferSize);
            receiveEventArgs.UserToken = new DarqClientNetworkSession<SingleDarqProducerClient>(socket, this);
            receiveEventArgs.Completed += RecvEventArg_Completed;
            var response = socket.ReceiveAsync(receiveEventArgs);
            Debug.Assert(response);
            return socket;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var connState = (DarqClientNetworkSession<SingleDarqProducerClient>)e.UserToken;
            if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success || disposed)
            {
                connState.socket.Dispose();
                e.Dispose();
                return false;
            }

            connState.AddBytesRead(e.BytesTransferred);
            var newHead = connState.TryConsumeMessages(e.Buffer);
            if (newHead == e.Buffer.Length)
            {
                // Need to grow input buffer
                var newBuffer = new byte[e.Buffer.Length * 2];
                Array.Copy(e.Buffer, newBuffer, e.Buffer.Length);
                e.SetBuffer(newBuffer, newHead, newBuffer.Length - newHead);
            }
            else
                e.SetBuffer(newHead, e.Buffer.Length - newHead);

            return true;
        }

        private void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                var connState = (DarqClientNetworkSession<SingleDarqProducerClient>)e.UserToken;
                do
                {
                    // No more things to receive
                    if (!HandleReceiveCompletion(e)) break;
                } while (!connState.socket.ReceiveAsync(e));
            }
            // ignore session socket disposed due to client session dispose
            catch (ObjectDisposedException)
            {
            }
        }
    }
}