using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using FASTER.common;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;

namespace FASTER.server
{
    internal class ProducerResponseBuffer : IDisposable
    {
        internal long version;
        internal byte[] buf;
        internal int size;
        internal INetworkSender networkSender;
        internal SimpleObjectPool<ProducerResponseBuffer> pool;

        public ProducerResponseBuffer(int bufferSize, INetworkSender networkSender,
            SimpleObjectPool<ProducerResponseBuffer> pool)
        {
            buf = new byte[bufferSize];
            this.pool = pool;
            this.networkSender = networkSender;
        }

        public void Dispose() => pool?.Return(this);
    }

    internal sealed unsafe class DarqProducerSession<TVersionScheme> : ServerSessionBase where TVersionScheme : IVersionScheme
    {
        readonly HeaderReaderWriter hrw;
        int readHead;
        int seqNo, msgnum, start;
        private Darq darq;
        private readonly SimpleObjectPool<ProducerResponseBuffer> sendBufferPool;
        private ConcurrentQueue<ProducerResponseBuffer> responseQueue;

        public DarqProducerSession(INetworkSender networkSender, Darq darq,
            ConcurrentQueue<ProducerResponseBuffer> responseQueue) : base(
            networkSender)
        {
            this.darq = darq;
            var size = BufferSizeUtils.ServerBufferSize(networkSender.GetMaxSizeSettings);
            sendBufferPool = new SimpleObjectPool<ProducerResponseBuffer>(() =>
                new ProducerResponseBuffer(size, this.networkSender, sendBufferPool));
            this.responseQueue = responseQueue;
        }

        public override int TryConsumeMessages(byte* req_buf, int bytesReceived)
        {
            bytesRead = bytesReceived;
            readHead = 0;
            while (TryReadMessages(req_buf, out var offset))
                ProcessBatch(req_buf, offset);
            return readHead;
        }

        private bool TryReadMessages(byte* buf, out int offset)
        {
            offset = default;

            var bytesAvailable = bytesRead - readHead;
            // Need to at least have read off of size field on the message
            if (bytesAvailable < sizeof(int)) return false;

            // MSB is 1 to indicate binary protocol
            var size = -(*(int*)(buf + readHead));

            // Not all of the message has arrived
            if (bytesAvailable < size + sizeof(int)) return false;
            offset = readHead + sizeof(int);

            // Consume this message and the header
            readHead += size + sizeof(int);
            return true;
        }

        private void ProcessBatch(byte* buf, int offset)
        {
            var response = sendBufferPool.Checkout();
            byte* b = buf + offset;
            fixed (byte* d = response.buf)
            {
                var dend = d + response.buf.Length;
                var dcurr = d + sizeof(int); // reserve space for size

                var src = b;
                ref var header = ref Unsafe.AsRef<BatchHeader>(src);
                var num = header.NumMessages;
                src += BatchHeader.Size;
                dcurr += BatchHeader.Size;

                var dprResponseOffset = dcurr;
                dcurr += DprMessageHeader.FixedLenSize;
                start = 0;
                msgnum = 0;

                var dprHeaderSize = *(int*)src;
                src += sizeof(int);
                var request = new ReadOnlySpan<byte>(src, dprHeaderSize);
                src += dprHeaderSize;
                if (!darq.TryReceiveAndStartAction(request))
                {
                    for (msgnum = 0; msgnum < num; msgnum++)
                        hrw.Write((byte)DarqCommandType.INVALID, ref dcurr, (int)(dend - dcurr));
                    // Can immediately send DPR error version regardless of version or status
                    response.version = 0;
                }
                else
                {
                    for (msgnum = 0; msgnum < num; msgnum++)
                    {
                        var message = (DarqCommandType)(*src++);
                        Debug.Assert(message == DarqCommandType.DarqEnqueue);
                        var producer = *(long*)src;
                        src += sizeof(long);
                        var lsn = *(long*)src;
                        src += sizeof(long);
                        var batch = new SerializedDarqEntryBatch(src);

                        darq.Enqueue(batch, producer, lsn);
                        src += batch.TotalSize();
                        hrw.Write((byte)message, ref dcurr, (int)(dend - dcurr));
                    }

                    response.version = darq.Version();
                }


                darq.ProduceTagAndEndAction(new Span<byte>(dprResponseOffset, DprMessageHeader.FixedLenSize));
                SendResponseBuffer(d, dcurr, response);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SendResponseBuffer(byte* d, byte* dcurr, ProducerResponseBuffer response)
        {
            var dstart = d + sizeof(int);
            ((BatchHeader*)dstart)->SetNumMessagesProtocol(msgnum - start, (WireFormat)DarqProtocolType.DarqProducer);
            ((BatchHeader*)dstart)->SeqNo = seqNo++;
            var payloadSize = response.size = (int)(dcurr - d);
            // Set packet size in header
            *(int*)d = -(payloadSize - sizeof(int));

            if (responseQueue == null || response.version >= darq.CommittedVersion())
                // TODO(Tianyu): Figure out how to handle errors
                networkSender.SendResponse(response.buf, 0, payloadSize, response.Dispose);
            else
                responseQueue.Enqueue(response);
        }


        public override void Publish(ref byte* keyPtr, int keyLength, ref byte* valPtr, int valLength,
            ref byte* inputPtr, int sid)
        {
            throw new System.NotImplementedException();
        }

        public override void PrefixPublish(byte* prefixPtr, int prefixLength, ref byte* keyPtr, int keyLength,
            ref byte* valPtr, int valLength,
            ref byte* inputPtr, int sid)
        {
            throw new System.NotImplementedException();
        }
    }
}