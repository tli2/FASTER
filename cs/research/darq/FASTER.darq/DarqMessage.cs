using System.Diagnostics;
using System.Text;
using System.Text.Unicode;
using FASTER.common;
using FASTER.core;
using FASTER.darq;

namespace FASTER.libdpr
{
    public enum DarqProtocolType : byte
    {
        DarqSubscribe = 125,
        DarqProcessor = 126,
        DarqProducer = 127
    }

    /// <summary>
    /// DARQ Command Type
    /// </summary>
    public enum DarqCommandType : byte
    {
        INVALID = 0,

        /// <summary>
        /// DARQ Enqueue
        /// </summary>
        DarqEnqueue,

        /// <summary>
        /// DARQ Step
        /// </summary>
        DarqStep,

        /// <summary>
        /// DARQ Register Processor
        /// </summary>
        DarqRegisterProcessor,

        /// <summary>
        ///  DARQ start push
        /// </summary>
        DarqStartPush,
    }

    /// <summary>
    /// DARQ Message Type
    /// </summary>
    public enum DarqMessageType : byte
    {
        /// <summary></summary>
        IN,

        /// <summary></summary>
        OUT,

        /// <summary></summary>
        RECOVERY,

        /// <summary></summary>
        COMPLETION,
    }

    /// <summary>
    /// DARQ Message 
    /// </summary>
    public class DarqMessage : IDisposable
    {
        private DarqMessageType type;
        private long lsn, nextLsn;
        private byte[] message;
        private int messageSize;
        private SimpleObjectPool<DarqMessage> messagePool;

        /// <summary>
        /// Create a new DarqMessage object
        /// </summary>
        /// <param name="messagePool"> (optional) the object pool this message should be returned to on disposal </param>
        public DarqMessage(SimpleObjectPool<DarqMessage> messagePool = null)
        {
            message = new byte[1 << 20];
            this.messagePool = messagePool;
        }

        /// <inheritdoc/>
        public void Dispose() => messagePool?.Return(this);

        /// <summary></summary>
        /// <returns>Type of message</returns>
        public DarqMessageType GetMessageType() => type;

        /// <summary></summary>
        /// <returns>LSN of the message</returns>
        public long GetLsn() => lsn;

        /// <summary></summary>
        /// <returns>Lower bound for the LSN of the immediate next message in DARQ</returns>
        public long GetNextLsn() => nextLsn;

        /// <summary></summary>
        /// <returns>Get the message bytes</returns>
        public ReadOnlySpan<byte> GetMessageBody() => new(message, 0, messageSize);

        /// <summary>
        /// Reset this message to hold supplied values instead
        /// </summary>
        /// <param name="type"></param>
        /// <param name="lsn"></param>
        /// <param name="nextLsn"></param>
        /// <param name="msg"></param>
        public void Reset(DarqMessageType type, long lsn, long nextLsn, ReadOnlySpan<byte> msg)
        {
            this.type = type;
            this.lsn = lsn;
            this.nextLsn = nextLsn;
            Debug.Assert(message.Length > msg.Length);
            msg.CopyTo(message);
            messageSize = msg.Length;
        }
    }

    /// <summary>
    /// StepRequests represents a DARQ step
    /// </summary>
    public class StepRequest : IReadOnlySpanBatch
    {
        internal List<long> consumedMessages;
        internal List<int> offsets;
        internal int size;
        internal byte[] serializationBuffer;

        /// <summary>
        /// Create a new StepRequest object.
        /// </summary>
        /// <param name="requestPool"> (optional) the object pool this request should be returned to on disposal </param>
        public StepRequest()
        {
            serializationBuffer = new byte[1 << 15];
            consumedMessages = new List<long>();
            offsets = new List<int>();
        }

        internal void Reset()
        {
            consumedMessages.Clear();
            offsets.Clear();
            size = 0;
        }

        /// <summary></summary>
        /// <returns>list of messages consumed in this step</returns>
        public List<long> ConsumedMessages() => consumedMessages;

        /// <inheritdoc/>
        public int TotalEntries()
        {
            return offsets.Count;
        }

        /// <summary>
        /// Grow the underlying serialization buffer to be double of its original size, in case the step no longer fits.
        /// </summary>
        public void Grow()
        {
            var oldBuffer = serializationBuffer;
            serializationBuffer = new byte[2 * oldBuffer.Length];
            Array.Copy(oldBuffer, serializationBuffer, oldBuffer.Length);
        }

        /// <inheritdoc/>
        public ReadOnlySpan<byte> Get(int index)
        {
            return new Span<byte>(serializationBuffer, offsets[index],
                (index == (offsets.Count - 1) ? size : offsets[index + 1]) - offsets[index]);
        }
    }

    /// <summary>
    /// Builder to populate StepRequest
    /// </summary>
    public struct StepRequestBuilder
    {
        private StepRequest request;

        /// <summary>
        /// Constructs a new StepRequestBuilder
        /// </summary>
        /// <param name="toBuild"> the StepRequest object to populate </param>
        /// <param name="me"> ID of the DARQ instance the step is for </param>
        public StepRequestBuilder(StepRequest toBuild)
        {
            request = toBuild;
            request.Reset();
        }

        /// <summary>
        /// Mark a message as consumed by this step 
        /// </summary>
        /// <param name="lsn"> LSN of the consumed message </param>
        /// <returns>self-reference for chaining</returns>
        public StepRequestBuilder MarkMessageConsumed(long lsn)
        {
            request.consumedMessages.Add(lsn);
            return this;
        }

        /// <summary>
        /// Add an out message to this step.
        /// </summary>
        /// <param name="recipient">Intended recipient</param>
        /// <param name="message">message body, in bytes</param>
        /// <returns> self-reference for chaining </returns>
        public unsafe StepRequestBuilder AddOutMessage(DarqId recipient, ReadOnlySpan<byte> message)
        {
            while (request.serializationBuffer.Length - request.size <
                   message.Length + sizeof(DarqMessageType) + sizeof(DarqId))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;

                *(DarqMessageType*)head++ = DarqMessageType.OUT;
                *(DarqId*)head = recipient;
                head += sizeof(DarqId);

                message.CopyTo(new Span<byte>(head, message.Length));
                head += message.Length;
                request.size = (int)(head - b);
            }

            return this;
        }

        public unsafe StepRequestBuilder AddOutMessage(DarqId recipient, ILogEnqueueEntry message)
        {
            var messageLength = message.SerializedLength;
            while (request.serializationBuffer.Length - request.size <
                   messageLength + sizeof(DarqMessageType) + sizeof(DarqId))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;

                *(DarqMessageType*)head++ = DarqMessageType.OUT;
                *(DarqId*)head = recipient;
                head += sizeof(DarqId);

                message.SerializeTo(new Span<byte>(head, messageLength));
                head += messageLength;
                request.size = (int)(head - b);
            }

            return this;
        }
        
        public unsafe StepRequestBuilder AddOutMessage(DarqId recipient, string message)
        {
            while (request.serializationBuffer.Length - request.size <
                   message.Length + sizeof(DarqMessageType) + sizeof(DarqId))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;

                *(DarqMessageType*) head++ = DarqMessageType.OUT;
                *(DarqId*) head = recipient;
                head += sizeof(DarqId);
                
                var ret = Encoding.UTF8.GetBytes(message, new Span<byte>(head, message.Length));
                Debug.Assert(ret == message.Length);
                head += message.Length;
                request.size = (int) (head - b);
            }

            return this;
        }

        /// <summary>
        /// Add an out message to this step.
        /// </summary>
        /// <param name="recipient">Intended recipient</param>
        /// <param name="message">message body, in bytes</param>
        /// <returns> self-reference for chaining </returns>
        public unsafe StepRequestBuilder AddSelfMessage(ReadOnlySpan<byte> message)
        {
            while (request.serializationBuffer.Length - request.size <
                   message.Length + sizeof(DarqMessageType))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                *(DarqMessageType*)head++ = DarqMessageType.IN;
                message.CopyTo(new Span<byte>(head, message.Length));
                head += message.Length;
                request.size = (int)(head - b);
            }

            return this;
        }
        
        public unsafe StepRequestBuilder AddSelfMessage(long partitionId, ReadOnlySpan<byte> message)
        {
            while (request.serializationBuffer.Length - request.size <
                   sizeof(long) + message.Length + sizeof(DarqMessageType))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                *(DarqMessageType*)head++ = DarqMessageType.IN;
                *(long*)head = partitionId;
                head += sizeof(long);
                message.CopyTo(new Span<byte>(head, message.Length));
                head += message.Length;
                request.size = (int)(head - b);
            }

            return this;
        }

        public unsafe StepRequestBuilder AddSelfMessage(ILogEnqueueEntry message)
        {
            var messageLength = message.SerializedLength;
            while (request.serializationBuffer.Length - request.size <
                   messageLength + sizeof(DarqMessageType))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                *(DarqMessageType*)head++ = DarqMessageType.IN;
                message.SerializeTo(new Span<byte>(head, messageLength));
                head += messageLength;
                request.size = (int)(head - b);
            }

            return this;
        }
        
        public unsafe StepRequestBuilder AddSelfMessage(string message)
        {
            while (request.serializationBuffer.Length - request.size <
                   message.Length + sizeof(DarqMessageType))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;

                *(DarqMessageType*) head++ = DarqMessageType.IN;
                head += sizeof(DarqMessageType);
                
                Encoding.UTF8.GetBytes(message, new Span<byte>(head, message.Length));
                head += message.Length;
                request.size = (int) (head - b);
            }

            return this;
        }

        
        /// <summary>
        /// Add a self message to this step.
        /// </summary>
        /// <param name="message">message body, as bytes</param>
        /// <returns> self-reference for chaining </returns>
        public unsafe StepRequestBuilder AddRecoveryMessage(ReadOnlySpan<byte> message)
        {
            while (request.serializationBuffer.Length - request.size <
                   message.Length + sizeof(DarqMessageType))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                *(DarqMessageType*)head++ = DarqMessageType.RECOVERY;
                message.CopyTo(new Span<byte>(head, message.Length));
                head += message.Length;
                request.size = (int)(head - b);
            }

            return this;
        }
        
        public unsafe StepRequestBuilder AddRecoveryMessage(long partitionId, ReadOnlySpan<byte> message)
        {
            while (request.serializationBuffer.Length - request.size <
                   sizeof(long) + message.Length + sizeof(DarqMessageType))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                *(DarqMessageType*)head++ = DarqMessageType.RECOVERY;
                *(long*)head = partitionId;
                head += sizeof(long);
                message.CopyTo(new Span<byte>(head, message.Length));
                head += message.Length;
                request.size = (int)(head - b);
            }

            return this;
        }

        public unsafe StepRequestBuilder AddRecoveryMessage(ILogEnqueueEntry message)
        {
            var messageLength = message.SerializedLength;

            while (request.serializationBuffer.Length - request.size <
                   messageLength + sizeof(DarqMessageType))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                *(DarqMessageType*)head++ = DarqMessageType.RECOVERY;
                message.SerializeTo(new Span<byte>(head, messageLength));
                head += messageLength;
                request.size = (int)(head - b);
            }

            return this;
        }

        /// <summary>
        /// Finishes a step for submission
        /// </summary>
        /// <returns> composed step object </returns>
        public unsafe StepRequest FinishStep()
        {
            // Step needs to do something at least
            if (request.consumedMessages.Count < 1 && request.offsets.Count == 0)
                throw new FasterException("Empty step detected");

            while (request.serializationBuffer.Length - request.size <
                   sizeof(DarqMessageType) + sizeof(long) * request.consumedMessages.Count)
                request.Grow();

            if (request.consumedMessages.Count != 0)
            {
                request.offsets.Add(request.size);
                fixed (byte* b = request.serializationBuffer)
                {
                    var head = b + request.size;
                    *(DarqMessageType*)head++ = DarqMessageType.COMPLETION;
                    foreach (var lsn in request.consumedMessages)
                    {
                        *(long*)head = lsn;
                        head += sizeof(long);
                    }

                    request.size = (int)(head - b);
                }
            }

            return request;
        }
    }
}