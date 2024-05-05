using FASTER.darq;

namespace darq.client;

public interface IDarqProducer : IDisposable
{
    public void EnqueueMessageWithCallback(DarqId darqId, ReadOnlySpan<byte> message, Action<bool> callback,
        long producerId, long lsn);

    public void ForceFlush();
}