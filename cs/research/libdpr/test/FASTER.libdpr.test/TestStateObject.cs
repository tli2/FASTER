using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
namespace FASTER.libdpr;

public class TestMessage
{
    internal byte[] dprHeader = new byte[1 << 15];
    internal WorkerId originator;
    internal long originatorStateSerialNum;
}

public class TestStateObject : IStateObject
{
    private struct TestCheckpointInfo
    {
        internal long stateSerialNum;
        internal int numReceivedMessages;
        internal byte[] dprMetadata;
    }
    
    private WorkerId me;
    private List<TestMessage> receivedMessages = new();
    public long stateSerialNum;
    private long persistedSerialNum;
    private Dictionary<long, TestCheckpointInfo> checkpoints = new();
    private Dictionary<long, Action> pendingPersists;

    public TestStateObject(WorkerId me, bool autoCompleteCheckpoints = true)
    {
        this.me = me; 
        if (!autoCompleteCheckpoints) pendingPersists = new Dictionary<long, Action>();
    }

    public void Receive(TestMessage m)
    {
        Interlocked.Increment(ref stateSerialNum);
    }

    public void DoLocalWork()
    {
        Interlocked.Increment(ref stateSerialNum);
    }

    public TestMessage GenerateMessageToSend()
    {
        var incremented = Interlocked.Increment(ref stateSerialNum);
        return new TestMessage
        {
            originator = me,
            originatorStateSerialNum = incremented
        };
    }

    public void PerformCheckpoint(long version, ReadOnlySpan<byte> metadata, Action onPersist)
    {
        // TODO(Tianyu): This might not be thread-safe in the new version of DPR that does not extend epoch protection
        // into client code
        var capturedState = stateSerialNum;
        var capturedMessageCount = receivedMessages.Count;
        var metadataArray = metadata.ToArray();
        var callback = () =>
        {
            persistedSerialNum = capturedState;
            checkpoints[version] = new TestCheckpointInfo
            {
                stateSerialNum = capturedState,
                numReceivedMessages = capturedMessageCount,
                dprMetadata = metadataArray
            };
            onPersist();
        };
        
        // Immediately complete checkpoint if auto completion is on
        if (pendingPersists == null)
            callback();
        else
            pendingPersists[version] = callback;
    }

    public void CompleteCheckpoint(long version)
    {
        Debug.Assert(pendingPersists != null);
        pendingPersists[version]();
    }

    public void RestoreCheckpoint(long version, out ReadOnlySpan<byte> metadata)
    {
        var restored = checkpoints[version];
        persistedSerialNum = stateSerialNum = restored.stateSerialNum;
        receivedMessages.RemoveRange(restored.numReceivedMessages,
            receivedMessages.Count - restored.numReceivedMessages);
        metadata = restored.dprMetadata;
    }

    public void PruneVersion(long version)
    {
        checkpoints.Remove(version);
    }

    public IEnumerable<(byte[], int)> GetUnprunedVersions()
    {
        return checkpoints.Values.Select(info => ValueTuple.Create(info.dprMetadata, 0));
    }
}