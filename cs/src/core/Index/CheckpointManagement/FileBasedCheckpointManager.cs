using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace FASTER.core;

public class FileBasedCheckpointManager : ILogCommitManager, ICheckpointManager
{
    const byte indexTokenCount = 2;
    const byte logTokenCount = 1;
    const byte flogCommitCount = 1;

    protected readonly ICheckpointNamingScheme checkpointNamingScheme;
    protected readonly INamedDeviceFactory deviceFactory;
    private readonly bool removeOutdated;

    /// <summary>
    /// Track historical commits for automatic purging
    /// </summary>
    private readonly Guid[] indexTokenHistory, logTokenHistory;

    private readonly long[] flogCommitHistory;
    private byte indexTokenHistoryOffset, logTokenHistoryOffset, flogCommitHistoryOffset;

    readonly ILogger logger;
    readonly WorkQueueFIFO<long> deleteQueue;
    readonly int fastCommitThrottleFreq;
    int commitCount;

    public FileBasedCheckpointManager(INamedDeviceFactory deviceFactory, ICheckpointNamingScheme checkpointNamingScheme, bool removeOutdated = true,
        int fastCommitThrottleFreq = 0, ILogger logger = null)
    {
        this.logger = logger;
        this.checkpointNamingScheme = checkpointNamingScheme;
        this.fastCommitThrottleFreq = fastCommitThrottleFreq;
        this.deviceFactory = deviceFactory;

        this.removeOutdated = removeOutdated;
        if (removeOutdated)
        {
            deleteQueue = new WorkQueueFIFO<long>(prior => DeleteIfExists(checkpointNamingScheme.FasterLogCommitMetadata(prior)));

            // We keep two index checkpoints as the latest index might not have a
            // later log checkpoint to work with
            indexTokenHistory = new Guid[indexTokenCount];
            // We only keep the latest log checkpoint
            logTokenHistory = new Guid[logTokenCount];
            // // We only keep the latest FasterLog commit
            flogCommitHistory = new long[flogCommitCount];
        }
    }

    private void DeleteIfExists(FileDescriptor descriptor)
    {
        if (descriptor.fileName != null)
        {
            var file = new FileInfo(Path.Combine(this.checkpointNamingScheme.BaseName(), descriptor.directoryName,
                descriptor.fileName));
            if (file.Exists) file.Delete();
        }
        else
        {
            var dir = new DirectoryInfo(Path.Combine(this.checkpointNamingScheme.BaseName(), descriptor.directoryName));
            if (dir.Exists) dir.Delete(true);

        }
    }

    private FileStream GetFile(FileDescriptor fd)
    {
        var filename = Path.Combine(checkpointNamingScheme.BaseName(), fd.directoryName, fd.fileName);
        var path = new FileInfo(filename).Directory.FullName;
        if (!Directory.Exists(path)) Directory.CreateDirectory(path);
        return new FileStream(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite);
    }

    private byte[] ReadFileBodyFully(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read);
        using var reader = new BinaryReader(fs);
        var size = reader.ReadInt32();
        var result = new byte[size];
        for (var read = 0; read < size; )
        {
            var bytesReceived = reader.Read(result, sizeof(int) + read, size - read);
            if (bytesReceived == 0) throw new IOException("unexpected end of file");
            read += bytesReceived;
        }

        return result;
    }
    

    public void PurgeAll()
    {
        var dir = new DirectoryInfo(this.checkpointNamingScheme.BaseName());
        if (dir.Exists) dir.Delete(true);
    }

    /// <inheritdoc />
    public void Purge(Guid token)
    {
        DeleteIfExists(checkpointNamingScheme.LogCheckpointBase(token));
        DeleteIfExists(checkpointNamingScheme.IndexCheckpointBase(token));
    }

    #region ILogCommitManager

    /// <inheritdoc />
    public unsafe void Commit(long beginAddress, long untilAddress, byte[] commitMetadata, long commitNum,
        bool forceWriteMetadata)
    {
        if (!forceWriteMetadata && fastCommitThrottleFreq > 0 && (commitCount++ % fastCommitThrottleFreq != 0)) return;

        using var fileStream = GetFile(checkpointNamingScheme.FasterLogCommitMetadata(commitNum));

        // Two phase to ensure we write metadata in single Write operation
        using var writer = new BinaryWriter(fileStream);
        writer.Write(commitMetadata.Length);
        writer.Write(commitMetadata);
        writer.Flush();
        
        if (removeOutdated)
        {
            var prior = flogCommitHistory[flogCommitHistoryOffset];
            flogCommitHistory[flogCommitHistoryOffset] = commitNum;
            flogCommitHistoryOffset = (byte)((flogCommitHistoryOffset + 1) % flogCommitCount);
            if (prior != default)
            {
                // System.Threading.Tasks.Task.Run(() => deviceFactory.Delete(checkpointNamingScheme.FasterLogCommitMetadata(prior)));
                deleteQueue.EnqueueAndTryWork(prior, true);
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    private IEnumerable<FileDescriptor> ListContents(string path)
    {
        var pathInfo = new DirectoryInfo(Path.Combine(checkpointNamingScheme.BaseName(), path));
        if (!pathInfo.Exists) yield break;
        
        foreach (var folder in pathInfo.GetDirectories().OrderByDescending(f => f.LastWriteTime))
        {
            yield return new FileDescriptor(folder.Name, "");
        }

        foreach (var file in pathInfo.GetFiles().OrderByDescending(f => f.LastWriteTime))
        {
            yield return new FileDescriptor("", file.Name);
        }
    } 
    /// <inheritdoc />
    public IEnumerable<long> ListCommits()
    {
        return ListContents(checkpointNamingScheme.FasterLogCommitBasePath())
            .Select(e => checkpointNamingScheme.CommitNumber(e)).OrderByDescending(e => e);
    }

    /// <inheritdoc />
    public void RemoveCommit(long commitNum)
    {
        DeleteIfExists(checkpointNamingScheme.FasterLogCommitMetadata(commitNum));
    }

    /// <inheritdoc />
    public void RemoveAllCommits()
    {
        foreach (var commitNum in ListCommits())
            RemoveCommit(commitNum);
    }


    
    /// <inheritdoc />
    public byte[] GetCommitMetadata(long commitNum)
    {
        var fd = checkpointNamingScheme.FasterLogCommitMetadata(commitNum);
        return ReadFileBodyFully(Path.Combine(checkpointNamingScheme.BaseName(), fd.directoryName, fd.fileName));
    }

    #endregion

    #region ICheckpointManager

    /// <inheritdoc />
    public unsafe void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
    {
        using var file = GetFile(checkpointNamingScheme.IndexCheckpointMetadata(indexToken));
        // Two phase to ensure we write metadata in single Write operation
        using var writer = new BinaryWriter(file);
        writer.Write(commitMetadata.Length);
        writer.Write(commitMetadata);
        writer.Flush();

        if (removeOutdated)
        {
            var prior = indexTokenHistory[indexTokenHistoryOffset];
            indexTokenHistory[indexTokenHistoryOffset] = indexToken;
            indexTokenHistoryOffset = (byte)((indexTokenHistoryOffset + 1) % indexTokenCount);
            if (prior != default)
                DeleteIfExists(checkpointNamingScheme.IndexCheckpointBase(prior));
        }
    }

    /// <inheritdoc />
    public IEnumerable<Guid> GetIndexCheckpointTokens()
    {
        return ListContents(checkpointNamingScheme.IndexCheckpointBasePath())
            .Select(e => checkpointNamingScheme.Token(e));
    }

    /// <inheritdoc />
    public byte[] GetIndexCheckpointMetadata(Guid indexToken)
    {
        var fd = checkpointNamingScheme.IndexCheckpointMetadata(indexToken);
        return ReadFileBodyFully(Path.Combine(checkpointNamingScheme.BaseName(), fd.directoryName, fd.fileName));
    }

    /// <inheritdoc />
    public virtual unsafe void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
    {
        
        using var file = GetFile(checkpointNamingScheme.LogCheckpointMetadata(logToken));
        // Two phase to ensure we write metadata in single Write operation
        using var writer = new BinaryWriter(file);
        writer.Write(commitMetadata.Length);
        writer.Write(commitMetadata);
        writer.Flush();

        if (removeOutdated)
        {
            var prior = logTokenHistory[logTokenHistoryOffset];
            logTokenHistory[logTokenHistoryOffset] = logToken;
            logTokenHistoryOffset = (byte)((logTokenHistoryOffset + 1) % logTokenCount);
            if (prior != default)
                DeleteIfExists(checkpointNamingScheme.LogCheckpointBase(prior));
        }
    }

    /// <inheritdoc />
    public virtual unsafe void CommitLogIncrementalCheckpoint(Guid logToken, long version, byte[] commitMetadata,
        DeltaLog deltaLog)
    {
        deltaLog.Allocate(out int length, out long physicalAddress);
        if (length < commitMetadata.Length)
        {
            deltaLog.Seal(0, DeltaLogEntryType.CHECKPOINT_METADATA);
            deltaLog.Allocate(out length, out physicalAddress);
            if (length < commitMetadata.Length)
            {
                deltaLog.Seal(0);
                throw new Exception(
                    $"Metadata of size {commitMetadata.Length} does not fit in delta log space of size {length}");
            }
        }

        fixed (byte* ptr = commitMetadata)
        {
            Buffer.MemoryCopy(ptr, (void*)physicalAddress, commitMetadata.Length, commitMetadata.Length);
        }

        deltaLog.Seal(commitMetadata.Length, DeltaLogEntryType.CHECKPOINT_METADATA);
        deltaLog.FlushAsync().Wait();
    }

    /// <inheritdoc />
    public IEnumerable<Guid> GetLogCheckpointTokens()
    {
        return ListContents(checkpointNamingScheme.LogCheckpointBasePath())
            .Select(e => checkpointNamingScheme.Token(e));
    }

    /// <inheritdoc />
    public virtual byte[] GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog, bool scanDelta, long recoverTo)
    {
        byte[] metadata = null;
        if (deltaLog != null && scanDelta)
        {
            // Try to get latest valid metadata from delta-log
            deltaLog.Reset();
            while (deltaLog.GetNext(out long physicalAddress, out int entryLength, out var type))
            {
                switch (type)
                {
                    case DeltaLogEntryType.DELTA:
                        // consider only metadata records
                        continue;
                    case DeltaLogEntryType.CHECKPOINT_METADATA:
                        metadata = new byte[entryLength];
                        unsafe
                        {
                            fixed (byte* m = metadata)
                                Buffer.MemoryCopy((void*)physicalAddress, m, entryLength, entryLength);
                        }

                        HybridLogRecoveryInfo recoveryInfo = new();
                        using (StreamReader s = new(new MemoryStream(metadata)))
                        {
                            recoveryInfo.Initialize(s);
                            // Finish recovery if only specific versions are requested
                            if (recoveryInfo.version == recoverTo || recoveryInfo.version < recoverTo &&
                                recoveryInfo.nextVersion > recoverTo) goto LoopEnd;
                        }

                        continue;
                    default:
                        throw new FasterException("Unexpected entry type");
                }

                LoopEnd:
                break;
            }

            if (metadata != null) return metadata;
        }
        
        var fd = checkpointNamingScheme.LogCheckpointMetadata(logToken);
        return ReadFileBodyFully(Path.Combine(checkpointNamingScheme.BaseName(), fd.directoryName, fd.fileName));
    }

    /// <inheritdoc />
    public IDevice GetIndexDevice(Guid indexToken)
    {
        return deviceFactory.Get(checkpointNamingScheme.HashTable(indexToken));
    }

    /// <inheritdoc />
    public IDevice GetSnapshotLogDevice(Guid token)
    {
        return deviceFactory.Get(checkpointNamingScheme.LogSnapshot(token));
    }

    /// <inheritdoc />
    public IDevice GetSnapshotObjectLogDevice(Guid token)
    {
        return deviceFactory.Get(checkpointNamingScheme.ObjectLogSnapshot(token));
    }

    /// <inheritdoc />
    public IDevice GetDeltaLogDevice(Guid token)
    {
        return deviceFactory.Get(checkpointNamingScheme.DeltaLog(token));
    }

    /// <inheritdoc />
    public void InitializeIndexCheckpoint(Guid indexToken)
    {
    }

    /// <inheritdoc />
    public void InitializeLogCheckpoint(Guid logToken)
    {
    }

    /// <inheritdoc />
    public void OnRecovery(Guid indexToken, Guid logToken)
    {
        if (!removeOutdated) return;

        // Add recovered tokens to history, for eventual purging
        if (indexToken != default)
        {
            indexTokenHistory[indexTokenHistoryOffset] = indexToken;
            indexTokenHistoryOffset = (byte)((indexTokenHistoryOffset + 1) % indexTokenCount);
        }

        if (logToken != default)
        {
            logTokenHistory[logTokenHistoryOffset] = logToken;
            logTokenHistoryOffset = (byte)((logTokenHistoryOffset + 1) % logTokenCount);
        }

        // Purge all log checkpoints that were not used for recovery
        foreach (var recoveredLogToken in GetLogCheckpointTokens())
        {
            if (recoveredLogToken != logToken)
                DeleteIfExists(checkpointNamingScheme.LogCheckpointBase(recoveredLogToken));
        }

        // Purge all index checkpoints that were not used for recovery
        foreach (var recoveredIndexToken in GetIndexCheckpointTokens())
        {
            if (recoveredIndexToken != indexToken)
                DeleteIfExists(checkpointNamingScheme.IndexCheckpointBase(recoveredIndexToken));
        }
    }

    /// <inheritdoc />
    public void OnRecovery(long commitNum)
    {
        if (!removeOutdated) return;

        foreach (var recoveredCommitNum in ListCommits())
            if (recoveredCommitNum != commitNum)
                RemoveCommit(recoveredCommitNum);

        // Add recovered tokens to history, for eventual purging
        if (commitNum != default)
        {
            flogCommitHistory[flogCommitHistoryOffset] = commitNum;
            flogCommitHistoryOffset = (byte)((flogCommitHistoryOffset + 1) % flogCommitCount);
        }
    }
    #endregion
    

    /// <inheritdoc />
    public virtual void CheckpointVersionShift(long oldVersion, long newVersion)
    {
    }
}