using System.Diagnostics;
using FASTER.core;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using protobuf;
using Status = Grpc.Core.Status;

namespace dse.services.splog;

public class SpeculativeLog : StateObject
{
    private FasterLogSettings settings;
    public FasterLog log;
    
    public SpeculativeLog(FasterLogSettings settings, IVersionScheme versionScheme, DprWorkerOptions options) : base(versionScheme, options)
    {
        this.settings = settings;
        log = new FasterLog(settings);
    }

    public override void Dispose()
    {
        log.Dispose();
    }
    
    public override void PerformCheckpoint(long version, ReadOnlySpan<byte> metadata, Action onPersist)
    {
        log.CommitStrongly(out _, out _, false, metadata.ToArray(), version, onPersist);
    }

    public override void RestoreCheckpoint(long version, out ReadOnlySpan<byte> metadata)
    {
        log = new FasterLog(settings);
        log.Recover(version);
        metadata = log.RecoveredCookie;
    }

    public override void PruneVersion(long version)
    {
        settings.LogCommitManager.RemoveCommit(version);
    }

    public override IEnumerable<Memory<byte>> GetUnprunedVersions()
    {
        var commits = settings.LogCommitManager.ListCommits().ToList();
        return commits.Select(commitNum =>
        {
            // TODO(Tianyu): hacky
            var newLog = new FasterLog(settings);
            newLog.Recover(commitNum);
            var commitCookie = newLog.RecoveredCookie;
            newLog.Dispose();
            return new Memory<byte>(commitCookie);
        });    
    }
}

public class SplogBackgroundService : BackgroundService
{
    private SpeculativeLog backend;
    private ILogger<SplogBackgroundService> logger;

    public SplogBackgroundService(SpeculativeLog backend, ILogger<SplogBackgroundService> logger)
    {
        this.backend = backend;
        this.logger = logger;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Splog is starting...");
        backend.ConnectToCluster(out _);
        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        logger.LogInformation("Splog is shutting down");
    }

    public Task<SplogAppendResponse> Append(SplogAppendRequest request)
    {
        var lsn = backend.log.Enqueue(request.Entry.Span);
        return Task.FromResult(new SplogAppendResponse
        {
            Ok = true,
            Lsn = lsn
        });
    }

    private unsafe bool TryReadOneEntry(FasterLogScanIterator iterator, SplogScanResponse response)
    {
        if (!iterator.UnsafeGetNext(out var entry, out var entryLength, out var lsn, out var nextAddress)) return false;
        response.Entries.Add(new SplogEntry
        {
            Entry = ByteString.CopyFrom(new Span<byte>(entry, entryLength)),
            Lsn = lsn
        });
        response.NextLsn = nextAddress;
        iterator.UnsafeRelease();
        return true;
    }

    private async Task<bool> NextEntryWithTimeOut(FasterLogScanIterator iterator, int timeoutMilli, Stopwatch timer)
    {
        var currentTime = timer.ElapsedMilliseconds;
        if (currentTime > timeoutMilli) return false;
        var nextEntry = iterator.WaitAsync().AsTask();
        var session = backend.DetachFromWorkerAndPauseAction();
        var result =  await Task.WhenAny(nextEntry, Task.Delay((int)(timeoutMilli - currentTime)));
        if (!await backend.TryMergeAndStartActionAsync(session))
            throw new RpcException(Status.DefaultCancelled);
        return result == nextEntry;
    }

    public async Task<SplogScanResponse> Scan(SplogScanRequest request)
    {
        var responseObject = new SplogScanResponse();
        var scanner = backend.log.Scan(request.StartLsn, request.EndLsn, recover: false, scanUncommitted: true);
        var timer = Stopwatch.StartNew();
        for (var i = 0; i < request.MaxChunkSize; i++)
        {
            if (TryReadOneEntry(scanner, responseObject)) continue;
            if (!await NextEntryWithTimeOut(scanner, request.TimeoutMilli, timer)) break;
        }

        return responseObject;
    }

    public Task<SplogTruncateResponse> Truncate(SplogTruncateRequest request)
    {
        backend.log.TruncateUntil(request.NewStartLsn);
        return Task.FromResult(new SplogTruncateResponse
        {
            Ok = true
        });
    }
}

public class SplogService : protobuf.SplogService.SplogServiceBase
{
    private SplogBackgroundService backend;

    public SplogService(SplogBackgroundService backend)
    {
        this.backend = backend;
    }

    public override Task<SplogAppendResponse> Append(SplogAppendRequest request, ServerCallContext context)
    {
        return backend.Append(request);
    }
    

    public override Task<SplogScanResponse> Scan(SplogScanRequest request, ServerCallContext context)
    {
        return backend.Scan(request);
    }

    public override Task<SplogTruncateResponse> Truncate(SplogTruncateRequest request, ServerCallContext context)
    {
        return backend.Truncate(request);
    }
}