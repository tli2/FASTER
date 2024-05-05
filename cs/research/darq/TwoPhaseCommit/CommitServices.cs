using System.Collections.Concurrent;
using System.Diagnostics;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.Hosting;
using Nito.AsyncEx;
using protobuf;

namespace TwoPhaseCommit;

public enum CommitStatus
{
    STARTED,
    PREPARING,
    COMMIT,
    ABORT
}

public class CommitLog : StateObject
{
    private FasterLogSettings settings;
    public FasterLog log;
    public ConcurrentDictionary<long, CommitStatus> transactions = new();


    public CommitLog(FasterLogSettings settings, IVersionScheme versionScheme, DprWorkerOptions options) : base(
        versionScheme, options)
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

    public override unsafe void RestoreCheckpoint(long version, out ReadOnlySpan<byte> metadata)
    {
        log = new FasterLog(settings);
        log.Recover(version);
        metadata = log.RecoveredCookie;
        var iterator = log.Scan(0, long.MaxValue);

        while (iterator.UnsafeGetNext(out var bytes, out var len, out _, out _))
        {
            var m = TwoPCMessage.Parser.ParseFrom(new Span<byte>(bytes, len));
            iterator.UnsafeRelease();
            switch (m.Type)
            {
                case TwoPCMessageType.Start:
                    transactions[m.TxnId] = CommitStatus.STARTED;
                    break;
                case TwoPCMessageType.VoteY:
                    transactions[m.TxnId] = CommitStatus.PREPARING;
                    break;
                case TwoPCMessageType.Commit:
                    transactions[m.TxnId] = CommitStatus.COMMIT;
                    break;
                case TwoPCMessageType.Abort:
                    transactions[m.TxnId] = CommitStatus.ABORT;
                    break;
                default:
                    throw new NotImplementedException();
            }
        }

        iterator.Dispose();
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

public class CommitLogBackgroundService : BackgroundService
{
    public CommitLog so;

    public CommitLogBackgroundService(CommitLog so)
    {
        this.so = so;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        so.ConnectToCluster(out _);
        // Use a large number to force participants to synchronize on their commit schedule on the first message
        so.ForceCheckpoint(100000);
        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
    }
}

public struct TwoPCMessageEntryWrapper : ILogEnqueueEntry
{
    private TwoPCMessage message;

    public TwoPCMessageEntryWrapper(TwoPCMessage m)
    {
        message = m;
    }

    public int SerializedLength => message.CalculateSize();

    public void SerializeTo(Span<byte> dest)
    {
        message.WriteTo(dest);
    }
}

public class CommitCoordinatorSettings
{
    public List<GrpcChannel> participants;
    public bool speculative;
}

public class BackgroundSender
{
    private CommitCoordinatorSettings settings;
    private AsyncCollection<TwoPCMessage> messages = new AsyncCollection<TwoPCMessage>();
    private SemaphoreSlim rateLimiter = new SemaphoreSlim(4);
    private SimpleObjectPool<DprSession> sessionPool = new SimpleObjectPool<DprSession>(() => new DprSession());

    public BackgroundSender(CommitCoordinatorSettings settings)
    {
        this.settings = settings;
        Task.Run(async () =>
        {
            while (await messages.OutputAvailableAsync())
            {
                var m = messages.Take();
                await rateLimiter.WaitAsync();
                var session = sessionPool.Checkout();
                // Don't care about dependencies --- just need acks to come back fast 
                session.UnsafeReset();
                try
                {
                    foreach (var p in settings.participants)
                    {
                        var client =
                            new CommitParticipantService.CommitParticipantServiceClient(
                                p.Intercept(new DprClientInterceptor(session)));
                        await client.SendMessageAsync(m);
                    }
                }
                catch (Exception e)
                {
                    // Ignore exceptions
                }
                finally
                {
                    rateLimiter.Release();
                    sessionPool.Return(session);
                }
            }
        });
    }
    

    // Either a commit or abort to send to participants in the background
    public void AddOutcome(TwoPCMessage message)
    {
        messages.Add(message);
    }
}

// TODO(Tianyu): Currently not properly fault-tolerant with retries, etc, but will not cause anomalies.
public class CommitCoordinatorServiceImpl : CommitCoordinatorService.CommitCoordinatorServiceBase
{
    private CommitCoordinatorSettings settings;
    private CommitLogBackgroundService backend;
    private BackgroundSender sender;

    public CommitCoordinatorServiceImpl(CommitLogBackgroundService backend, CommitCoordinatorSettings settings)
    {
        this.settings = settings;
        this.backend = backend;
        this.sender = new BackgroundSender(settings);
    }

    private async Task<TwoPCMessage> SendMessageAbortOnFailure(GrpcChannel p, DprSession session, TwoPCMessage m) 
    {
        var client = settings.speculative ?
            new CommitParticipantService.CommitParticipantServiceClient(
                p.Intercept(new DprClientInterceptor(session)))
            : new CommitParticipantService.CommitParticipantServiceClient(p);

        try
        {
            return await client.SendMessageAsync(m);
        }
        catch (Exception e)
        {
            return new TwoPCMessage
            {
                Type = TwoPCMessageType.VoteN,
                TxnId = m.TxnId,
            };
        }
    }
    
    public override async Task<TransactionResponse> Commit(TransactionsRequest request, ServerCallContext context)
    {
        var session = backend.so.DetachFromWorkerAndPauseAction();
        var tasks = new List<Task<TwoPCMessage>>(4);
        foreach (var p in settings.participants)
        {
            var message = new TwoPCMessage
            {
                Type = TwoPCMessageType.Prepare,
                TxnId = request.TxnId,
            };
            var task = SendMessageAbortOnFailure(p, session, message);
            tasks.Add(task);
        }

        await Task.WhenAll(tasks);
        if (await backend.so.TryMergeAndStartActionAsync(session))
        {
            var response = new TransactionResponse();
            foreach (var t in tasks)
            {
                if (t.Result.Type == TwoPCMessageType.VoteN)
                {
                    backend.so.transactions[request.TxnId] = CommitStatus.ABORT;
                    response.Success = false;
                    sender.AddOutcome(new TwoPCMessage
                    {
                        Type = TwoPCMessageType.Abort,
                        TxnId = request.TxnId,
                    });
                    return response;
                }

                Debug.Assert(t.Result.Type == TwoPCMessageType.VoteY);
            }

            backend.so.transactions[request.TxnId] = CommitStatus.COMMIT;
            backend.so.log.Enqueue(new TwoPCMessageEntryWrapper(new TwoPCMessage
            {
                Type = TwoPCMessageType.Commit,
                TxnId = request.TxnId,
            }), null);
            response.Success = true;
            sender.AddOutcome(new TwoPCMessage
            {
                Type = TwoPCMessageType.Commit,
                TxnId = request.TxnId,
            });
            return response;
        }

        throw new DprSessionRolledBackException(backend.so.WorldLine());
    }
}

public class CommitParticipantServiceImpl : CommitParticipantService.CommitParticipantServiceBase
{
    private CommitLogBackgroundService backend;

    public CommitParticipantServiceImpl(CommitLogBackgroundService backend)
    {
        this.backend = backend;
    }

    public override Task<ForceFailoverMessage> ForceFailover(ForceFailoverMessage request, ServerCallContext context)
    {
        backend.so.EndAction();
        backend.so.ForceFailover();
        backend.so.StartLocalAction();
        return Task.FromResult(new ForceFailoverMessage());
    }

    public override Task<TransactionResponse> StartTransaction(TransactionsRequest request, ServerCallContext context)
    {
        Debug.Assert(!backend.so.transactions.ContainsKey(request.TxnId));
        backend.so.log.Enqueue(new TwoPCMessageEntryWrapper(new TwoPCMessage
        {
            Type = TwoPCMessageType.Start,
            TxnId = request.TxnId,
        }), null);
        backend.so.transactions[request.TxnId] = CommitStatus.STARTED;
        return Task.FromResult(new TransactionResponse
        {
            Success = true
        });
    }

    public override Task<TwoPCMessage> SendMessage(TwoPCMessage request, ServerCallContext context)
    {
        var response = new TwoPCMessage
        {
            TxnId = request.TxnId,
        };
        switch (request.Type)
        {
            case TwoPCMessageType.Prepare:
                if (!backend.so.transactions.TryGetValue(request.TxnId, out var status))
                {
                    // This transaction is lost. Cannot proceed
                    response.Type = TwoPCMessageType.VoteN;
                }
                else
                {
                    response.Type = TwoPCMessageType.VoteY;
                    if (status == CommitStatus.STARTED)
                    {
                        backend.so.log.Enqueue(new TwoPCMessageEntryWrapper(response), null);
                        backend.so.transactions[request.TxnId] = CommitStatus.PREPARING;
                    }
                }
                break;
            case TwoPCMessageType.Commit:
                response.Type = TwoPCMessageType.Commit;
                backend.so.log.Enqueue(new TwoPCMessageEntryWrapper(response), null);
                response.Type = TwoPCMessageType.Ack;
                backend.so.transactions[request.TxnId] = CommitStatus.COMMIT;
                break;
            case TwoPCMessageType.Abort:
                response.Type = TwoPCMessageType.Abort;
                backend.so.log.Enqueue(new TwoPCMessageEntryWrapper(response), null);
                response.Type = TwoPCMessageType.Ack;
                backend.so.transactions.TryRemove(request.TxnId, out _);
                break;
            default:
                throw new NotImplementedException();
        }

        return Task.FromResult(response);
    }
}