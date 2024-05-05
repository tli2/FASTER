using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using darq.client;
using FASTER.client;
using FASTER.common;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using pubsub;
using DarqMessageType = FASTER.libdpr.DarqMessageType;
using DarqStepStatus = pubsub.DarqStepStatus;
using Event = pubsub.Event;
using RegisterProcessorRequest = pubsub.RegisterProcessorRequest;
using RegisterProcessorResult = pubsub.RegisterProcessorResult;
using Status = Grpc.Core.Status;
using StepRequest = pubsub.StepRequest;

namespace dse.services;

internal struct EventDataAdapter : ILogEnqueueEntry
{
    internal string data;
    public int SerializedLength => sizeof(DarqMessageType) + data.Length;

    public unsafe void SerializeTo(Span<byte> dest)
    {
        fixed (byte* h = dest)
        {
            var head = h;
            *(DarqMessageType*)head = DarqMessageType.IN;
            head += sizeof(DarqMessageType);
            Encoding.UTF8.GetBytes(data, new Span<byte>(head, data.Length));
        }
    }
}

public class PubsubDarqProducer : IDarqProducer
{
    private SpPubSubServiceClient client;
    private DprSession session;
    private SimpleObjectPool<EnqueueRequest> requestPool = new(() => new EnqueueRequest());
    private SimpleObjectPool<List<Action<bool>>> callbackPool = new(() => new List<Action<bool>>(32));
    private Dictionary<DarqId, (EnqueueRequest, List<Action<bool>>)> currentRequest = new();

    public PubsubDarqProducer(Dictionary<int, (int, string)> clusterMap, DprSession session)
    {
        client = new SpPubSubServiceClient(clusterMap);
        this.session = session;
    }

    public void Dispose()
    {
    }

    public void EnqueueMessageWithCallback(DarqId darqId, ReadOnlySpan<byte> message, Action<bool> callback,
        long producerId, long lsn)
    {

        if (!currentRequest.TryGetValue(darqId, out var entry))
        {
            var request = requestPool.Checkout();
            request.TopicId = (int)darqId.guid;
            request.ProducerId = producerId;
            request.SequenceNum = 0;
            request.Events.Clear();
            var callbacks = callbackPool.Checkout();
            callbacks.Clear();
            
            entry = currentRequest[darqId] = (request, callbacks);
        }
        // Only expecting to call with a single producerId for now
        Debug.Assert(entry.Item1.ProducerId == producerId);
        // Only expecting to get monotonically increasing lsns for now
        Debug.Assert(entry.Item1.SequenceNum < lsn);
        entry.Item1.SequenceNum = lsn;
        entry.Item1.Events.Add(Encoding.UTF8.GetString(message));
        entry.Item2.Add(callback);
    }

    public void ForceFlush()
    {
        foreach (var entry in currentRequest.Values)
        {
            Task.Run(async () =>
            {
                await client.EnqueueEventsAsync(entry.Item1, session);
                foreach (var callback in entry.Item2) callback(true);
                requestPool.Return(entry.Item1);
                callbackPool.Return(entry.Item2);
            });
        }
        currentRequest.Clear();
    }
}

public class SpPubSubServiceSettings
{
    public Dictionary<int, (int, string)> clusterMap;
    public Func<int, DprWorkerId, Darq> factory;
    public int hostId;
}

public class SpPubSubBackendService : BackgroundService
{
    private SpPubSubServiceSettings settings;
    private ConcurrentDictionary<int, Darq> topics;
    private DarqMaintenanceBackgroundService maintenanceService;
    private StateObjectRefreshBackgroundService refreshService;
    private TaskCompletionSource started;

    public SpPubSubBackendService(SpPubSubServiceSettings settings,
        DarqMaintenanceBackgroundService maintenanceService,
        StateObjectRefreshBackgroundService refreshService)
    {
        this.settings = settings;
        topics = new ConcurrentDictionary<int, Darq>();
        this.maintenanceService = maintenanceService;
        this.refreshService = refreshService;
        started = new TaskCompletionSource();
    }

    public async ValueTask<Darq> GetTopic(int id)
    {
        await started.Task;
        return topics[id];
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        foreach (var entry in settings.clusterMap)
        {
            if (entry.Value.Item1 != settings.hostId) continue;
            var result = settings.factory(entry.Key, new DprWorkerId(entry.Key));
            result.ConnectToCluster(out _);
            maintenanceService.RegisterMaintenanceTask(result, new DarqMaintenanceBackgroundServiceSettings
            {
                morselSize = 512,
                batchSize = 64,
                producerFactory = session => new PubsubDarqProducer(settings.clusterMap, session),
                speculative = true
            });
            refreshService.RegisterRefreshTask(result);
            topics[entry.Key] = result;
        }

        started.SetResult();
        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
    }
}

public class SpPubSubService : SpPubSub.SpPubSubBase
{
    private SimpleObjectPool<FASTER.libdpr.StepRequest> stepRequestPool;
    private SpPubSubBackendService backend;


    public SpPubSubService(SpPubSubBackendService backend)
    {
        this.backend = backend;
        stepRequestPool = new SimpleObjectPool<FASTER.libdpr.StepRequest>(() => new FASTER.libdpr.StepRequest());
    }

    // private async Task<Darq> GetOrCreateTopic(int topicId)
    // {
    //     if (topics.TryGetValue(topicId, out var entry)) return entry;
    //     // Otherwise, check if topic has been created in manager
    //     var queryResult = await consul.KV.Get("topic-" + topicId);
    //     if (queryResult.Response == null)
    //         throw new RpcException(new Status(StatusCode.NotFound, "requested topic does not exist"));
    //
    //     var metadataEntry = JObject.Parse(Encoding.UTF8.GetString(queryResult.Response.Value));
    //
    //     if (((string)metadataEntry["hostId"])!.Equals(hostId))
    //         throw new RpcException(new Status(StatusCode.NotFound, "requested topic is not assigned to this host"));
    //
    //     lock (this)
    //     {
    //         if (topics.TryGetValue(topicId, out entry)) return entry;
    //         var dprWorkerId = new DprWorkerId((long)metadataEntry["dprWorkerId"]!);
    //         var result = factory(topicId, dprWorkerId);
    //         result.ConnectToCluster(out _);
    //         maintenanceService.RegisterMaintenanceTask(result, new DarqMaintenanceBackgroundServiceSettings
    //         {
    //             morselSize = 512,
    //             batchSize = 16,
    //             producerFactory = session => new PubsubDarqProducer(session, new ConsulClient(settings.consulConfig))
    //         });
    //         refreshService.RegisterRefreshTask(result);
    //         topics[topicId] = result;
    //         return result;
    //     }
    // }

    private ByteString PopulateHeaderAndEndAction(Darq topic)
    {
        unsafe
        {
            var dprHeaderBytes = stackalloc byte[DprMessageHeader.FixedLenSize];
            topic.ProduceTagAndEndAction(new Span<byte>(dprHeaderBytes, DprMessageHeader.FixedLenSize));
            return ByteString.CopyFrom(new Span<byte>(dprHeaderBytes, DprMessageHeader.FixedLenSize));
        }
    }

    public override async Task<EnqueueResult> EnqueueEvents(EnqueueRequest request, ServerCallContext context)
    {
        // TODO(Tianyu): Create Epoch Context
        var topic = await backend.GetTopic(request.TopicId);
        if (!request.DprHeader.IsEmpty)
        {
            // Speculative code path
            if (!await topic.TryReceiveAndStartActionAsync(request.DprHeader))
                // Use an error to signal to caller that this call cannot proceed
                // TODO(Tianyu): add more descriptive exception information
                throw new RpcException(Status.DefaultCancelled);
            var result = new EnqueueResult
            {
                Ok = topic.Enqueue(request.Events.Select(e => new EventDataAdapter { data = e }),
                    request.ProducerId, request.SequenceNum)
            };
            result.DprHeader = PopulateHeaderAndEndAction(topic);

            return result;
        }
        else
        {
            topic.StartLocalAction();
            var result = new EnqueueResult
            {
                Ok = topic.Enqueue(request.Events.Select(e => new EventDataAdapter { data = e }),
                    request.ProducerId, request.SequenceNum)
            };
            topic.EndAction();
            if (!request.FireAndForget)
                await topic.NextCommit();
            return result;
        }
    }

    public override async Task<StepResult> Step(StepRequest request, ServerCallContext context)
    {
        var topic = await backend.GetTopic(request.TopicId);
        // TODO(Tianyu): Pick the appropriate context 
        LightEpoch.EpochContext epochContext = null;

        var requestObject = stepRequestPool.Checkout();
        var requestBuilder = new StepRequestBuilder(requestObject);
        foreach (var consumed in request.ConsumedMessageOffsets)
            requestBuilder.MarkMessageConsumed(consumed);
        foreach (var self in request.RecoveryMessages)
            requestBuilder.AddRecoveryMessage(self.Span);
        foreach (var outBatch in request.OutMessages)
        {
            if (outBatch.TopicId == request.TopicId)
                requestBuilder.AddSelfMessage(outBatch.Event);
            else
                requestBuilder.AddOutMessage(new DarqId(outBatch.TopicId), outBatch.Event);
        }

        if (!request.DprHeader.IsEmpty)
        {
            // Speculative code path
            if (!await topic.TryReceiveAndStartActionAsync(request.DprHeader, epochContext))
                // Use an error to signal to caller that this call cannot proceed
                // TODO(Tianyu): add more descriptive exception information
                throw new RpcException(Status.DefaultCancelled);
            var status = topic.Step(request.IncarnationId, requestBuilder.FinishStep());
            var result = new StepResult
            {
                Status = status switch
                {
                    // Should never happen
                    StepStatus.INCOMPLETE => throw new NotImplementedException(),
                    StepStatus.SUCCESS => DarqStepStatus.Success,
                    StepStatus.INVALID => DarqStepStatus.Invalid,
                    StepStatus.REINCARNATED => DarqStepStatus.Reincarnated,
                    _ => throw new ArgumentOutOfRangeException()
                }
            };
            result.DprHeader = PopulateHeaderAndEndAction(topic);
            stepRequestPool.Return(requestObject);
            return result;
        }
        else
        {
            topic.StartLocalAction(epochContext);
            var status = topic.Step(request.IncarnationId, requestBuilder.FinishStep());
            var result = new StepResult
            {
                Status = status switch
                {
                    // Should never happen
                    StepStatus.INCOMPLETE => throw new NotImplementedException(),
                    StepStatus.SUCCESS => DarqStepStatus.Success,
                    StepStatus.INVALID => DarqStepStatus.Invalid,
                    StepStatus.REINCARNATED => DarqStepStatus.Reincarnated,
                    _ => throw new ArgumentOutOfRangeException()
                }
            };
            topic.EndAction(epochContext);
            stepRequestPool.Return(requestObject);
            if (!request.FireAndForget)
                await topic.NextCommit();
            return result;
        }
    }


    private unsafe bool TryReadOneEntry(Darq topic, long worldLine, DarqScanIterator scanner,
        LightEpoch.EpochContext context, out Event ev)
    {
        ev = default;
        var dprHeaderBytes = stackalloc byte[DprMessageHeader.FixedLenSize];
        try
        {
            topic.StartLocalAction(context);
            if (topic.WorldLine() != worldLine)
                throw new DprSessionRolledBackException(topic.WorldLine());

            if (!scanner.UnsafeGetNext(out var b, out var length, out var offset, out var nextOffset, out var type))
                return false;

            if (type is not (DarqMessageType.IN or DarqMessageType.RECOVERY))
            {
                scanner.UnsafeRelease();
                return false;
            }

            ev = new Event
            {
                Type = type switch
                {
                    DarqMessageType.IN => pubsub.DarqMessageType.In,
                    DarqMessageType.RECOVERY => pubsub.DarqMessageType.Recovery,
                    _ => throw new ArgumentOutOfRangeException()
                },
                Data = Encoding.UTF8.GetString(b, length),
                Offset = offset,
                NextOffset = nextOffset
            };
            scanner.UnsafeRelease();
            return true;
        }
        finally
        {
            topic.ProduceTagAndEndAction(new Span<byte>(dprHeaderBytes, DprMessageHeader.FixedLenSize), context);
            if (ev != default)
                ev.DprHeader = ByteString.CopyFrom(new Span<byte>(dprHeaderBytes, DprMessageHeader.FixedLenSize));
        }
    }

    public override async Task ReadEventsFromTopic(ReadEventsRequest request, IServerStreamWriter<Event> responseStream,
        ServerCallContext context)
    {
        var topic = await backend.GetTopic(request.TopicId);
        var worldLine = topic.WorldLine();
        var scanner = topic.StartScan(request.Speculative);

        // TODO(Tianyu): Pick the appropriate context 
        LightEpoch.EpochContext epochContext = null;
        long lastCommitted = 0;
        
        while (!context.CancellationToken.IsCancellationRequested)
        {
            if (TryReadOneEntry(topic, worldLine, scanner, epochContext, out var ev))
            {
                if (!request.Speculative && ev.NextOffset >= lastCommitted)
                {
                    // Avoid repeatedly wait for the newest commit
                    lastCommitted = topic.Tail;
                    await topic.NextCommit();
                }
                await responseStream.WriteAsync(ev);
            }
            else
            {
                await scanner.WaitAsync(context.CancellationToken);
            }
        }
    }

    public override async Task<RegisterProcessorResult> RegisterProcessor(RegisterProcessorRequest request,
        ServerCallContext context)
    {
        var topic = await backend.GetTopic(request.TopicId);
        var result = await topic.RegisterNewProcessorAsync();
        return new RegisterProcessorResult
        {
            IncarnationId = result
        };
    }

    public override async Task<GetNumBytesWrittenResult> GetNumBytesWritten(GetNumBytesWrittenRequest request, ServerCallContext context)
    {
        var topic = await backend.GetTopic(request.TopicId);
        return new GetNumBytesWrittenResult
        {
            NumBytes = topic.BytesWritten
        };
    }
}