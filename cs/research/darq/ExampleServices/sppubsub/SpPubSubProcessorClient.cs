using FASTER.core;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using pubsub;
using StepRequest = pubsub.StepRequest;

namespace dse.services;

public interface SpPubSubEventHandler
{
    // Invoked when there are no more entries and the handler is expected to await
    ValueTask HandleAwait();
    
    ValueTask HandleAsync(Event ev, CancellationToken token);

    void OnRestart(PubsubCapabilities capabilities);
}

public class PubsubCapabilities
{
    private AsyncDuplexStreamingCall<StepRequest, StepResult> stream;
    private DprSession session;
    private long incarnationId;
    private int topicId;
    private byte[] serializationArray = new byte[1 << 10];

    public PubsubCapabilities(AsyncDuplexStreamingCall<StepRequest, StepResult> stream, long incarnationId, int topicId, DprSession session)
    {
        if (session != null)
            Task.Run(async () =>
            {
                await foreach (var result in stream.ResponseStream.ReadAllAsync())
                    session.Receive(result.DprHeader.Span);
            });
        this.stream = stream;
        this.incarnationId = incarnationId;
        this.topicId = topicId;
        this.session = session;
    }
    
    public Task Step(pubsub.StepRequest request)
    {
        if (session != null)
        {
            var size = session.TagMessage(serializationArray);
            request.DprHeader = ByteString.CopyFrom(new Span<byte>(serializationArray, 0, size));
        }
        request.IncarnationId = incarnationId;
        request.TopicId = topicId;
        return stream.RequestStream.WriteAsync(request);
    }
}

public class SpPubSubProcessorClient
{
    private int topicId;
    private SpPubSubServiceClient client;
    private long incarnationId;

    public SpPubSubProcessorClient(int topicId, SpPubSubServiceClient client)
    {
        this.topicId = topicId;
        this.client = client;
    }

    public async Task StartProcessingAsync(SpPubSubEventHandler handler, bool speculative,
        CancellationToken token = default)
    {
        incarnationId = await client.RegisterProcessor(topicId);
        while (!token.IsCancellationRequested)
        {
            DprSession session = null;
            if (speculative)
            {
                session = new DprSession();
                var s = new SpPubSub.SpPubSubClient(await client.GetOrCreateConnection(topicId))
                    .StepStreamSpeculative(cancellationToken: token);
                handler.OnRestart(new PubsubCapabilities(s, incarnationId, topicId, session));
            }
            else
            {
                var s = new SpPubSub.SpPubSubClient(await client.GetOrCreateConnection(topicId))
                    .StepStream(cancellationToken: token);
                handler.OnRestart(new PubsubCapabilities(s, incarnationId, topicId, null));
            }
            var stream = client.ReadEventsFromTopic(new ReadEventsRequest
            {
                Speculative = speculative,
                TopicId = topicId
            }, session, cancellationToken: token);
            
            try
            {
                while (true)
                {
                    var task = stream.ResponseStream.MoveNext(token);
                    if (!task.IsCompleted)
                    {
                        await handler.HandleAwait();
                        await task;
                    }
                    if (!task.Result) break;
                    await handler.HandleAsync(stream.ResponseStream.Current, token);
                }
            }
            catch (TaskCanceledException e)
            {
                break;
            }
            catch (Exception e)
            {
                // Just continue and restart the stream from where it's supposed to
                continue;
            }
        }
    }
}