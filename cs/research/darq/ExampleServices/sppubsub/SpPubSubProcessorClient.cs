using FASTER.core;
using FASTER.libdpr;
using pubsub;

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
    internal SpPubSubServiceClient client;
    internal DprSession session;
    internal long incarnationId;
    internal int topicId;

    public Task Step(pubsub.StepRequest request)
    {
        request.IncarnationId = incarnationId;
        request.TopicId = topicId;
        request.FireAndForget = true;
        return client.StepAsync(request, session);
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
            var session = speculative ? new DprSession() : null;
            handler.OnRestart(new PubsubCapabilities
            {
                client = client,
                // To ensure that step returns quickly, make the return speculative even if processing is not 
                session = session,
                incarnationId = incarnationId,
                topicId = topicId
            });
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
                        if (!await task) break;
                    }
                    await handler.HandleAsync(stream.ResponseStream.Current, token);
                }
            }
            catch (DprSessionRolledBackException e)
            {
                // Just continue and restart the stream from where it's supposed to
                continue;
            }
            catch (TaskCanceledException e)
            {
                break;
            }
        }
    }
}