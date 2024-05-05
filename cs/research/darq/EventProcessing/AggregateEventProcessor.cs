using System.Diagnostics;
using dse.services;
using pubsub;
using StepRequest = pubsub.StepRequest;

namespace EventProcessing;

public class AggregateEventProcessor : SpPubSubEventHandler
{
    private int outputTopic;
    private PubsubCapabilities capabilities;
    private long currentBatchStartTime = -1;
    private long largestTimestampInBatch;
    private Dictionary<string, long> currentBatch;
    private StepRequest currentRequest;

    public AggregateEventProcessor(int outputTopic)
    {
        this.outputTopic = outputTopic;
        currentBatch = new Dictionary<string, long>();
        currentRequest = new StepRequest
        {
            TopicId = outputTopic
        };
    }

    public async ValueTask HandleAsync(Event ev, CancellationToken token)
    {
        if (ev.Data.Equals("termination"))
        {
            currentRequest.ConsumedMessageOffsets.Add(ev.Offset);
            currentRequest.OutMessages.Add(new OutMessage
            {
                TopicId = outputTopic,
                Event = ev.Data
            });
            await capabilities.Step(currentRequest);
            return;
        }
        var split = ev.Data.Split(":");
        Debug.Assert(split.Length == 3);
        var term = split[0].Trim();
        Debug.Assert(term.Equals(SearchListStreamUtils.relevantSearchTerm));
        var region = split[1].Trim();
        var timestamp = long.Parse(split[2].Trim());
        if (currentBatchStartTime == -1)
            currentBatchStartTime = timestamp;

        if (!currentBatch.TryGetValue(region, out var c))
            currentBatch[region] = 1;
        else
            currentBatch[region] = c + 1;

        if (timestamp > currentBatchStartTime + SearchListStreamUtils.WindowSizeMilli)
        {
            foreach (var (k, count) in currentBatch)
                currentRequest.OutMessages.Add(new OutMessage
                {
                    TopicId = outputTopic,
                    Event = $"{k} : {count} : {largestTimestampInBatch}"
                });

            await capabilities.Step(currentRequest);
            currentRequest = new StepRequest
            {
                TopicId = outputTopic
            };
            currentBatch.Clear();
            currentBatchStartTime = timestamp;
        }

        largestTimestampInBatch = Math.Max(largestTimestampInBatch, timestamp);
        currentRequest.ConsumedMessageOffsets.Add(ev.Offset);
    }

    public ValueTask HandleAwait()
    {
        return ValueTask.CompletedTask;
    }

    public void OnRestart(PubsubCapabilities capabilities)
    {
        this.capabilities = capabilities;
        currentRequest = new StepRequest
        {
            TopicId = outputTopic
        };
        currentBatch.Clear();
        currentBatchStartTime = -1;
        largestTimestampInBatch = 0;
    }
}