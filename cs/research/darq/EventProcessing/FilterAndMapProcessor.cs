using System.Diagnostics;
using dse.services;
using Newtonsoft.Json;
using pubsub;

namespace EventProcessing;

public class FilterAndMapEventProcessor : SpPubSubEventHandler
{
    private int outputTopic;
    private PubsubCapabilities capabilities;
    private pubsub.StepRequest currentBatch;
    private int batchSize, numBatchedSteps = 0;

    public FilterAndMapEventProcessor(int outputTopic, int batchSize = 32)
    {
        this.outputTopic = outputTopic;
        this.batchSize = batchSize;
        currentBatch = new pubsub.StepRequest();
    }

    public async ValueTask HandleAsync(Event ev, CancellationToken token)
    {
        Debug.Assert(ev.Type == pubsub.DarqMessageType.In);
        if (ev.Data.Equals("termination"))
        {
            // Forward termination signal
            numBatchedSteps++;
            currentBatch.ConsumedMessageOffsets.Add(ev.Offset);
            currentBatch.OutMessages.Add(new OutMessage
            {
                TopicId = outputTopic,
                Event = ev.Data
            });
            await Flush();
            return;
        }
        
        var searchListItem =
            JsonConvert.DeserializeObject<SearchListJson>(ev.Data);
        Debug.Assert(searchListItem != null);
        currentBatch.ConsumedMessageOffsets.Add(ev.Offset);
        if (searchListItem.SearchTerm.Contains(SearchListStreamUtils.relevantSearchTerm))
        {
            currentBatch.OutMessages.Add(new OutMessage
            {
                TopicId = outputTopic,
                Event = $"{SearchListStreamUtils.relevantSearchTerm} : {SearchListStreamUtils.GetRegionCode(searchListItem.IP)} : {searchListItem.Timestamp}"
            });
        }

        if (++numBatchedSteps == batchSize)
            await Flush();
    }

    public ValueTask HandleAwait()
    {
        return Flush();
    }

    private async ValueTask Flush()
    {
        if (numBatchedSteps == 0) return;
        await capabilities.Step(currentBatch);
        currentBatch = new pubsub.StepRequest();
        numBatchedSteps = 0;
    }

    public void OnRestart(PubsubCapabilities capabilities)
    {
        this.capabilities = capabilities;
        currentBatch = new pubsub.StepRequest();
        numBatchedSteps = 0;
    }
}