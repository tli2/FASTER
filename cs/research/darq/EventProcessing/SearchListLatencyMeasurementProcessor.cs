using System.Diagnostics;
using dse.services;
using pubsub;

namespace EventProcessing;

public class SearchListLatencyMeasurementProcessor : SpPubSubEventHandler
{
    public Dictionary<string, (long, long)> results = new();
    public TaskCompletionSource workloadTerminationed = new();
    public long totalBytesWritten = 0;
    private Stopwatch stopwatch;
    private SpPubSubServiceClient client;

    public SearchListLatencyMeasurementProcessor(Stopwatch stopwatch, SpPubSubServiceClient client)
    {
        this.stopwatch = stopwatch;
        this.client = client;
    }
    
    public async ValueTask HandleAsync(Event ev, CancellationToken token)
    {
        if (ev.Data.Equals("termination"))
        {
            stopwatch.Stop();
            for (var i = 0; i < 4; i++)
                totalBytesWritten += (await client.GetNumBytesWritten(i)).NumBytes;
            workloadTerminationed.SetResult();
            return;
        }
        var split = ev.Data.Split(":");
        var timestamp = long.Parse(split[2]);
        var endTime = stopwatch.ElapsedMilliseconds;
        results[ev.Data] = (timestamp, endTime);
        // Console.WriteLine($"Received {ev.Data}, {timestamp}, {endTime}");
    }
    
    public ValueTask HandleAwait()
    {
        return ValueTask.CompletedTask;
    }


    public void OnRestart(PubsubCapabilities capabilities)
    {
    }
}