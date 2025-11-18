using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using Azure.Storage.Blobs;
using CommandLine;
using Microsoft.Azure.Cosmos;
using Temporalio.Activities;
using Temporalio.Client;
using Temporalio.Worker;
using Temporalio.Workflows;

namespace TravelReservation;

public class Options
{
    [Option('t', "type", Required = true,
        HelpText = "type of worker to launch")]
    public string Type { get; set; }

    [Option('w', "workload-trace", Required = false,
        HelpText = "Workload trace file to use")]
    public string WorkloadTrace { get; set; }

    [Option('o', "output-file", Required = false,
        HelpText = "Name of file to output")]
    public string OutputFile { get; set; }

    [Option('n', "numServices", Required = false,
        HelpText = "number of services to load")]
    public int NumServices { get; set; }

    [Option('i', "issue-window", Required = false, Default = 128,
        HelpText = "how many requests can be concurrently in-flight")]
    public int IssueWindow { get; set; }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        // GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
        ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
        if (result.Tag == ParserResultType.NotParsed) return;
        var options = result.MapResult(o => o, xs => new Options());
        
        var runGuid = await GetOrCreateRunGuid();
        switch (options.Type.Trim())
        {
            case "client":
                Console.WriteLine("Starting client");

                var tasks = new List<Task>();
                for (var i = 0; i < options.NumServices; i++)
                {
                    var i1 = i;
                    tasks.Add(Task.Run(() => LoadCosmosDB($"{options.WorkloadTrace}-service-{i1}.csv")));
                }

                await Task.WhenAll(tasks);
                await LaunchTemporalDriver(options, runGuid); 
                break;
            case "worker":
                Console.WriteLine("Starting worker");
                await LaunchTemporalWorker(options, runGuid);
                break;
            default:
                throw new NotImplementedException();
        }
    }
    
    private static async Task<string> GetOrCreateRunGuid()
    {
        using var cosmosClient = new CosmosClient(Environment.GetEnvironmentVariable("COSMOS_CONN_STRING"));
        var container = cosmosClient.GetDatabase("dsebench").GetContainer("offerings");
        
        var candidateId = Guid.NewGuid().ToString();
        
        var configDoc = new BenchmarkRunConfigDocument
        {
            PartitionId = 0,
            Id = "config",
            RunGuid = candidateId
        };

        try
        {
            await container.CreateItemAsync(configDoc, new PartitionKey(configDoc.PartitionId));
            return candidateId;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
        {
            // 3b. FAILURE: Someone beat us to it. 
            // The document exists, so we must READ what the winner wrote.
            Console.WriteLine("[Init] Run ID already exists. Fetching it...");

            var existingDoc = await container.ReadItemAsync<BenchmarkRunConfigDocument>(
                configDoc.Id,
                new PartitionKey(configDoc.PartitionId));
            return existingDoc.Resource.RunGuid;
        }
    }

    private static async Task WriteResults(Options options, ConcurrentBag<long> measurements)
    {
        using var memoryStream = new MemoryStream();
        await using var streamWriter = new StreamWriter(memoryStream);
        foreach (var line in measurements)
            streamWriter.WriteLine(line);
        await streamWriter.FlushAsync();
        memoryStream.Position = 0;
        var connString = Environment.GetEnvironmentVariable("AZURE_RESULTS_CONN_STRING");
        var blobServiceClient = new BlobServiceClient(connString);
        var blobContainerClient = blobServiceClient.GetBlobContainerClient("results");

        await blobContainerClient.CreateIfNotExistsAsync();
        var blobClient = blobContainerClient.GetBlobClient(options.OutputFile);

        await blobClient.UploadAsync(memoryStream, overwrite: true);
    }

    private static async Task LoadCosmosDB(string filename)
    {
        var cosmosOptions = new CosmosClientOptions { AllowBulkExecution = true };
        using var cosmosClient = new CosmosClient(Environment.GetEnvironmentVariable("COSMOS_CONN_STRING"),
            cosmosOptions);
        var container = cosmosClient.GetDatabase("dsebench").GetContainer("offerings");
        
        Console.WriteLine($"Loading data from {filename}");

        var semaphore = new SemaphoreSlim(64, 64);

        using var reader = new StreamReader(filename);
        string? line;
        var count = 0;
        while ((line = await reader.ReadLineAsync()) != null)
        {
            await semaphore.WaitAsync();

            var currentLine = line;

            var parts = currentLine.Split(',');
            var offeringId = long.Parse(parts[0]);
            var entityId = long.Parse(parts[1]);
            var price = int.Parse(parts[2]);
            var initialCount = int.Parse(parts[3]);

            var doc = new OfferingDocument
            {
                PartitionId = offeringId,
                Id = $"offering-{offeringId}", // Construct string ID
                EntityId = entityId,
                Price = price,
                RemainingCount = initialCount
            };
            
            container.UpsertItemAsync(doc, new PartitionKey(offeringId)).ContinueWith(t =>
            {
                
                if (!t.IsCompletedSuccessfully)
                    Console.WriteLine($"Error processing line '{currentLine}': {t.Exception?.Message}");
                semaphore.Release();
                Interlocked.Increment(ref count);
                if (count % 1000 == 0)
                    Console.Write($"Loaded {count} items...\n");
            }); 

        }

        while (semaphore.CurrentCount < 32)
            await Task.Delay(10);

        Console.WriteLine($"Data loading complete. Total items: {count}\n");
    }

    private static async Task LaunchTemporalDriver(Options options, string runGuid)
    {
        Console.WriteLine("Parsing workload file...");
        var timedRequests = new List<(long Timestamp, string WorkflowId, string Input)>();

        foreach (var line in File.ReadLines($"{options.WorkloadTrace}-client-0.csv"))
        {
            var args = line.Split(',');
            var timestamp = long.Parse(args[0]);
            var workflowId = args[1]; // Use the ID from the trace

            // We pass the whole line as input, matching your Workflow RunAsync signature
            timedRequests.Add((timestamp, workflowId, line));
        }

        Console.WriteLine($"Loaded {timedRequests.Count} requests.");
        
        Console.WriteLine("Connecting to Temporal...");
        var client = await TemporalClient.ConnectAsync(new("temporal-frontend.temporal.svc.cluster.local:7233"));

        var measurements = new ConcurrentBag<long>();
        var rateLimiter = new SemaphoreSlim(options.IssueWindow, options.IssueWindow);
        var stopwatch = Stopwatch.StartNew();

        Console.WriteLine("Starting Workload...");

        for (var i = 0; i < timedRequests.Count; i++)
        {
            var request = timedRequests[i];

            while (stopwatch.ElapsedMilliseconds <= request.Timestamp)
                Thread.Yield();

            await rateLimiter.WaitAsync();

            _ = Task.Run(async () =>
            {
                try
                {
                    var wfOptions = new WorkflowOptions(id: $"${runGuid}:{request.WorkflowId}", taskQueue: $"travel-task-queue{runGuid}");
                    
                    var handle = await client.StartWorkflowAsync(
                        (TemporalReservationWorkflow wf) => wf.RunAsync(request.Input),
                        wfOptions);

                    await handle.GetResultAsync();

                    var endTime = stopwatch.ElapsedMilliseconds;
                    var latency = endTime - request.Timestamp; // End Time - Scheduled Start Time
                    measurements.Add(latency);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Workflow {request.WorkflowId} failed: {ex.Message}");
                }
                finally
                {
                    rateLimiter.Release();
                }
            });
        }

        Console.WriteLine("Issuing complete. Waiting for pending workflows...");
        while (measurements.Count != timedRequests.Count)
        {
            await Task.Delay(100);
        }

        Console.WriteLine("Benchmark finished, cleaning up database...");
        
        await WriteResults(options, measurements);
        await CleanupDatabaseAsync();
    }
    
    private static async Task CleanupDatabaseAsync()
    {
        // 1. Get reference to the container
        var client = new CosmosClient(Environment.GetEnvironmentVariable("COSMOS_CONN_STRING"));
        var container = client.GetDatabase("dsebench").GetContainer("offerings");

        try
        {
            // 2. Delete the container (Drop Table)
            // This is much faster/cheaper than deleting individual items
            await container.DeleteContainerAsync();
            Console.WriteLine("Container deleted.");
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            Console.WriteLine("Container did not exist, skipping delete.");
        }
        
        await client.GetDatabase("dsebench").CreateContainerIfNotExistsAsync(
            id: "offering", 
            partitionKeyPath: "/partitionId", 
            throughput: 100000
        );
    }

    private static async Task LaunchTemporalWorker(Options options, string runGuid)
    {
        var cosmosClient = new CosmosClient(Environment.GetEnvironmentVariable("COSMOS_CONN_STRING"));
        var container = cosmosClient.GetDatabase("dsebench").GetContainer("offerings");
        
        var client = await TemporalClient.ConnectAsync(new("temporal-frontend.temporal.svc.cluster.local:7233"));
        var activities = new TemporalReservationActivities(container);

        var workerOptions = new TemporalWorkerOptions($"travel-task-queue{runGuid}")
        {
            Workflows = { WorkflowDefinition.Create<TemporalReservationWorkflow>() },
            Activities =
            {
                ActivityDefinition.Create(activities.MakeReservationAsync),
                ActivityDefinition.Create(activities.CancelReservationAsync)
            }
        };

        using var worker = new TemporalWorker(client, workerOptions);
        Console.WriteLine("Worker started. Press Ctrl+C to quit.");
        await worker.ExecuteAsync(CancellationToken.None);
    }
}