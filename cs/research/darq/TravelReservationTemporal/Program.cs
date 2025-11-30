using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Security.Authentication.ExtendedProtection;
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
    
    [Option('m', "mode", Required = false, Default = "throughput",
        HelpText = "Mode of benchmark (latency or throughput)")]
    public string Mode { get; set; }
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

                for (var i = 0; i < options.NumServices; i++)
                    await LoadCosmosDB($"{options.WorkloadTrace}-service-{i}.csv", i);

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

        var indexingPolicy = new IndexingPolicy
        {
            Automatic = true,
            IndexingMode = IndexingMode.Consistent,
        };
        // NO need to index anything beyond the Id or PartitionId
        indexingPolicy.ExcludedPaths.Add(
            new ExcludedPath { Path = "/*" }
        );
        indexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/partitionId/?" });

        var containerProperties = new ContainerProperties("offerings", "/partitionId");
        containerProperties.IndexingPolicy = indexingPolicy;

        await cosmosClient.GetDatabase("dsebench")
            .CreateContainerIfNotExistsAsync(containerProperties, throughput: 100000);

        var container = cosmosClient.GetDatabase("dsebench").GetContainer("offerings");

        var candidateId = Guid.NewGuid().ToString();

        var configDoc = new BenchmarkRunConfigDocument
        {
            PartitionId = "config",
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

    private static async Task WriteResults(Options options, ConcurrentBag<long> measurements, double throughput)
    {
        using var memoryStream = new MemoryStream();
        await using var streamWriter = new StreamWriter(memoryStream);
        foreach (var line in measurements)
            streamWriter.WriteLine(line);
        streamWriter.WriteLine($"Throughput: {throughput}");
        var avg = measurements.Average();
        streamWriter.WriteLine($"Average latency: {avg}");
        streamWriter.WriteLine($"Latency std: {Math.Sqrt(measurements.Sum(x => (x - avg) * (x - avg)) / (measurements.Count - 1))}");
        await streamWriter.FlushAsync();
        memoryStream.Position = 0;
        var connString = Environment.GetEnvironmentVariable("AZURE_RESULTS_CONN_STRING");
        var blobServiceClient = new BlobServiceClient(connString);
        var blobContainerClient = blobServiceClient.GetBlobContainerClient("results");

        await blobContainerClient.CreateIfNotExistsAsync();
        var blobClient = blobContainerClient.GetBlobClient(options.OutputFile);

        await blobClient.UploadAsync(memoryStream, overwrite: true);
    }

    private static async Task LoadCosmosDB(string filename, int serviceId)
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
                PartitionId = $"{serviceId}-{offeringId}",
                Id = $"offering-{serviceId}-{offeringId}", // Construct string ID
                EntityId = entityId,
                Price = price,
                RemainingCount = initialCount
            };

            Task.Run(async () =>
            {
                try
                {
                    await container.CreateItemAsync(doc, new PartitionKey($"{serviceId}-{offeringId}"));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing line '{currentLine}': {ex.Message}");
                }

                Interlocked.Increment(ref count);
                if (count % 1000 == 0)
                    Console.Write($"Loaded {count} items...\n");
                semaphore.Release();
            });
        }

        while (semaphore.CurrentCount < 64)
            await Task.Delay(10);

        Console.WriteLine($"Data loading complete. Total items: {count}\n");
    }

    private static async Task LaunchTemporalDriver(Options options, string runGuid)
    {
        Console.WriteLine("Parsing workload file...");
        var timedRequests = new List<(long Timestamp, string WorkflowId, string Input)>();
        var latencyMode = options.Mode.Equals("latency");

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

            if (latencyMode)
            {
                while (stopwatch.ElapsedMilliseconds <= request.Timestamp)
                    await Task.Yield();
            }

            await rateLimiter.WaitAsync();

            _ = Task.Run(async () =>
            {
                try
                {
                    var startTime = stopwatch.ElapsedMilliseconds;

                    var wfOptions = new WorkflowOptions(id: $"${runGuid}:{request.WorkflowId}",
                        taskQueue: $"travel-task-queue{runGuid}");

                    var handle = await client.StartWorkflowAsync(
                        (TemporalReservationWorkflow wf) => wf.RunAsync(request.Input),
                        wfOptions);

                    await handle.GetResultAsync();

                    var endTime = stopwatch.ElapsedMilliseconds;
                    var latency = latencyMode ? endTime - request.Timestamp : endTime - startTime;
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
            Console.WriteLine($"Waiting for {timedRequests.Count - measurements.Count} more results...");
            await Task.Delay(10);
        }

        Console.WriteLine("Benchmark finished, cleaning up database...");
        
        var throughput = measurements.Count * 1000.0 / stopwatch.ElapsedMilliseconds;
        await WriteResults(options, measurements, throughput);
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
            },
            MaxConcurrentActivityTaskPolls = 32,
            MaxConcurrentWorkflowTaskPolls = 32,
        };

        using var worker = new TemporalWorker(client, workerOptions);
        Console.WriteLine("Worker started. Press Ctrl+C to quit.");
        await worker.ExecuteAsync(CancellationToken.None);
    }
}