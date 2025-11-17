using System.Collections.Concurrent;
using System.Diagnostics;
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

        switch (options.Type.Trim())
        {
            case "client":
                Console.WriteLine("Starting client");

                var tasks = new List<Task>();
                for (var i = 0; i < options.NumServices; i++)
                {
                    var i1 = i;
                    var cosmosOptions = new CosmosClientOptions { AllowBulkExecution = true };
                    var cosmosClient = new CosmosClient(Environment.GetEnvironmentVariable("COSMOS_CONN_STRING"),
                        cosmosOptions);
                    tasks.Add(Task.Run(() => LoadCosmosDB(cosmosClient, $"{options.WorkloadTrace}-service-{i1}.csv")));
                }

                await Task.WhenAll(tasks);
                await LaunchTemporalDriver(options);
                break;
            case "worker":
                Console.WriteLine("Starting worker");
                await LaunchTemporalWorker(options);
                break;
            default:
                throw new NotImplementedException();
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

    private static async Task LoadCosmosDB(CosmosClient client, string filename)
    {
        var container = client.GetContainer("dsebench", "offerings");
        Console.WriteLine($"Loading data from {filename}");

        var semaphore = new SemaphoreSlim(32, 32);

        using var reader = new StreamReader(filename);
        string? line;
        var count = 0;

        while ((line = await reader.ReadLineAsync()) != null)
        {
            await semaphore.WaitAsync();

            var currentLine = line;

            Task.Run(async () =>
            {
                try
                {
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
                    await container.UpsertItemAsync(doc, new PartitionKey(offeringId));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"\nError processing line '{currentLine}': {ex.Message}");
                }
                finally
                {
                    semaphore.Release();
                }
            });

            count++;
            if (count % 1000 == 0)
            {
                Console.Write($"\rLoaded {count} items...");
            }
        }

        while (semaphore.CurrentCount > 0)
            await Task.Delay(10);

        Console.WriteLine($"\nData loading complete. Total items: {count}");
    }

    private static async Task LaunchTemporalDriver(Options options)
    {
        Console.WriteLine("Parsing workload file...");
        var timedRequests = new List<(long Timestamp, string WorkflowId, string Input)>();

        foreach (var line in File.ReadLines(options.WorkloadTrace))
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
                    var wfOptions = new WorkflowOptions(id: request.WorkflowId, taskQueue: "travel-task-queue");
                    
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
    }
    
    private static async Task CleanupDatabaseAsync(CosmosClient client)
    {
        // 1. Get reference to the container
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
            partitionKeyPath: "/offeringId", 
            throughput: 1000
        );
    }

    private static async Task LaunchTemporalWorker(Options options)
    {
        var cosmosClient = new CosmosClient("AccountEndpoint=...;", "Key=...;");

        var client = await TemporalClient.ConnectAsync(new("localhost:7233"));

        var workerOptions = new TemporalWorkerOptions("travel-task-queue")
        {
            Workflows = { WorkflowDefinition.Create<TemporalReservationWorkflow>() },
            Activities =
            {
                ActivityDefinition.Create(() =>
                    new TemporalReservationActivities(cosmosClient.GetContainer("travel", "offering")))
            }
        };

        using var worker = new TemporalWorker(client, workerOptions);
        Console.WriteLine("Worker started. Press Ctrl+C to quit.");
        await worker.ExecuteAsync(CancellationToken.None);
    }
}