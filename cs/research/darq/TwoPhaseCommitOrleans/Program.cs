using System.Collections.Concurrent;
using System.Diagnostics;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using CommandLine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime.Placement;
using TwoPhaseCommitOrleans;

namespace TwoPhaseCommit;

public class Options
{
    [Option('t', "type", Required = true,
        HelpText = "type of worker to launch")]
    public string Type { get; set; }

    [Option('o', "output-file", Required = false,
        HelpText = "Name of file to output")]
    public string OutputFile { get; set; }

    [Option('w', "window", Required = false, Default = 64,
        HelpText = "number of outstanding client requests allowed")]
    public int Window { get; set; }
    
    [Option('n', "num-silos", Required = false, Default = 4,
        HelpText = "number of silos to run")]
    public int NumSilos { get; set; }

    [Option('x', "num-transactions", Required = false, Default = 10000,
        HelpText = "number of total transactions to run")]
    public int NumTransactions { get; set; }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
        if (result.Tag == ParserResultType.NotParsed) return;
        var options = result.MapResult(o => o, xs => new Options());

        switch (options.Type.Trim())
        {
            case "client":
                await LaunchBenchmarkClient(options);
                break;
            case "silo":
                await LaunchSilo();
                break;

            default:
                throw new NotImplementedException();
        }
    }

    private static async Task LaunchBenchmarkClient(Options options)
    {
        var builder = Host.CreateDefaultBuilder();
        
        builder.UseOrleansClient(cl =>
        {
            var connString = Environment.GetEnvironmentVariable("AZURE_TABLE_CONN_STRING");
            var tableServiceClient = new TableServiceClient(connString);
            cl.UseAzureStorageClustering(op => op.TableServiceClient = tableServiceClient);
        });
        
        var host = builder.Build();
        await host.StartAsync();

        var client = host.Services.GetRequiredService<IClusterClient>();
        Console.WriteLine($"Populating databases...");
        var stopwatch = Stopwatch.StartNew();
        var items = TpccWorkloadGenerator.GenerateItems(new Random());
        var loadTasks = new List<Task>();
        for (var i = 0; i < options.NumSilos; i++)
        {

            var assignedWarehouseIds = new List<int>();;
            for (var j = i; j < TpccConstants.NUM_WAREHOUSES; j += options.NumSilos)            
                assignedWarehouseIds.Add(j);
            
            loadTasks.Add(client.GetGrain<IBulkLoaderWorker>($"{i}").LoadData(i, assignedWarehouseIds, items)); 
        }
        await Task.WhenAll(loadTasks);
        Console.WriteLine($"Populated {options.NumSilos} shards in {stopwatch.Elapsed.TotalSeconds:F2}s");
        
        Console.WriteLine($"Pre-generating {options.NumTransactions} transactions...");
        stopwatch.Restart();
        var workload = TpccWorkloadGenerator.GenerateWorkload(client, options.NumTransactions);
        Console.WriteLine($"Generation complete in {stopwatch.Elapsed.TotalSeconds:F2}s");
        
        Console.WriteLine($"Executing workload...");
        var rateLimiter = new SemaphoreSlim(options.Window, options.Window);
        // Use a thread-safe counter for successful transactions
        long transactionsProcessed = 0;
        stopwatch.Restart();
        var measurements = new ConcurrentQueue<(long, long)>();

        for (var i = 0; i < workload.Count; i++)
        {
            await rateLimiter.WaitAsync();
            var i1 = i;
            _ = Task.Run(async () =>
            {
                try
                {
                    var startTime = stopwatch.ElapsedTicks;
                    await workload[i1]();
                    var latency = stopwatch.ElapsedTicks - startTime;
                    measurements.Enqueue((startTime, latency));
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Client Error: {ex.Message}");
                }
                finally
                {
                    Interlocked.Increment(ref transactionsProcessed);
                    rateLimiter.Release();
                }
            });
        }
        
        while (transactionsProcessed < options.NumTransactions)
            await Task.Yield();
        
        stopwatch.Stop();
        Console.WriteLine("Execution complete.");
        
        // --- RESULTS ---
        Console.WriteLine($"\n--- RESULTS ---");
        double elapsedSeconds = stopwatch.Elapsed.TotalSeconds;
        double tps = transactionsProcessed / elapsedSeconds;
        
        Console.WriteLine($"Processed: {transactionsProcessed:N0} transactions");
        Console.WriteLine($"Time:      {elapsedSeconds:F2}s");
        Console.WriteLine($"TPS:       {tps:N2}");
        await WriteResults(options, measurements.ToList());
    }
    
    private static async Task WriteResults(Options options, List<(long, long)> measurements)
    {
        using var memoryStream = new MemoryStream();
        await using var streamWriter = new StreamWriter(memoryStream);
        var aborted = 0;
        foreach (var (startTime, latency) in measurements)
        {
            if (latency < 0)
                aborted++;
            streamWriter.WriteLine($"{startTime}, {1000.0 * latency / Stopwatch.Frequency}");
        }
        streamWriter.WriteLine($"Aborted: {aborted} out of {measurements.Count}");
        await streamWriter.FlushAsync();
        memoryStream.Position = 0;
        var connString = Environment.GetEnvironmentVariable("AZURE_RESULTS_CONN_STRING");
        var blobServiceClient = new BlobServiceClient(connString);
        var blobContainerClient = blobServiceClient.GetBlobContainerClient("results");

        await blobContainerClient.CreateIfNotExistsAsync();
        var blobClient = blobContainerClient.GetBlobClient(options.OutputFile);

        await blobClient.UploadAsync(memoryStream, overwrite: true);
    }
    
    private static async Task LaunchSilo()
    {
        var builder = Host.CreateDefaultBuilder();

        builder.UseOrleans((_, silo) =>
        {
            var connString = Environment.GetEnvironmentVariable("AZURE_TABLE_CONN_STRING");

            var tableServiceClient = new TableServiceClient(connString);
            silo.UseAzureStorageClustering(op => op.TableServiceClient = tableServiceClient);

            silo.AddAzureTableTransactionalStateStorage("TransactionStore", op =>
                op.TableServiceClient = tableServiceClient);

            silo.ConfigureServices(services =>
            {
                // 1. Register the Strategy (Singleton)
                services.AddSingleton<TpccPlacementStrategy>();
        
                // 2. Register the Director
                services.AddSingleton<IPlacementDirector, TpccPlacementDirector>();
            });
            
            silo.AddPlacementDirector<TpccPlacementStrategy, TpccPlacementDirector>();
            
            silo.UseTransactions();
            silo.UseKubernetesHosting();
            silo.ConfigureLogging(logging => logging.AddConsole());
        });

        var host = builder.Build();
        await host.RunAsync();
    }
}