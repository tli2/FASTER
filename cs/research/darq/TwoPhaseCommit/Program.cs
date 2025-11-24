using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using CommandLine;
using FASTER.core;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Grpc.Core;
using protobuf;

namespace TwoPhaseCommit;
public class Options
{
    [Option('t', "type", Required = true,
        HelpText = "type of worker to launch")]
    public string Type { get; set; }
    
    [Option('o', "output-file", Required = false,
        HelpText = "Name of file to output")]
    public string OutputFile { get; set; }

    [Option('n', "name", Required = false,
        HelpText = "identifier of the service to launch")]
    public int WorkerName { get; set; }
    
    [Option('s', "speculative", Required = false, Default = false,
        HelpText = "whether services proceed speculatively")]
    public bool Speculative { get; set; }
    
    [Option('w', "window", Required = false, Default = 16,
        HelpText = "number of outstanding client requests allowed")]
    public int Window { get; set; }
    
    [Option('x', "num-transactions", Required = false, Default = 10000,
        HelpText = "number of total transactions to run")]
    public int NumTransactions { get; set; }
    
    [Option('f', "fail", Required = false,
        HelpText = "Whether to force a rollback halfway through the workload")]
    public bool Fail { get; set; }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
        if (result.Tag == ParserResultType.NotParsed) return;
        var options = result.MapResult(o => o, xs => new Options());
        var environment = new KubernetesLocalStorageEnvironment();

        switch (options.Type.Trim())
        {
            case "client":
                await LaunchBenchmarkClient(options, environment);
                break;
            case "participant":
                await LaunchTpccShardService(options, environment);
                break;
            case "dprfinder":
                await LaunchDprFinder(options, environment);
                break;
            default:
                throw new NotImplementedException();
        }
    }

    private static async Task LaunchBenchmarkClient(Options options, IEnvironment environment)
    {
        Console.WriteLine("Populating databases...");
        var stopwatch = Stopwatch.StartNew();
        var items = TpccWorkloadGenerator.GenerateItems(new Random());
        var connections = new ConcurrentDictionary<int, GrpcChannel>();
        var loadTasks = new List<Task>();
        for (var i = 0; i < environment.GetNumShards(); i++)
        {
            var channel = GrpcChannel.ForAddress(environment.GetShardConnString(i));
            connections.TryAdd(i, channel);
            var loadDataRequest = new LoadDataRequest
            {
                Seed = i
            };
            foreach (var item in items)
                loadDataRequest.Items.Add(new protobuf.Item
                {
                    IId = item.iId,
                    IPrice = item.iPrice
                });
            
            for (var j = i; j < TpccConstants.NUM_WAREHOUSES; j += environment.GetNumShards())            
                loadDataRequest.AssignedWarehouseIds.Add(j);
            
            loadTasks.Add(new TpccShardService.TpccShardServiceClient(channel).LoadDataAsync(loadDataRequest).ResponseAsync);
        }
        await Task.WhenAll(loadTasks);
        Console.WriteLine($"Populated {environment.GetNumShards()} shards in {stopwatch.Elapsed.TotalSeconds:F2}s");
        
        Console.WriteLine($"Pre-generating {options.NumTransactions} transactions...");
        stopwatch.Restart();
        var clients = new Dictionary<byte, TpccShardService.TpccShardServiceClient>();
        for (byte i = 0; i < TpccConstants.NUM_WAREHOUSES; i++)
        {
            var channel = connections[i % environment.GetNumShards()];
            clients.Add(i, new TpccShardService.TpccShardServiceClient(channel));
        }
        var workload = TpccWorkloadGenerator.GenerateWorkload(clients, options.NumTransactions);
        Console.WriteLine($"Generation complete in {stopwatch.Elapsed.TotalSeconds:F2}s");
        
        
        Console.WriteLine($"Executing workload...");
        var rateLimiter = new SemaphoreSlim(options.Window, options.Window);
        // Use a thread-safe counter for successful transactions
        long transactionsProcessed = 0;
        stopwatch.Restart();
        var measurements = new ConcurrentQueue<(long, long)>();
        var finder = new GrpcDprFinder(environment.GetDprFinderConnString());
        
        for (var i = 0; i < workload.Count; i++)
        {
            var warehouse = workload[i];
            Task.Run(async () =>
            {
                foreach (var w in warehouse)
                {
                    try
                    {
                        await rateLimiter.WaitAsync();
                        var startTime = stopwatch.ElapsedTicks;
                        var success = await w();
                        Interlocked.Increment(ref transactionsProcessed);
                        var latency = stopwatch.ElapsedTicks - startTime;
                        measurements.Enqueue((startTime, success ? latency : -latency));
                        rateLimiter.Release();
                    }
                    catch (RpcException ex)
                    {
                        Console.Error.WriteLine($"RPC Error: {ex.Status.Detail}");
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine($"Client Error: {ex.Message}");
                    }
                }
            });
        }

        while (transactionsProcessed < options.NumTransactions)
        {
            // TODO(Tianyu): simulate node fail over here
            // if (options.Fail && i == workload.Count / 2)
            // {
            //     if (options.Speculative)
            //         finder.ForceRollback();
            // }
            await Task.Yield();
        }

        stopwatch.Stop();
        Console.WriteLine("Execution complete.");
        
        // --- RESULTS ---
        Console.WriteLine($"\n--- RESULTS ---");
        double elapsedSeconds = stopwatch.Elapsed.TotalSeconds;
        double tps = transactionsProcessed / elapsedSeconds;
        
        Console.WriteLine($"Processed: {transactionsProcessed:N0} transactions");
        Console.WriteLine($"Time:      {elapsedSeconds:F2}s");
        Console.WriteLine($"TPS:       {tps:N2}");
        await WriteResults(options, environment, measurements.ToList());
    }

    private static async Task WriteResults(Options options, IEnvironment environment, List<(long, long)> measurements)
    {
        using var memoryStream = new MemoryStream();
        await using var streamWriter = new StreamWriter(memoryStream);
        var aborted = 0;
        foreach (var (startTime, latency) in measurements)
        {
            if (latency < 0)
            {
                aborted++;
                streamWriter.WriteLine($"{startTime}, 0");

            }
            else
                streamWriter.WriteLine($"{startTime}, {1000.0 * latency / Stopwatch.Frequency}");
        }
        streamWriter.WriteLine($"Aborted: {aborted} out of {measurements.Count}");
        await streamWriter.FlushAsync();
        memoryStream.Position = 0;
        await environment.PublishResultsAsync(options.OutputFile, memoryStream);
    }

    public static async Task LaunchDprFinder(Options options, IEnvironment environment)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, environment.GetDprFinderPort(),
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
            serverOptions.Limits.MinRequestBodyDataRate = null;
        });
        using var dprFinderServiceDevice = environment.GetDprFinderDevice();
        builder.Services.AddSingleton(dprFinderServiceDevice);
        builder.Services.AddSingleton<GraphDprFinderBackend>();
        builder.Services.AddSingleton<DprFinderGrpcBackgroundService>();
        builder.Services.AddSingleton<DprFinderGrpcService>();
        
        builder.Services.AddGrpc();
        builder.Services.AddHostedService<DprFinderGrpcBackgroundService>(provider =>
            provider.GetRequiredService<DprFinderGrpcBackgroundService>());
        var app = builder.Build();
        
        app.MapGrpcService<DprFinderGrpcService>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        await app.RunAsync();
    }

    public static async Task LaunchTpccShardService(Options options, IEnvironment environment)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);

        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, environment.GetShardPort(options),
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
            serverOptions.Limits.MinRequestBodyDataRate = null;
        });
        var checkpointManager = environment.GetShardCheckpointManager(options);
        
        builder.Services.AddSingleton(new TpccShardSettings
        {
            logSettings = new FasterLogSettings
            {
                LogDevice = environment.GetShardDevice(options),
                MemorySizeBits = 30,
                LogCommitManager = checkpointManager,
                FastCommitMode = true,
                RemoveOutdatedCommits = false,
                TryRecoverLatest = false,
                AutoRefreshSafeTailAddress = true,
                AutoCommit = false
            },
            environment = environment,
            speculative = options.Speculative
        });
        
        builder.Services.AddSingleton(new DprWorkerOptions
        {
            Me = new DprWorkerId(options.WorkerName),
            DprFinder = new GrpcDprFinder(environment.GetDprFinderConnString()),
            CheckpointPeriodMilli = 10,
            RefreshPeriodMilli = 5
        });

        builder.Services.AddSingleton(typeof(IVersionScheme), typeof(RwLatchVersionScheme));
        builder.Services.AddSingleton<TpccShard>();
        builder.Services.AddSingleton<TpccShardBackgroundService>();
        builder.Services.AddSingleton<StateObject>(sp => sp.GetService<TpccShard>());
        builder.Services.AddSingleton<DprServerInterceptor<TpccShard>>();
        builder.Services.AddSingleton<TpccShardServiceImpl>();
        
        builder.Services.AddHostedService<TpccShardBackgroundService>(provider =>
            provider.GetRequiredService<TpccShardBackgroundService>());
        builder.Services.AddHostedService<StateObjectRefreshBackgroundService>();
        builder.Services.AddGrpc(opt => { opt.Interceptors.Add<DprServerInterceptor<TpccShard>>(); });
        var app = builder.Build();
        
        app.MapGrpcService<TpccShardServiceImpl>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        await app.RunAsync();
    }
}