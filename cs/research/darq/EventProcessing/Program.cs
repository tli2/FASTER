// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using System.Net;
using CommandLine;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using dse.services;
using FASTER.client;
using Microsoft.Extensions.Logging;

namespace EventProcessing;

public class Options
{
    [Option('t', "type", Required = true,
        HelpText = "type of worker to launch")]
    public string Type { get; set; }

    [Option('w', "workload-trace", Required = false,
        HelpText = "Workload trace file to use")]
    public string WorkloadTrace { get; set; }
    
    [Option('o', "output-name", Required = false,
        HelpText = "Name of output file")]
    public string OutputName { get; set; }

    [Option('h', "host-id", Required = false,
        HelpText = "identifier of the service to launch")]
    public int HostId { get; set; }
    
    [Option('s', "speculative", Required = false, Default = false,
        HelpText = "whether services proceed speculatively")]
    public bool Speculative { get; set; }
    
    
    [Option('i', "checkpoint-interval", Required = false, Default = 10,
        HelpText = "checkpoint interval")]
    public int CheckpointInterval { get; set; }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
        if (result.Tag == ParserResultType.NotParsed) return;
        var options = result.MapResult(o => o, xs => new Options());
        // IEnvironment environment = new LocalDebugEnvironment();
        var environment = new KubernetesLocalStorageEnvironment(true);
        switch (options.Type.Trim())
        {
            case "client":
                await LaunchBenchmarkClient(options, environment);
                break;
            case "filter":
            case "aggregate":
            case "detection":
                await LaunchProcessor(options, environment);
                break;
            case "worker":
                await LaunchPubsubService(options, environment);
                break;
            case "dprfinder":
                await LaunchDprFinder(options, environment);
                break;
            case "generate":
                new SearchListDataGenerator().SetOutputFile("C:\\Users\\tianyu\\Desktop\\workloads\\EventProcessing\\workloads\\events-50k.txt")
                    .SetSearchTermRelevantProb(0.2)
                    .SetTrendParameters(0.1, 50000, 25000)
                    .SetSearchTermLength(80)
                    .SetThroughput(50000)
                    .SetNumSearchTerms(50000 * 30)
                    .Generate();
                break;
            default:
                throw new NotImplementedException();
        }
    }

    private static async Task LaunchBenchmarkClient(Options options, IEnvironment environment)
    {
        var client = new SpPubSubServiceClient(environment.GetClusterMap());
        var stopwatch = new Stopwatch();
        var loader = new SearchListDataLoader(options.WorkloadTrace, client, 0, stopwatch);
        var numRecords = loader.LoadData();
        _ = Task.Run(loader.Run);
        var processingClient = new SpPubSubProcessorClient(3, client);
        var measurementProcessor = new SearchListLatencyMeasurementProcessor(stopwatch, client);
        _ = Task.Run(async () => await processingClient.StartProcessingAsync(measurementProcessor, false));
        await measurementProcessor.workloadTerminationed.Task;
        var throughput = numRecords * 1000.0 / stopwatch.ElapsedMilliseconds;
        await WriteLatencyResults(options, environment, measurementProcessor);
        await WriteOtherResults(options, environment, throughput, measurementProcessor.totalBytesWritten);
    }

    private static async Task WriteLatencyResults(Options options, IEnvironment environment, SearchListLatencyMeasurementProcessor processor)
    {
        using var memoryStream = new MemoryStream();
        await using var streamWriter = new StreamWriter(memoryStream);
        foreach (var line in processor.results)
            streamWriter.WriteLine(line.Value.Item2 - line.Value.Item1);
        await streamWriter.FlushAsync();
        memoryStream.Position = 0;
        
        await environment.PublishResultsAsync($"{options.OutputName}-lat.csv", memoryStream);
    }
    
    private static async Task WriteOtherResults(Options options, IEnvironment environment, double throughput, long bytesWritten)
    {
        using var memoryStream = new MemoryStream();
        await using var streamWriter = new StreamWriter(memoryStream);
        streamWriter.WriteLine($"Throughput: {throughput}");
        streamWriter.WriteLine($"BytesWritten: {bytesWritten}");
        await streamWriter.FlushAsync();
        memoryStream.Position = 0;
        
        await environment.PublishResultsAsync($"{options.OutputName}-stats.csv", memoryStream);
    }

    public static Task LaunchPubsubService(Options options, IEnvironment environment)
    {
        var builder = WebApplication.CreateBuilder();
        
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);

        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, environment.GetPubsubServicePort(options.HostId),
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
            serverOptions.Limits.MinRequestBodyDataRate = null;
        });
        
        builder.Services.AddSingleton<DarqMaintenanceBackgroundService>();
        builder.Services.AddSingleton<StateObjectRefreshBackgroundService>();
        
        builder.Services.AddSingleton(new SpPubSubServiceSettings
        {
            clusterMap = environment.GetClusterMap(),
            factory = (id, dprId) => new Darq(new DarqSettings
            {
                Me = new DarqId(id),
                MyDpr = dprId,
                DprFinder = new GrpcDprFinder(GrpcChannel.ForAddress(environment.GetDprFinderConnString())),
                LogDevice = environment.GetDarqDevice(id),
                LogCommitManager = environment.GetDarqCheckpointManager(id),
                PageSize = 1L << 22,
                MemorySize = 1L << 30,
                SegmentSize = 1L << 30,
                CheckpointPeriodMilli = options.CheckpointInterval,
                RefreshPeriodMilli = 5,
                FastCommitMode = true
            }, new RwLatchVersionScheme()),
            hostId = options.HostId,
        });
        builder.Services.AddSingleton<SpPubSubBackendService>();
        
        builder.Services.AddHostedService<StateObjectRefreshBackgroundService>(provider =>
            provider.GetRequiredService<StateObjectRefreshBackgroundService>());
        builder.Services.AddHostedService<DarqMaintenanceBackgroundService>(provider =>
            provider.GetRequiredService<DarqMaintenanceBackgroundService>());
        builder.Services.AddHostedService<SpPubSubBackendService>(provider =>
            provider.GetRequiredService<SpPubSubBackendService>());

        builder.Services.AddSingleton<SpPubSubService>();
        builder.Services.AddGrpc();
        var app = builder.Build();
        app.MapGrpcService<SpPubSubService>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        return app.RunAsync();
    }

    public static async Task LaunchProcessor(Options options, IEnvironment environment)
    {
        var client = new SpPubSubServiceClient(environment.GetClusterMap());
        var outputTopic = options.Type switch
        {
            "filter" => 1,
            "aggregate" => 2,
            "detection" => 3,
            _ => throw new ArgumentOutOfRangeException()
        };

        var processingClient = new SpPubSubProcessorClient(outputTopic - 1, client);
        SpPubSubEventHandler handler = options.Type switch
        {
            "filter" => new FilterAndMapEventProcessor(outputTopic),
            "aggregate" => new AggregateEventProcessor(outputTopic),
            "detection" => new AnomalyDetectionEventProcessor(outputTopic, 1.0),
            _ => throw new ArgumentOutOfRangeException()
        };
        await processingClient.StartProcessingAsync(handler, options.Speculative);
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
}