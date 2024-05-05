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
using FASTER.common;
using Grpc.Core.Interceptors;
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
    
    [Option('w', "window", Required = false,
        HelpText = "number of outstanding client requests allowed")]
    public int Window { get; set; }
    
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
        var environment = new KubernetesLocalStorageEnvironment(!options.Fail);

        switch (options.Type.Trim())
        {
            case "client":
                await LaunchBenchmarkClient(options, environment);
                break;
            case "coordinator":
                await LaunchCommitCoordinatorService(options, environment);
                break;
            case "participant":
                await LaunchParticipantService(options, environment);
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
        var numTransactionsToRun = 10000;
        var finder = new GrpcDprFinder(environment.GetDprFinderConnString());
        var sessionPool = new SimpleObjectPool<DprSession>(() => new DprSession());
        var channels = new List<GrpcChannel>();
        for (var i = 0; i < 4; i++)
            channels.Add(GrpcChannel.ForAddress(environment.GetParticipantConnString(i)));

        var coordinator =
            new CommitCoordinatorService.CommitCoordinatorServiceClient(
                GrpcChannel.ForAddress(environment.GetCoordinatorConnString()));
        var measurements = new ConcurrentBag<long>();
        var stopwatch = Stopwatch.StartNew();
        var rateLimiter = new SemaphoreSlim(options.Window, options.Window);
        for (var i = 0; i < numTransactionsToRun; i++)
        {
            if (options.Fail && i == numTransactionsToRun / 2)
            {
                if (options.Speculative)
                    finder.ForceRollback();
                else
                {
                    var session = sessionPool.Checkout();
                    session.UnsafeReset();
                    var client = new CommitParticipantService.CommitParticipantServiceClient(
                        channels[0].Intercept(new DprClientInterceptor(session)));
                    await client.ForceFailoverAsync(new ForceFailoverMessage());
                    sessionPool.Return(session);

                }
            }
            await rateLimiter.WaitAsync();
            var transaction = new TransactionsRequest
            {
                TxnId = i
            };
            var startTime = stopwatch.ElapsedTicks;
            _ = Task.Run(async () =>
            {
                // Console.WriteLine($"Starting transaction number {transaction.TxnId}");
                var session = sessionPool.Checkout();
                session.UnsafeReset();
                try
                {
                    foreach (var channel in channels)
                    {
                        // Speculatively send transactions 
                        var client = new CommitParticipantService.CommitParticipantServiceClient(
                            channel.Intercept(new DprClientInterceptor(session)));
                        await client.StartTransactionAsync(transaction);
                    }

                    // Commit is non-speculative
                    var response = await coordinator.CommitAsync(new TransactionsRequest(transaction));
                    if (response.Success)
                    {
                        var endTime = stopwatch.ElapsedTicks;
                        measurements.Add(endTime - startTime);
                    }
                    else
                    {
                        measurements.Add(-1);
                    }

                }
                catch (Exception e1)
                {
                    // Console.WriteLine($"transaction {transaction.TxnId} threw exception {e1.Message} -- treating as an abort");
                    // negative to indicate abort
                    measurements.Add(-1);
                }
                finally
                {
                    sessionPool.Return(session);
                    rateLimiter.Release();
                }
            });
        }

        while (measurements.Count != numTransactionsToRun)
            await Task.Delay(5);
        await WriteResults(options, environment, measurements);
    }

    private static async Task WriteResults(Options options, IEnvironment environment,ConcurrentBag<long> measurements)
    {
        using var memoryStream = new MemoryStream();
        await using var streamWriter = new StreamWriter(memoryStream);
        var aborted = 0;
        foreach (var line in measurements)
        {
            if (line > 0)
                streamWriter.WriteLine(line * 1000.0 / Stopwatch.Frequency);
            else
                aborted++;
        }
        streamWriter.WriteLine($"Aborted: {aborted} out of {measurements.Count}");
        await streamWriter.FlushAsync();
        memoryStream.Position = 0;
        await environment.PublishResultsAsync(options.OutputFile, memoryStream);
    }

    public static async Task LaunchCommitCoordinatorService(Options options, IEnvironment environment)
    {
        var builder = WebApplication.CreateBuilder();
        
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, environment.GetCoordinatorPort(options),
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
            serverOptions.Limits.MinRequestBodyDataRate = null;
        });
        
        var checkpointManager = environment.GetCoordinatorCheckpointManager(options);
        builder.Services.AddSingleton(new FasterLogSettings
        {
            LogDevice = environment.GetCoordinatorDevice(options),
            MemorySizeBits = 30,
            LogCommitManager = checkpointManager,
            FastCommitMode = true,
            RemoveOutdatedCommits = false,
            TryRecoverLatest = false,
            AutoRefreshSafeTailAddress = true,
            AutoCommit = false
        });
        
        builder.Services.AddSingleton(new DprWorkerOptions
        {
            Me = new DprWorkerId(options.WorkerName),
            DprFinder = new GrpcDprFinder(environment.GetDprFinderConnString()),
            CheckpointPeriodMilli = 5,
            RefreshPeriodMilli = 5
        });
        
        var channels = new List<GrpcChannel>();
        for (var i = 0; i < 4; i++)
            channels.Add(GrpcChannel.ForAddress(environment.GetParticipantConnString(i)));
        builder.Services.AddSingleton(new CommitCoordinatorSettings
        {
            participants = channels,
            speculative = options.Speculative
        });

        // TODO(Tianyu): Switch to epoch after testing
        builder.Services.AddSingleton(typeof(IVersionScheme), typeof(RwLatchVersionScheme));
        builder.Services.AddSingleton<CommitLog>();
        builder.Services.AddSingleton<CommitLogBackgroundService>();
        builder.Services.AddSingleton<StateObject>(sp => sp.GetService<CommitLog>());
        builder.Services.AddSingleton<DprServerInterceptor<CommitLog>>();
        builder.Services.AddSingleton<CommitCoordinatorServiceImpl>();
        
        builder.Services.AddHostedService<CommitLogBackgroundService>(provider =>
            provider.GetRequiredService<CommitLogBackgroundService>());
        builder.Services.AddHostedService<StateObjectRefreshBackgroundService>();
        builder.Services.AddGrpc(opt => { opt.Interceptors.Add<DprServerInterceptor<CommitLog>>(); });
        var app = builder.Build();
        
        app.MapGrpcService<CommitCoordinatorServiceImpl>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        await app.RunAsync();
        foreach (var channel in channels)
            channel.Dispose();
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

    public static async Task LaunchParticipantService(Options options, IEnvironment environment)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);

        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, environment.GetParticipantPort(options),
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
            serverOptions.Limits.MinRequestBodyDataRate = null;
        });
        var checkpointManager = environment.GetParticipantCheckpointManager(options);
        
        builder.Services.AddSingleton(new FasterLogSettings
        {
            LogDevice = environment.GetParticipantDevice(options),
            MemorySizeBits = 30,
            LogCommitManager = checkpointManager,
            FastCommitMode = true,
            RemoveOutdatedCommits = false,
            TryRecoverLatest = false,
            AutoRefreshSafeTailAddress = true,
            AutoCommit = false
        });
        
        builder.Services.AddSingleton(new DprWorkerOptions
        {
            Me = new DprWorkerId(options.WorkerName),
            DprFinder = new GrpcDprFinder(environment.GetDprFinderConnString()),
            CheckpointPeriodMilli = 5,
            RefreshPeriodMilli = 5
        });

        // TODO(Tianyu): Switch to epoch after testing
        builder.Services.AddSingleton(typeof(IVersionScheme), typeof(RwLatchVersionScheme));
        builder.Services.AddSingleton<CommitLog>();
        builder.Services.AddSingleton<CommitLogBackgroundService>();
        builder.Services.AddSingleton<StateObject>(sp => sp.GetService<CommitLog>());
        builder.Services.AddSingleton<DprServerInterceptor<CommitLog>>();
        builder.Services.AddSingleton<CommitParticipantServiceImpl>();
        
        builder.Services.AddHostedService<CommitLogBackgroundService>(provider =>
            provider.GetRequiredService<CommitLogBackgroundService>());
        builder.Services.AddHostedService<StateObjectRefreshBackgroundService>();
        builder.Services.AddGrpc(opt => { opt.Interceptors.Add<DprServerInterceptor<CommitLog>>(); });
        var app = builder.Build();
        
        app.MapGrpcService<CommitParticipantServiceImpl>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        await app.RunAsync();
    }
}