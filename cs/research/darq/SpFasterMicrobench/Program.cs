using System.Diagnostics;
using System.Net;
using CommandLine;
using dse.services;
using FASTER.core;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using protobuf;
using Task = System.Threading.Tasks.Task;

namespace microbench;

public class Options
{
    [Option('t', "type", Required = true,
        HelpText = "type of worker to launch (client or server)")]
    public string Type { get; set; }

    [Option('d', "dse", Required = false, Default = false,
        HelpText = "Use dse")]
    public bool Dse { get; set; }
    
    [Option('i', "input-file", Required = true,
        HelpText = "input file containing workload")]
    public string InputFile { get; set; }

    [Option('o', "output-file", Required = false,
        HelpText = "Output file to dump latencies")]
    public string OutputFile { get; set; }

    [Option('w', "window", Required = false,
        HelpText = "number of outstanding client requests allowed")]
    public int Window { get; set; }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
        if (result.Tag == ParserResultType.NotParsed) return;
        var options = result.MapResult(o => o, xs => new Options());
        switch (options.Type)
        {
            case "server":
                if (options.Dse)
                    await LaunchDseReservationService(options);
                else
                    await LaunchNonDseReservationService(options);
                break;
            case "client":
            {
                var requests = new List<ReservationRequest>();
                foreach (var line in File.ReadLines(options.InputFile))
                {
                    var split = line.Split(',');
                    requests.Add(new ReservationRequest
                    {
                        ReservationId = long.Parse(split[2]),
                        OfferingId = long.Parse(split[3]),
                        CustomerId = long.Parse(split[4]),
                        Count = int.Parse(split[5])
                    });
                }

                var latencies = new List<long>();
                for (var i = 0; i < requests.Count; i++)
                    latencies.Add(0);

                var clients = new List<FasterKVReservationService.FasterKVReservationServiceClient>();
                for (var i = 0; i < Environment.ProcessorCount; i++)
                {
                    var channel = GrpcChannel.ForAddress("http://10.0.0.6:15721");
                    if (options.Dse)
                        clients.Add(
                            new FasterKVReservationService.FasterKVReservationServiceClient(
                                channel.Intercept(new DprClientInterceptor(new DprSession()))));
                    else
                        clients.Add(new FasterKVReservationService.FasterKVReservationServiceClient(channel));
                }

                var semaphore = new SemaphoreSlim(options.Window, options.Window);
                var stopwatch = Stopwatch.StartNew();
                for (var i = 0; i < requests.Count; i++)
                {
                    await semaphore.WaitAsync();
                    var startTime = stopwatch.ElapsedTicks;
                    var i1 = i;
                    _ = Task.Run(async () =>
                    {
                        await clients[i1 % clients.Count].MakeReservationAsync(requests[i1]);
                        semaphore.Release();
                        latencies[i1] = stopwatch.ElapsedTicks - startTime;
                    });
                }

                await semaphore.WaitAsync();
                var totalTime = stopwatch.ElapsedMilliseconds;
                Console.WriteLine($"Throughput: {1000.0 * requests.Count / totalTime}");

                var ticksPerMillisecond = Stopwatch.Frequency / 1000.0;

                // Convert Stopwatch ticks to milliseconds
                var milliseconds = latencies.Select(t => t / ticksPerMillisecond).ToList();
                milliseconds.Sort();
                var average = milliseconds.Average();

                // Calculate median
                double median = 0;
                var midIndex = milliseconds.Count / 2;
                if (milliseconds.Count % 2 == 0)
                    median = (milliseconds[midIndex - 1] + milliseconds[midIndex]) / 2.0;
                else
                    median = milliseconds[midIndex];

                // Calculate 95th percentile
                var p95Index = (int)Math.Ceiling(0.95 * milliseconds.Count) - 1;
                var p95 = milliseconds[p95Index];

                // Output results
                Console.WriteLine($"Average Latency: {average}");
                Console.WriteLine($"Median Latency: {median}");
                Console.WriteLine($"95th Percentile Latency: {p95}");

                if (!options.OutputFile.Equals(""))
                {
                    File.WriteAllLines(options.OutputFile, milliseconds.Select(t => t.ToString()));
                }
                break;
            }
        }
    }

    public static async Task LaunchDseReservationService(Options options)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);

        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, 15721,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
            serverOptions.Limits.MinRequestBodyDataRate = null;
        });

        var checkpointManager = new DeviceLogCommitCheckpointManager(
            new NullNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"D:\\service"), removeOutdated: false);
        builder.Services.AddSingleton(new FasterKVSettings<Key, Value>
        {
            IndexSize = 1 << 25,
            LogDevice = new NullDevice(),
            PageSize = 1 << 25,
            SegmentSize = 1 << 30,
            MemorySize = 1L << 32,
            CheckpointManager = checkpointManager,
            TryRecoverLatest = false,
        });
        builder.Services.AddSingleton<FasterKV<Key, Value>>();
        builder.Services.AddSingleton(new DprWorkerOptions
        {
            Me = new DprWorkerId(0),
            DprFinder = new LocalStubDprFinder(),
            CheckpointPeriodMilli = 10,
            RefreshPeriodMilli = 5
        });
        // TODO(Tianyu): Switch implementation to epoch after testing
        builder.Services.AddSingleton(typeof(IVersionScheme), typeof(RwLatchVersionScheme));
        builder.Services.AddSingleton<FasterKvReservationStateObject>();
        builder.Services.AddSingleton(new FasterKvReservationStartFile
        {
            file = options.InputFile
        });
        builder.Services.AddSingleton<FasterKvReservationBackgroundService>();

        builder.Services.AddSingleton<FasterKvReservationService>();
        builder.Services.AddSingleton<StateObject>(sp => sp.GetService<FasterKvReservationStateObject>());
        builder.Services.AddSingleton<DprServerInterceptor<FasterKvReservationService>>();

        builder.Services.AddGrpc(opt => { opt.Interceptors.Add<DprServerInterceptor<FasterKvReservationService>>(); });
        builder.Services.AddHostedService<FasterKvReservationBackgroundService>(provider =>
            provider.GetRequiredService<FasterKvReservationBackgroundService>());
        builder.Services.AddHostedService<StateObjectRefreshBackgroundService>();
        var app = builder.Build();

        app.MapGrpcService<FasterKvReservationService>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        await app.RunAsync();
    }

    public static async Task LaunchNonDseReservationService(Options options)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.AddConsole();
        builder.WebHost.ConfigureKestrel(serverOptions =>
        {
            serverOptions.Listen(IPAddress.Any, 15721,
                listenOptions => { listenOptions.Protocols = HttpProtocols.Http2; });
        });
        builder.Logging.SetMinimumLevel(LogLevel.Warning);

        var checkpointManager = new DeviceLogCommitCheckpointManager(
            new NullNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"D:\\service"), removeOutdated: false);
        builder.Services.AddSingleton(new FasterKVSettings<Key, Value>
        {
            IndexSize = 1 << 25,
            LogDevice = new NullDevice(),
            PageSize = 1 << 25,
            SegmentSize = 1 << 30,
            MemorySize = 1L << 32,
            CheckpointManager = checkpointManager,
            TryRecoverLatest = false,
        });
        builder.Services.AddSingleton<FasterKV<Key, Value>>();
        builder.Services.AddSingleton(new FasterKvReservationStartFile
        {
            file = options.InputFile
        });
        builder.Services.AddSingleton<NonDseFasterBackgroundService>();

        builder.Services.AddSingleton<NonDseReservationService>();
        builder.Services.AddGrpc();
        builder.Services.AddHostedService<NonDseFasterBackgroundService>(provider =>
            provider.GetRequiredService<NonDseFasterBackgroundService>());
        var app = builder.Build();

        app.MapGrpcService<NonDseReservationService>();
        app.MapGet("/",
            () =>
                "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
        await app.RunAsync();
    }
}