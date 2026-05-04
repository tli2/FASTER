using System.Diagnostics;
using System.Net;
using CommandLine;
using dse.services;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using Google.Protobuf;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using protobuf;
using protobuf.noint;
using Task = System.Threading.Tasks.Task;

namespace microbench;

public class Options
{
    [Option('t', "type", Required = true,
        HelpText = "type of worker to launch (client or server)")]
    public string Type { get; set; }

    [Option('m', "moder", Required = false, Default = "none",
        HelpText = "none, int (dse with interceptor), or noint (dse without interceptor)")]
    public string Mode { get; set; }
    
    [Option('i', "input-file", Required = true,
        HelpText = "input file containing workload")]
    public string InputFile { get; set; }

    [Option('o', "output-file", Required = false, Default = "",
        HelpText = "Output file to dump latencies")]
    public string OutputFile { get; set; }

    [Option('w', "window", Required = false,
        HelpText = "number of outstanding client requests allowed")]
    public int Window { get; set; }
}

public interface IRequestIssuer
{
    void Initialize(string file);

    int NumRequestsLoaded();

    Task MakeReservationAsync(int requestId);
}

public class NormalRequestIssuer : IRequestIssuer
{
    private List<ReservationRequest> requests = new();
    private List<FasterKVReservationService.FasterKVReservationServiceClient> clients = new();
    private bool dse;

    public NormalRequestIssuer(bool dse)
    {
        this.dse = dse;
    }
    
    public void Initialize(string file)
    {
        foreach (var line in File.ReadLines(file))
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
        
        for (var i = 0; i < Environment.ProcessorCount; i++)
        {
            var channel = GrpcChannel.ForAddress("http://10.0.0.6:15721");
            if (dse)
                clients.Add(
                    new FasterKVReservationService.FasterKVReservationServiceClient(
                        channel.Intercept(new DprClientInterceptor(new DprSession()))));
            else
                clients.Add(new FasterKVReservationService.FasterKVReservationServiceClient(channel));
        }
    }

    public int NumRequestsLoaded()
    {
        return requests.Count;
    }

    public Task MakeReservationAsync(int requestId)
    {
        return clients[requestId % clients.Count].MakeReservationAsync(requests[requestId]).ResponseAsync;
    }
}


public class NoInterceptorRequestIssuer : IRequestIssuer
{
    private List<ReservationRequestWithHeader> requests = new();
    private SimpleObjectPool<byte[]> pool = new(() => new byte[1 << 10]);
    private List<(DprSession, FasterKVReservationNoInterceptorService.FasterKVReservationNoInterceptorServiceClient)> clients = new();
    
    public void Initialize(string file)
    {
        foreach (var line in File.ReadLines(file))
        {
            var split = line.Split(',');
            requests.Add(new ReservationRequestWithHeader
            {
                ReservationId = long.Parse(split[2]),
                OfferingId = long.Parse(split[3]),
                CustomerId = long.Parse(split[4]),
                Count = int.Parse(split[5])
            });
        }    
        
        for (var i = 0; i < Environment.ProcessorCount; i++)
        {
            var channel = GrpcChannel.ForAddress("http://10.0.0.6:15721");
            clients.Add((new DprSession(), new FasterKVReservationNoInterceptorService.FasterKVReservationNoInterceptorServiceClient(channel)));
        }
    }

    public int NumRequestsLoaded()
    {
        return requests.Count;
    }

    public async Task MakeReservationAsync(int requestId)
    {
        var (session, client) = clients[requestId % clients.Count];
        var buf = pool.Checkout();
        var size = session.TagMessage(buf);
        var request = requests[requestId];
        request.DprHeader = ByteString.CopyFrom(new Span<byte>(buf, 0, size));
        pool.Return(buf);
        var response = await client.MakeReservationAsync(request);
        session.Receive(response.DprHeader.Span);
    }
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
                if (options.Mode.Equals("int"))
                    await LaunchDseReservationService(options);
                else if (options.Mode.Equals("noint"))
                    await LaunchNoInterceptorDseReservationService(options);
                else if (options.Mode.Equals("none"))
                    await LaunchNonDseReservationService(options);
                else
                    throw new NotImplementedException();
                break;
            case "client":
            {
                IRequestIssuer issuer;
                if (options.Mode.Equals("int"))
                    issuer = new NormalRequestIssuer(true);
                else if (options.Mode.Equals("noint"))
                    issuer = new NoInterceptorRequestIssuer();
                else if (options.Mode.Equals("none"))
                    issuer = new NormalRequestIssuer(false);
                else
                    throw new NotImplementedException();
                
                issuer.Initialize(options.InputFile);
                var latencies = new List<long>();
                for (var i = 0; i < issuer.NumRequestsLoaded(); i++)
                    latencies.Add(0);

                var semaphore = new SemaphoreSlim(options.Window, options.Window);
                var stopwatch = Stopwatch.StartNew();
                for (var i = 0; i < issuer.NumRequestsLoaded(); i++)
                {
                    await semaphore.WaitAsync();
                    var startTime = stopwatch.ElapsedTicks;
                    var i1 = i;
                    _ = Task.Run(async () =>
                    {
                        await issuer.MakeReservationAsync(i1);
                        latencies[i1] = stopwatch.ElapsedTicks - startTime;
                        semaphore.Release();
                    });
                }

                await semaphore.WaitAsync();
                var totalTime = stopwatch.ElapsedMilliseconds;
                Console.WriteLine($"Throughput: {1000.0 * issuer.NumRequestsLoaded() / totalTime}");

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
                    File.WriteAllLines(options.OutputFile, milliseconds.Select(t => t.ToString()));
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
            new DefaultCheckpointNamingScheme($"./service"), removeOutdated: false);
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
        
        builder.Services.AddSingleton<LightEpoch>();
        builder.Services.AddSingleton(typeof(IVersionScheme), typeof(EpochProtectedVersionScheme));
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
    
    public static async Task LaunchNoInterceptorDseReservationService(Options options)
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
            new DefaultCheckpointNamingScheme($"./service"), removeOutdated: false);
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
        
        builder.Services.AddSingleton<LightEpoch>();
        builder.Services.AddSingleton(typeof(IVersionScheme), typeof(EpochProtectedVersionScheme));
        builder.Services.AddSingleton<FasterKvReservationStateObject>();
        builder.Services.AddSingleton(new FasterKvReservationStartFile
        {
            file = options.InputFile
        });
        builder.Services.AddSingleton<FasterKvReservationBackgroundServiceNoInt>();

        builder.Services.AddSingleton<FasterKvReservationServiceNoInt>();

        builder.Services.AddGrpc();
        builder.Services.AddHostedService<FasterKvReservationBackgroundServiceNoInt>(provider =>
            provider.GetRequiredService<FasterKvReservationBackgroundServiceNoInt>());
        builder.Services.AddHostedService<StateObjectRefreshBackgroundService>();
        var app = builder.Build();

        app.MapGrpcService<FasterKvReservationServiceNoInt>();
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
            new DefaultCheckpointNamingScheme($"./service"), removeOutdated: false);
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