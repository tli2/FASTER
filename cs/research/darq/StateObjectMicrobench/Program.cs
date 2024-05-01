using System.Diagnostics;
using System.Runtime.InteropServices;
using CommandLine;
using FASTER.core;
using FASTER.libdpr;
using Task = System.Threading.Tasks.Task;

namespace microbench;

public class Options
{
    [Option('t', "type", Required = true,
        HelpText = "Type of benchmark: local (0), receive-send (1), detach-merge (2)")]
    public int Type { get; set; }
    
    [Option('n', "num-threads", Required = false, Default = 5, 
        HelpText = "number of threads doing work")]
    public int NumThreads { get; set; }
    
    [Option('i', "checkpoint-interval", Required = false, Default = 10,
        HelpText = "number of threads doing work")]
    public int CheckpointInterval { get; set; }
    
    [Option('o', "num-ops", Required = false, Default = 1000000,
        HelpText = "number of operations each thread will execute")]
    public int NumOps { get; set; }
}

public class TestStateObject : StateObject
{
    public TestStateObject(IVersionScheme versionScheme, DprWorkerOptions options) : base(versionScheme, options)
    {
    }

    public override void PerformCheckpoint(long version, ReadOnlySpan<byte> metadata, Action onPersist)
    {
        onPersist();
    }

    public override void RestoreCheckpoint(long version, out ReadOnlySpan<byte> metadata)
    {
        throw new NotImplementedException();
    }

    public override void PruneVersion(long version)
    {
    }

    public override IEnumerable<Memory<byte>> GetUnprunedVersions()
    {
        yield break;
    }

    public override void Dispose()
    {
    }
}

public class Program
{
    public static void Main(string[] args)
    {
        ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
        if (result.Tag == ParserResultType.NotParsed) return;
        var options = result.MapResult(o => o, xs => new Options());
        
        var tested = new TestStateObject(new EpochProtectedVersionScheme(new LightEpoch()), new DprWorkerOptions
        {
            Me = new DprWorkerId(0),
            DprFinder = new LocalStubDprFinder(),
            CheckpointPeriodMilli = options.CheckpointInterval,
            RefreshPeriodMilli = 5
        });
        var backgroundTask = new StateObjectRefreshBackgroundService(null, tested);
        _ = Task.Run(() => backgroundTask.StartAsync(default));

        var random = new Random();
        var threads = new List<Thread>();
        for (var i = 0; i < options.NumThreads; i++)
        {
            threads.Add(new Thread(() => RunBenchmarkThread(tested, options.NumOps, options.Type)));
        }

        var stopwatch = Stopwatch.StartNew();
        tested.ConnectToCluster(out _);
        foreach (var thread in threads)
            thread.Start();
        foreach (var thread in threads)
            thread.Join();
        Console.WriteLine(options.NumThreads * options.NumOps * 1000.0 / stopwatch.ElapsedMilliseconds);
        backgroundTask.StopAsync(default);
    }

    public static unsafe void RunBenchmarkThread(TestStateObject so, int numOps, int mode)
    {
        var headers = new List<byte[]>();
        if (mode == 1)
        {
            var random = new Random();
            for (var i = 0; i < numOps; i++)
            {
                headers.Add(new byte[DprMessageHeader.FixedLenSize]);
                ref var header =
                    ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprMessageHeader>(headers[i]));
                header.WorldLine = 0;
                header.Version = 1;
                header.SrcWorkerId = new DprWorkerId(random.Next() % LightDependencySet.MaxClusterSize);
            }
        }

        var prevSession = so.DetachFromWorker();
        var headerBytes = stackalloc byte[DprMessageHeader.FixedLenSize];
        for (var i = 0; i < numOps; i++)
        {
            switch (mode)
            {
                case 0:
                    so.StartLocalAction();
                    so.EndAction();
                    break;
                case 1:
                    so.TryReceiveAndStartAction(headers[i]);
                    so.ProduceTagAndEndAction(new Span<byte>(headerBytes, 1 << 10));
                    break;
                case 2:
                    so.TryMergeAndStartAction(prevSession);
                    prevSession = so.DetachFromWorkerAndPauseAction();
                    break;
                default:
                    throw new NotImplementedException();
            }
        }
    }
}