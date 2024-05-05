using System.Diagnostics;
using CommandLine;
using FASTER.core;
using FASTER.libdpr;
using Task = System.Threading.Tasks.Task;

namespace microbench;

public class Options
{
    [Option('p', "detach-probability", Required = false, Default = 0.0,
        HelpText = "probably of detach-merge in the workload. The remaining operations will be actions")]
    public double DetachProbability { get; set; }
    
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
            var workload = new byte[options.NumOps];
            for (var j = 0; j < options.NumOps; j++)
                workload[j] = (byte)(random.NextDouble() < options.DetachProbability ? 1 : 0);
            threads.Add(new Thread(() => RunBenchmarkThread(tested, workload)));
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

    public static void RunBenchmarkThread(TestStateObject so, byte[] workload)
    {
        DprSession prevSession = null;
        foreach (var op in workload)
        {
            if (prevSession != null)
                so.TryMergeAndStartAction(prevSession);
            else
                so.StartLocalAction();
            switch (op)
            {
                case 0:
                    so.EndAction();
                    break;
                case 1:
                    prevSession = so.DetachFromWorkerAndPauseAction();
                    break;
                default:
                    throw new NotImplementedException();
            }
        }
    }
}