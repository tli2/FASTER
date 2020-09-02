using System;
using System.Runtime.CompilerServices;
using CommandLine;

namespace FASTER.benchmark
{
    public class BenchmarkConsts
    {
#if DEBUG
        public const bool kUseSmallData = true;
        public const bool kUseSyntheticData = true;
#else
        public const bool kUseSmallData = false;
        public const bool kUseSyntheticData = false;
#endif
        public const long kInitCount = kUseSmallData ? 2500480 : 250000000;
        public const long kTxnCount = kUseSmallData ? 10000000 : 1000000000;
        public const int kMaxKey = kUseSmallData ? 1 << 22 : 1 << 28;

        public const int kFileChunkSize = 4096;
        public const long kChunkSize = 640;
        public const int kRunSeconds = 30;

        public const int kWorkerIdBits = 3;
    }

    class Options
    {
        [Option('t', "type", Required = true, HelpText = "worker or coordinator")]
        public string Type { get; set; }

        [Option('c', "client-threads", Required = false, Default = 8,
            HelpText = "Number of threads to run the workload on in the client")]
        public int ClientThreadCount { get; set; }
        
        [Option('s', "server-threads", Required = false, Default = 2,
            HelpText = "Number of threads to run the workload on in the server")]
        public int ServerThreadCount { get; set; }
        
        [Option('b', "batch-size", Required = false, Default = 1024,
            HelpText = "Number of requests to batch per client before sending to the server")]
        public int BatchSize { get; set; }
        
        [Option('d', "distribution", Required = false, Default = "uniform",
            HelpText = "Distribution of keys in workload")]
        public string Distribution { get; set; }

        [Option('r', "read_percent", Required = false, Default = 50,
            HelpText = "Percentage of reads (-1 for 100% read-modify-write")]
        public int ReadPercent { get; set; }
        
        [Option('p', "remote_percent", Required = false, Default = 0)]
        public int RemotePercent { get; set; }

        [Option('n', "worker_id", Required = false, Default = 0)]
        public int WorkerId { get; set; }

        [Option('i', "checkpoint_interval", Required = false, Default = -1)]
        public int CheckpointInterval { get; set; }

        [Option('w', "window_size", Required = false, Default = 4096)]
        public int WindowSize { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) throw new Exception();

            var options = result.MapResult(o => o, xs => new Options());
            if (options.Type.Equals("coordinator"))
            {
                var c = new YcsbCoordinator(new BenchmarkConfiguration
                {
                    clientThreadCount = options.ClientThreadCount,
                    execThreadCount = options.ServerThreadCount,
                    distribution = options.Distribution,
                    readPercent = options.ReadPercent,
                    remotePercent = options.RemotePercent,
                    checkpointMilli = options.CheckpointInterval, // no checkpoints
                    windowSize = options.WindowSize,
                    batchSize = options.BatchSize
                });
                c.Run();
            }
            else if (options.Type.Equals("worker"))
            {
                var w = new YcsbWorker(options.WorkerId);
                w.Run();
            }
            else
            {
                throw new Exception();
            }
        }
    }
}