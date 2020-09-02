using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using FASTER.serverless;
using Nito.AsyncEx;

namespace FASTER.benchmark
{
    public class YcsbCoordinator
    {
        public static ClusterConfiguration clusterConfig;
        static YcsbCoordinator()
        {
            clusterConfig = new ClusterConfiguration();
            clusterConfig.AddWorker("10.0.1.8", 15721)
            .AddWorker("10.0.1.9", 15721)
            .AddWorker("10.0.1.11", 15721)
            .AddWorker("10.0.1.10", 15721)
            .AddWorker("10.0.1.12", 15721)
            .AddWorker("10.0.1.13", 15721)
            .AddWorker("10.0.1.14", 15721)
            .AddWorker("10.0.1.15", 15721);
        }
        
        private BenchmarkConfiguration benchmarkConfig;
        
        
        public YcsbCoordinator(BenchmarkConfiguration benchmarkConfig)
        {
            this.benchmarkConfig = benchmarkConfig;
        }

        public void Run()
        {
            foreach (var workerInfo in clusterConfig.workers)
            {
                var ip = IPAddress.Parse(workerInfo.GetAddress());
                var endPoint = new IPEndPoint(ip, 15000);
                var sender = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                sender.Connect(endPoint);
                sender.Close();
            }

            Thread.Sleep(5000);

            var metadataStore = new AzureSqlOwnershipMapping(benchmarkConfig.connString);
            // Setup metadata store
            foreach (var workerInfo in clusterConfig.workers)
            {
                var owner = AsyncContext.Run(async () =>
                    await metadataStore.ObtainOwnershipAsync(workerInfo.GetWorker().guid, workerInfo.GetWorker(),
                        Worker.INVALID));
                Debug.Assert(owner.Equals(workerInfo.GetWorker()));
            }

            var handlerThreads = new List<Thread>();
            var setupFinished = new CountdownEvent(clusterConfig.workers.Count);
            var workerFinished = new CountdownEvent(clusterConfig.workers.Count);
            long totalOps = 0, totalRemote = 0, totalBackground = 0;
            var stopwatch = new Stopwatch();
            foreach (var workerInfo in clusterConfig.workers)
            {
                var ip = IPAddress.Parse(workerInfo.GetAddress());
                var endPoint = new IPEndPoint(ip, workerInfo.GetPort() + 1);
                var sender = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                sender.Connect(endPoint);

                sender.SendBenchmarkControlMessage(benchmarkConfig);
                var handlerThread = new Thread(() =>
                {
                    while (true)
                    {
                        var message = sender.ReceiveBenchmarkMessage();
                        if (message.type == 1)
                        {
                            if (setupFinished.Signal())
                                stopwatch.Start();
                            break;
                        }
                    }

                    setupFinished.Wait();
                    sender.SendBenchmarkControlMessage("start benchmark");

                    while (true)
                    {
                        var message = sender.ReceiveBenchmarkMessage();
                        if (message == null) break;
                        if (message.type == 1)
                        {
                            var (ops, numRemote, numBackground) = (ValueTuple<long, long, long>) message.content;
                            Interlocked.Add(ref totalOps, ops);
                            Interlocked.Add(ref totalRemote, numRemote);
                            Interlocked.Add(ref totalBackground, numBackground);
                            if (workerFinished.Signal()) stopwatch.Stop();
                            workerFinished.Wait();
                            sender.SendBenchmarkControlMessage("shutdown");
                        }
                    }
                    sender.Close();
                });
                handlerThreads.Add(handlerThread);
                handlerThread.Start();
            }

            foreach (var thread in handlerThreads)
                thread.Join();

            var bgPercentage = totalRemote == 0 ? 0.0 : (double) totalBackground / totalRemote;
            Console.WriteLine($"############total throughput {1000.0 * totalOps / stopwatch.ElapsedMilliseconds}, %Background {100 * bgPercentage}, {benchmarkConfig}");
        }
    }
}