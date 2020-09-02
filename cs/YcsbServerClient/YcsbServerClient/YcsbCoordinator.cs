using System;
using System.Collections.Generic;
using System.Data.SqlClient;
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
            clusterConfig.AddServer("10.0.1.8", 15721)
                .AddServer("10.0.1.9", 15721)
                .AddServer("10.0.1.11", 15721)
                .AddServer("10.0.1.10", 15721)
                .AddServer("10.0.1.12", 15721)
                .AddServer("10.0.1.13", 15721)
                .AddServer("10.0.1.14", 15721)
                .AddServer("10.0.1.15", 15721)
                .AddClient("10.0.1.16", 15721)
                .AddClient("10.0.1.17", 15721)
                .AddClient("10.0.1.18", 15721)
                .AddClient("10.0.1.19", 15721)
                .AddClient("10.0.1.20", 15721)
                .AddClient("10.0.1.21", 15721)
                .AddClient("10.0.1.22", 15721)
                .AddClient("10.0.1.23", 15721);
        }
        
        private BenchmarkConfiguration benchmarkConfig;
        
        
        public YcsbCoordinator(BenchmarkConfiguration benchmarkConfig)
        {
            this.benchmarkConfig = benchmarkConfig;
        }

        public void Run()
        {
            var metadataStore = new AzureSqlOwnershipMapping(benchmarkConfig.connString);
            // Setup metadata store
            foreach (var workerInfo in clusterConfig.servers)
            {
                var owner = AsyncContext.Run(async () =>
                    await metadataStore.ObtainOwnershipAsync(workerInfo.GetWorker().guid, workerInfo.GetWorker(),
                        Worker.INVALID));
                Debug.Assert(owner.Equals(workerInfo.GetWorker()));
            }
            var conn = new SqlConnection(benchmarkConfig.connString);
            conn.Open();
            var cleanup = new SqlCommand("EXEC cleanup", conn);
            cleanup.ExecuteNonQuery();

            foreach (var workerInfo in clusterConfig.members)
            {
                var ip = IPAddress.Parse(workerInfo.GetAddress());
                var endPoint = new IPEndPoint(ip, 15000);
                var sender = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                sender.Connect(endPoint);
                sender.Close();
            }

            Thread.Sleep(5000);

            var handlerThreads = new List<Thread>();
            var setupFinished = new CountdownEvent(clusterConfig.members.Count);
            long totalOps = 0, totalRemote = 0, totalBackground = 0;
            var stopwatch = new Stopwatch();
            var clientCountdown = new CountdownEvent(clusterConfig.members.Count - clusterConfig.servers.Count);
            var shutdown = new ManualResetEventSlim();
            foreach (var memberInfo in clusterConfig.clients)
            {
                var ip = IPAddress.Parse(memberInfo.GetAddress());
                var endPoint = new IPEndPoint(ip, memberInfo.GetPort() + 1);
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
                            clientCountdown.Signal();
                            shutdown.Wait();
                            sender.SendBenchmarkControlMessage("shutdown");
                        }
                    }
                    sender.Close();
                });
                handlerThreads.Add(handlerThread);
                handlerThread.Start();
            }
            
            foreach (var memberInfo in clusterConfig.servers)
            {
                var ip = IPAddress.Parse(memberInfo.GetAddress());
                var endPoint = new IPEndPoint(ip, memberInfo.GetPort() + 1);
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
                    
                    shutdown.Wait();
                    sender.SendBenchmarkControlMessage("shutdown");
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
                        }
                    }
                    sender.Close();
                });
                handlerThreads.Add(handlerThread);
                handlerThread.Start();
            }

            if (BenchmarkConsts.kTriggerRecovery)
            {
                while (stopwatch.ElapsedMilliseconds < BenchmarkConsts.kRunSeconds / 2 * 1000)
                {
                    Thread.Sleep(1000);
                }
                var command = new SqlCommand($"EXEC setSystemWorldLine @worldLine=1", conn);
                command.ExecuteNonQuery();
            }
            

            clientCountdown.Wait();
            stopwatch.Stop();
            clientCountdown.Reset(clusterConfig.servers.Count);
            shutdown.Set();
            foreach (var thread in handlerThreads)
                thread.Join();

            Console.WriteLine($"############total throughput {1000.0 * totalOps / stopwatch.ElapsedMilliseconds}, {benchmarkConfig}");
        }
    }
}