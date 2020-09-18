﻿using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
 using System.Linq;
 using System.Net;
using System.Net.Sockets;
 using System.Reflection.PortableExecutable;
 using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using FASTER.serverless;
using Nito.AsyncEx;

namespace FASTER.benchmark
{
    public enum Op : ulong
    {
        Upsert = 0,
        Read = 1,
        ReadModifyWrite = 2
    }

    public class YcsbBucketingScheme : IBucketingScheme<Key>
    {
        public long GetBucket(Key key)
        {
            // TODO(Tianyu): is bucketing scheme based on string too inefficient?
            return (long) ((ulong) key.value >> (64 - BenchmarkConsts.kWorkerIdBits));
        }
    }

    public class YcsbServer
    {
        internal int workerId;
        internal volatile bool done;
        internal Key[] init_keys_;
        internal IDevice device;
        
        internal long idx_;

        internal FasterServerless<Key, Value, Input, Output, Functions> fasterServerless;
        internal string lastDist = "";
        internal ServerfulMessageManager messageManager;
        internal FasterServerlessBackgroundThreadPool<Key, Value, Input, Output, Functions> threadPool;

        public YcsbServer(int workerId)
        {
            this.workerId = workerId;
        }

        private void PrintToCoordinator(string message, Socket coordinatorConn)
        {
            Console.WriteLine(message);
            coordinatorConn.SendBenchmarkInfoMessage($"worker {workerId}: {message}" + Environment.NewLine);
        }

        public void Run()
        {
            var info = YcsbCoordinator.clusterConfig.GetInfoForId(workerId);
            var addr = IPAddress.Parse(info.GetAddress());
            var servSock = new Socket(addr.AddressFamily,
                SocketType.Stream, ProtocolType.Tcp);
            var local = new IPEndPoint(addr, info.GetPort() + 1);
            servSock.Bind(local);
            servSock.Listen(512);

            var clientSocket = servSock.Accept();
            var message = clientSocket.ReceiveBenchmarkMessage();
            Debug.Assert(message.type == 1);
            var config = (BenchmarkConfiguration) message.content;
            Reset();
            ExecuteOnce(info.GetWorker(), config, clientSocket);
            Reset();
            clientSocket.Close();
        }

        public void Reset()
        {
            done = true;
            messageManager?.Dispose();
            fasterServerless?.localFaster.Dispose();
            threadPool?.Dispose();
            device?.Close();
            GC.Collect();
            done = false;
        }

        private void RunMonitorThread(BenchmarkConfiguration configuration)
        {
            var sw = new Stopwatch();
            sw.Start();
            fasterServerless.checkpointLatencies.Clear();
            while (true)
            {
                var timeElapsed = sw.ElapsedMilliseconds;
                if (timeElapsed >= 1000 * BenchmarkConsts.kRunSeconds) break;
                var expectedVersion = timeElapsed / configuration.checkpointMilli + 1;
                if (fasterServerless.CurrentVersion() < expectedVersion)
                    fasterServerless.InitiateVersionBump(expectedVersion);
                fasterServerless.RefreshDprTable();
                if (fasterServerless.CurrentVersion() < fasterServerless.inProgressBump)
                    fasterServerless.localFaster._fasterKV.BumpVersion(out _, fasterServerless.inProgressBump, out _);
            }
            if (fasterServerless.checkpointLatencies.Count != 0)
                Console.WriteLine($"server performed {fasterServerless.checkpointLatencies.Count} checkpoints, average duration {fasterServerless.checkpointLatencies.Average()} ms");
        }

        private void Setup(BenchmarkConfiguration configuration)
        {
            var setupSessions = new Thread[Environment.ProcessorCount];
            var countdown = new CountdownEvent(setupSessions.Length);
            var completed = new ManualResetEventSlim();
            for (var idx = 0; idx < setupSessions.Length; ++idx)
            {
                var x = idx;
                setupSessions[idx] = new Thread(() =>
                {
                    var s = fasterServerless.NewServerlessSession(configuration.windowSize,
                        configuration.batchSize, configuration.checkpointMilli != -1);

                    Value value = default;
                    for (var chunkStart = Interlocked.Add(ref idx_, BenchmarkConsts.kChunkSize) -
                                          BenchmarkConsts.kChunkSize;
                        chunkStart < BenchmarkConsts.kInitCount;
                        chunkStart = Interlocked.Add(ref idx_, BenchmarkConsts.kChunkSize) -
                                     BenchmarkConsts.kChunkSize)
                    {
                        for (var idx = chunkStart; idx < chunkStart + BenchmarkConsts.kChunkSize; ++idx)
                        {
                            if (idx % 256 == 0)
                            {
                                s.Refresh();
                            }

                            var status = s.Upsert(ref init_keys_[idx], ref value, out _);
                            Debug.Assert(status == Status.OK);
                        }
                    }

                    countdown.Signal();
                    while (!completed.IsSet) s.Refresh(); 
                    s.Dispose();
                });
                setupSessions[idx].Start();
            }

            countdown.Wait();
            AsyncContext.Run(async () => await fasterServerless.PerformCheckpoint());
            completed.Set();
            foreach (var s in setupSessions)
                s.Join();
        }

        public void ExecuteOnce(Worker me, BenchmarkConfiguration configuration, Socket coordinatorConn)
        {
            messageManager = new ServerfulMessageManager(me, YcsbCoordinator.clusterConfig.GetRoutingTable());
            var metadataStore =
                new MetadataStore(new AzureSqlOwnershipMapping(configuration.connString), messageManager);
            var dprManager = new AzureSqlDprManagerV1(configuration.connString, me);
            device = Devices.CreateLogDevice("D:\\hlog", true, true);
            fasterServerless = new FasterServerless<Key, Value, Input, Output, Functions>(
                metadataStore, messageManager, dprManager, BenchmarkConsts.kMaxKey / 2, new Functions(),
                new LogSettings {LogDevice = device, PreallocateLog = true},
                checkpointSettings: new CheckpointSettings {CheckpointDir = "D:\\checkpoints"}, 
                bucketingScheme: new YcsbBucketingScheme(),
                serializer: new YcsbParameterSerializer());
            threadPool = new FasterServerlessBackgroundThreadPool<Key, Value, Input, Output, Functions>();

            // Warm up local metadata store cache for self
            AsyncContext.Run(async () => await metadataStore.LookupAsync(workerId));

            if (!configuration.distribution.Equals(lastDist))
            {
                LoadData(configuration, coordinatorConn);
                lastDist = configuration.distribution;
            }
            
            PrintToCoordinator("Starting Server", coordinatorConn);
            threadPool.Start(1, fasterServerless);
            messageManager.StartServer(fasterServerless, threadPool);

            PrintToCoordinator("Executing setup.", coordinatorConn);
            idx_ = 0;
            
            var sw = new Stopwatch();
            sw.Start();
            Setup(configuration);
            if (configuration.execThreadCount < 16)
            {
                long affinityMask = (1 << (configuration.execThreadCount + 1)) - 1;
                Process.GetCurrentProcess().ProcessorAffinity = (IntPtr) affinityMask;
            }
            sw.Stop();
            PrintToCoordinator($"Loading time: {sw.ElapsedMilliseconds}ms", coordinatorConn);
            coordinatorConn.SendBenchmarkControlMessage("setup finished");
            coordinatorConn.ReceiveBenchmarkMessage();
            RunMonitorThread(configuration);
            coordinatorConn.ReceiveBenchmarkMessage();
            coordinatorConn.SendBenchmarkControlMessage(ValueTuple.Create(0.0, 0.0));
        }

        public static Key KeyForWorker(Key original, int workerId)
        {
            // Construct the local key by dropping the highest-order 8 bits and replacing with worker id
            return new Key
            {
                value = (long) ((ulong) original.value >> BenchmarkConsts.kWorkerIdBits) |
                        ((long) workerId << (64 - BenchmarkConsts.kWorkerIdBits))
            };
        }

        #region Load Data

        private unsafe void LoadDataFromFile(string filePath, BenchmarkConfiguration configuration, Socket coordinatorConn)
        {
            var init_filename = filePath + "\\load_" + configuration.distribution + "_250M_raw.dat";
            var txn_filename = filePath + "\\run_" + configuration.distribution + "_250M_1000M_raw.dat";

            long count = 0;
            using (var stream = File.Open(init_filename, FileMode.Open, FileAccess.Read,
                FileShare.Read))
            {
                PrintToCoordinator("loading keys from " + init_filename + " into memory...", coordinatorConn);
                init_keys_ = new Key[BenchmarkConsts.kInitCount];

                var chunk = new byte[BenchmarkConsts.kFileChunkSize];
                var chunk_handle = GCHandle.Alloc(chunk, GCHandleType.Pinned);
                var chunk_ptr = (byte*) chunk_handle.AddrOfPinnedObject();

                long offset = 0;

                while (true)
                {
                    stream.Position = offset;
                    int size = stream.Read(chunk, 0, BenchmarkConsts.kFileChunkSize);
                    for (int idx = 0; idx < size; idx += 8)
                    {
                        init_keys_[count] = KeyForWorker(new Key {value = *(long*) (chunk_ptr + idx)}, workerId);
                        ++count;
                    }

                    if (size == BenchmarkConsts.kFileChunkSize)
                        offset += BenchmarkConsts.kFileChunkSize;
                    else
                        break;

                    if (count == BenchmarkConsts.kInitCount)
                        break;
                }

                if (count != BenchmarkConsts.kInitCount)
                {
                    throw new InvalidDataException("Init file load fail!");
                }
            }

            PrintToCoordinator($"loaded {BenchmarkConsts.kInitCount} keys.", coordinatorConn);
        }

        private void LoadData(BenchmarkConfiguration configuration, Socket coordinatorConn)
        {
            if (BenchmarkConsts.kUseSyntheticData)
            {
                LoadSyntheticData(coordinatorConn);
                return;
            }

            var filePath = "Z:\\ycsb_files";
            LoadDataFromFile(filePath, configuration, coordinatorConn);
        }

        private void LoadSyntheticData(Socket coordinatorConn)
        {
            PrintToCoordinator("Loading synthetic data (uniform distribution)", coordinatorConn);

            init_keys_ = new Key[BenchmarkConsts.kInitCount];
            long val = 0;
            for (var idx = 0; idx < BenchmarkConsts.kInitCount; idx++)
            {
                var generatedValue = new Key {value = val++};
                init_keys_[idx] = KeyForWorker(generatedValue, workerId);
            }

            PrintToCoordinator($"loaded {BenchmarkConsts.kInitCount} keys.", coordinatorConn);
        }

        #endregion
    }
}