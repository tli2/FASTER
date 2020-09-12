﻿using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
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

    public class YcsbWorker
    {
        internal int workerId;
        internal volatile bool done;
        internal long totalOps;
        internal Key[] init_keys_;
        internal Key[] txn_keys_;

        internal long idx_;
        internal Input[] input_;

        internal IDevice device;
        internal FasterServerless<Key, Value, Input, Output, Functions> fasterServerless;
        internal string lastDist = "";
        internal ServerfulMessageManager messageManager;
        internal FasterServerlessBackgroundThreadPool<Key, Value, Input, Output, Functions> threadPool;

        public YcsbWorker(int workerId)
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
            servSock.Listen(128);
            var worker = new YcsbWorker(workerId);

            var clientSocket = servSock.Accept();
            var message = clientSocket.ReceiveBenchmarkMessage();
            Debug.Assert(message.type == 1);
            var config = (BenchmarkConfiguration) message.content;
            worker.Reset();
            worker.ExecuteOnce(info.GetWorker(), config, clientSocket);
            clientSocket.Close();
            worker.Reset();
        }

        public void Reset()
        {
            done = false;
            totalOps = 0;
            messageManager?.Dispose();
            fasterServerless?.localFaster.Dispose();
            threadPool?.Dispose();
            device?.Close();
            GC.Collect();
        }

        private void Setup(Worker me, BenchmarkConfiguration configuration, Socket coordinatorConn)
        {
            var setupSessions = new Thread[Environment.ProcessorCount];
            var countdown = new CountdownEvent(setupSessions.Length);
            var done = new ManualResetEventSlim();
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
                    while (!done.IsSet) s.Refresh(); 
                    s.Dispose();
                });
                setupSessions[idx].Start();
            }

            countdown.Wait();
            AsyncContext.Run(async () => await fasterServerless.PerformCheckpoint());
            done.Set();
            foreach (var s in setupSessions)
                s.Join();
        }

        public void ExecuteOnce(Worker me, BenchmarkConfiguration configuration, Socket coordinatorConn)
        {
            var numWorkers = YcsbCoordinator.clusterConfig.workers.Count;
            messageManager = new ServerfulMessageManager(me, YcsbCoordinator.clusterConfig.GetRoutingTable());
            var metadataStore =
                new MetadataStore(new AzureSqlOwnershipMapping(configuration.connString), messageManager);
            var dprManager = new AzureSqlDprManagerV2(configuration.connString, me);
            device = Devices.CreateLogDevice("D:\\hlog", deleteOnClose: true, preallocateFile:true);
            fasterServerless = new FasterServerless<Key, Value, Input, Output, Functions>(
                metadataStore, messageManager, dprManager, BenchmarkConsts.kMaxKey / 2, new Functions(),
                new LogSettings {LogDevice = device, PreallocateLog = true},
                checkpointSettings: new CheckpointSettings {CheckpointDir = "D:\\checkpoints"}, 
                bucketingScheme: new YcsbBucketingScheme(),
                serializer: new YcsbParameterSerializer());
            threadPool = new FasterServerlessBackgroundThreadPool<Key, Value, Input, Output, Functions>();

            // Warm up local metadata store cache
            for (var i = 0; i < numWorkers; i++)
            {
                var i1 = i;
                AsyncContext.Run(async () => await metadataStore.LookupAsync(i1));
            }

            if (!configuration.distribution.Equals(lastDist))
            {
                LoadData(configuration, coordinatorConn);
                lastDist = configuration.distribution;
            }

            input_ = new Input[8];
            for (var i = 0; i < 8; i++)
                input_[i].value = i;

            PrintToCoordinator("Starting Server", coordinatorConn);
            threadPool.Start(2, fasterServerless);
            messageManager.StartServer(fasterServerless, threadPool);
            
            PrintToCoordinator("Executing setup.", coordinatorConn);
            idx_ = 0;
            var sw = new Stopwatch();
            sw.Start();
            Setup(me, configuration, coordinatorConn);
            sw.Stop();
            if (configuration.execThreadCount < 16)
            {
                long affinityMask = (1 << (configuration.execThreadCount + 1)) - 1;
                Process.GetCurrentProcess().ProcessorAffinity = (IntPtr) affinityMask;
            }
            var logStartAddress = fasterServerless.localFaster.Log.TailAddress;
            PrintToCoordinator($"Loading time: {sw.ElapsedMilliseconds}ms", coordinatorConn);
            coordinatorConn.SendBenchmarkControlMessage("setup finished");
            coordinatorConn.ReceiveBenchmarkMessage();

            PrintToCoordinator($"Executing experiment, start log address {logStartAddress}", coordinatorConn);
            idx_ = 0;
            var localSessions = new Thread[configuration.clientThreadCount];
            var completionCountdown = new CountdownEvent(configuration.clientThreadCount);
            for (var idx = 0; idx < localSessions.Length; ++idx)
            {
                var x = idx;
                localSessions[idx] = new Thread(() =>
                {
                    var s = fasterServerless.NewServerlessSession(configuration.windowSize,
                        configuration.batchSize, configuration.checkpointMilli != -1);
                    Value value = default;
                    Input input = default;
                    var output = new ReadResult<Output>();

                    var rng = new RandomGenerator((uint) (workerId * YcsbCoordinator.clusterConfig.workers.Count + x));
                    while (!done)
                    {
                        var chunk_idx = Interlocked.Add(ref idx_, BenchmarkConsts.kChunkSize) -
                                        BenchmarkConsts.kChunkSize;
                        while (chunk_idx >= BenchmarkConsts.kTxnCount)
                        {
                            if (chunk_idx == BenchmarkConsts.kTxnCount) idx_ = 0;
                            chunk_idx = Interlocked.Add(ref idx_, BenchmarkConsts.kChunkSize) -
                                        BenchmarkConsts.kChunkSize;
                        }

                        for (var idx = chunk_idx;
                            idx < chunk_idx + BenchmarkConsts.kChunkSize && !done;
                            ++idx)
                        {
                            Op op;
                            var r = rng.Generate(100);
                            if (r < configuration.readPercent)
                                op = Op.Read;
                            else if (configuration.readPercent >= 0)
                                op = Op.Upsert;
                            else
                                op = Op.ReadModifyWrite;

                            if (idx % 256 == 0)
                            {
                                s.Refresh();
                            }

                            while (s.NumRemotePendingOps() >= configuration.windowSize)
                            {
                                s.CompletePending(TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond / 2));
                                if (s.NumRemotePendingOps() < configuration.windowSize) break;
                                s.Refresh();
                                Thread.Yield();
                            }
                            

                            switch (op)
                            {
                                case Op.Upsert:
                                    s.Upsert(ref txn_keys_[idx], ref value, out _);
                                    break;
                                case Op.Read:
                                    s.Read(ref txn_keys_[idx], ref input, output, out _);
                                    break;
                                case Op.ReadModifyWrite:
                                    s.RMW(ref txn_keys_[idx], ref input_[idx & 0x7], out _);
                                    break;
                                default:
                                    throw new InvalidOperationException("Unexpected op: " + op);
                            }
                        }
                    }

                    Interlocked.Add(ref totalOps, s.NextSerialNum() - s.NumRemotePendingOps());
                    completionCountdown.Signal();
                    s.CompletePending(TimeSpan.FromMilliseconds(1), true);
                    s.Dispose();
                });
                localSessions[idx].Start();
            }
            sw.Restart();
            if (configuration.checkpointMilli > 0)
            {
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
            }
            while (sw.ElapsedMilliseconds < 1000 * BenchmarkConsts.kRunSeconds)
                Thread.Sleep((int) (1000 * BenchmarkConsts.kRunSeconds - sw.ElapsedMilliseconds));

            done = true;
            PrintToCoordinator($"Done issuing operations", coordinatorConn);
            completionCountdown.Wait();
            sw.Stop();
            var seconds = sw.ElapsedMilliseconds / 1000.0;
            var logEndAddress = fasterServerless.localFaster.Log.TailAddress;
            PrintToCoordinator($"## received {fasterServerless.numRemote} remote operations, {fasterServerless.numBackground} processed in the background", coordinatorConn);
            PrintToCoordinator(
                $"##, {configuration}, {totalOps / seconds}, {logEndAddress - logStartAddress}", coordinatorConn);
            foreach (var session in localSessions)
                session.Join();
            coordinatorConn.SendBenchmarkControlMessage(ValueTuple.Create(totalOps, fasterServerless.numRemote, fasterServerless.numBackground));
            coordinatorConn.ReceiveBenchmarkMessage();
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
        private unsafe void LoadDataFromFile(string filePath, BenchmarkConfiguration configuration,
            Socket coordinatorConn)
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


            using (var stream = File.Open(txn_filename, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                var chunk = new byte[BenchmarkConsts.kFileChunkSize];
                var chunk_handle = GCHandle.Alloc(chunk, GCHandleType.Pinned);
                var chunk_ptr = (byte*) chunk_handle.AddrOfPinnedObject();

                PrintToCoordinator($"loading txns from {txn_filename} into memory...", coordinatorConn);

                txn_keys_ = new Key[BenchmarkConsts.kTxnCount];

                count = 0;
                long offset = 0;
                
                var rng = new RandomGenerator((uint) workerId);

                while (true)
                {
                    stream.Position = offset;
                    var size = stream.Read(chunk, 0, BenchmarkConsts.kFileChunkSize);
                    for (var idx = 0; idx < size; idx += 8)
                    {
                        var ownerId = workerId;
                        var p = (int) rng.Generate(100);
                        if (p < configuration.remotePercent)
                            ownerId = (int) rng.Generate((uint) YcsbCoordinator.clusterConfig.workers.Count);
                        txn_keys_[count] = KeyForWorker(new Key {value = *(long*) (chunk_ptr + idx)}, ownerId);
                        ++count;

                        if (count % (BenchmarkConsts.kTxnCount / 100) == 0)
                            Console.Write(".");
                    }

                    if (size == BenchmarkConsts.kFileChunkSize)
                        offset += BenchmarkConsts.kFileChunkSize;
                    else
                        break;

                    if (count == BenchmarkConsts.kTxnCount)
                        break;
                }

                if (count != BenchmarkConsts.kTxnCount)
                {
                    throw new InvalidDataException($"Txn file load fail! {count}: {BenchmarkConsts.kTxnCount}");
                }
            }
            PrintToCoordinator($"loaded {BenchmarkConsts.kTxnCount} txns.", coordinatorConn);
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

            var generator = new RandomGenerator();

            txn_keys_ = new Key[BenchmarkConsts.kTxnCount];

            for (var idx = 0; idx < BenchmarkConsts.kTxnCount; idx++)
            {
                var generatedValue = new Key {value = (long) generator.Generate64(BenchmarkConsts.kInitCount)};
                txn_keys_[idx] = KeyForWorker(generatedValue, workerId);
            }

            PrintToCoordinator($"loaded {BenchmarkConsts.kTxnCount} txns.", coordinatorConn);
        }

        #endregion
    }
}