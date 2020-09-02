using System;
using System.Collections.Generic;
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
    public class YcsbClient
    {
        internal int workerId;
        internal volatile bool done;
        internal long totalOps;
        internal Key[] txn_keys_;
        internal IDevice device;

        internal long idx_;
        internal Input[] input_;

        internal FasterServerless<Key, Value, Input, Output, Functions> fasterServerless;
        internal string lastDist = "";
        internal ServerfulMessageManager messageManager;

        public YcsbClient(int workerId)
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
            var worker = new YcsbClient(workerId);

            var clientSocket = servSock.Accept();
            var message = clientSocket.ReceiveBenchmarkMessage();
            Debug.Assert(message.type == 1);
            var config = (BenchmarkConfiguration) message.content;
            worker.Reset();
            worker.ExecuteOnce(info.GetWorker(), config, clientSocket);
            worker.Reset();
            clientSocket.Close();
        }

        public void Reset()
        {
            done = false;
            totalOps = 0;
            messageManager?.Dispose();
            fasterServerless?.localFaster.Dispose();
            device?.Close();
            GC.Collect();
        }

        public void ExecuteOnce(Worker me, BenchmarkConfiguration configuration, Socket coordinatorConn)
        {
            messageManager =
                new ServerfulMessageManager(me, YcsbCoordinator.clusterConfig.GetRoutingTable());
            var metadataStore =
                new MetadataStore(new AzureSqlOwnershipMapping(configuration.connString), messageManager);
            var dprManager = new AzureSqlDprManagerV2(configuration.connString, me, false);
            device = Devices.CreateLogDevice("D:\\hlog", deleteOnClose: true);
            fasterServerless = new FasterServerless<Key, Value, Input, Output, Functions>(
                metadataStore, messageManager, dprManager, 1, new Functions(),
                new LogSettings {LogDevice = device},
                checkpointSettings: new CheckpointSettings {CheckpointDir = "D:\\checkpoints"}, 
                bucketingScheme: new YcsbBucketingScheme(),
                serializer: new YcsbParameterSerializer(),
                clientOnly: true);

            // Warm up local metadata store cache
            foreach (var worker in YcsbCoordinator.clusterConfig.servers)
            {
                AsyncContext.Run(async () => await metadataStore.LookupAsync(worker.GetWorker().guid));
            }

            if (!configuration.distribution.Equals(lastDist))
            {
                LoadData(configuration, coordinatorConn);
                lastDist = configuration.distribution;
            }

            input_ = new Input[8];
            for (var i = 0; i < 8; i++)
                input_[i].value = i;
            var execThreads = new Thread[configuration.clientThreadCount];
            var localSessions = new FasterServerlessSession<Key, Value, Input, Output, Functions>[configuration.clientThreadCount];
            messageManager.StartServer(fasterServerless, null);

            coordinatorConn.SendBenchmarkControlMessage("setup finished");
            coordinatorConn.ReceiveBenchmarkMessage();
            idx_ = 0;
            var sw = new Stopwatch();
            sw.Start();
            for (var idx = 0; idx < configuration.clientThreadCount; ++idx)
            {
                var x = idx;
                execThreads[idx] = new Thread(() =>
                {
                    var s = fasterServerless.NewServerlessSession(configuration.windowSize,
                        configuration.batchSize, configuration.checkpointMilli != -1, BenchmarkConsts.kCollectLatency);
                    localSessions[x] = s;
                    var startSeq = s.NextSerialNum();
                    Value value = default;
                    Input input = default;
                    var output = new ReadResult<Output>();

                    var rng = new RandomGenerator((uint) (workerId * YcsbCoordinator.clusterConfig.members.Count + x));
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

                        for (var idx = chunk_idx; idx < chunk_idx + BenchmarkConsts.kChunkSize && !done; ++idx)
                        {
                            try
                            {
                                Op op;
                                int r = (int) rng.Generate(100);
                                if (r < configuration.readPercent)
                                    op = Op.Read;
                                else if (configuration.readPercent >= 0)
                                    op = Op.Upsert;
                                else
                                    op = Op.ReadModifyWrite;

                                var key = txn_keys_[idx];

                                while (s.NumRemotePendingOps() >= configuration.windowSize)
                                {
                                    s.CompletePending(TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond / 2));
                                    if (s.NumRemotePendingOps() < configuration.windowSize) break;
                                    s.Refresh();
                                    Thread.Yield();
                                }

                                if (idx % 256 == 0)
                                    s.Refresh();

                                switch (op)
                                {
                                    case Op.Upsert:
                                        s.Upsert(ref key, ref value, out _);
                                        break;
                                    case Op.Read:
                                        s.Read(ref key, ref input, output, out _);
                                        break;
                                    case Op.ReadModifyWrite:
                                        s.RMW(ref key, ref input_[idx & 0x7], out _);
                                        break;
                                    default:
                                        throw new InvalidOperationException("Unexpected op: " + op);
                                }
                            }
                            catch (FasterServerlessRollbackException) {}
                        }
                    }

                    s.CompletePending(TimeSpan.FromMilliseconds(1), true);

                    Interlocked.Add(ref totalOps, s.NextSerialNum() - startSeq);
                    s.Dispose();
                });
                execThreads[idx].Start();
            }

            if (configuration.checkpointMilli <= 0)
            {
                while (sw.ElapsedMilliseconds < 1000 * BenchmarkConsts.kRunSeconds)
                {
                    Thread.Sleep(1000);
                }
            }
            else
            {
                while (true)
                {
                    var timeElapsed = sw.ElapsedMilliseconds;
                    if (timeElapsed >= 1000 * BenchmarkConsts.kRunSeconds) break;
                    fasterServerless.RefreshDprTable();
                }
            }

            done = true;
            PrintToCoordinator($"Done issuing operations", coordinatorConn);
            foreach (var session in execThreads)
                session.Join();
            sw.Stop();
            var seconds = sw.ElapsedMilliseconds / 1000.0;
            PrintToCoordinator($"##, {totalOps / seconds}", coordinatorConn);
            coordinatorConn.SendBenchmarkControlMessage(ValueTuple.Create(totalOps, fasterServerless.numRemote, fasterServerless.numBackground));
            coordinatorConn.ReceiveBenchmarkMessage();

            if (BenchmarkConsts.kCollectLatency)
            {
                using var commitFile = new StreamWriter($"Z:\\commit{workerId}.txt");
                using var opFile = new StreamWriter($"Z:\\op{workerId}.txt");
                if (BenchmarkConsts.kTriggerRecovery)
                {
                    using var uncommittedFile = new StreamWriter($"Z:\\uncommit{workerId}.txt");
                    var uncommitted = new long[120];
                    var commits = new long[120];
                    var ops = new long[120];
                    foreach (var s in localSessions)
                    {
                        for (var i = 0; i < s.NextSerialNum(); i++)
                        {
                            var endTimeBracket = 1000 * s.opEndTick[i] / Stopwatch.Frequency / 250;
                            if (endTimeBracket < 120)
                                ops[endTimeBracket]++;
                            var commitTimeBracket = 1000 * s.opCommitTick[i] / Stopwatch.Frequency / 250;
                            if (s.opCommitTick[i] == 0)
                            {
                                var startTimeBracket = 1000 * s.opStartTick[i] / Stopwatch.Frequency / 250;
                                if (startTimeBracket < 120)
                                    uncommitted[startTimeBracket]++;
                            }
                            else
                            {
                                if (commitTimeBracket < 120)
                                    commits[commitTimeBracket]++;
                            }
                        }
                    }

                    for (var i = 0; i < 30; i++)
                    {
                        opFile.WriteLine(ops[i]);
                        commitFile.WriteLine(commits[i]);
                        uncommittedFile.WriteLine(uncommitted[i]);
                    }
                }
                else
                {
                    var startTimes = new List<double>();
                    var endTimes = new List<double>();
                    var commitTimes = new List<double>();
                    
                    foreach (var s in localSessions)
                    {
                        for (var i = 0; i < s.NextSerialNum(); i++)
                        {
                            startTimes.Add(1000.0 * s.opStartTick[i] / Stopwatch.Frequency);
                            endTimes.Add(1000.0 * s.opEndTick[i] / Stopwatch.Frequency);
                            commitTimes.Add(1000.0 * s.opCommitTick[i] / Stopwatch.Frequency);
                        }
                    }
                
                    var random =  new Random();
                    for (var i = 0; i < startTimes.Count; i++)
                    {
                        if (random.NextDouble() < 0.01)
                        {
                            if (commitTimes[i] == 0.0) continue;
                            opFile.WriteLine(endTimes[i] - startTimes[i]);
                            commitFile.WriteLine(commitTimes[i] - startTimes[i]);
                        }
                    } 
                }
            }

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
            var txn_filename = filePath + "\\run_" + configuration.distribution + "_250M_1000M_raw.dat";

            long count = 0;

            using (var stream = File.Open(txn_filename, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                var chunk = new byte[BenchmarkConsts.kFileChunkSize];
                var chunk_handle = GCHandle.Alloc(chunk, GCHandleType.Pinned);
                var chunk_ptr = (byte*) chunk_handle.AddrOfPinnedObject();

                PrintToCoordinator($"loading txns from {txn_filename} into memory...", coordinatorConn);

                txn_keys_ = new Key[BenchmarkConsts.kTxnCount];

                long offset = 0;

                var rng = new RandomGenerator((uint) workerId);

                while (true)
                {
                    stream.Position = offset;
                    var size = stream.Read(chunk, 0, BenchmarkConsts.kFileChunkSize);
                    for (var idx = 0; idx < size; idx += 8)
                    {
                        var ownerId = (int) rng.Generate((uint) YcsbCoordinator.clusterConfig.servers.Count);
                        var owner = YcsbCoordinator.clusterConfig.servers[ownerId].GetWorker().guid;
                        txn_keys_[count] = KeyForWorker(new Key {value = *(long*) (chunk_ptr + idx)}, (int) owner);
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

            var generator = new RandomGenerator();

            txn_keys_ = new Key[BenchmarkConsts.kTxnCount];
            var rng = new RandomGenerator((uint) workerId);

            for (var idx = 0; idx < BenchmarkConsts.kTxnCount; idx++)
            {
                var ownerId = (int) rng.Generate((uint) YcsbCoordinator.clusterConfig.servers.Count);
                var owner = YcsbCoordinator.clusterConfig.servers[ownerId].GetWorker().guid;
                var generatedValue = new Key {value = (long) generator.Generate64(BenchmarkConsts.kInitCount)};
                txn_keys_[idx] = KeyForWorker(generatedValue, (int) owner);
            }

            PrintToCoordinator($"loaded {BenchmarkConsts.kTxnCount} txns.", coordinatorConn);
        }

        #endregion
    }
}