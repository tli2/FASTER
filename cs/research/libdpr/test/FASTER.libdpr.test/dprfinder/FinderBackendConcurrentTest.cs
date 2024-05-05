using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.libdpr
{
    internal class SimulatedWorker
    {
        private WorkerId me;
        private List<SimulatedWorker> cluster;
        private long worldLine, version, lastChecked;
        private Dictionary<WorkerVersion, List<WorkerVersion>> versions;
        private Func<GraphDprFinderBackend> backend;
        private TestPrecomputedResponse response;
        private double depProb;
        private Random random;
        private bool finished = false;

        public SimulatedWorker(WorkerId me, List<SimulatedWorker> cluster, Func<GraphDprFinderBackend> backend, double depProb)
        {
            this.me = me;
            worldLine = 1;
            version = 1;
            lastChecked = 0;
            versions = new Dictionary<WorkerVersion, List<WorkerVersion>>();
            this.backend = backend;
            this.depProb = depProb;
            this.cluster = cluster;
            random = new Random();
            response = new TestPrecomputedResponse();
            backend().AddWorker(me);
            backend().AddResponseObjectToPrecompute(response);
        }

        private void SimulateOneVersion(bool generateDeps = true)
        {
            Thread.Sleep(random.Next(10, 20));
            var deps = new List<WorkerVersion>();
            var wv = new WorkerVersion(me, version);
            versions[wv] = deps;
            if (generateDeps)
            {
                for (var i = 0; i < cluster.Count; i++)
                {
                    var worker = cluster[random.Next(cluster.Count)]; 
                    var depVersion = Interlocked.Read(ref worker.version);
                    if (me.Equals(worker.me)) continue;
                    if (random.NextDouble() >= depProb || worker.finished)
                        continue;

                    if (depVersion > version)
                    {
                        // end version now
                        backend().NewCheckpoint(worldLine, wv, deps);
                        version = worker.version;
                        return;
                    }
                    deps.Add(new WorkerVersion(worker.me, depVersion));
                }
            }
            backend().NewCheckpoint(worldLine, wv, deps);
            Interlocked.Increment(ref version);
        }
        
        private void CheckInvariants()
        {
            response.rwLatch.EnterReadLock();
            var deserializedState = response.clusterState;
            if (response.currentCut.Count == 0)
            {
                response.rwLatch.ExitReadLock();
                worldLine = deserializedState.currentWorldLine;
                version = deserializedState.worldLinePrefix[me];
                versions = versions.Where(kv => kv.Key.Version <= version && kv.Key.Version > lastChecked)
                    .ToDictionary(kv => kv.Key, kv => kv.Value);
                
                foreach (var (wv, deps) in versions)
                {
                    backend().NewCheckpoint(worldLine, wv, deps);
                }
                backend().MarkWorkerAccountedFor(me);
                return;
            }

            var deserializedCut = new Dictionary<WorkerId, long>(response.currentCut);
            response.rwLatch.ExitReadLock();
            
            var persistedUntil = deserializedCut[me];
            // Guarantees should never regress, even if backend failed
            Assert.GreaterOrEqual(persistedUntil, lastChecked);
            // Check that all committed versions have persistent dependencies
            for (var v = lastChecked + 1; v <= persistedUntil; v++)
            {
                if (!versions.TryGetValue(new WorkerVersion(me, v), out var deps)) continue;
                foreach (var dep in deps)
                {
                    Assert.LessOrEqual(dep.Version,  cluster[(int) dep.WorkerId.guid].version);
                }
            }
            lastChecked = persistedUntil;
        }
        

        public void Simulate(ManualResetEventSlim termination)
        {
            while (!termination.IsSet)
            {
                SimulateOneVersion();
                CheckInvariants();
            }
            finished = true;
            var lastVersion = version;
            SimulateOneVersion(false);
            while (lastChecked < lastVersion)
                CheckInvariants();
        }
    }

    internal class SimulatedCluster
    {
        // Randomly reset to simulate DprFinder failure
        private SimulatedDprFinderService backend;
        private Thread failOver, compute;
        private double failureProb;
        private List<SimulatedWorker> cluster;

        public SimulatedCluster(SimulatedDprFinderService backend, double failureProb, IEnumerable<SimulatedWorker> cluster)
        {
            this.backend = backend;
            this.failureProb = failureProb;
            this.cluster = cluster.ToList();
        }

        public GraphDprFinderBackend GetDprFinder() => backend.GetDprFinderBackend();

        public void Simulate(int simulationTimeMilli)
        {
            var failOverTermination = new ManualResetEventSlim();
            var workerTermination = new ManualResetEventSlim();
            var backendTermination = new ManualResetEventSlim();
            failOver = new Thread(() =>
            {
                var rand = new Random();
                // failure simulator terminate before worker threads are joined so they can at least have one failure-free
                // version to ensure we make progress
                while (!failOverTermination.IsSet)
                {
                    Thread.Sleep(10);
                    if (rand.NextDouble() < failureProb)
                        backend.FailOver(5);
                }
            });
            compute = new Thread(() =>
            {
                while (!backendTermination.IsSet)
                    backend.ProcessOnce();
            });
            compute.Start();
            failOver.Start();
            
            var threads = new List<Thread>();
            foreach (var worker in cluster)
            {
                var t = new Thread(() => worker.Simulate(workerTermination));
                threads.Add(t);
                t.Start();
            }

            Thread.Sleep(simulationTimeMilli);
            failOverTermination.Set();
            failOver.Join();
            
            workerTermination.Set();
            foreach (var t in threads)
                t.Join();

            backendTermination.Set();
            compute.Join();
        }
    }

    [TestFixture]
    public class GraphDprFinderConcurrentTest
    {
        [Test]
        public void ConcurrentTestDprFinderSmallNoFailure()
        {
            var testedBackend = new SimulatedDprFinderService();
            var clusterInfo = new List<SimulatedWorker>();
            for (var i = 0; i < 3; i++)
                clusterInfo.Add(new SimulatedWorker(new WorkerId(i), clusterInfo, testedBackend.GetDprFinderBackend, 0.5));
            var testCluster = new SimulatedCluster(testedBackend, 0.0, clusterInfo);
            testCluster.Simulate(1000);
        }
        
        [Test]
        public void ConcurrentTestDprFinderLargeNoFailure()
        {
            var testedBackend = new SimulatedDprFinderService();
            var clusterInfo = new List<SimulatedWorker>();
            for (var i = 0; i < 30; i++)
                clusterInfo.Add(new SimulatedWorker(new WorkerId(i), clusterInfo, testedBackend.GetDprFinderBackend, 0.75));
            var testedCluster = new SimulatedCluster(testedBackend, 0.0, clusterInfo);
            testedCluster.Simulate(30000);
        }
        
        [Test]
        public void ConcurrentTestDprFinderFailure()
        {
            var testedBackend = new SimulatedDprFinderService();
            var clusterInfo = new List<SimulatedWorker>();
            for (var i = 0; i < 10; i++)
                clusterInfo.Add(new SimulatedWorker(new WorkerId(i), clusterInfo, testedBackend.GetDprFinderBackend, 0.75));
            var testedCluster = new SimulatedCluster(testedBackend, 0.05, clusterInfo);
            testedCluster.Simulate(1000);
        }
    }
}