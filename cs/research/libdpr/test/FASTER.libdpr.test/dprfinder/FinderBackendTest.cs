using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.libdpr
{
    [TestFixture]
    public class FinderBackendTest
    {
        private static void CheckClusterState(GraphDprFinderBackend backend, TestPrecomputedResponse response, long expectedWorldLine,
            Dictionary<WorkerId, long> expectedPrefix)
        {
            try
            {
                response.rwLatch.EnterReadLock();
                Assert.AreEqual(expectedWorldLine, response.clusterState.currentWorldLine);
                Assert.AreEqual(expectedPrefix, response.clusterState.worldLinePrefix);
            }
            finally
            {
                response.rwLatch.ExitReadLock();
            }

        }
        private static void CheckDprCut(GraphDprFinderBackend backend, TestPrecomputedResponse response, Dictionary<WorkerId, long> expectedCut)
        {
            try
            {
                response.rwLatch.EnterReadLock();
                Assert.AreEqual(expectedCut, response.currentCut);
            }
            finally
            {
                response.rwLatch.ExitReadLock();
            }
        }
        
        [Test]
        public void TestDprFinderBackendSequential()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var testResponse = new TestPrecomputedResponse();
            var testedBackend = new GraphDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));
            testedBackend.AddResponseObjectToPrecompute(testResponse);

            var A = new WorkerId(0);
            var B = new WorkerId(1);
            var C = new WorkerId(2);
            
            var addComplete = new CountdownEvent(3);
            testedBackend.AddWorker(A, _ => addComplete.Signal());
            testedBackend.AddWorker(B, _ => addComplete.Signal());
            testedBackend.AddWorker(C, _ => addComplete.Signal());
            testedBackend.Process();
            addComplete.Wait();
            CheckClusterState(testedBackend, testResponse, 1, new Dictionary<WorkerId, long>
            {
                {A, 0},
                {B, 0},
                {C, 0}
            });
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 0},
                {B, 0},
                {C, 0}
            });
            
            var A1 = new WorkerVersion(A, 1);
            var B1 = new WorkerVersion(B, 1);
            var A2 = new WorkerVersion(A, 2);
            var B2 = new WorkerVersion(B, 2);
            var C2 = new WorkerVersion(C, 2);
            
            testedBackend.NewCheckpoint(1, A1, Enumerable.Empty<WorkerVersion>());
            testedBackend.Process();
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 1},
                {B, 0},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, B1, new[] {A1});
            testedBackend.Process();
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, A2, new []{ A1, B2 });
            testedBackend.Process();
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, B2, new []{ B1, C2 });
            testedBackend.Process();
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, C2, new []{ A2 });
            testedBackend.Process();
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 2},
                {B, 2},
                {C, 2}
            });
            
            localDevice1.Dispose();
            localDevice2.Dispose();
        }
        
        [Test]
        public void TestDprFinderBackendWithWorkerFailure()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var testResponse = new TestPrecomputedResponse();
            var testedBackend = new GraphDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));
            testedBackend.AddResponseObjectToPrecompute(testResponse);

            var A = new WorkerId(0);
            var B = new WorkerId(1);
            var C = new WorkerId(2);
            
            var addComplete = new CountdownEvent(3);
            testedBackend.AddWorker(A, _ => addComplete.Signal());
            testedBackend.AddWorker(B, _ => addComplete.Signal());
            testedBackend.AddWorker(C, _ => addComplete.Signal());
            testedBackend.Process();
            addComplete.Wait();
            CheckClusterState(testedBackend, testResponse, 1, new Dictionary<WorkerId, long>
            {
                {A, 0},
                {B, 0},
                {C, 0}
            });
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 0},
                {B, 0},
                {C, 0}
            });
            
            var A1 = new WorkerVersion(A, 1);
            var B1 = new WorkerVersion(B, 1);
            var A2 = new WorkerVersion(A, 2);
            var B2 = new WorkerVersion(B, 2);
            var C2 = new WorkerVersion(C, 2);
            var A3 = new WorkerVersion(A, 3);

            
            testedBackend.NewCheckpoint(1, A1, Enumerable.Empty<WorkerVersion>());
            testedBackend.Process();
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 1},
                {B, 0},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, B1, new[] {A1});
            testedBackend.Process();
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, A2, new []{ A1, B2 });
            testedBackend.Process();
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, B2, new []{ B1, C2 });
            testedBackend.Process();
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            var restartDone = new ManualResetEventSlim();
            testedBackend.AddWorker(A, _ => restartDone.Set());
            testedBackend.Process();
            restartDone.Wait();
            CheckClusterState(testedBackend, testResponse, 2, new Dictionary<WorkerId, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, C2, new []{ A2 });
            testedBackend.Process();
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            // C2 Should have be rejected and never commit
            testedBackend.NewCheckpoint(2, A3, Enumerable.Empty<WorkerVersion>());
            testedBackend.Process();
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 3},
                {B, 1},
                {C, 0}
            });
            
            localDevice1.Dispose();
            localDevice2.Dispose();
        }

        [Test]
        public void TestDprFinderBackendRestart()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var testResponse = new TestPrecomputedResponse();
            var testedBackend = new GraphDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));
            testedBackend.AddResponseObjectToPrecompute(testResponse);
        
            var A = new WorkerId(0);
            var B = new WorkerId(1);
            var C = new WorkerId(2);
            
            var addComplete = new CountdownEvent(3);
            testedBackend.AddWorker(A, _ => addComplete.Signal());
            testedBackend.AddWorker(B, _ => addComplete.Signal());
            testedBackend.AddWorker(C, _ => addComplete.Signal());
            testedBackend.Process();
            addComplete.Wait();
            CheckClusterState(testedBackend, testResponse, 1, new Dictionary<WorkerId, long>
            {
                {A, 0},
                {B, 0},
                {C, 0}
            });
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 0},
                {B, 0},
                {C, 0}
            });
            
            
            var A1 = new WorkerVersion(A, 1);
            var B1 = new WorkerVersion(B, 1);
            var A2 = new WorkerVersion(A, 2);
            var B2 = new WorkerVersion(B, 2);
            var C2 = new WorkerVersion(C, 2);

            testedBackend.NewCheckpoint(1, A1, Enumerable.Empty<WorkerVersion>());
            testedBackend.NewCheckpoint(1, B1, new[] {A1});
            testedBackend.NewCheckpoint(1, A2, new []{ A1, B2 });
            testedBackend.NewCheckpoint(1, B2, new []{ B1, C2 });
            testedBackend.Process();
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            // Get a new test backend to simulate restart from disk
            testedBackend = new GraphDprFinderBackend(new PingPongDevice(localDevice1, localDevice2)); 
            testResponse = new TestPrecomputedResponse();
            testedBackend.AddResponseObjectToPrecompute(testResponse);
            CheckClusterState(testedBackend, testResponse, 1, new Dictionary<WorkerId, long>
            {
                {A, 0},
                {B, 0},
                {C, 0}
            });
            // Cut should be temporarily unavailable during recovery
            CheckDprCut(testedBackend, testResponse, null);
            
            // Simulate resending of graph
            testedBackend.NewCheckpoint(1, A2, new []{ B2 });
            testedBackend.MarkWorkerAccountedFor(A);
            testedBackend.NewCheckpoint(1, B2, new []{ C2 });
            testedBackend.MarkWorkerAccountedFor(B);
            testedBackend.MarkWorkerAccountedFor(C);
            testedBackend.Process();

            // We should reach the same cut when dpr finder recovery is complete
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, C2, new []{ A2 });
            testedBackend.Process();
            CheckDprCut(testedBackend, testResponse, new Dictionary<WorkerId, long>
            {
                {A, 2},
                {B, 2},
                {C, 2}
            });
            
            localDevice1.Dispose();
            localDevice2.Dispose();
        }
    }
}