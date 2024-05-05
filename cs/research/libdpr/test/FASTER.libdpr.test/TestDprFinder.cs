using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FASTER.libdpr;

public class SimulatedDprFinderService : IDisposable
{
    private IDevice frontDevice, backDevice;

    // Randomly reset to simulate DprFinder failure
    private volatile GraphDprFinderBackend backend;
    private TestPrecomputedResponse response;
    private TaskCompletionSource nextProcess = new();


    public SimulatedDprFinderService()
    {
        frontDevice = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
        backDevice = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
        response = new TestPrecomputedResponse();
        backend = new GraphDprFinderBackend(new PingPongDevice(frontDevice, backDevice));
        backend.AddResponseObjectToPrecompute(response);
    }

    public void Dispose()
    {
        frontDevice.Dispose();
        backDevice.Dispose();
    }

    public TestPrecomputedResponse GetResponseObject() => response;
    
    public GraphDprFinderBackend GetDprFinderBackend()
    {
        GraphDprFinderBackend reference = null;
        do
        {
            reference = backend;
        } while (reference == null);

        return reference;
    }

    public void FailOver(int delayMilli)
    {
        backend = null;
        Thread.Sleep(delayMilli);
        var newResponse = new TestPrecomputedResponse();
        var newBackend = new GraphDprFinderBackend(new PingPongDevice(frontDevice, backDevice));
        newBackend.AddResponseObjectToPrecompute(newResponse);
        backend = newBackend;
        response = newResponse;
    }

    public void ProcessOnce()
    {
        backend?.Process();
    }

    public Task NextBackgroundProcessComplete() => nextProcess.Task;

    public void ProcessInBackground(ManualResetEventSlim termination)
    {
        Task.Run(() =>
        {
            while (!termination.IsSet)
            {
                var tcs = nextProcess;
                nextProcess = new TaskCompletionSource();
                ProcessOnce();
                tcs.SetResult();
            }

            Dispose();
        });
    }
}

public class TestDprFinder : DprFinderBase
{
    private SimulatedDprFinderService backend;
    
    public TestDprFinder(SimulatedDprFinderService backend)
    {
        this.backend = backend;
    }
    
    public override void ReportNewPersistentVersion(long worldLine, WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
    {
        backend.GetDprFinderBackend().NewCheckpoint(worldLine, persisted, deps);
    }

    protected override bool Sync(ClusterState stateToUpdate, Dictionary<WorkerId, long> cutToUpdate)
    {
        var response = backend.GetResponseObject();
        try
        {
            response.rwLatch.EnterReadLock();
            if (response.currentCut.Count == 0) return false;
            stateToUpdate.currentWorldLine = response.clusterState.currentWorldLine;
            foreach (var entry in response.clusterState.worldLinePrefix)
                stateToUpdate.worldLinePrefix.Add(entry.Key, entry.Value);
            foreach (var entry in response.currentCut)
                cutToUpdate.Add(entry.Key, entry.Value);
            return true;    
        }
        finally
        {
            response.rwLatch.ExitReadLock();
        }
    }

    protected override void SendGraphReconstruction(WorkerId id, IStateObject stateObject)
    {
        var checkpoints = stateObject.GetUnprunedVersions();
        var service = backend.GetDprFinderBackend();
        foreach (var (bytes, offset) in checkpoints)
        {
            SerializationUtil.DeserializeCheckpointMetadata(new Span<byte>(bytes, offset, bytes.Length - offset),
                out var worldLine, out var wv, out var deps);
            service.NewCheckpoint(worldLine, wv, deps);
        }
        service.MarkWorkerAccountedFor(id);
    }

    protected override void AddWorkerInternal(WorkerId id)
    {
        var manualResetEvent = new ManualResetEventSlim();
        backend.GetDprFinderBackend().AddWorker(id, _ => manualResetEvent.Set());
        manualResetEvent.Wait();
    }

    public override void RemoveWorker(WorkerId id)
    {
        var manualResetEvent = new ManualResetEventSlim();
        backend.GetDprFinderBackend().DeleteWorker(id, () => manualResetEvent.Set());
        manualResetEvent.Wait();
    }
}