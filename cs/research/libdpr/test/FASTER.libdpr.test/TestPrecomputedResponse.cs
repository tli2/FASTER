using System.Collections.Generic;

namespace FASTER.libdpr
{

    public class TestPrecomputedResponse : PrecomputedSyncResponseBase
    {
        public ClusterState clusterState;
        public Dictionary<WorkerId, long> currentCut = null;
        
        public override void ResetClusterState(ClusterState clusterState)
        {
            rwLatch.EnterWriteLock();
            this.clusterState = new ClusterState();
            this.clusterState.currentWorldLine = clusterState.currentWorldLine;
            foreach (var entry in clusterState.worldLinePrefix)
                this.clusterState.worldLinePrefix.Add(entry.Key, entry.Value);
            currentCut = null;
            rwLatch.ExitWriteLock();
        }

        public override void UpdateCut(Dictionary<WorkerId, long> newCut)
        {
            rwLatch.EnterWriteLock();
            currentCut ??= new Dictionary<WorkerId, long>();
            currentCut.Clear();
            foreach (var entry in newCut)
                currentCut.Add(entry.Key, entry.Value);
            rwLatch.ExitWriteLock();
        }
    }
}