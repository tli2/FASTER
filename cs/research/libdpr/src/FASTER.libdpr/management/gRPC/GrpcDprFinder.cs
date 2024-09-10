using System;
using System.Collections.Generic;
using FASTER.libdpr.proto;
using Grpc.Core;
using Grpc.Net.Client;

namespace FASTER.libdpr
{
    public class GrpcDprFinder : DprFinderBase
    {
        private string connString;
        private DprFinder.DprFinderClient finderClient;
        private long drift;

        public GrpcDprFinder(string connString)
        {
            this.connString = connString;
            finderClient = new DprFinder.DprFinderClient(GrpcChannel.ForAddress(connString));
        }

        // public override long CurrentTime()
        // {
            // return DateTimeOffset.Now.ToUnixTimeMilliseconds() + drift;
        // }
        
        public override void ReportNewPersistentVersion(long worldLine, WorkerVersion persisted,
            IEnumerable<WorkerVersion> deps)
        {
            try
            {
                var request = new NewCheckpointRequest
                {
                    Id = persisted.DprWorkerId.guid,
                    Version = persisted.Version,
                    WorldLine = worldLine
                };
                foreach (var dep in deps)
                    request.Deps.Add(new proto.WorkerVersion
                    {
                        Id = dep.DprWorkerId.guid,
                        Version = dep.Version
                    });

                // Can just leave async without waiting to complete
                finderClient.NewCheckpointAsync(request);
            }
            catch (RpcException e)
            {
                finderClient = new DprFinder.DprFinderClient(GrpcChannel.ForAddress(connString));
            }
        }

        protected override bool Sync(ClusterState stateToUpdate, Dictionary<DprWorkerId, long> cutToUpdate)
        {
            try
            {
                var response = finderClient.Sync(new SyncRequest());
                if (response.CurrentCut.Count == 0) return false;

                drift = (response.CurrentTime - DateTimeOffset.Now.ToUnixTimeMilliseconds()) / 2;
                stateToUpdate.currentWorldLine = response.WorldLine;
                foreach (var entry in response.WorldLinePrefix)
                    stateToUpdate.worldLinePrefix.Add(new DprWorkerId(entry.Id), entry.Version);
                foreach (var entry in response.CurrentCut)
                    cutToUpdate.Add(new DprWorkerId(entry.Id), entry.Version);
            }
            catch (RpcException e)
            {
                finderClient = new DprFinder.DprFinderClient(GrpcChannel.ForAddress(connString));
            }

            return true;
        }

        protected override void SendGraphReconstruction(DprWorkerId id, IDprFinder.UnprunedVersionsProvider provider)
        {
            try
            {
                var checkpoints = provider();
                var request = new ResendGraphRequest
                {
                    Id = id.guid
                };
                foreach (var m in checkpoints)
                {
                    SerializationUtil.DeserializeCheckpointMetadata(m.Span,
                        out var worldLine, out var wv, out var deps);
                    var checkpointRequest = new NewCheckpointRequest
                    {
                        Id = id.guid,
                        Version = wv.Version,
                        WorldLine = worldLine
                    };
                    foreach (var dep in deps)
                        checkpointRequest.Deps.Add(new proto.WorkerVersion
                        {
                            Id = dep.DprWorkerId.guid,
                            Version = dep.Version
                        });
                    request.GraphNodes.Add(checkpointRequest);
                    finderClient.NewCheckpoint(checkpointRequest);
                }

                finderClient.ResendGraph(request);
            }
            catch (RpcException e)
            {
                finderClient = new DprFinder.DprFinderClient(GrpcChannel.ForAddress(connString));
            }
        }

        protected override void AddWorkerInternal(DprWorkerId id)
        {
            finderClient.AddWorker(new AddWorkerRequest
            {
                Id = id.guid
            });
        }

        public override void RemoveWorker(DprWorkerId id)
        {
            finderClient.RemoveWorker(new RemoveWorkerRequest
            {
                Id = id.guid
            });
        }

        public void ForceRollback()
        {
            finderClient.ForceRollback(new ForceRollbackRequest());
        }
    }
}