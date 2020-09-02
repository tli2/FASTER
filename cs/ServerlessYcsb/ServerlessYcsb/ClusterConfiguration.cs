﻿using System.Collections.Concurrent;
using System.Collections.Generic;
using FASTER.serverless;

namespace FASTER.benchmark
{
    public class ClusterConfiguration
    {
        internal List<ServerfulWorkerInfo> workers = new List<ServerfulWorkerInfo>();

        public ClusterConfiguration AddWorker(string ip, int port)
        {
            var info = new ServerfulWorkerInfo(workers.Count, ip, port);
            workers.Add(info);
            return this;
        }

        public ServerfulWorkerInfo GetInfoForId(int id)
        {
            return workers[id];
        }
        
        // TODO(Tianyu): Populate this from the DPR table instead of hard-coded
        public ConcurrentDictionary<Worker, ServerfulWorkerInfo> GetRoutingTable()
        {
            var result = new ConcurrentDictionary<Worker, ServerfulWorkerInfo>();
            foreach (var worker in workers)
                result.TryAdd(worker.GetWorker(), worker);
            return result;
        }
    }
}