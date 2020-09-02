﻿using System.Collections.Concurrent;
using System.Collections.Generic;
using FASTER.serverless;

namespace FASTER.benchmark
{
    public class ClusterConfiguration
    {
        internal List<ServerfulWorkerInfo> pods = new List<ServerfulWorkerInfo>();

        public ClusterConfiguration AddWorker(string ip, int port)
        {
            var info = new ServerfulWorkerInfo(pods.Count, ip, port);
            pods.Add(info);
            return this;
        }

        public ServerfulWorkerInfo GetInfoForId(int id)
        {
            return pods[id];
        }
        
        // TODO(Tianyu): Populate this from the DPR table instead of hard-coded
        public ConcurrentDictionary<Worker, ServerfulWorkerInfo> GetRoutingTable()
        {
            var result = new ConcurrentDictionary<Worker, ServerfulWorkerInfo>();
            foreach (var worker in pods)
                result.TryAdd(worker.GetWorker(), worker);
            return result;
        }
    }
}