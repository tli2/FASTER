﻿using System.Collections.Concurrent;
using System.Collections.Generic;
using FASTER.serverless;

namespace FASTER.benchmark
{
    public class ClusterConfiguration
    {
        internal List<ServerfulWorkerInfo> members = new List<ServerfulWorkerInfo>(),
            servers = new List<ServerfulWorkerInfo>(),
            clients = new List<ServerfulWorkerInfo>();
        internal List<bool> isServer = new List<bool>();

        public ClusterConfiguration AddServer(string ip, int port)
        {
            var info = new ServerfulWorkerInfo(members.Count, ip, port);
            members.Add(info);
            servers.Add(info);
            isServer.Add(true);
            return this;
        }

        public ClusterConfiguration AddClient(string ip, int port)
        {
            var info = new ServerfulWorkerInfo(members.Count, ip, port);
            members.Add(info);
            clients.Add(info);
            isServer.Add(false);
            return this;
        }

        public ServerfulWorkerInfo GetInfoForId(int id)
        {
            return members[id];
        }

        public bool IsServer(int id)
        {
            return isServer[id];
        }
        
        // TODO(Tianyu): Populate this from the DPR table instead of hard-coded
        public ConcurrentDictionary<Worker, ServerfulWorkerInfo> GetRoutingTable()
        {
            var result = new ConcurrentDictionary<Worker, ServerfulWorkerInfo>();
            foreach (var member in members)
                result.TryAdd(member.GetWorker(), member);
            return result;
        }
    }
}