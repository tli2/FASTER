using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;

namespace FASTER.libdpr
{
    /// <summary>
    /// DPR Finder client stub that connects to a backend RespGraphDprFinderServer
    /// </summary>
    public sealed class RespGraphDprFinder : DprFinderBase, IDisposable
    {
        private readonly DprFinderSocketReaderWriter socket;
        private readonly DprFinderResponseParser parser = new DprFinderResponseParser();
        private readonly byte[] recvBuffer = new byte[1 << 20];
        
        /// <summary>
        /// Create a new DprFinder using the supplied socket
        /// </summary>
        /// <param name="dprFinderConn"> socket to use</param>
        /// 
        public RespGraphDprFinder(Socket dprFinderConn)
        {
            dprFinderConn.NoDelay = true;
            socket = new DprFinderSocketReaderWriter(dprFinderConn);
        }

        /// <summary>
        /// Create a new DprFinder by connecting to the given endpoint
        /// </summary>
        /// <param name="ip">IP address of the desired endpoint</param>
        /// <param name="port">port of the desired endpoint</param>
        public RespGraphDprFinder(string ip, int port)
        {
            var ipEndpoint = new IPEndPoint(IPAddress.Parse(ip), port);
            var conn = new Socket(ipEndpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            conn.NoDelay = true;
            conn.Connect(ipEndpoint);
            socket = new DprFinderSocketReaderWriter(conn);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            socket.Dispose();
        }

        /// <inheritdoc/>
        public override void ReportNewPersistentVersion(long worldLine, WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
        {
            lock (socket)
                socket.SendNewCheckpointCommand(worldLine, persisted, deps);
        }

        protected override bool Sync(ClusterState stateToUpdate, Dictionary<DprWorkerId, long> cutToUpdate)
        {
            lock (socket)
            {
                socket.SendSyncCommand();
                ProcessRespResponse();

                var head = stateToUpdate.PopulateFromBuffer(recvBuffer, parser.stringStart + sizeof(long));
                if (BitConverter.ToInt32(recvBuffer, head) == -1) return false;

                RespUtil.ReadDictionaryFromBytes(recvBuffer, head, cutToUpdate);
                return true;
            }        
        }

        protected override void SendGraphReconstruction(DprWorkerId id, IDprFinder.UnprunedVersionsProvider provider)
        {
            lock (socket)
            {
                var acks = socket.SendGraphReconstruction(id, provider);
                socket.WaitForAcks(acks);
            }
        }

        protected override void AddWorkerInternal(DprWorkerId id)
        {
            lock (socket)
            {
                socket.SendAddWorkerCommand(id);
                ProcessRespResponse();
            }
        }

        /// <inheritdoc/>
        public override void RemoveWorker(DprWorkerId id)
        {
            lock (socket)
            {
                socket.SendDeleteWorkerCommand(id);
                socket.WaitForAcks(1);
            }
        }

        private void ProcessRespResponse()
        {
            int i = 0, receivedSize = 0;
            while (true)
            {
                receivedSize += socket.ReceiveInto(recvBuffer);
                for (; i < receivedSize; i++)
                    if (parser.ProcessChar(i, recvBuffer))
                        return;
            }
        }
    }
}