
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace FASTER.serverless
{
    public class ConnectionState
    {
        public byte[] buffer = new byte[1 << 15];
        public int head = 0, packetSize = 0, received = 0;
        public Socket socket;

        public ConnectionState(Socket socket)
        {
            this.socket = socket;
        }

        public bool HasCompleteMessage()
        {
            if (received < sizeof(int)) return false;
            packetSize = BitConverter.ToInt32(buffer, head);
            head += sizeof(int);
            return received - head >= packetSize;
        }
        
        public (WorkerVersion, List<WorkerVersion>) ReadMessage()
        {
            var persistent = new WorkerVersion();
            var deps = new List<WorkerVersion>();
            
            persistent.Worker = new Worker(BitConverter.ToInt64(buffer, head));
            head += sizeof(long);
            persistent.Version = BitConverter.ToInt64(buffer, head);
            head += sizeof(long);
            var numDeps = BitConverter.ToInt32(buffer, head);
            head += sizeof(int);
            for (var i = 0; i < numDeps; i++)
            {
                var wv = new WorkerVersion();
                wv.Worker = new Worker(BitConverter.ToInt64(buffer, head));
                head += sizeof(long);
                wv.Version = BitConverter.ToInt64(buffer, head);
                head += sizeof(long);
                deps.Add(wv);
            }
            Array.Copy(buffer, head, buffer, 0, received - head);
            received -= head;
            head = 0;
            return ValueTuple.Create(persistent, deps);
        }
    }
    
    public class V3DprFinder : IDisposable
    {
        private readonly SqlConnection writeConn;
        private Dictionary<Worker, long> currentDprCut;
        private Dictionary<WorkerVersion, List<WorkerVersion>> precedenceGraph;
        private Queue<WorkerVersion> wvs;

        private HashSet<WorkerVersion> visited;
        private Queue<WorkerVersion> frontier;

        private Socket servSocket;
        private ConcurrentQueue<(WorkerVersion, List<WorkerVersion>)> pendingUpdates = new ConcurrentQueue<(WorkerVersion, List<WorkerVersion>)>();
        private bool done = false;

        public V3DprFinder(string connString)
        {
            currentDprCut = new Dictionary<Worker, long>();
            precedenceGraph = new Dictionary<WorkerVersion, List<WorkerVersion>>();
            wvs = new Queue<WorkerVersion>();
            writeConn = new SqlConnection(connString);
            writeConn.Open();
            
            visited = new HashSet<WorkerVersion>();
            frontier = new Queue<WorkerVersion>();
        }

        public void Dispose()
        {
            writeConn.Dispose();
        }

        private void WriteDprCut()
        {
            var queryBuilder = new StringBuilder();
            foreach (var (w, v) in currentDprCut)
            {
                queryBuilder.AppendFormat($"UPDATE dpr SET safeVersion={v} WHERE workerId={w.guid};");
            }
            var insert = new SqlCommand(queryBuilder.ToString(), writeConn);
            insert.ExecuteNonQuery();
        }

        private bool TryCommit(WorkerVersion wv)
        {
            visited.Clear();
            frontier.Enqueue(wv);
            while (frontier.Count != 0)
            {
                var node = frontier.Dequeue();
                if (visited.Contains(node)) continue;
                
                visited.Add(node);
                if (currentDprCut.GetValueOrDefault(wv.Worker, 0) > wv.Version) continue;
                if (!precedenceGraph.TryGetValue(wv, out var val)) return false;
                
                foreach (var dep in val)
                    frontier.Enqueue(dep);
            }

            foreach (var committed in visited)
            {
                if (committed.Version > currentDprCut.GetValueOrDefault(committed.Worker, 0))
                    currentDprCut[committed.Worker] = committed.Version;
                precedenceGraph.Remove(committed);
            }

            return true;
        }
        
        public void TryFindDprCut()
        {
            
            var newQueue = new Queue<WorkerVersion>();
            var hasUpdates = false;
            while (wvs.Count != 0)
            {
                var wv = wvs.Dequeue();
                if (TryCommit(wv))
                {
                    hasUpdates = true;
                }
                else
                {
                    newQueue.Enqueue(wv);
                }
            }

            if (hasUpdates) WriteDprCut();
            wvs = newQueue;
        }

        public void UpdateDeps()
        {
            while (pendingUpdates.TryDequeue(out var entry))
            {
                precedenceGraph.Add(entry.Item1, entry.Item2);
                wvs.Enqueue(entry.Item1);
            }
        }

        private void ReceiveCallback(IAsyncResult ar)
        {
            var connState = (ConnectionState) ar.AsyncState;
            var read = connState.socket.EndReceive(ar);
            connState.received += read;
            while (connState.HasCompleteMessage())
                pendingUpdates.Enqueue(connState.ReadMessage());
            connState.socket.BeginReceive(connState.buffer, connState.head, connState.buffer.Length - connState.head, SocketFlags.None, ReceiveCallback, connState);
        }

        public async void StartServer(string hostname, int port)
        {
            var ip = IPAddress.Parse(hostname);
            var endPoint = new IPEndPoint(ip, port);
            servSocket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            servSocket.Bind(endPoint);
            servSocket.Listen(512);

            var e = new ManualResetEventSlim();
            while (!done)
            {
                e.Reset();
                servSocket.BeginAccept(ar =>
                {
                    e.Set();
                    var socket = servSocket.EndAccept(ar);
                    var state = new ConnectionState(socket);
                    socket.BeginReceive(state.buffer, state.head, state.buffer.Length, SocketFlags.None, ReceiveCallback , state);
                }, null);
                
                while (!e.Wait(TimeSpan.FromMilliseconds(10)))
                    if (done) break;
            }
        }

        public void StopServer()
        {
            done = true;
        }

    }
}