
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace FASTER.serverless
{
    public class V3DprFinder : IDisposable
    {
        private readonly SqlConnection writeConn, readConn;
        private Dictionary<Worker, long> currentDprCut;
        private Dictionary<WorkerVersion, List<WorkerVersion>> precedenceGraph;
        private Queue<WorkerVersion> wvs;

        private HashSet<WorkerVersion> visited;
        private Queue<WorkerVersion> frontier;
        
        public V3DprFinder(string connString)
        {
            currentDprCut = new Dictionary<Worker, long>();
            precedenceGraph = new Dictionary<WorkerVersion, List<WorkerVersion>>();
            wvs = new Queue<WorkerVersion>();
            writeConn = new SqlConnection(connString);
            readConn = new SqlConnection(connString);
            writeConn.Open();
            readConn.Open();
            
            visited = new HashSet<WorkerVersion>();
            frontier = new Queue<WorkerVersion>();
        }

        public void Dispose()
        {
            writeConn.Dispose();
            readConn.Dispose();
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
            var selectCommand = new SqlCommand($"EXEC drainDeps", readConn);
            var reader = selectCommand.ExecuteReader();
            while (reader.Read())
            {
                var fromWv = new WorkerVersion(long.Parse((string) reader[0]), (long) reader[1]);
                var toWv = new WorkerVersion(long.Parse((string) reader[2]), (long) reader[3]);
                if (!precedenceGraph.TryGetValue(fromWv, out var deps))
                {
                    deps = new List<WorkerVersion>();
                    precedenceGraph.Add(fromWv, deps);
                    wvs.Enqueue(fromWv);
                }
                deps.Add(toWv);
            }
            reader.Close();
        }

    }
}