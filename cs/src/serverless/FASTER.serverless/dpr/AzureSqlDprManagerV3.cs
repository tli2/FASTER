﻿using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;

namespace FASTER.serverless
{
    public class AzureSqlDprManagerV3 : IDprManager
    {
        private readonly Worker me;
        private Dictionary<Worker, long> recoverableCut;
        private long systemWorldLine;
        private readonly SqlConnection writeConn, readConn;
        private DateTime lastRefreshed = DateTime.UtcNow;

        public AzureSqlDprManagerV3(string connString, Worker me)
        {
            this.me = me;
            recoverableCut = new Dictionary<Worker, long>();
            writeConn = new SqlConnection(connString);
            readConn = new SqlConnection(connString);
            writeConn.Open();
            readConn.Open();
            var registration = new SqlCommand($"EXEC upsertVersion @worker={me.guid}, @version=0, @oegVersion=0", writeConn);
            registration.ExecuteNonQuery();
            var worldLines = new SqlCommand($"INSERT INTO worldLines VALUES({me.guid}, 0)", writeConn);
            worldLines.ExecuteNonQuery();
        }

        public long SafeVersion(Worker worker)
        {
            return !recoverableCut.TryGetValue(worker, out var safeVersion) ? 0 : safeVersion;
        }

        public IDprTableSnapshot ReadSnapshot()
        {
            return new V3DprTableSnapshot(recoverableCut);
        }

        public long SystemWorldLine()
        {
            return systemWorldLine;
        }

        public void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion)
        {
            // V1 does not use the safeVersion column, can always use 0
            var upsert = new SqlCommand($"EXEC reportRecoveryV3 @workerId={latestRecoveredVersion.Worker.guid}," +
                                        $"@worldLine={worldLine}, @survivingVersion={latestRecoveredVersion.Version}", writeConn);
            upsert.ExecuteNonQuery();
        }

        public long GlobalMaxVersion()
        {
            // Only used for fast-forwarding of versions. Not required for v3.
            return 0;
        }

        public void ReportNewPersistentVersion(WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
        {
            var queryBuilder = new StringBuilder("INSERT INTO deps VALUES ");
            foreach (var dep in deps)
                queryBuilder.Append(
                    $"({persisted.Worker.guid}, {persisted.Version}, {dep.Worker.guid}, {dep.Version}),");
            // Always add an edge pointing the previous version at the end to ensure that the worker-version has at
            // least one entry in the deps table
            queryBuilder.Append($"({persisted.Worker.guid}, {persisted.Version}, {persisted.Worker.guid}, {persisted.Version - 1});");
            var insert = new SqlCommand(queryBuilder.ToString(), writeConn);
            insert.ExecuteNonQuery();
        }

        public void Refresh()
        {
            var newRecoverableCut = new Dictionary<Worker, long>(recoverableCut);
            var selectCommand = new SqlCommand($"EXEC getTableUpdatesV3", readConn);
            var reader = selectCommand.ExecuteReader();
            var hasNextRow = reader.Read();
            Debug.Assert(hasNextRow);
            lastRefreshed = (DateTime) reader[0];
            systemWorldLine = (long) reader[1];

            var hasNextResultSet = reader.NextResult();
            Debug.Assert(hasNextResultSet);
            while (reader.Read())
            {
                var worker = new Worker(long.Parse((string) reader[0]));
                newRecoverableCut[worker] = (long) reader[1];
            }

            // Has to be after the update to global min, so any races are benign
            recoverableCut = newRecoverableCut;
            reader.Close();
        }

        public void Clear()
        {
            writeConn.Dispose();
            readConn.Dispose();
        }
    }
}