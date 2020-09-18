﻿using System.Collections.Concurrent;
using System.Collections.Generic;
using FASTER.core;

namespace FASTER.serverless
{
    public class OutstandingLocalVersion
    {
        private long version;

        // The log address that is before the first entry of this version on the log. When a rollback is issued
        // the log scan will start from this location. Entries from previous versions may still appear after this
        // boundary due to the fuzziness in the CPR protocol.
        private long versionStart;
        internal ConcurrentDictionary<string, CommitPoint> checkpointSessionProgress;
        private LightDependencySet deps;

        public OutstandingLocalVersion(long version, long versionStart)
        {
            this.version = version;
            this.versionStart = versionStart;
            deps = new LightDependencySet();
        }

        public long Version() => version;

        public long FuzzyVersionStartLogOffset() => versionStart;

        public void AddDependency(Worker worker, long version) => deps.Update(worker, version);

        public List<WorkerVersion> GetDependenciesSlow()
        {
            var result = new List<WorkerVersion>();
            for (var i = 0; i < deps.DependentVersions.Length; i++)
            {
                var dep = deps.DependentVersions[i];
                if (dep != LightDependencySet.NoDependency)
                    result.Add(new WorkerVersion(i, dep));
                
            }

            return result;
        }
    }
}