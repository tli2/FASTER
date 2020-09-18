using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace FASTER.serverless
{
    public interface IDprTableSnapshot
    {
        long SafeVersion(Worker worker);
    }
    
    public interface IDprManager
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        long SafeVersion(Worker worker);
        
        IDprTableSnapshot ReadSnapshot();

        long SystemWorldLine();

        long GlobalMaxVersion();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="newVersion"></param>
        /// <returns></returns>
        void ReportNewPersistentVersion(WorkerVersion persisted, List<WorkerVersion> deps);
        
        /// <summary>
        /// 
        /// </summary>
        /// <returns>system world line</returns>
        void Refresh();

        void Clear();

        void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion);
    }
}