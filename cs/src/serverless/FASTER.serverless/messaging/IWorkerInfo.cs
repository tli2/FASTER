namespace FASTER.serverless
{
    /// <summary>
    /// Identifying information for a worker in the Faster-Serverless system. A working system should consist
    /// of unique workers.
    /// </summary>
    public interface IWorkerInfo
    {
        /// <summary></summary>
        /// <returns>A globally unique identifier for a participant in Faster-Serverless</returns>
        Worker GetWorker();
        /// <summary>
        /// Deserialize information from serialized form
        /// </summary>
        /// <param name="bytes">serialized bytes</param>
        void InitializeFromByteArray(byte[] bytes);
        /// <summary>
        /// Serialize information to bytes
        /// </summary>
        /// <returns>serialized bytes</returns>
        byte[] AsByteArray();
    }
}