using System;

namespace FASTER.benchmark
{
    [Serializable]
    public class BenchmarkConfiguration
    {
        public int clientThreadCount, execThreadCount;
        public string distribution;
        public int readPercent;
        public int checkpointMilli;
        public string connString;
        public int windowSize;
        public int batchSize;

        public override string ToString()
        {
            return $"{nameof(clientThreadCount)}: {clientThreadCount}, {nameof(execThreadCount)}: {execThreadCount}, {nameof(distribution)}: {distribution}, {nameof(readPercent)}: {readPercent}, {nameof(checkpointMilli)}: {checkpointMilli}, {nameof(windowSize)}: {windowSize}, {nameof(batchSize)}: {batchSize}";
        }
    }
}