namespace FASTER.libdpr
{
    public class DprWorkerOptions
    {
        public DprWorkerId Me;
        public IDprFinder DprFinder = null;
        public long CheckpointPeriodMilli = 5;
        public long RefreshPeriodMilli = 5;
    }
}