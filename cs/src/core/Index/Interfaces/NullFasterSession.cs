namespace FASTER.core
{
    struct NullFasterSession : IFasterSession
    {
        public static readonly NullFasterSession Instance;
        private static CommitPoint commitPoint;

        public void CheckpointCompletionCallback(string guid, CommitPoint commitPoint)
        {
        }

        public void UnsafeResumeThread()
        {
        }

        public void UnsafeSuspendThread()
        {
        }
        
        public long Version() => 0;

        public string Id() => "";

        public FasterRollbackException GetCannedException() => null;

        public void SetCannedException(FasterRollbackException e) {}

        public ref CommitPoint CommitPoint() => ref commitPoint;
    }
}