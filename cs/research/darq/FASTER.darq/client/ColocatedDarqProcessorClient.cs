using System.Diagnostics;
using System.Runtime.CompilerServices;
using FASTER.common;
using FASTER.libdpr;

namespace FASTER.darq
{
    /// <summary>
    /// A DarqConsumer that runs in the same process as a DARQ instance
    /// </summary>
    public class ColocatedDarqProcessorClient : IDarqProcessorClient
    {
        private Darq darq;
        private SimpleObjectPool<DarqMessage> messagePool;
        private ManualResetEventSlim terminationComplete;

        // TODO(Tianyu): Reason about behavior in the case of rollback
        public long incarnation;
        private DarqScanIterator iterator;
        private DprSession session;
        private Capabilities capabilities;

        private bool speculative;

        private enum ProcessResult
        {
            CONTINUE,
            NO_ENTRY,
            TERMINATED
        }
        
        private class Capabilities : IDarqProcessorClientCapabilities
        {
            private readonly ColocatedDarqProcessorClient parent;
            private DprSession session;

            public Capabilities(ColocatedDarqProcessorClient parent)
            {
                this.parent = parent;
                session = parent.session;
            }

            public async ValueTask<StepStatus> Step(StepRequest request)
            {
                // If step results in a version mismatch, rely on the scan to trigger a rollback for simplicity
                if (!await parent.darq.TakeOnDependencyAndStartActionAsync(session))
                    return StepStatus.REINCARNATED;
                var status = parent.darq.Step(parent.incarnation, request);
                parent.darq.EndAction();
                return status;
            }

            public DprSession GetDprSession() => session;
        }

        /// <summary>
        /// Constructs a new ColocatedDarqProcessorClient
        /// </summary>
        /// <param name="darq">DARQ DprServer that this consumer attaches to </param>
        /// <param name="clusterInfo"> information about the DARQ cluster </param>
        public ColocatedDarqProcessorClient(Darq darq, bool speculative)
        {
            this.darq = darq;
            messagePool = new SimpleObjectPool<DarqMessage>(() => new DarqMessage(messagePool));
            this.speculative = speculative;
        }

        public void Dispose()
        {
            messagePool.Dispose();
            iterator?.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool TryReadEntry(out DarqMessage message)
        {
            message = null;
            if (!iterator.UnsafeGetNext(out var entry, out var entryLength,
                    out var lsn, out var nextLsn, out var type))
                return false;

            // Short circuit without looking at the entry -- no need to process in background
            if (type != DarqMessageType.IN && type != DarqMessageType.RECOVERY)
            {
                iterator.UnsafeRelease();
                return true;
            }

            // Copy out the entry before dropping protection
            message = messagePool.Checkout();
            message.Reset(type, lsn, nextLsn, new ReadOnlySpan<byte>(entry, entryLength));
            iterator.UnsafeRelease();

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ProcessResult TryConsumeNext<T>(T processor) where T : IDarqProcessor
        {
            try
            {
                var hasNext = TryReadEntry(out var m);
                if (!hasNext)
                    return ProcessResult.NO_ENTRY;
                // Not a message we need to worry about
                if (m == null) return ProcessResult.CONTINUE;

                session.DependOn(darq);
                switch (m.GetMessageType())
                {
                    case DarqMessageType.IN:
                    case DarqMessageType.RECOVERY:
                        if (processor.ProcessMessage(m))
                            return ProcessResult.CONTINUE;
                        return ProcessResult.TERMINATED;
                    default:
                        throw new NotImplementedException();
                }
            }
            catch (DprSessionRolledBackException)
            {
                Console.WriteLine("Processor detected rollback, restarting");
                OnProcessorClientRestart(processor);
                // Reset to next iteration without doing anything
                return ProcessResult.CONTINUE;
            }
        }

        private void OnProcessorClientRestart<T>(T processor) where T : IDarqProcessor
        {
            session = new DprSession();
            capabilities = new Capabilities(this);
            processor.OnRestart(capabilities);
            iterator = darq.StartScan(speculative);
        }
        
        /// <inheritdoc/>
        public async Task StartProcessingAsync<T>(T processor, CancellationToken token)
            where T : IDarqProcessor
        {
            try
            {
                terminationComplete = new ManualResetEventSlim();
                incarnation = darq.RegisterNewProcessor();
                OnProcessorClientRestart(processor);
                Console.WriteLine("Starting Processor...");
                while (!token.IsCancellationRequested)
                {
                    ProcessResult result;
                    do
                    {
                        result = TryConsumeNext(processor);
                    } while (result == ProcessResult.CONTINUE);

                    if (result == ProcessResult.TERMINATED)
                        break;

                    // FASTER.darq.StateObject().RefreshSafeReadTail();
                    try
                    {
                        await iterator.WaitAsync(token);
                    }
                    catch (OperationCanceledException) {}
                }

                Console.WriteLine($"Colocated processor has exited on worker {darq.Me().guid}");
                terminationComplete.Set();
            }
            catch (Exception e)
            {
                Console.WriteLine("C# why you eat exceptions");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
        }
    }
}