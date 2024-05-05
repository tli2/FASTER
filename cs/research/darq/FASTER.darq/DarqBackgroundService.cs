using System.Collections.Concurrent;
using System.Diagnostics;
using darq.client;
using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FASTER.client
{
    public class DarqMaintenanceBackgroundServiceSettings
    {
        // Processing chunk size before the task yields
        public int morselSize = 512;

        // batch size for background sends
        public int batchSize = 16;
        
        public bool speculative = false;
        
        public Func<DprSession, IDarqProducer> producerFactory;
    }

    public class DarqBackgroundMaintenanceTask : IDisposable
    {
        private Darq darq;
        private DarqMaintenanceBackgroundServiceSettings settings;
        private DprSession session;

        private DarqScanIterator iterator;
        private DarqCompletionTracker completionTracker;
        private long processedUpTo;

        private IDarqProducer currentProducerClient;
        private int numBatched = 0;

        private SimpleObjectPool<DarqMessage> messagePool;
        private ILogger<DarqMaintenanceBackgroundService> logger;

        /// <summary>
        /// Constructs a new ColocatedDarqProcessorClient
        /// </summary>
        /// <param name="darq">DARQ DprServer that this consumer attaches to </param>
        /// <param name="clusterInfo"> information about the DARQ cluster </param>
        public DarqBackgroundMaintenanceTask(Darq darq, DarqMaintenanceBackgroundServiceSettings settings,
            SimpleObjectPool<DarqMessage> messagePool, ILogger<DarqMaintenanceBackgroundService> logger)
        {
            this.darq = darq;
            this.settings = settings;
            this.messagePool = messagePool;
            this.logger = logger;
            Reset();
        }
        
        private void Reset()
        {
            session = darq.DetachFromWorker();
            currentProducerClient = settings.producerFactory?.Invoke(settings.speculative ? new DprSession() : null);
            completionTracker = new DarqCompletionTracker();
            iterator = darq.StartBackgroundScan(settings.speculative);
        }

        public long ProcessingLag => darq.log.TailAddress - processedUpTo;

        public void Dispose()
        {
            iterator?.Dispose();
            currentProducerClient?.Dispose();
        }

        private unsafe bool TryReadEntry(out DarqMessage message)
        {
            message = null;
            long nextAddress = 0;

            if (!iterator.UnsafeGetNext(out var entry, out var entryLength,
                    out var lsn, out processedUpTo, out var type))
                return false;

            completionTracker.AddEntry(lsn, processedUpTo);
            // Short circuit without looking at the entry -- no need to process in background
            if (type != DarqMessageType.OUT && type != DarqMessageType.COMPLETION)
            {
                iterator.UnsafeRelease();
                return true;
            }

            // Copy out the entry before dropping protection
            message = messagePool.Checkout();
            message.Reset(type, lsn, processedUpTo,
                new ReadOnlySpan<byte>(entry, entryLength));
            iterator.UnsafeRelease();

            return true;
        }

        // TODO(Tianyu): Create variants that allow DARQ instances to talk with each other through more than just the FASTER wire protocol
        private unsafe void SendMessage(DarqMessage m)
        {
            Debug.Assert(m.GetMessageType() == DarqMessageType.OUT);
            var body = m.GetMessageBody();
            fixed (byte* h = body)
            {
                var dest = *(DarqId*)h;
                var toSend = new ReadOnlySpan<byte>(h + sizeof(DarqId),
                    body.Length - sizeof(DarqId));
                var completionTrackerLocal = completionTracker;
                var lsn = m.GetLsn();
                // TODO(Tianyu): Make ack more efficient through batching
                currentProducerClient.EnqueueMessageWithCallback(dest, toSend,
                    _ => { completionTrackerLocal.RemoveEntry(lsn); }, darq.Me().guid, lsn);
                if (++numBatched == settings.batchSize)
                {
                    numBatched = 0;
                    currentProducerClient.ForceFlush();
                }
            }

            m.Dispose();
        }

        private bool TryConsumeNext()
        {
            var hasNext = TryReadEntry(out var m);
            // Don't go through the normal receive code path for performance
            if (!darq.IsCompatible(session))
            {
                logger.LogWarning("Processor detected rollback, restarting");
                Reset();
                // Reset to next iteration without doing anything
                return true;
            }

            if (!hasNext) return false;
            // Not a message we care about
            if (m == null) return true;

            switch (m.GetMessageType())
            {
                case DarqMessageType.OUT:
                {
                    SendMessage(m);
                    break;
                }
                case DarqMessageType.COMPLETION:
                {
                    var body = m.GetMessageBody();
                    unsafe
                    {
                        fixed (byte* h = body)
                        {
                            for (var completed = (long*)h; completed < h + body.Length; completed++)
                                completionTracker.RemoveEntry(*completed);
                        }
                    }

                    completionTracker.RemoveEntry(m.GetLsn());
                    m.Dispose();
                    break;
                }
                default:
                    throw new NotImplementedException();
            }

            if (completionTracker.GetTruncateHead() > darq.log.BeginAddress)
            {
                // logger.LogInformation($"Truncating log until {completionTracker.GetTruncateHead()}");
                darq.StartLocalAction();
                darq.TruncateUntil(completionTracker.GetTruncateHead());
                darq.EndAction();
            }

            return true;
        }

        internal async Task RunAsync(CancellationToken stoppingToken)
        {
            Console.WriteLine($"Starting background send from address {darq.log.BeginAddress}");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    for (var i = 0; i < settings.morselSize; i++)
                        if (!TryConsumeNext())
                            break;

                    currentProducerClient?.ForceFlush();
                    await iterator.WaitAsync(stoppingToken);
                }
                catch (Exception e)
                {
                    // Just restart the failed background thread
                    logger.LogWarning($"Exception {e.Message} was thrown, restarting background worker");
                    Reset();
                }
            }
        }
    }

    public class DarqMaintenanceBackgroundService : BackgroundService
    {
        private SimpleObjectPool<DarqMessage> messagePool;
        private ILogger<DarqMaintenanceBackgroundService> logger;
        private CancellationToken stoppingToken;
        private ConcurrentDictionary<DarqBackgroundMaintenanceTask, bool> dispatchedTasks = new();

        private Darq defaultDarq;
        private DarqMaintenanceBackgroundServiceSettings defaultSettings;

        public DarqMaintenanceBackgroundService(ILogger<DarqMaintenanceBackgroundService> logger,
            Darq defaultDarq = null, DarqMaintenanceBackgroundServiceSettings defaultSettings = null)
        {
            messagePool = new SimpleObjectPool<DarqMessage>(() => new DarqMessage(messagePool));
            this.logger = logger;
            this.defaultDarq = defaultDarq;
            this.defaultSettings = defaultSettings;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("maintenance background service is starting");
            if (defaultDarq != null)
            {
                Debug.Assert(defaultSettings != null);
                RegisterMaintenanceTask(defaultDarq, defaultSettings);
            }
            await Task.Delay(Timeout.Infinite, stoppingToken);
            logger.LogInformation("stop signal received. maintenance background service is cleaning up...");

            foreach (var task in dispatchedTasks.Keys)
            {
                if (!dispatchedTasks[task])
                    await Task.Yield();
                task.Dispose();
            }

            logger.LogInformation("maintenance background service has finished clean-up, shutting down...");
        }

        public DarqBackgroundMaintenanceTask RegisterMaintenanceTask(Darq darq, DarqMaintenanceBackgroundServiceSettings settings)
        {
            if (stoppingToken.IsCancellationRequested) throw new TaskCanceledException();
            if ((defaultDarq != null && darq != defaultDarq) || (defaultSettings != null && settings != defaultSettings))
                throw new InvalidOperationException(
                    "Runtime creation of maintenance task is only allowed if no singleton default DARQ is configured");
            var task = new DarqBackgroundMaintenanceTask(darq, settings, messagePool, logger);
            dispatchedTasks[task] = false;
            Task.Run(async () =>
            {
                logger.LogInformation($"maintenance background task for DARQ {darq.settings.Me} is starting");
                await task.RunAsync(stoppingToken);
                dispatchedTasks[task] = true;
                logger.LogInformation($"maintenance background task for DARQ {darq.settings.Me} exited");
            });
            return task;
        }
    }
}