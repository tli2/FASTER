using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FASTER.libdpr
{
    public class StateObjectRefreshBackgroundService : BackgroundService
    {
        private ILogger<StateObjectRefreshBackgroundService> logger;
        private CancellationToken stoppingToken;
        private Thread refreshThread;
        private List<StateObject> stateObjects = new List<StateObject>();

        public StateObjectRefreshBackgroundService(ILogger<StateObjectRefreshBackgroundService> logger,
            StateObject defaultObject = null)
        {
            this.logger = logger;
            if (defaultObject != null) stateObjects.Add(defaultObject);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            logger?.LogInformation("Refresh background service is starting");
            refreshThread = new Thread(() =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    lock (stateObjects)
                    {
                        foreach (var so in stateObjects)
                        {
                            // Must not begin refreshing before the state object is connected
                            if (so.ConnectedToCluster())
                                so.Refresh();
                        }
                    }

                    Thread.Yield();
                }

            });
            refreshThread.Start();
            await Task.Delay(Timeout.Infinite, this.stoppingToken);
            logger?.LogInformation("Refresh background service is winding down");
            refreshThread.Join();
        }

        public void RegisterRefreshTask(StateObject toRegister)
        {
            lock (stateObjects)
            {
                if (stoppingToken.IsCancellationRequested) throw new TaskCanceledException();
                stateObjects.Add(toRegister);
            }
        }
    }
}