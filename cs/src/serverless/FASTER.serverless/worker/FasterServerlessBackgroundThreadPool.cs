﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FASTER.serverless
{
    /// <summary>
    /// A pool of threads to execute remote requests.
    ///  
    /// </summary>
    public class FasterServerlessBackgroundThreadPool<Key, Value, Input, Output, Functions> : IDisposable
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        private List<BlockingCollection<ReusableObject<ParsedMessageBatch<Key, Value, Input, Output>>>> queues;
        private List<Thread> threads;

        public void Dispose()
        {
            foreach (var queue in queues)
                // Send null to signal termination
                queue.CompleteAdding();
            foreach (var thread in threads)
                thread.Join();
        }
        
        public void SubmitRequest(ReusableObject<ParsedMessageBatch<Key, Value, Input, Output>> toExecute)
        {
            // TODO(Tianyu): For YCSB benchmark, use a custom hash scheme for performance?
            // This, in combination with the fact that a worker's socket is read single-threadedly in the 
            // message layer ensures single-threaded and in-order execution of a request from a single session.
            var i = (int) ((uint) toExecute.obj.header.sessionId.GetHashCode() % queues.Count);
            queues[i].Add(toExecute);
        }

        public void Start(int taskCount,
            FasterServerless<Key, Value, Input, Output, Functions> worker)
        {
            queues = new List<BlockingCollection<ReusableObject<ParsedMessageBatch<Key, Value, Input, Output>>>>();
            threads = new List<Thread>();
            for (var i = 0; i < taskCount; i++)
            {
                var queue = new BlockingCollection<ReusableObject<ParsedMessageBatch<Key, Value, Input, Output>>>();
                queues.Add(queue);
                var thread = new Thread(() =>
                {
                    while(true)
                    {
                        try
                        {
                            var request = queue.Take();
                            worker.RetryBatch(request.obj);
                            request.Dispose();
                        }
                        catch (Exception)
                        {
                            return;
                        }
                    }
                });
                thread.Start();
                threads.Add(thread);
            }
        }
    }
}