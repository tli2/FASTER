// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FasterLogStress
{
    public class Program
    {
        private static FasterLog log;
        private static IDevice device;
        static readonly byte[] entry = new byte[100];
        private static string commitPath;

        private static byte[] buffer;

        public static async Task Main()
        {
            using var settings = new FasterLogSettings("./Test", deleteDirOnDispose: true)
            {
                AutoRefreshSafeTailAddress = true
            };
            log = new FasterLog(settings);

            buffer = new byte[2048];
            Random.Shared.NextBytes(buffer);

            await Task.WhenAll(EnqueueThread(), ScanThread());
            log.Dispose();
        }
        
        static async Task EnqueueThread()
        {
            for (int count = 0; count < 5; ++count)
            {
                await log.EnqueueAsync(buffer);
                await Task.Delay(1000);
            }

            log.CompleteLog();
            Console.WriteLine("Enqueue complete");
        }

        static async Task ScanThread()
        {
            using var iterator = log.Scan(log.BeginAddress, long.MaxValue, scanUncommitted: true);
            while (true)
            {
                byte[] result;
                while (!iterator.GetNext(out result, out _, out _))
                {
                    if (iterator.Ended)
                    {
                        Console.WriteLine("Scan complete");
                        return;
                    }

                    await iterator.WaitAsync();
                }
    
                Console.WriteLine("Received buffer");
                Debug.Assert(result.SequenceEqual(buffer));
            }
        }


        public static void ManagedLocalStoreBasicTest()
        {
            int entryLength = 20;
            int numEntries = 500_000;
            int numEnqueueThreads = 1;
            int numIterThreads = 1;
            bool commitThread = false;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            bool disposeCommitThread = false;
            var commit =
                new Thread(() =>
                {
                    while (!disposeCommitThread)
                    {
                        Thread.Sleep(10);
                        log.Commit(true);
                    }
                });

            if (commitThread)
                commit.Start();

            Thread[] th = new Thread[numEnqueueThreads];
            for (int t = 0; t < numEnqueueThreads; t++)
            {
                th[t] =
                new Thread(() =>
                {
                    // Enqueue but set each Entry in a way that can differentiate between entries
                    for (int i = 0; i < numEntries; i++)
                    {
                        // Flag one part of entry data that corresponds to index
                        entry[0] = (byte)i;

                        // Default is add bytes so no need to do anything with it
                        log.Enqueue(entry);
                    }
                });
            }

            Console.WriteLine("Populating log...");
            var sw = Stopwatch.StartNew();

            for (int t = 0; t < numEnqueueThreads; t++)
                th[t].Start();
            for (int t = 0; t < numEnqueueThreads; t++)
                th[t].Join();

            sw.Stop();
            Console.WriteLine($"{numEntries} items enqueued to the log by {numEnqueueThreads} threads in {sw.ElapsedMilliseconds} ms");

            if (commitThread)
            {
                disposeCommitThread = true;
                commit.Join();
            }

            // Final commit to the log
            log.Commit(true);

            // flag to make sure data has been checked 
            bool datacheckrun = false;

            Thread[] th2 = new Thread[numIterThreads];
            for (int t = 0; t < numIterThreads; t++)
            {
                th2[t] =
                    new Thread(() =>
                    {
                        // Read the log - Look for the flag so know each entry is unique
                        int currentEntry = 0;
                        using (var iter = log.Scan(0, long.MaxValue))
                        {
                            while (iter.GetNext(out byte[] result, out _, out _))
                            {
                                // set check flag to show got in here
                                datacheckrun = true;

                                if (numEnqueueThreads == 1)
                                    if (result[0] != (byte)currentEntry)
                                        throw new Exception("Fail - Result[" + currentEntry.ToString() + "]:" + result[0].ToString());
                                currentEntry++;
                            }
                        }

                        if (currentEntry != numEntries * numEnqueueThreads)
                            throw new Exception("Error");
                    });
            }

            sw.Restart();

            for (int t = 0; t < numIterThreads; t++)
                th2[t].Start();
            for (int t = 0; t < numIterThreads; t++)
                th2[t].Join();

            sw.Stop();
            Console.WriteLine($"{numEntries} items iterated in the log by {numIterThreads} threads in {sw.ElapsedMilliseconds} ms");

            // if data verification was skipped, then pop a fail
            if (datacheckrun == false)
                throw new Exception("Failure -- data loop after log.Scan never entered so wasn't verified. ");
        }
    }
}
