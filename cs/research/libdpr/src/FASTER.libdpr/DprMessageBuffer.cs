using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using FASTER.core;

namespace FASTER.libdpr
{
    // internal class DprMessageBuffer
    // {
    //     private ConcurrentDictionary<WorkerVersion, (long, ConcurrentQueue<Action>)> queues;
    //     private List<WorkerVersion> toDelete;
    //
    //     public DprMessageBuffer()
    //     {
    //         queues = new ConcurrentDictionary<WorkerVersion, (long, ConcurrentQueue<Action>)>();
    //         toDelete = new List<WorkerVersion>();
    //     }
    //
    //     public void Buffer(ref DprMessageHeader header, Action item)
    //     {
    //         var wv = new WorkerVersion(header.SrcWorkerId, header.Version);
    //         var wl = header.worldLine;
    //         var entry = queues.GetOrAdd(wv, _ => ValueTuple.Create(wl, new ConcurrentQueue<Action>()));
    //         entry.Item2.Enqueue(item);
    //     }
    //
    //     public void ProcessBuffer(IDprFinder dprFinder)
    //     {
    //         toDelete.Clear();
    //         foreach (var (wv, entry) in queues)
    //         {
    //             switch (dprFinder.CheckStatus(entry.Item1, wv))
    //             {
    //                 case DprStatus.COMMITTED:
    //                     while (!entry.Item2.TryDequeue(out var a)) a();
    //                     toDelete.Add(wv);
    //                     break;
    //                 case DprStatus.SPECULATIVE:
    //                     break;
    //                 case DprStatus.ROLLEDBACK:
    //                     toDelete.Add(wv);
    //                     break;
    //                 default:
    //                     throw new ArgumentOutOfRangeException();
    //             }
    //         }
    //
    //
    //         foreach (var wv in toDelete)
    //             queues.TryRemove(wv, out _);
    //     }
    // }
}