﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using FASTER.core;

namespace FASTER.serverless
{
    public class VersionExceptionListEnumerator : IEnumerator<(long, WorkerVersion)>
    {
        private IEnumerator<KeyValuePair<WorkerVersion, ReusableObject<List<long>>>> versionEnumerator;
        private IEnumerator<long> opEnumerator;

        public VersionExceptionListEnumerator(Dictionary<WorkerVersion, ReusableObject<List<long>>> dict)
        {
            versionEnumerator = dict.GetEnumerator();
        } 
        
        public bool MoveNext()
        {
            if (opEnumerator != null && opEnumerator.MoveNext()) return true;
            if (!versionEnumerator.MoveNext()) return false;
            opEnumerator?.Dispose();
            opEnumerator = versionEnumerator.Current.Value.obj.GetEnumerator();
            return true;
        }

        public void Reset()
        {
            versionEnumerator.Reset();
            opEnumerator = versionEnumerator.Current.Value.obj.GetEnumerator();
        }

        public (long, WorkerVersion) Current => ValueTuple.Create(opEnumerator.Current, versionEnumerator.Current.Key);

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            versionEnumerator.Dispose();
        }
    }
    
    public class VersionExceptionList : IEnumerable<(long, WorkerVersion)>
    {
        private SimpleObjectPool<List<long>> listPool = new SimpleObjectPool<List<long>>(() => new List<long>(), null, 512);
        private Dictionary<WorkerVersion, ReusableObject<List<long>>> exceptionMappings = new Dictionary<WorkerVersion, ReusableObject<List<long>>>();
        // WTF C# cannot remove things from dictionary while iterating?
        private List<WorkerVersion> toRemove = new List<WorkerVersion>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(long serialNum, WorkerVersion executedAt)
        {
            if (!exceptionMappings.TryGetValue(executedAt, out var list))
            {
                list = listPool.Checkout();
                list.obj.Clear();
                exceptionMappings.Add(executedAt, list);
            }
            list.obj.Add(serialNum);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool PendingOperationRecoverable(ref WorkerVersion executedAt, IDprTableSnapshot dprTable)
        {
            return executedAt.Version != -1 && executedAt.Version <= dprTable.SafeVersion(executedAt.Worker);
        }

        public void ResolveExceptions(IDprTableSnapshot dprTable, long[] commitTimes, long currentTime)
        {
            foreach (var entry in exceptionMappings)
            {
                if (dprTable.SafeVersion(entry.Key.Worker) >= entry.Key.Version)
                {
                    if (commitTimes != null)
                    {
                        foreach (var serialNum in entry.Value.obj)
                            commitTimes[serialNum] = currentTime;
                    }
                    entry.Value.obj.Clear();
                    entry.Value.Dispose();
                    toRemove.Add(entry.Key);
                }
            }

            foreach (var wv in toRemove)
                exceptionMappings.Remove(wv);
            
            toRemove.Clear();
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool OperationRecovered(ref CommitPoint recoveredProgress, long sequenceNumber)
        {
            // TODO(Tianyu): Currently, because remote workers do not return results that are pending, the only
            // exceptions in recovered progress will be locally pending operations, which is disjoint from the
            // exceptions tracked in this class. Therefore there is no need to check the exception list here.
            return sequenceNumber < recoveredProgress.UntilSerialNo;
        }

        public void DropRolledbackExceptions(ref CommitPoint recoveredProgress)
        {
           // TODO(Tianyu): Implement more fine-grained later, will probably be easier if recovery comes with the exact version cut off
           // for each worker.
           foreach (var entry in exceptionMappings)
           {
               entry.Value.obj.Clear();
               entry.Value.Dispose();
           }
           exceptionMappings.Clear();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<(long, WorkerVersion)> GetEnumerator()
        {
            return new VersionExceptionListEnumerator(exceptionMappings);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}