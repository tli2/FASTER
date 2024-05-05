// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using System.Diagnostics;

namespace FASTER.core
{
    /// <summary>
    /// Epoch protection
    /// </summary>
    public unsafe sealed class LightEpoch
    {
        /// <summary>
        /// Store thread-static metadata separately; see https://github.com/microsoft/FASTER/pull/746
        /// </summary>
        public class EpochContext
        {
            /// <summary>
            /// Managed thread id of this thread
            /// </summary>
            [ThreadStatic] internal static int threadId;

            /// <summary>
            /// Start offset to reserve entry in the epoch table
            /// </summary>
            [ThreadStatic] internal static ushort threadLocalStartOffset1;

            /// <summary>
            /// Alternate start offset to reserve entry in the epoch table (to reduce probing if <see cref="startOffset1"/> slot is already filled)
            /// </summary>
            [ThreadStatic] internal static ushort threadLocalStartOffset2;

            /// <summary>
            /// A thread's entry in the epoch table.
            /// </summary>
            [ThreadStatic] internal static int threadLocalThreadEntryIndex;

            /// <summary>
            /// Number of instances using this entry
            /// </summary>
            [ThreadStatic] internal static int threadLocalEntryIndexCount;

            /// <summary>
            /// custom id of this epochParticipant,
            /// </summary>
            public int customId;

            internal ushort startOffset1;
            internal ushort startOffset2;
            internal int threadEntryIndex;
            internal int threadEntryIndexCount;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static long GetId(EpochContext context)
            {
                if (context == null)
                    return threadId;
                // Ensure threadId and customId are complete disjoint
                return ((long)context.customId << 32) | 1L;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static ushort GetStartOffset1(EpochContext context) =>
                context?.startOffset1 ?? threadLocalStartOffset1;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static ushort GetStartOffset2(EpochContext context) =>
                context?.startOffset2 ?? threadLocalStartOffset2;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int GetThreadEntryIndex(EpochContext context) =>
                context?.threadEntryIndex ?? threadLocalThreadEntryIndex;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int GetThreadEntryIndexCount(EpochContext context) =>
                context?.threadEntryIndexCount ?? threadLocalEntryIndexCount;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static void SetThreadEntryIndex(EpochContext context, int i)
            {
                if (context == null)
                    threadLocalThreadEntryIndex = i;
                else
                    context.threadEntryIndex = i;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static void SetStartOffset1(EpochContext context, ushort i)
            {
                if (context == null)
                    threadLocalStartOffset1 = i;
                else
                    context.startOffset1 = i;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static void SetStartOffset2(EpochContext context, ushort i)
            {
                if (context == null)
                    threadLocalStartOffset2 = i;
                else
                    context.startOffset2 = i;
            }
            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int IncrementThreadIndexEntryCount(EpochContext context)
            {
                if (context == null)
                    return ++threadLocalEntryIndexCount;
                return ++context.threadEntryIndexCount;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int DecrementThreadIndexEntryCount(EpochContext context)
            {
                if (context == null)
                    return --threadLocalEntryIndexCount;
                return --context.threadEntryIndexCount;
            }
            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static void InvalidateContext(EpochContext context)
            {
                if (context == null)
                    threadLocalThreadEntryIndex = 0;
                else 
                    context.threadEntryIndex = 0;
            }        
        }


        /// <summary>
        /// Size of cache line in bytes
        /// </summary>
        const int kCacheLineBytes = 64;

        /// <summary>
        /// Default invalid index entry.
        /// </summary>
        const int kInvalidIndex = 0;

        /// <summary>
        /// Default number of entries in the entries table
        /// </summary>
        static readonly ushort kTableSize = Math.Max((ushort)128, (ushort)(Environment.ProcessorCount * 2));

        /// <summary>
        /// Default drainlist size
        /// </summary>
        const int kDrainListSize = 16;

        /// <summary>
        /// Thread protection status entries.
        /// </summary>
        readonly Entry[] tableRaw;

        readonly Entry* tableAligned;
#if !NET5_0_OR_GREATER
        GCHandle tableHandle;
#endif

        static readonly Entry[] threadIndex;
        static readonly Entry* threadIndexAligned;
#if !NET5_0_OR_GREATER
        static GCHandle threadIndexHandle;
#endif

        /// <summary>
        /// List of action, epoch pairs containing actions to be performed when an epoch becomes safe to reclaim.
        /// Marked volatile to ensure latest value is seen by the last suspended thread.
        /// </summary>
        volatile int drainCount = 0;

        readonly EpochActionPair[] drainList = new EpochActionPair[kDrainListSize];

        /// <summary>
        /// Global current epoch value
        /// </summary>
        long CurrentEpoch;

        /// <summary>
        /// Cached value of latest epoch that is safe to reclaim
        /// </summary>
        long SafeToReclaimEpoch;

        /// <summary>
        /// Static constructor to setup shared cache-aligned space
        /// to store per-entry count of instances using that entry
        /// </summary>
        static LightEpoch()
        {
            long p;

            // Over-allocate to do cache-line alignment
#if NET5_0_OR_GREATER
            threadIndex = GC.AllocateArray<Entry>(kTableSize + 2, true);
            p = (long)Unsafe.AsPointer(ref threadIndex[0]);
#else
            threadIndex = new Entry[kTableSize + 2];
            threadIndexHandle = GCHandle.Alloc(threadIndex, GCHandleType.Pinned);
            p = (long)threadIndexHandle.AddrOfPinnedObject();
#endif
            // Force the pointer to align to 64-byte boundaries
            long p2 = (p + (kCacheLineBytes - 1)) & ~(kCacheLineBytes - 1);
            threadIndexAligned = (Entry*)p2;
        }

        /// <summary>
        /// Instantiate the epoch table
        /// </summary>
        public LightEpoch()
        {
            long p;

#if NET5_0_OR_GREATER
            tableRaw = GC.AllocateArray<Entry>(kTableSize + 2, true);
            p = (long)Unsafe.AsPointer(ref tableRaw[0]);
#else
            // Over-allocate to do cache-line alignment
            tableRaw = new Entry[kTableSize + 2];
            tableHandle = GCHandle.Alloc(tableRaw, GCHandleType.Pinned);
            p = (long)tableHandle.AddrOfPinnedObject();
#endif
            // Force the pointer to align to 64-byte boundaries
            long p2 = (p + (kCacheLineBytes - 1)) & ~(kCacheLineBytes - 1);
            tableAligned = (Entry*)p2;

            CurrentEpoch = 1;
            SafeToReclaimEpoch = 0;

            // Mark all epoch table entries as "available"
            for (int i = 0; i < kDrainListSize; i++)
                drainList[i].epoch = long.MaxValue;
            drainCount = 0;
        }

        /// <summary>
        /// Clean up epoch table
        /// </summary>
        public void Dispose()
        {
#if !NET5_0_OR_GREATER
            tableHandle.Free();
#endif
            CurrentEpoch = 1;
            SafeToReclaimEpoch = 0;
        }

        /// <summary>
        /// Check whether current epoch instance is protected on this thread
        /// </summary>
        /// <returns>Result of the check</returns>
        public bool ThisInstanceProtected(EpochContext context = null)
        {
            int entry = EpochContext.GetThreadEntryIndex(context);
            if (kInvalidIndex != entry)
            {
                if ((*(tableAligned + entry)).contextId == entry)
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Enter the thread into the protected code region
        /// </summary>
        /// <returns>Current epoch</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ProtectAndDrain(EpochContext context = null)
        {
            int entry = EpochContext.GetThreadEntryIndex(context);

            // Protect CurrentEpoch by making an entry for it in the non-static epoch table so ComputeNewSafeToReclaimEpoch() will see it.
            (*(tableAligned + entry)).contextId = entry;
            (*(tableAligned + entry)).localCurrentEpoch = CurrentEpoch;

            if (drainCount > 0)
            {
                Drain((*(tableAligned + entry)).localCurrentEpoch);
            }
        }

        /// <summary>
        /// Thread suspends its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Suspend(EpochContext context = null)
        {
            Release(context);
            if (drainCount > 0) SuspendDrain(context);
        }

        /// <summary>
        /// Thread resumes its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Resume(EpochContext context = null)
        {
            Acquire(context);
            ProtectAndDrain(context);
        }

        /// <summary>
        /// Increment global current epoch
        /// </summary>
        /// <returns></returns>
        long BumpCurrentEpoch(EpochContext context = null)
        {
            // TODO(Tianyu): Temporarily disabling because DARQ relies on bumping outside of protection
            // Debug.Assert(this.ThisInstanceProtected(), "BumpCurrentEpoch must be called on a protected thread");
            long nextEpoch = Interlocked.Increment(ref CurrentEpoch);

            if (drainCount > 0)
                Drain(nextEpoch);

            return nextEpoch;
        }

        /// <summary>
        /// Increment current epoch and associate trigger action
        /// with the prior epoch
        /// </summary>
        /// <param name="onDrain">Trigger action</param>
        /// <returns></returns>
        public void BumpCurrentEpoch(Action onDrain, EpochContext context = null)
        {
            long PriorEpoch = BumpCurrentEpoch(context) - 1;

            int i = 0;
            while (true)
            {
                if (drainList[i].epoch == long.MaxValue)
                {
                    // This was an empty slot. If it still is, assign this action/epoch to the slot.
                    if (Interlocked.CompareExchange(ref drainList[i].epoch, long.MaxValue - 1, long.MaxValue) ==
                        long.MaxValue)
                    {
                        drainList[i].action = onDrain;
                        drainList[i].epoch = PriorEpoch;
                        Interlocked.Increment(ref drainCount);
                        break;
                    }
                }
                else
                {
                    var triggerEpoch = drainList[i].epoch;

                    if (triggerEpoch <= SafeToReclaimEpoch)
                    {
                        // This was a slot with an epoch that was safe to reclaim. If it still is, execute its trigger, then assign this action/epoch to the slot.
                        if (Interlocked.CompareExchange(ref drainList[i].epoch, long.MaxValue - 1, triggerEpoch) ==
                            triggerEpoch)
                        {
                            var triggerAction = drainList[i].action;
                            drainList[i].action = onDrain;
                            drainList[i].epoch = PriorEpoch;
                            triggerAction();
                            break;
                        }
                    }
                }

                if (++i == kDrainListSize)
                {
                    // We are at the end of the drain list and found no empty or reclaimable slot. ProtectAndDrain, which should clear one or more slots.
                    ProtectAndDrain(context);
                    i = 0;
                    Thread.Yield();
                }
            }

            // Now ProtectAndDrain, which may execute the action we just added.
            ProtectAndDrain(context);
        }

        /// <summary>
        /// Mechanism for threads to mark some activity as completed until
        /// some version by this thread
        /// </summary>
        /// <param name="markerIdx">ID of activity</param>
        /// <param name="version">Version</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Mark(int markerIdx, long version, EpochContext context = null)
        {
            Debug.Assert(markerIdx < 6);
            (*(tableAligned + EpochContext.GetThreadEntryIndex(context))).markers[markerIdx] = version;
        }

        /// <summary>
        /// Check if all active threads have completed the some
        /// activity until given version.
        /// </summary>
        /// <param name="markerIdx">ID of activity</param>
        /// <param name="version">Version</param>
        /// <returns>Whether complete</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CheckIsComplete(int markerIdx, long version)
        {
            Debug.Assert(markerIdx < 6);

            // check if all threads have reported complete
            for (int index = 1; index <= kTableSize; ++index)
            {
                long entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                long fc_version = (*(tableAligned + index)).markers[markerIdx];
                if (0 != entry_epoch)
                {
                    if ((fc_version != version) && (entry_epoch < long.MaxValue))
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Looks at all threads and return the latest safe epoch
        /// </summary>
        /// <param name="currentEpoch">Current epoch</param>
        /// <returns>Safe epoch</returns>
        long ComputeNewSafeToReclaimEpoch(long currentEpoch)
        {
            long oldestOngoingCall = currentEpoch;

            for (int index = 1; index <= kTableSize; ++index)
            {
                long entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                if (0 != entry_epoch)
                {
                    if (entry_epoch < oldestOngoingCall)
                    {
                        oldestOngoingCall = entry_epoch;
                    }
                }
            }

            // The latest safe epoch is the one just before the earliest unsafe epoch.
            SafeToReclaimEpoch = oldestOngoingCall - 1;
            return SafeToReclaimEpoch;
        }

        /// <summary>
        /// Take care of pending drains after epoch suspend
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SuspendDrain(EpochContext context)
        {
            while (drainCount > 0)
            {
                // Barrier ensures we see the latest epoch table entries. Ensures
                // that the last suspended thread drains all pending actions.
                Thread.MemoryBarrier();
                for (int index = 1; index <= kTableSize; ++index)
                {
                    long entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                    if (0 != entry_epoch)
                    {
                        return;
                    }
                }

                Resume(context);
                Release(context);
            }
        }

        /// <summary>
        /// Check and invoke trigger actions that are ready
        /// </summary>
        /// <param name="nextEpoch">Next epoch</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void Drain(long nextEpoch)
        {
            ComputeNewSafeToReclaimEpoch(nextEpoch);

            for (int i = 0; i < kDrainListSize; i++)
            {
                var trigger_epoch = drainList[i].epoch;

                if (trigger_epoch <= SafeToReclaimEpoch)
                {
                    if (Interlocked.CompareExchange(ref drainList[i].epoch, long.MaxValue - 1, trigger_epoch) ==
                        trigger_epoch)
                    {
                        // Store off the trigger action, then set epoch to int.MaxValue to mark this slot as "available for use".
                        var trigger_action = drainList[i].action;
                        drainList[i].action = null;
                        drainList[i].epoch = long.MaxValue;
                        Interlocked.Decrement(ref drainCount);

                        // Execute the action
                        trigger_action();
                        if (drainCount == 0) break;
                    }
                }
            }
        }

        /// <summary>
        /// Thread acquires its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void Acquire(EpochContext context)
        {
            if (EpochContext.GetThreadEntryIndex(context) == kInvalidIndex)
                ReserveEntryForThread(context);

            Debug.Assert((*(tableAligned + EpochContext.GetThreadEntryIndex(context))).localCurrentEpoch == 0,
                "Trying to acquire protected epoch. Make sure you do not re-enter FASTER from callbacks or IDevice implementations. If using tasks, use TaskCreationOptions.RunContinuationsAsynchronously.");

            // This corresponds to AnyInstanceProtected(). We do not mark "ThisInstanceProtected" until ProtectAndDrain().
            EpochContext.IncrementThreadIndexEntryCount(context);
        }

        /// <summary>
        /// Thread releases its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void Release(EpochContext context)
        {
            int entry = EpochContext.GetThreadEntryIndex(context);

            Debug.Assert((*(tableAligned + entry)).localCurrentEpoch != 0,
                "Trying to release unprotected epoch. Make sure you do not re-enter FASTER from callbacks or IDevice implementations. If using tasks, use TaskCreationOptions.RunContinuationsAsynchronously.");

            // Clear "ThisInstanceProtected()" (non-static epoch table)
            (*(tableAligned + entry)).localCurrentEpoch = 0;
            (*(tableAligned + entry)).contextId = 0;

            // Decrement "AnyInstanceProtected()" (static thread table)
            if (EpochContext.DecrementThreadIndexEntryCount(context) == 0)
            {
                (threadIndexAligned + EpochContext.GetThreadEntryIndex(context))->contextId = 0;
                EpochContext.InvalidateContext(context);
            }
        }

        /// <summary>
        /// Reserve entry for thread. This method relies on the fact that no
        /// thread will ever have ID 0.
        /// </summary>
        /// <returns>Reserved entry</returns>
        static void ReserveEntry(EpochContext context)
        {
            while (true)
            {
                // Try to acquire entry
                if (0 == (threadIndexAligned + EpochContext.GetStartOffset1(context))->contextId)
                {
                    if (0 == Interlocked.CompareExchange(
                            ref (threadIndexAligned + EpochContext.GetStartOffset1(context))->contextId,
                            EpochContext.GetId(context), 0))
                    {
                        EpochContext.SetThreadEntryIndex(context, EpochContext.GetStartOffset1(context));
                        return;
                    }
                }

                if (EpochContext.GetStartOffset2(context) > 0)
                {
                    // Try alternate entry
                    EpochContext.SetStartOffset1(context, EpochContext.GetStartOffset2(context));
                    EpochContext.SetStartOffset2(context, 0);
                }
                else
                {
                    // Probe next sequential entry
                    EpochContext.SetStartOffset1(context, (ushort) (EpochContext.GetStartOffset1(context) + 1));
                }

                if (EpochContext.GetStartOffset1(context)> kTableSize)
                {
                    EpochContext.SetStartOffset1(context, (ushort)(EpochContext.GetStartOffset1(context) - kTableSize));
                    Thread.Yield();
                }
            }
        }

        // TODO(Tianyu): This is GPT-generated so double check before merging
        static int Murmur3Long(long value)
        {
            ulong h = (ulong)value;

            // Mixing initial bits
            h ^= h >> 33;
            h *= 0xff51afd7ed558ccdUL;
            h ^= h >> 33;
            h *= 0xc4ceb9fe1a85ec53UL;
            h ^= h >> 33;

            // Since we need to return an int, and our hash is 64 bits, we mix down to 32 bits.
            uint high = (uint)(h >> 32);
            uint low = (uint)h;
            uint mixed = high ^ low;

            // Final mix functions similar to the int version
            mixed ^= mixed >> 16;
            mixed *= 0x85ebca6b;
            mixed ^= mixed >> 13;
            mixed *= 0xc2b2ae35;
            mixed ^= mixed >> 16;

            return (int)mixed;
        }

        /// <summary>
        /// Allocate a new entry in epoch table. This is called 
        /// once for a thread.
        /// </summary>
        /// <returns>Reserved entry</returns>
        static void ReserveEntryForThread(EpochContext context)
        {
            if (context == null && EpochContext.threadId == 0)
            {
                EpochContext.threadId = Environment.CurrentManagedThreadId;
                uint code = (uint)Murmur3Long(EpochContext.GetId(context));
                EpochContext.threadLocalStartOffset1 = (ushort)(1 + (code % kTableSize));
                EpochContext.threadLocalStartOffset2 = (ushort)(1 + ((code >> 16) % kTableSize));
            }
            else if (context != null)
            {
                uint code = (uint)Murmur3Long(EpochContext.GetId(context));
                context.startOffset1 = (ushort)(1 + (code % kTableSize));
                context.startOffset2 = (ushort)(1 + ((code >> 16) % kTableSize));
            }

            ReserveEntry(context);
        }

        /// <summary>
        /// Epoch table entry (cache line size).
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = kCacheLineBytes)]
        struct Entry
        {
            /// <summary>
            /// Thread-local value of epoch
            /// </summary>
            [FieldOffset(0)] public long localCurrentEpoch;

            /// <summary>
            /// ID of thread associated with this entry.
            /// </summary>
            [FieldOffset(8)] public long contextId;

            [FieldOffset(16)] public fixed long markers[6];

            public override string ToString() => $"lce = {localCurrentEpoch}, tid = {contextId}";
        }

        struct EpochActionPair
        {
            public long epoch;
            public Action action;

            public override string ToString() =>
                $"epoch = {epoch}, action = {(action is null ? "n/a" : action.Method.ToString())}";
        }
    }
}