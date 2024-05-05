using System.Runtime.InteropServices;
using FASTER.core;

namespace FASTER.libdpr
{
    /// <summary>
    /// Iterator for scanning a DARQ
    /// </summary>
    public class DarqScanIterator : IDisposable
    {
        private FasterLogScanIterator iterator;
        private long replayEnd;
        private Queue<(long, long, byte[])> recoveryMessages;
        private Dictionary<long, long> replayMessages;
        private bool disposed = false;
        private byte[] reusedReadBuffer;
        private GCHandle? handle = null;
        private bool hideRecoveryMessages;

        internal DarqScanIterator(FasterLog log, long replayEnd, bool speculative, bool hideRecoveryMessages = true)
        {
            iterator = log.Scan(0, long.MaxValue, scanUncommitted: speculative);
            recoveryMessages = new Queue<(long, long, byte[])>();
            replayMessages = new Dictionary<long, long>();
            this.replayEnd = replayEnd;
            this.hideRecoveryMessages = hideRecoveryMessages;
            ScanOnRecovery();
        }

        /// <inheritdoc/>>
        public void Dispose()
        {
            disposed = true;
            iterator.Dispose();
        }

        private unsafe void ScanOnRecovery()
        {
            while (true)
            {
                while (iterator.UnsafeGetNext(out var entry, out var length, out var currentAddress,
                           out var nextAddress))
                {
                    // Should not be inclusive -- replay end is the start address of the last completion record in stepped
                    if (currentAddress > replayEnd)
                    {
                        Console.WriteLine(
                            $"Current addr {currentAddress} is beyond replay end {replayEnd}, finishing processor recovery...");
                        iterator.UnsafeRelease();
                        break;
                    }

                    switch (*(DarqMessageType *) entry)
                    {
                        case DarqMessageType.OUT:
                            break;
                        case DarqMessageType.RECOVERY:
                            recoveryMessages.Enqueue((currentAddress, nextAddress,
                                new Span<byte>(entry, length).ToArray()));
                            break;
                        case DarqMessageType.IN:
                            replayMessages.Add(currentAddress, length);
                            break;
                        case DarqMessageType.COMPLETION:
                            var completed = (long*)(entry + sizeof(DarqMessageType));
                            while (completed < entry + length)
                            {
                                var completedLsn = *completed++;
                                replayMessages.Remove(completedLsn);
                            }
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                    
                    iterator.UnsafeRelease();
                }

                if (iterator.NextAddress >= replayEnd) break;
                iterator.WaitAsync().AsTask().GetAwaiter().GetResult();
            }
            iterator.Reset();
        }

        /// <summary>
        /// Scan the next entry in DARQ. If successful, must be followed by a UnsafeRelease call to release any
        /// resources held in-place for unsafe consumption.
        /// </summary>
        /// <param name="entry"> pointer to the start of next entry body</param>
        /// <param name="entryLength">length of the next entry</param>
        /// <param name="currentAddress">address of the entry on DARQ (lsn)</param>
        /// <param name="nextAddress"> lower bound of the address of the next entry on DARQ</param>
        /// <param name="type"> type of entry </param>
        /// <returns>whether a next entry is available at this moment</returns>
        public unsafe bool UnsafeGetNext(out byte* entry, out int entryLength, out long currentAddress,
            out long nextAddress, out DarqMessageType type)
        {
            if (handle.HasValue)
                throw new FasterException("Trying to get next without release previous");
            type = default;
            
            // Try to replay state messages first
            if (recoveryMessages.Count != 0)
            {
                while (recoveryMessages.TryDequeue(out var m))
                {
                    currentAddress = m.Item1;
                    nextAddress = m.Item2;
                    handle = GCHandle.Alloc(m.Item3, GCHandleType.Pinned);
                    type = DarqMessageType.RECOVERY;
                    entry = (byte*)handle.Value.AddrOfPinnedObject();
                    entryLength = m.Item3.Length;
                    return true;
                }
            }

            while (true)
            {
                if (!iterator.UnsafeGetNext(out entry, out entryLength, out currentAddress, out nextAddress))
                    return false;
                
                type = (DarqMessageType) (*entry);
                switch (type)
                {
                    case DarqMessageType.IN:
                        if (currentAddress > replayEnd) break;
                        // If still replaying messages, only allow messages that should be replayed to go through
                        if (replayMessages.Remove(currentAddress)) break;
                        // Otherwise, skip this message because there is a later completion message
                        iterator.UnsafeRelease();
                        continue;
                    case DarqMessageType.OUT:
                    case DarqMessageType.COMPLETION:
                        break;
                    // Should be seen by DARQ consumer only if requested and not replayed
                    case DarqMessageType.RECOVERY:
                        if (!hideRecoveryMessages && currentAddress > replayEnd) break;
                        iterator.UnsafeRelease();
                        continue;
                    default:
                        throw new FasterException("Unexpected entry type");
                }

                // Skip header byte
                entry += sizeof(byte);
                entryLength -= 1;
                return true;
            }
        }

        /// <summary>
        /// Releases resources held from a previous successful UnsafeGetNext call 
        /// </summary>
        public void UnsafeRelease()
        {
            if (handle.HasValue)
            {
                handle.Value.Free();
                handle = null;
            }
            else
                iterator.UnsafeRelease();
        }

        /// <summary>
        /// Wait until the next entry is available or when no more entries will be available
        /// </summary>
        /// <param name="token">cancellation token</param>
        /// <returns> task for the availability of next entry </returns>
        public ValueTask<bool> WaitAsync(CancellationToken token = default)
        {
            return iterator.WaitAsync(token);
        }
    }
}