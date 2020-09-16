﻿using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    internal class WritePageAsyncResult : IAsyncResult
    {
        public WritePageAsyncResult(long frameNum, long nextFetch)
        {
            this.frameNum = frameNum;
            this.nextFetch = nextFetch;
        }

        internal long frameNum;
        internal long nextFetch;
        public object AsyncState { get; }
        public WaitHandle AsyncWaitHandle { get; }
        public bool CompletedSynchronously { get; }
        public bool IsCompleted { get; }
    }

    internal class PageStatus
    {
        internal PageStatus(long pageStartAddress)
        {
            this.pageStartAddress = pageStartAddress;
            flushed = new CountdownEvent(1);
        }

        internal long pageStartAddress;
        internal CountdownEvent flushed, loaded;
    }

    internal class FasterLogMetadataIterator<Key, Value> : IDisposable
    {
        // TODO(Tianyu): Hardcoded constant
        private readonly int frameSize = 5;

        // Always ok to use the blittable allocatpr class
        private readonly BlittableAllocator<Key, Value> hlog;
        private readonly long endAddress;

        private BlittableFrame frame;
        private readonly PageStatus[] framePageStatus;

        private long currentPage, currentOffset, currentLogicalAddress, currentPhysicalAddress;
        
        public FasterLogMetadataIterator(AllocatorBase<Key, Value> hlog, long beginAddress, long persistedBoundary)
        {
            // TODO(Tianyu): Hard-wired for blittable at the moment
            this.hlog = (BlittableAllocator<Key, Value>) hlog;

            if (beginAddress == 0)
                beginAddress = hlog.GetFirstValidLogicalAddress(0);

            endAddress = persistedBoundary;
            currentLogicalAddress = beginAddress;

            frame = new BlittableFrame(frameSize, hlog.PageSize, hlog.GetDeviceSectorSize());
            framePageStatus = new PageStatus[frameSize];
            for (var i = 0; i < frameSize; i++)
            {
                // TODO(Tianyu): Technically there is no need to read a page from disk if it still exists in memory, but
                // might as well for simplicity for now
                var pageStartAddress = (hlog.GetPage(currentLogicalAddress) + i) * (1 << hlog.LogPageSizeBits);
                if (pageStartAddress >= endAddress) break;
                LoadPageFromDisk(pageStartAddress);
            }
            
            currentPage = currentLogicalAddress >> hlog.LogPageSizeBits;
            currentOffset = currentLogicalAddress & hlog.PageSizeMask;
        }

        private unsafe void AsyncReadPagesCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);

            var result = (PageAsyncReadResult<Empty>) context;

            if (result.freeBuffer1 != null)
            {
                hlog.PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                result.freeBuffer1.Return();
                result.freeBuffer1 = null;
            }

            result.handle?.Signal();

            Interlocked.MemoryBarrier();
        }

        private unsafe void AsyncFlushPageCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);

            // Set the page status to flushed
            var result = (WritePageAsyncResult) context;

            framePageStatus[result.frameNum].flushed.Signal();
            if (result.nextFetch != -1)
                LoadPageFromDisk(result.nextFetch);
            Interlocked.MemoryBarrier();
        }

        private unsafe void LoadPageFromDisk(long pageStartAddress)
        {
            var frameNumber = (pageStartAddress >> hlog.LogPageSizeBits) % frameSize;
            framePageStatus[frameNumber] = new PageStatus(pageStartAddress);

            hlog.AsyncReadPagesFromDeviceToFrame(pageStartAddress >> hlog.LogPageSizeBits,
                1, endAddress, AsyncReadPagesCallback, Empty.Default,
                frame, out framePageStatus[frameNumber].loaded);
        }


        public ref RecordInfo CurrentRecord()
        { 
            framePageStatus[currentPage % frameSize].loaded.Wait();
            currentPhysicalAddress = frame.GetPhysicalAddress(currentPage % frameSize, currentOffset);
            return ref hlog.GetInfo(currentPhysicalAddress);
        }

        private long NextRecordStart()
        {
            var recordSize = hlog.GetRecordSize(currentPhysicalAddress);
            if (currentOffset + recordSize > hlog.PageSize)
                return (currentPage + 1) << hlog.LogPageSizeBits;
            return currentLogicalAddress + hlog.GetRecordSize(currentPhysicalAddress);
        }

        public bool HasNext()
        {
            return NextRecordStart() < endAddress;
        }

        private unsafe void FlushCurrentPageAndPrefetchNext()
        {
            var nextPageInFrame = (frameSize + currentPage) * hlog.PageSize;
            if (nextPageInFrame > endAddress) nextPageInFrame = -1;
            hlog.device.WriteAsync((IntPtr) frame.pointers[currentPage % frameSize],
                (ulong) (hlog.PageSize * currentPage), (uint) hlog.PageSize, AsyncFlushPageCallback,
                new WritePageAsyncResult(currentPage % frameSize, nextPageInFrame));
        }

        public void Next()
        {
            if (!HasNext() || currentLogicalAddress < hlog.BeginAddress) throw new FasterException();

            // Not supposed to use this for anything not flushed
            if (currentLogicalAddress > hlog.FlushedUntilAddress) throw new FasterException();

            currentLogicalAddress = NextRecordStart();
            if (currentPage != currentLogicalAddress >> hlog.LogPageSizeBits)
                FlushCurrentPageAndPrefetchNext();

            currentPage = currentLogicalAddress >> hlog.LogPageSizeBits;
            currentOffset = currentLogicalAddress & hlog.PageSizeMask;

            for (var status = framePageStatus[currentPage % frameSize];
                status.pageStartAddress != currentPage << hlog.LogPageSizeBits;
                status = framePageStatus[currentPage % frameSize])
                // Wait for previous page to be flushed before starting to wait on the load of this page
                status.flushed.Wait();
            
            framePageStatus[currentPage % frameSize].loaded.Wait();

            currentPhysicalAddress = frame.GetPhysicalAddress(currentPage % frameSize, currentOffset);
        }

        public void Dispose()
        {
            // Only need to issue a flush on the current page, everything else must already be flushing.
            FlushCurrentPageAndPrefetchNext();
            for (var i = 0; i < frameSize; i++)
            {
                if (framePageStatus[i] != null)
                    framePageStatus[i].flushed.Wait();
            }
            frame.Dispose();
        }
    }

    public partial class FasterKV<Key, Value>
    {
        private void RollbackRecordsOnDisk(long start, long end, long startVersion, long endVersion, CountdownEvent countdown = null)
        {
            Console.WriteLine($"Rolling back disk record {start} - {end}");
            var iterator = new FasterLogMetadataIterator<Key, Value>(hlog, start, end);
            for (; iterator.HasNext(); iterator.Next())
            {
                ref var record = ref iterator.CurrentRecord();
                if (record.Version >= startVersion && record.Version < endVersion)
                    iterator.CurrentRecord().Invalid = true;
            }

            countdown?.Signal();
            iterator.Dispose();
        }
        
        private void PruneRolledbackVersionsForPage(long pageStartAddress, long pageEndAddress, long startVersion,
            long endVersion)
        {
            var recordHead = pageStartAddress;
            while (recordHead < pageEndAddress)
            {
                ref var record = ref hlog.GetInfo(recordHead);

                if (record.IsNull())
                {
                    recordHead += RecordInfo.GetLength();
                    continue;
                }

                if (!record.Invalid && record.Version >= startVersion && record.Version < endVersion)
                {
                    record.Invalid = true;
                    // TODO(Tianyu): Should also remove this record from the version chain for faster read access
                    // later on, although not sure how to do that.
                }

                recordHead += hlog.GetRecordSize(recordHead);
            }
        }

        internal void RollbackLogVersions(long startAddress, long untilAddress, long startVersion, long endVersion)
        {
            Console.WriteLine($"Rolling back address range {startAddress} - {untilAddress}, from version {endVersion} => {startVersion}");
            var sw = new Stopwatch();
            sw.Start();
            var diskWriteComplete = new CountdownEvent(1);
            // TODO(Tianyu): If the rolled-back section is large, should be a better idea to bump epoch a couple
            // of times during operation.
            epoch.Resume();

            var startPage = hlog.GetPage(startAddress);
            // These is guaranteed to not change until we are done due to epoch protection
            var inMemoryPageStart = hlog.HeadAddress;
            var outstandingPageStart = hlog.FlushedUntilAddress;
            var mutablePageStart = hlog.ReadOnlyAddress;
            var endPage = hlog.GetPage(untilAddress);
            if (untilAddress > hlog.GetStartLogicalAddress(endPage) && untilAddress > startAddress)
                endPage++;

            // Issue reads and writes to disks, but there is no need to wait for those to complete in
            // the protected region
            Task.Run(() => RollbackRecordsOnDisk(startAddress, outstandingPageStart, startVersion, endVersion, diskWriteComplete));
            
            // Need to change all records in memory in case they are still around after the rollback. This has no
            // impact on correctness though even if it coincides with the write
            for (var page = Math.Max(startPage, hlog.GetPage(inMemoryPageStart)); page < endPage; page++)
            {
                var startLogicalAddress = hlog.GetStartLogicalAddress(page);
                var endLogicalAddress = hlog.GetStartLogicalAddress(page + 1);

                var pageFromAddress = 0L;
                if (startAddress > startLogicalAddress && untilAddress < endLogicalAddress)
                    pageFromAddress = hlog.GetOffsetInPage(startAddress);

                var pageUntilAddress = hlog.GetPageSize();
                if (endLogicalAddress > untilAddress)
                    pageUntilAddress = hlog.GetOffsetInPage(untilAddress);

                PruneRolledbackVersionsForPage(hlog.GetPhysicalAddress(pageFromAddress),
                    hlog.GetPhysicalAddress(pageUntilAddress), startVersion, endVersion);
            }

            epoch.Suspend();
            
            // // Rollback the previous fuzzy region once the content is on disk
            if (outstandingPageStart < mutablePageStart)
            {
                // Wait until the system flushes the pages between previous persisted boundary and previous readonly boundary
                while (hlog.FlushedUntilAddress < mutablePageStart)
                    Thread.Yield();
                RollbackRecordsOnDisk(outstandingPageStart, mutablePageStart, startVersion, endVersion);
            }
            diskWriteComplete.Wait();
            sw.Stop();
            Console.WriteLine($"Rollback code took {sw.ElapsedMilliseconds} ms");
        }
    }
}