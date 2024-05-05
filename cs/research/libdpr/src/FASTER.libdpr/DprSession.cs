using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.libdpr
{
    public class DprSessionRolledBackException : Exception
    {
        public readonly long NewWorldLine;

        public DprSessionRolledBackException(long newWorldLine)
        {
            NewWorldLine = newWorldLine;
        }
    }

    /// <summary>
    /// A DprSession is a DPR entity that cannot commit/restore state, but communicates with other DPR entities and may
    /// convey DPR dependencies (e.g., a client session).
    /// </summary>
    public class DprSession
    {
        internal long version, worldLine;
        internal LightDependencySet deps;

        /// <summary>
        /// WorldLine of the session
        /// </summary>
        public long WorldLine => worldLine >= 0 ? worldLine : -worldLine;

        public bool RolledBack => worldLine < 0;

        /// <summary>
        /// Create a DPR session working on the supplied worldLine (or 1 by default, in a cluster that has never failed)
        /// </summary>
        /// <param name="initialWorldLine"> the worldLine to start at, or 0 (wildcard that matches to the first received message's worldline) by default </param>
        public DprSession(long initialWorldLine = 0)
        {
            version = 1;
            // 0 denotes that the session does not yet exist in a worldline
            worldLine = initialWorldLine;
            deps = new LightDependencySet();
        }


        internal void UnsafeReset(long initialWorldLine = 0)
        {
            version = 1;
            // 0 denotes that the session does not yet exist in a worldline
            worldLine = initialWorldLine;
            deps.UnsafeClear();
        }


        internal void UnsafeReset(StateObject to)
        {
            version = to.Version();
            worldLine = to.WorldLine();
            deps.UnsafeClear();
            deps.Update(to.Me(), version);
        }

        /// <summary>
        /// Obtain a DPR header that encodes session dependency for an outgoing message
        /// </summary>
        /// <param name="headerBytes"> byte array to write header into </param>
        /// <returns> size of the header, or negative of the required size to fit if supplied header is to small </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int TagMessage(Span<byte> headerBytes)
        {
            if (RolledBack)
                throw new DprSessionRolledBackException(WorldLine);

            fixed (byte* b = headerBytes)
            {
                var bend = b + headerBytes.Length;
                ref var dprHeader = ref Unsafe.AsRef<DprMessageHeader>(b);

                // Populate header with relevant request information
                if (headerBytes.Length >= DprMessageHeader.FixedLenSize)
                {
                    dprHeader.SrcWorkerId = DprWorkerId.INVALID;
                    dprHeader.WorldLine = worldLine;
                    dprHeader.Version = version;
                    dprHeader.NumClientDeps = 0;
                }

                // Populate tracking information into the batch
                var copyHead = b + dprHeader.ClientDepsOffset;
                foreach (var wv in deps)
                {
                    dprHeader.NumClientDeps++;
                    // only copy if it fits
                    if (copyHead < bend - sizeof(WorkerVersion))
                        Unsafe.AsRef<WorkerVersion>(copyHead) = wv;
                    copyHead += sizeof(WorkerVersion);
                }

                // Invert depends on whether or not we fit
                return (int)(copyHead <= bend ? copyHead - b : b - copyHead);
            }
        }

        /// <summary>
        /// Receive a message with the given header in this session. 
        /// </summary>
        /// <param name="dprMessage"> DPR header of the message to receive </param>
        /// <param name="version"> version of the message </param>
        /// <returns> status of the batch. If status is ROLLBACK, this session must be rolled-back </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool Receive(ReadOnlySpan<byte> dprMessage)
        {
            if (RolledBack)
                throw new DprSessionRolledBackException(WorldLine);

            fixed (byte* h = dprMessage)
            {
                ref var responseHeader = ref Unsafe.AsRef<DprMessageHeader>(h);
                if (worldLine == 0)
                    Interlocked.CompareExchange(ref worldLine, responseHeader.WorldLine, 0);

                var wl = worldLine;
                if (responseHeader.WorldLine > wl)
                {
                    Interlocked.CompareExchange(ref worldLine, -responseHeader.WorldLine, wl);
                    throw new DprSessionRolledBackException(WorldLine);
                }

                if (responseHeader.WorldLine < worldLine)
                    return false;

                Debug.Assert(responseHeader.WorldLine == worldLine);

                // Add largest worker-version as dependency for future ops
                if (!responseHeader.SrcWorkerId.Equals(DprWorkerId.INVALID))
                    deps.Update(responseHeader.SrcWorkerId, responseHeader.Version);
                else
                {
                    fixed (byte* d = responseHeader.data)
                    {
                        var depsHead = d + responseHeader.ClientDepsOffset;
                        for (var i = 0; i < responseHeader.NumClientDeps; i++)
                        {
                            ref var wv = ref Unsafe.AsRef<WorkerVersion>(depsHead);
                            deps.Update(wv.DprWorkerId, wv.Version);
                            depsHead += sizeof(WorkerVersion);
                        }
                    }
                }

                // Update versioning information
                core.Utility.MonotonicUpdate(ref this.version, responseHeader.Version, out _);
            }

            return true;
        }

        public bool DependOn(StateObject so)
        {
            var version = so.Version();
            var wl = so.WorldLine();
            if (worldLine == 0)
                Interlocked.CompareExchange(ref worldLine, wl, 0);
            if (worldLine < wl) throw new DprSessionRolledBackException(wl);
            if (worldLine > wl) return false;
            deps.Update(so.Me(), version);
            return true;
        }
        

        // TODO(Tianyu): Need to find a way for long-running sessions to prune its dependencies
        // Not safe to invoke concurrently with other methods on this session
        public async Task SpeculationBarrier(IDprFinder dprFinder, bool autoRefresh = false)
        {
            while (true)
            {
                if (autoRefresh)
                    dprFinder.RefreshStateless();
                if (worldLine != 0 && dprFinder.SystemWorldLine() != worldLine)
                {
                    worldLine = -dprFinder.SystemWorldLine();
                    throw new DprSessionRolledBackException(WorldLine);
                }

                if (deps.All(wv => dprFinder.SafeVersion(wv.DprWorkerId) >= wv.Version))
                {
                    deps.UnsafeClear();
                    return;
                }

                // TODO(Tianyu): Fix busy wait
                await Task.Yield();
            }
        }
    }
}