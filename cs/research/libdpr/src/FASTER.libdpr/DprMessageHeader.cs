using System;
using System.Runtime.InteropServices;

namespace FASTER.libdpr
{
    /// <summary>
    ///     DPR metadata associated with each batch. Laid out continuously as:
    ///     header | deps (WorkerVersion[]) | versionTracking (long[])
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 36)]
    public unsafe struct DprMessageHeader
    {
        public const int FixedLenSize = 36;
        public const string GprcMetadataKeyName = "DprHeader-bin";
        [FieldOffset(0)] public fixed byte data[FixedLenSize];
        [FieldOffset(0)] public DprWorkerId SrcWorkerId;
        [FieldOffset(8)] public SUId SrcSU;
        // a batch should always consist of messages from the same world-lines on the client side.
        // We can artificially write the servers to not write reply batches with more than one world-line. 
        [FieldOffset(16)] public long WorldLine;
        [FieldOffset(24)] public long Version;
        [FieldOffset(32)] public int NumClientDeps;
        internal int ClientDepsOffset => FixedLenSize;
    }
}