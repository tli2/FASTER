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
        [FieldOffset(0)] internal fixed byte data[FixedLenSize];
        [FieldOffset(0)] internal DprWorkerId SrcWorkerId;
        [FieldOffset(8)] internal SUId SrcSU;
        // a batch should always consist of messages from the same world-lines on the client side.
        // We can artificially write the servers to not write reply batches with more than one world-line. 
        [FieldOffset(16)] internal long WorldLine;
        [FieldOffset(24)] internal long Version;
        [FieldOffset(32)] internal int NumClientDeps;
        internal int ClientDepsOffset => FixedLenSize;
    }
}