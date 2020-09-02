using System;
using System.Threading.Tasks;
using FASTER.core;

namespace FASTER.serverless
{
    public enum FasterServerlessReturnCode : byte
    {
        OK,
        NotFound,
        Error,
        NotOwner,
        WorldLineShift
    }
    
    /// <summary>
    /// Different types of messages sent by Faster-Serverless
    /// </summary>
    public enum FasterServerlessMessageType : byte
    {
        // TODO(Tianyu): Add more
        /// <summary>
        /// A request to read some value in a remote Faster instance
        /// </summary>
        ReadRequest,
        /// <summary>
        /// A request to upsert some value in a remote Faster instance
        /// </summary>
        UpsertRequest,
        /// <summary>
        /// A request to rmw on some value in a remote Faster instance
        /// </summary>
        RmwRequest,
        /// <summary>
        /// A request to delete some value in a remote Faster instance
        /// </summary>
        DeleteRequest,
        /// <summary>
        /// A reply from a remote Faster instance containing results from a read
        /// </summary>
        ReadResult,
        /// <summary>
        /// A reply from a remote Faster instance signaling operation completion
        /// </summary>
        RequestComplete,
        
        TransferOwnership,
        OwnershipDropped,
        
        RecoveryStatusCheck,
        RecoveryResult
    }
}