using System;

namespace FASTER.libdpr
{
    /// <summary>
    ///     A worker in the system manipulates uniquely exactly one state object.
    /// </summary>
    public struct DprWorkerId : IEquatable<DprWorkerId>
    {
        public static readonly DprWorkerId INVALID = new DprWorkerId(-1);

        /// <summary>
        ///  globally-unique worker ID within a DPR cluster
        /// </summary>
        public readonly long guid;

        /// <summary>
        ///     Constructs a worker with the given guid
        /// </summary>
        /// <param name="guid"> worker guid </param>
        public DprWorkerId(long guid)
        {
            this.guid = guid;
        }

        public readonly bool Equals(DprWorkerId other)
        {
            return guid == other.guid;
        }

        public static bool operator ==(DprWorkerId left, DprWorkerId right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(DprWorkerId left, DprWorkerId right)
        {
            return !left.Equals(right);
        }

        /// <inheritdoc cref="object" />
        public override bool Equals(object obj)
        {
            return obj is DprWorkerId other && Equals(other);
        }

        /// <inheritdoc cref="object" />
        public override int GetHashCode()
        {
            return guid.GetHashCode();
        }
    }

    /// <summary>
    ///     A worker-version is a tuple of worker and checkpoint version.
    /// </summary>
    public struct WorkerVersion : IEquatable<WorkerVersion>
    {
        /// <summary>
        ///     Worker
        /// </summary>
        public DprWorkerId DprWorkerId { get; set; }
        
        /// <summary>
        ///     Version
        /// </summary>
        public long Version { get; set; }

        /// <summary>
        ///     Constructs a new worker version object with given parameters
        /// </summary>
        /// <param name="dprWorkerId">worker</param>
        /// <param name="version">version</param>
        public WorkerVersion(DprWorkerId dprWorkerId, long version)
        {
            DprWorkerId = dprWorkerId;
            Version = version;
        }

        internal WorkerVersion(long worker, long version) : this(new DprWorkerId(worker), version)
        {
        }

        public static bool operator ==(WorkerVersion left, WorkerVersion right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(WorkerVersion left, WorkerVersion right)
        {
            return !left.Equals(right);
        }

        public bool Equals(WorkerVersion other)
        {
            return DprWorkerId.Equals(other.DprWorkerId) && Version == other.Version;
        }

        /// <inheritdoc cref="object" />
        public override bool Equals(object obj)
        {
            return obj is WorkerVersion other && Equals(other);
        }

        /// <inheritdoc cref="object" />
        public override int GetHashCode()
        {
            unchecked
            {
                return (DprWorkerId.GetHashCode() * 397) ^ Version.GetHashCode();
            }
        }
    }
    
    /// <summary>
    ///     Speculation Unit ID
    /// </summary>
    public struct SUId : IEquatable<SUId>
    {
        /// <summary>
        /// The EXTERNAL SU is a special SU that can be used in either messages or workers. EXTERNAL messages are always
        /// consumed only after commit. EXTERNAL workers wait until committed to consume any message.
        /// </summary>
        public static readonly SUId EXTERNAL = new SUId(-1);

        /// <summary>
        ///  globally-unique worker ID within a DPR cluster
        /// </summary>
        public readonly long guid;

        /// <summary>
        ///     Constructs a worker with the given guid
        /// </summary>
        /// <param name="guid"> worker guid </param>
        public SUId(long guid)
        {
            this.guid = guid;
        }

        public bool Equals(SUId other)
        {
            return guid == other.guid;
        }

        public static bool operator ==(SUId left, SUId right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(SUId left, SUId right)
        {
            return !left.Equals(right);
        }

        /// <inheritdoc cref="object" />
        public override bool Equals(object obj)
        {
            return obj is SUId other && Equals(other);
        }

        /// <inheritdoc cref="object" />
        public override int GetHashCode()
        {
            return guid.GetHashCode();
        }
    }
}
    
