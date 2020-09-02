﻿using System;
using System.Data;
using System.Runtime.CompilerServices;
using FASTER.core;

namespace FASTER.serverless
{
    // TODO(Tianyu): Cannot support more than a handful of dependencies. Need to change for larger cluster size
    public class LightDependencySet
    {
        public const int MaxSizeBits = 4;
        public const long NoDependency = -1;
        private const int MaxSizeMask = (1 << MaxSizeBits) - 1;

        public readonly long[] DependentVersions;
        private bool maybeNotEmpty;

        public LightDependencySet()
        {
            DependentVersions = new long[1 << MaxSizeBits];
            for (var i = 0; i < DependentVersions.Length; i++)
                DependentVersions[i] = NoDependency;
            maybeNotEmpty = false;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Update(Worker worker, long version)
        {
            maybeNotEmpty = true;
            ref var originalVersion = ref DependentVersions[worker.guid & MaxSizeMask];
            Utility.MonotonicUpdate(ref originalVersion, version, out _);
        }

        public bool MaybeNotEmpty()
        {
            return maybeNotEmpty;
        }
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeClear()
        {
            for (var i = 0; i < DependentVersions.Length; i++)
                DependentVersions[i] = NoDependency;
            maybeNotEmpty = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeRemove(Worker worker, long version)
        {
            ref var originalVersion = ref DependentVersions[worker.guid & MaxSizeBits];
            if (originalVersion <= version) originalVersion = NoDependency;
        }
    }
}