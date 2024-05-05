// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging.Abstractions;

namespace FASTER.core
{
    /// <summary>
    /// Epoch Protected Version Scheme
    /// </summary>
    public class EpochProtectedVersionScheme : VersionSchemeBase
    {
        private LightEpoch epoch;
        
        /// <summary>
        /// Construct a new EPVS backed by the given epoch framework. Multiple EPVS instances can share an underlying
        /// epoch framework (WARNING: re-entrance is not yet supported, so nested protection of these shared instances
        /// likely leads to error)
        /// </summary>
        /// <param name="epoch"> The backing epoch protection framework </param>
        public EpochProtectedVersionScheme(LightEpoch epoch)
        {
            this.epoch = epoch;
        }
        
        /// <summary> Get the underlying epoch framework. For advanced use cases only </summary>
        /// <returns> the underlying epoch framework </returns>
        public LightEpoch GetUnderlyingEpoch() => epoch;
        
        public override void SignalStepAvailable(LightEpoch.EpochContext context = null)
        {
            TryStepStateMachine(null, context);
        }

        
        /// <summary>
        /// Enter protection on the current thread. During protection, no version transition is possible. For the system
        /// to make progress, protection must be later relinquished on the same thread using Leave() or Refresh()
        /// </summary>
        /// <returns> the state of the EPVS as of protection, which extends until the end of protection </returns>
        public override VersionSchemeState Enter(LightEpoch.EpochContext context = null)
        {
            epoch.Resume(context);
            TryStepStateMachine(null, context);

            VersionSchemeState result;
            while (true)
            {
                result = state;
                if (!result.IsIntermediate()) break;
                epoch.Suspend(context);
                Thread.Yield();
                epoch.Resume(context);
            }

            return result;
        }

        /// <summary>
        /// Refreshes protection --- equivalent to dropping and immediately reacquiring protection, but more performant.
        /// </summary>
        /// <returns> the state of the EPVS as of protection, which extends until the end of protection</returns>
        public override VersionSchemeState Refresh(LightEpoch.EpochContext context = null)
        {
            epoch.ProtectAndDrain(context);
            VersionSchemeState result = default;
            TryStepStateMachine(null, context);

            while (true)
            {
                result = state;
                if (!result.IsIntermediate()) break;
                epoch.Suspend(context);
                Thread.Yield();
                epoch.Resume(context);
            }
            return result;
        }

        /// <summary>
        /// Drop protection of the current thread
        /// </summary>
        public override void Leave(LightEpoch.EpochContext context = null)
        {
            epoch.Suspend(context);
        }
        
        // Atomic transition from expectedState -> nextState
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool MakeTransition(VersionSchemeState expectedState, VersionSchemeState nextState)
        {
            if (Interlocked.CompareExchange(ref state.Word, nextState.Word, expectedState.Word) != expectedState.Word) 
                return false;
            Debug.WriteLine("Moved to {0}, {1}", nextState.Phase, nextState.Version);
            return true;
        }

        protected override void TryStepStateMachine(VersionSchemeStateMachine expectedMachine, LightEpoch.EpochContext context)
        {
            var machineLocal = currentMachine;
            var oldState = state;

            // Nothing to step
            if (machineLocal == null) return;

            // Should exit to avoid stepping infinitely (until stack overflow)
            if (expectedMachine != null && machineLocal != expectedMachine) return;

            // Still computing actual to version
            if (machineLocal.actualToVersion == -1) return;

            // Machine finished, but not reset yet. Should reset and avoid starting another cycle
            if (oldState.Phase == VersionSchemeState.REST && oldState.Version == machineLocal.actualToVersion)
            {
                Interlocked.CompareExchange(ref currentMachine, null, machineLocal);
                return;
            }

            // Step is in progress or no step is available
            if (oldState.IsIntermediate() || !machineLocal.GetNextStep(oldState, out var nextState)) return;

            var intermediate = VersionSchemeState.MakeIntermediate(oldState);
            if (!MakeTransition(oldState, intermediate)) return;

            // Avoid upfront memory allocation by going to a function
            StepMachineHeavy(machineLocal, oldState, nextState, context);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void StepMachineHeavy(VersionSchemeStateMachine machineLocal, VersionSchemeState old, VersionSchemeState next, LightEpoch.EpochContext context)
        {
            // // Resume epoch to ensure that state machine is able to make progress
            // if this thread is the only active thread. Also, StepMachineHeavy calls BumpCurrentEpoch, which requires a protected thread.
            bool isProtected = epoch.ThisInstanceProtected(context);
            if (!isProtected)
                epoch.Resume(context);
            try
            {
                epoch.BumpCurrentEpoch(() =>
                {
                    machineLocal.OnEnteringState(old, next);
                    var success = MakeTransition(VersionSchemeState.MakeIntermediate(old), next);
                    machineLocal.AfterEnteringState(next);
                    Debug.Assert(success);
                    TryStepStateMachine(machineLocal, context);
                }, context);
            }
            finally
            {
                if (!isProtected)
                    epoch.Suspend(context);
            }
        }
    }
}