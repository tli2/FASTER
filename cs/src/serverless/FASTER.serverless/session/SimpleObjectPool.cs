﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.serverless
{
    internal class LightConcurrentStack<T> where T : class
    {
        private T[] stack;
        private int tail;
        // Not expecting a lot of concurrency on the stack. Should be pretty cheap.
        private SpinLock latch;

        public LightConcurrentStack(int maxCapacity = 128)
        {
            stack = new T[maxCapacity];
            tail = 0;
            latch = new SpinLock();
        }
        
        public bool TryPush(T elem)
        {
            var lockTaken = false;
            latch.Enter(ref lockTaken);
            Debug.Assert(lockTaken);
            if (tail == stack.Length)
            {
                latch.Exit();
                return false;
            }
            stack[tail++] = elem;
            latch.Exit();
            return true;
        }

        public bool TryPop(out T elem)
        {
            elem = null;
            var lockTaken = false;
            latch.Enter(ref lockTaken);
            Debug.Assert(lockTaken);
            if (tail == 0)
            {
                latch.Exit();
                return false;
            }

            elem = stack[--tail];
            latch.Exit();
            return true;
        }
    }
    
    
    public struct ReusableObject<T> : IDisposable where T : class
    {
        public static ReusableObject<T> INVALID = new ReusableObject<T>(null, null); 
        private LightConcurrentStack<T> pool;
        public T obj;

        internal ReusableObject(T obj, LightConcurrentStack<T> pool)
        {
            this.pool = pool;
            this.obj = obj;
        }

        public void Dispose()
        {
            pool?.TryPush(obj);
        }
        
        public bool Equals(ReusableObject<T> other)
        {
            return EqualityComparer<T>.Default.Equals(obj, other.obj);
        }

        public override bool Equals(object obj)
        {
            return obj is ReusableObject<T> other && Equals(other);
        }

        public override int GetHashCode()
        {
            return EqualityComparer<T>.Default.GetHashCode(obj);
        }
    }
    
    public class SimpleObjectPool<T> : IDisposable where T : class
    {
        private Func<T> factory;
        private Action<T> destructor;
        private ThreadLocal<LightConcurrentStack<T>> objects;
        private ThreadLocal<int> allocatedObjects = new ThreadLocal<int>(() => 0);
        private int maxObjectPerThread;

        public SimpleObjectPool(Func<T> factory, Action<T> destructor = null, int maxObjectPerThread = 128)
        {
            this.factory = factory;
            this.destructor = destructor;
            this.maxObjectPerThread = maxObjectPerThread;
            objects = new ThreadLocal<LightConcurrentStack<T>>(() =>
            {
                return new LightConcurrentStack<T>();
            }, true);
        }

        public void Dispose()
        {
            if (destructor == null) return;
            foreach (var stack in objects.Values)
            {
                while (stack.TryPop(out var elem))
                    destructor.Invoke(elem);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReusableObject<T> Checkout()
        {
            if (!objects.Value.TryPop(out var obj))
            {
                if (allocatedObjects.Value < maxObjectPerThread)
                {
                    allocatedObjects.Value++;
                    return new ReusableObject<T>(factory(), objects.Value);
                }
                // Overflow objects are simply discarded after use
                return new ReusableObject<T>(factory(), null);
            }
            return new ReusableObject<T>(obj, objects.Value);
        }
    }
}