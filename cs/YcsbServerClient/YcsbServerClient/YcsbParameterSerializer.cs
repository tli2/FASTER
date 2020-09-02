using System;
using System.Runtime.CompilerServices;
using FASTER.serverless;

namespace FASTER.benchmark
{
    public unsafe class YcsbParameterSerializer : IParameterSerializer<Key, Value, Input, Output>
    {
        public IntPtr WriteKey(IntPtr dst, ref Key k)
        {
            Unsafe.Copy(dst.ToPointer(), ref k);
            return dst + sizeof(Key);
        }

        public IntPtr WriteValue(IntPtr dst, ref Value v)
        {
            Unsafe.Copy(dst.ToPointer(), ref v);
            return dst + sizeof(Value);
        }

        public IntPtr WriteInput(IntPtr dst, ref Input i)
        {
            Unsafe.Copy(dst.ToPointer(), ref i);
            return dst + sizeof(Input);
        }

        public IntPtr WriteOutput(IntPtr dst, ref Output o)
        {
            Unsafe.Copy(dst.ToPointer(), ref o);
            return dst + sizeof(Output);
        }

        public IntPtr ReadKey(IntPtr src, out Key k)
        {
            k = default;
            Unsafe.Copy(ref k, src.ToPointer());
            return src + sizeof(Key);
        }

        public IntPtr ReadValue(IntPtr src, out Value v)
        {
            v = default;
            Unsafe.Copy(ref v, src.ToPointer());
            return src + sizeof(Value);
        }

        public IntPtr ReadInput(IntPtr src, out Input i)
        {
            i = default;
            Unsafe.Copy(ref i, src.ToPointer());
            return src + sizeof(Input);
        }

        public IntPtr ReadOutput(IntPtr src, out Output o)
        {
            o = default;
            Unsafe.Copy(ref o, src.ToPointer());
            return src + sizeof(Output);
        }
    }
}