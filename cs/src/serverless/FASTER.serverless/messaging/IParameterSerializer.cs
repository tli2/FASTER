﻿using System;
using System.IO;

namespace FASTER.serverless
{
    public interface IParameterSerializer<Key, Value, Input, Output>
        where Key : new()
        where Value : new()
    {
        IntPtr WriteKey(IntPtr dst, ref Key k);
        IntPtr WriteValue(IntPtr dst, ref Value v);
        IntPtr WriteInput(IntPtr dst, ref Input i);
        IntPtr WriteOutput(IntPtr dst, ref Output o);
        IntPtr ReadKey(IntPtr src, out Key k);
        IntPtr ReadValue(IntPtr src, out Value v);
        IntPtr ReadInput(IntPtr src, out Input i);
        IntPtr ReadOutput(IntPtr src, out Output o);
    }
}