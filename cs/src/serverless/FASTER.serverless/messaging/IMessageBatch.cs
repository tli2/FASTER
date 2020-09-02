﻿using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace FASTER.serverless
{
    public interface IMessageBatch<Key, Value, Input, Output>
        where Key : new()
        where Value : new()

    {
        ref BatchHeader GetHeader();
        int WriteTo(byte[] buffer, int offset, IParameterSerializer<Key, Value, Input, Output> serializer);
    }
}