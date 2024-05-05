using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Runtime.CompilerServices;
using FASTER.common;

[assembly:InternalsVisibleTo("TableTests")]
namespace DB { 

/// <summary>
/// Stores data as a byte array (TODO: change to generic type)
/// Variable length pointers are stored as {size_of_variable}{address_of_variable} and have a size of IntPtr.Size * 2
/// Always uses ReadOnlySpan<byte> with the user
/// Assumes schema is never changed after creation 
/// </summary>
public unsafe class Table : IDisposable{
    private int id;
    private long lastId = 0;
    public int rowSize;
    // TODO: bool can be a single bit
    internal long[] metadataOrder;
    internal Dictionary<long, (int, int)> metadata; // (size, offset), size=-1 if varLen
    internal ConcurrentDictionary<PrimaryKey, byte[]> data;

    // Secondary index
    protected ConcurrentDictionary<byte[], PrimaryKey> secondaryIndex;

    protected ILogger logger;
    IWriteAheadLog? wal;

    public Table(int id, (long, int)[] schema, IWriteAheadLog? wal = null, ILogger logger = null){
        this.id = id;
        this.metadata = new Dictionary<long,(int, int)>();
        this.metadataOrder = new long[schema.Length];
        this.wal = wal;
        this.logger = logger;
        this.secondaryIndex = new ConcurrentDictionary<byte[], PrimaryKey>(new ByteArrayComparer());
        
        int offset = 0;
        int size = 0;
        for(int i = 0; i < schema.Length; i++) {
            long attr = schema[i].Item1;
            size = schema[i].Item2;
            if (size <= 0 && size != -1) {
                throw new ArgumentException();
            }
            this.metadata[attr] = (size, offset);
            this.metadataOrder[i] = attr;
            offset += (size == -1) ? IntPtr.Size * 2 : size;
        }
        this.rowSize = offset;
        this.data = new ConcurrentDictionary<PrimaryKey, byte[]>();
    }
    
    // will never return null, empty 
    virtual public ReadOnlySpan<byte> Read(PrimaryKey tupleId, TupleDesc[] tupleDescs, TransactionContext ctx) {
        Validate(tupleDescs, null, false);
        // PrintDebug($"Reading normal {tupleId}", ctx);

        ReadOnlySpan<byte> value = ctx.GetFromReadset(tupleId);
        if (value == null) {
            value = Read(tupleId);
        }

        ReadOnlySpan<byte> result;
        // apply writeset
        (TupleDesc[], byte[]) changes = ctx.GetFromWriteset(tupleId);
        if (changes.Item2 != null) {
            Span<byte> updatedValue = value.ToArray();
            foreach (TupleDesc td in changes.Item1) {
                int offset = this.metadata[td.Attr].Item2;
                changes.Item2.AsSpan(td.Offset, td.Size).CopyTo(updatedValue.Slice(offset));
            }
            result = updatedValue;
        } else {
            result = value;
        }
        // TODO: deal with varLen
        // project out the attributes
        ctx.AddReadSet(tupleId, result);
        return project(result, tupleDescs);
    }

    virtual public (byte[], PrimaryKey) ReadSecondary(byte[] key, TupleDesc[] tupleDescs, TransactionContext ctx){
        if (secondaryIndex == null){
            throw new InvalidOperationException("Secondary index not set");
        }
        PrimaryKey pk = secondaryIndex[key];
        return (Read(pk, tupleDescs, ctx).ToArray(), pk);
    }

    protected ReadOnlySpan<byte> project(ReadOnlySpan<byte> value, TupleDesc[] tupleDescs){
        // TODO: do this without allocating 
        int totalSize = tupleDescs[tupleDescs.Length - 1].Offset + tupleDescs[tupleDescs.Length - 1].Size;

        Span<byte> result = new byte[totalSize];
        foreach (TupleDesc td in tupleDescs){
            int offset = this.metadata[td.Attr].Item2;
            value.Slice(offset, td.Size).CopyTo(result.Slice(td.Offset, td.Size));
        }
        return result;
    }

    // Assumes attribute is valid 
    protected internal ReadOnlySpan<byte> Read(PrimaryKey tupleId){
        if (!this.data.ContainsKey(tupleId)){ // TODO: validate table
            return new byte[this.rowSize];
        }
        // TODO: deal with varLen 
        return this.data[tupleId];
    }

    protected Pointer GetVarLenPtr(PrimaryKey key, int offset){
        byte[] addr = (new ReadOnlySpan<byte>(this.data[key], offset + IntPtr.Size, IntPtr.Size)).ToArray();
        byte[] size = (new ReadOnlySpan<byte>(this.data[key], offset, IntPtr.Size)).ToArray();
        IntPtr res = new IntPtr(BitConverter.ToInt64(addr)); //TODO convert based on nint size
        return new Pointer(res, BitConverter.ToInt32(size));
    }

    /// <summary>
    /// Insert entire row
    /// </summary>
    /// <param name="tupleDescs"></param>
    /// <param name="value"></param>
    /// <param name="ctx"></param>
    /// <returns></returns>
    public PrimaryKey Insert(ReadOnlySpan<byte> value, TransactionContext ctx){
        if (value.Length != this.rowSize){
            throw new ArgumentException($"Expected size {this.rowSize} for new record but instead got size {value.Length}");
        }

        long id = NewRecordId(); // TODO: make sure this new record id falls within range of this partition in shardedBenchmark
        PrimaryKey tupleId = new PrimaryKey(this.id, id);
        ctx.AddWriteSet(ref tupleId, GetSchema(), value);
        if (wal != null){
            wal.Write(ctx.tid, ref tupleId, GetSchema(), value.ToArray());
        }

        return tupleId;
    }

    /// <summary>
    /// Insert specified attributes into table. Non-specified attributes will be 0 
    /// </summary>
    /// <param name="tupleDescs"></param>
    /// <param name="value"></param>
    /// <param name="ctx"></param>
    /// <exception cref="ArgumentException">Key already exists</exception>
    /// <returns>whether insert succeeded</returns>
    public bool Insert(ref PrimaryKey id, ReadOnlySpan<byte> value, TransactionContext ctx){
        if (value.Length != this.rowSize){
            throw new ArgumentException($"Expected size {this.rowSize} for new record but instead got size {value.Length}");
        }
        // PrintDebug($"Inserting {id}", ctx);
        if (this.data.ContainsKey(id)){
            return false;
        }
        if (wal != null){
            wal.Write(ctx.tid, ref id, GetSchema(), value.ToArray());
        }
        ctx.AddWriteSet(ref id, GetSchema(), value);

        return true;
    }

    /// <summary>
    /// Update values described by tupleDescs
    /// </summary>
    /// <param name="tupleId"></param>
    /// <param name="tupleDescs">Describes size and offset of what is in value</param>
    /// <param name="value"></param>
    /// <param name="ctx"></param>
    /// <exception cref="ArgumentException"></exception>
    public void Update(ref PrimaryKey tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, TransactionContext ctx){
        // TODO: how to check it already exists in other shard? 
        // if (!this.data.ContainsKey(tupleId) && !ctx.InWriteSet(ref tupleId)){
        //     throw new ArgumentException($"Key {tupleId} does not exist");
        // }
        Validate(tupleDescs, value, true);

        ctx.AddWriteSet(ref tupleId, tupleDescs, value);
        if (wal != null){
            wal.Write(ctx.tid, ref tupleId, tupleDescs, value.ToArray());
        }
    }

    /// <summary>
    /// Write value to specific attribute of key. If key does not exist yet, this is an insert
    /// /// </summary>
    /// <param name="keyAttr"></param>
    /// <param name="value"></param>
    protected internal void Write(ref PrimaryKey pk, TupleDesc[] tds, byte[] value){
        // TODO: is it safe to assume if key exists in writeset, it is an update?
        // this will receive pk over and over again with tds building 
        if (!this.data.ContainsKey(pk)){
            // insert
            if (value.Length != this.rowSize){
                throw new ArgumentException($"Expected size {this.rowSize} for new record {pk} but instead got size {value.Length}");
            }
            this.data[pk] = value;
        } else {
            // update 
            int start = 0;
            foreach (TupleDesc td in tds){
                (int size, int offset) = this.metadata[td.Attr];
                value.AsSpan(start,td.Size).CopyTo(this.data[pk].AsSpan(offset));
                start += td.Size;
            }
        }
    }

    public void AddSecondaryIndex(Dictionary<byte[], PrimaryKey> index){
        foreach (var entry in index){
            bool success = secondaryIndex.TryAdd(entry.Key, entry.Value);
            if (!success){
                throw new ArgumentException($"Secondary index already has {entry.Key}");
            }
        }
    }

    public void Dispose(){
        // iterate through all of the table to find pointers and dispose of 
        foreach (var field in metadata){
            if (field.Value.Item1 == -1) {
                int offset = field.Value.Item2;
                foreach (var entry in data){
                    IntPtr ptrToFree = GetVarLenPtr(entry.Key, offset).IntPointer;
                    if (ptrToFree != IntPtr.Zero){
                        Marshal.FreeHGlobal(ptrToFree);
                    }
                }
            }
        }
    }

    public TupleDesc[] GetSchema(){
        TupleDesc[] schema = new TupleDesc[this.metadata.Count];
        for (int i = 0; i < this.metadata.Count; i++){
            long attr = this.metadataOrder[i];
            schema[i] = new TupleDesc(attr, this.metadata[attr].Item1, this.metadata[attr].Item2);
        }
        return schema;
    }

    public (int, int) GetAttrMetadata(long attr){
        return this.metadata[attr];
    }

    public int GetId(){
        return this.id;
    }

    virtual public void PrintDebug(string msg, TransactionContext ctx = null){
        if (logger != null) logger.LogInformation($"[Table TID {(ctx != null ? ctx.tid : -1)}]: {msg}");
    }

    public void PrintTable(){
        Console.WriteLine("Metadata: ");
        foreach (var field in metadata){
            Console.Write($"{field.Key} is {field.Value.Item1 == -1} {field.Value.Item1} {field.Value.Item2}\n");
            // for (int i=0; i < entry.Value.Length; i++) {
            //     System.Console.Write(entry.Value[i] + ",");
            // }
            // Console.WriteLine("\n");// + Encoding.ASCII.GetBytes(entry.Value));
        }
        foreach (var entry in data){
            Console.WriteLine(entry.Key);
            for (int i=0; i < entry.Value.Length; i++) {
                System.Console.Write(entry.Value[i] + ",");
            }
            Console.WriteLine("\n");// + Encoding.ASCII.GetBytes(entry.Value));
        }
    }

    protected long NewRecordId() {
        return Interlocked.Increment(ref lastId);
    }

    protected void Validate(TupleDesc[] tupleDescs, ReadOnlySpan<byte> value, bool write) {
        int totalSize = 0;
        foreach (TupleDesc desc in tupleDescs) {
            if (!this.metadata.ContainsKey(desc.Attr)) {
                throw new ArgumentException($"Attribute {desc.Attr} is not a valid attribute for this table");
            }
            if (this.metadata[desc.Attr].Item1 != -1 && desc.Size != this.metadata[desc.Attr].Item1) {
                throw new ArgumentException($"Expected size {this.metadata[desc.Attr].Item1} for attribute {desc.Attr} but instead got size {desc.Size}");
            }
            totalSize += desc.Size;
        }
        if (write && totalSize > value.Length) {
            throw new ArgumentException($"Expected size {totalSize} from tuple description but instead got size {value.Length}");
        }
    }

}

public class ShardedTable : Table {
    private RpcClient rpcClient;
    // extracts relevant values from secondary key to primary key for correct shard
    protected Func<byte[], PrimaryKey> buildTempPk;
    public ShardedTable(int id, (long, int)[] schema, RpcClient rpcClient, IWriteAheadLog? wal = null, ILogger logger = null) : base(id, schema, wal, logger) {
        this.rpcClient = rpcClient;
    }

    public override ReadOnlySpan<byte> Read(PrimaryKey tupleId, TupleDesc[] tupleDescs, TransactionContext ctx) {
        Validate(tupleDescs, null, false);
        // PrintDebug($"Reading {tupleId}", ctx);

        ReadOnlySpan<byte> value = ctx.GetFromReadset(tupleId);
        if (value == null) {
            if (rpcClient.IsLocalKey(tupleId)) {
                value = Read(tupleId);
            } else {
                value = rpcClient.Read(tupleId, ctx);
            }
        }

        ReadOnlySpan<byte> result;
        // apply writeset
        (TupleDesc[], byte[]) changes = ctx.GetFromWriteset(tupleId);
        if (changes.Item2 != null) {
            Span<byte> updatedValue = value.ToArray();
            foreach (TupleDesc td in changes.Item1) {
                int offset = this.metadata[td.Attr].Item2;
                changes.Item2.AsSpan(td.Offset, td.Size).CopyTo(updatedValue.Slice(offset));
            }
            result = updatedValue;
        } else {
            result = value;
        }
        // TODO: deal with varLen
        // project out the attributes
        ctx.AddReadSet(tupleId, result);
        return project(result, tupleDescs);
    }

    override public (byte[], PrimaryKey) ReadSecondary(byte[] key, TupleDesc[] tupleDescs, TransactionContext ctx){
        if (secondaryIndex == null){
            throw new InvalidOperationException("Secondary index not set");
        }
        ReadOnlySpan<byte> value;
        PrimaryKey pk;
        PrimaryKey tempPk = buildTempPk(key);
        if (rpcClient.IsLocalKey(tempPk)){
            pk = secondaryIndex[key];
            value = Read(pk, tupleDescs, ctx).ToArray();
        } else {
            (value, pk) = rpcClient.ReadSecondary(tempPk, key, ctx);
        }

        ReadOnlySpan<byte> result;
        // apply writeset
        (TupleDesc[], byte[]) changes = ctx.GetFromWriteset(pk);
        if (changes.Item2 != null) {
            Span<byte> updatedValue = value.ToArray();
            foreach (TupleDesc td in changes.Item1) {
                int offset = this.metadata[td.Attr].Item2;
                changes.Item2.AsSpan(td.Offset, td.Size).CopyTo(updatedValue.Slice(offset));
            }
            result = updatedValue;
        } else {
            result = value;
        }
        // TODO: deal with varLen
        // project out the attributes
        ctx.AddReadSet(pk, result);
        return (project(result, tupleDescs).ToArray(), pk);
    }

    public void AddSecondaryIndex(Dictionary<byte[], PrimaryKey> index, Func<byte[], PrimaryKey> buildTempPk){
        base.AddSecondaryIndex(index);
        this.buildTempPk = buildTempPk;
    }

    override public void PrintDebug(string msg, TransactionContext ctx = null){
        if (logger != null) logger.LogInformation($"[ST {rpcClient.GetId()} TID {(ctx != null ? ctx.tid : -1)}]: {msg}");
    }
}
}