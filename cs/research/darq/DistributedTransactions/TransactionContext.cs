using System.Runtime.InteropServices;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DB {
/// <summary>
/// Data structure holding transaction context
/// </summary>
public class TransactionContext {
    // TODO: find better value
    private static int SET_SIZE = 0;
    internal TransactionStatus status;
    internal int startTxnNum;
    internal List<byte[]> Rset; // byte[] is the entire record
    internal List<(TupleDesc[], byte[])> Wset; // byte[] corresponds to the TupleDesc
    internal List<PrimaryKey> WsetKeys;
    internal List<PrimaryKey> RsetKeys;
    public long tid;
    public Dictionary<int, Table> tables;
    public Action<bool> callback;
    public TransactionContext(Dictionary<int, Table> tables){
        this.tables = tables;
    }

    public void Init(int startTxn, long tid){
        this.startTxnNum = startTxn;
        this.tid = tid;
        status = TransactionStatus.Idle;
        Rset = new List<byte[]>(SET_SIZE);
        Wset = new List<(TupleDesc[], byte[])>(SET_SIZE);
        WsetKeys = new List<PrimaryKey>(SET_SIZE);
        RsetKeys = new List<PrimaryKey>(SET_SIZE);
    }

    public bool InReadSet(ref PrimaryKey tupleId){
        return GetReadsetKeyIndex(ref tupleId) != -1;
    }
    public bool InWriteSet(ref PrimaryKey tupleId){
        return GetWriteSetKeyIndex(ref tupleId) != -1;
    }

    public (TupleDesc[], byte[]) GetFromWriteset(PrimaryKey tupleId){
        int index = GetWriteSetKeyIndex(ref tupleId);
        if (index == -1){
            return (null, null);
        }
        return (Wset[index].Item1, Wset[index].Item2);
    }
    public (TupleDesc[], byte[]) GetFromWriteset(int i){
        return (Wset[i].Item1, Wset[i].Item2);
    }

    public ReadOnlySpan<byte> GetFromReadset(PrimaryKey tupleId){
        int index = GetReadsetKeyIndex(ref tupleId);
        if (index == -1){
            return null;
        }
        return Rset[index];
    }

    public void AddReadSet(PrimaryKey tupleId, ReadOnlySpan<byte> val){
        // TODO: varlen
        if (val.Length != tables[tupleId.Table].rowSize){
            throw new ArgumentException($"Readset value length {val.Length} does not match table row size {tables[tupleId.Table].rowSize}");
        }
        Rset.Add(val.ToArray());
        RsetKeys.Add(tupleId);
    }

    public void AddWriteSet(ref PrimaryKey tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> val){
        int index = GetWriteSetKeyIndex(ref tupleId);
        if (index != -1){
            (TupleDesc[], byte[]) existing = Wset[index];
            // List<byte> result = new List<byte>();
            // int start = 0;
            // foreach (TupleDesc td in existing.Item2){
            //     bool included = false;
            //     int newStart = 0;
            //     foreach (TupleDesc newTd in tupleDescs){
            //         if (td.Attr == newTd.Attr){
            //             included = true;

            //             result.AddRange(val.Slice(newStart, td.Size));
            //         }
            //         newStart += newTd.Size;
            //     }
            //     if (!included) finalSize += td.Size;


            //     if (td.Attr == tupleDescs[0].Attr){
            //         result.AddRange(val.ToArray());
            //     } else {
            //         result.AddRange(existing.Item3.AsSpan(start, td.Size).ToArray());
            //     }
            //     start += td.Size;
            // }

            // calculate final size
            int finalSize = existing.Item2.Length;
            foreach (TupleDesc td in tupleDescs){
                bool included = false;
                foreach (TupleDesc existingTd in existing.Item1){
                    if (td.Attr == existingTd.Attr){
                        included = true;
                    }
                }
                if (!included) finalSize += td.Size;
            }

            // copy values, replacing existing values with new ones
            byte[] newVal = new byte[finalSize];
            Span<byte> newValSpan = newVal;
            bool[] includedTd = new bool[tupleDescs.Length];
            foreach (TupleDesc existingTd in existing.Item1){
                bool included = false;
                for (int i = 0; i < tupleDescs.Length; i++){
                    TupleDesc newTd = tupleDescs[i];
                    if (existingTd.Attr == newTd.Attr){
                        included = true;
                        includedTd[i] = true;
                        val.Slice(newTd.Offset, newTd.Size).CopyTo(newValSpan.Slice(existingTd.Offset, newTd.Size));
                        break;
                    }
                }
                if (!included) {
                    existing.Item2.AsSpan(existingTd.Offset, existingTd.Size).CopyTo(newValSpan.Slice(existingTd.Offset, existingTd.Size));
                } 
            }

            // add remaining values, also to tupleDescs
            TupleDesc[] newTupleDescs = new TupleDesc[existing.Item1.Length + includedTd.Count(x => !x)];
            existing.Item1.CopyTo(newTupleDescs, 0);
            int start = existing.Item2.Length;
            int j = existing.Item1.Length;
            for (int i = 0; i < tupleDescs.Length; i++){
                if (!includedTd[i]){
                    val.Slice(tupleDescs[i].Offset, tupleDescs[i].Size).CopyTo(newValSpan.Slice(start, tupleDescs[i].Size));
                    newTupleDescs[j++] = tupleDescs[i];
                    start += tupleDescs[i].Size;
                }
            }

            Wset.Add((newTupleDescs, newVal));
            WsetKeys.Add(tupleId);
        } else {
            Wset.Add((tupleDescs, val.ToArray()));
            WsetKeys.Add(tupleId);
        }
    }

    public List<PrimaryKey> GetReadsetKeys(){
        return RsetKeys;
    }
    public List<PrimaryKey>GetWritesetKeys(){
        return WsetKeys;
    }

    private int GetWriteSetKeyIndex(ref PrimaryKey tupleId){
        var span = CollectionsMarshal.AsSpan(WsetKeys);
        for (int i = span.Length-1; i >= 0; i--){
            ref PrimaryKey pk = ref span[i];
            // TupleDesc[] tupleDescs = span[i].Item1;
            // if (tupleDescs[0].Attr == -1){
            //     return i;
            // }
            // if (pk.Equals(tupleId)){
            //     return i;
            // }
            if (pk.Table == tupleId.Table && pk.Key1 == tupleId.Key1
                && pk.Key2 == tupleId.Key2 && pk.Key3 == tupleId.Key3
                && pk.Key4 == tupleId.Key4 && pk.Key5 == tupleId.Key5
                && pk.Key6 == tupleId.Key6 
            ){
                return i;
            }
            // if (Wset[i].Item1.Equals(tupleId)){
            //     return i;
            // }
        }
        return -1;
    }
    
    private int GetReadsetKeyIndex(ref PrimaryKey tupleId){
        var span = CollectionsMarshal.AsSpan(RsetKeys);
        for (int i = span.Length-1; i >= 0; i--){
            ref PrimaryKey pk = ref span[i];
            if (pk.Table == tupleId.Table && pk.Key1 == tupleId.Key1
                && pk.Key2 == tupleId.Key2 && pk.Key3 == tupleId.Key3
                && pk.Key4 == tupleId.Key4 && pk.Key5 == tupleId.Key5
                && pk.Key6 == tupleId.Key6 
            ){
                return i;
            }
            // if (Rset[i].Item1.Equals(tupleId)){
            //     return i;
            // }
        }
        return -1;
    }

    public override string ToString(){
        return $"Readset: {string.Join(Environment.NewLine, GetReadsetKeys())}\nWriteset: {string.Join(Environment.NewLine, GetWritesetKeys())}";
    }
}

}