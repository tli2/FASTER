using System;
using System.Drawing;
using System.Runtime.InteropServices;
using SharpNeat.Utility;


namespace DB {

    public enum TransactionStatus {
        Idle,
        Pending,
        Validated,
        Committed,
        Aborted
    }

    public enum OperationType {
        Read,
        Insert,
        Update
    }

    public unsafe struct Pointer {
        public Pointer(IntPtr ptr, int size){
            Size = size;
            IntPointer = ptr;
            Ptr = ptr.ToPointer();
        }
        public Pointer(void* ptr, int size){
            Size = size;
            IntPointer = new IntPtr(ptr);
            Ptr = ptr;
        }
        public int Size;
        public IntPtr IntPointer;
        public void* Ptr;
    }

    // public struct Operation {
    //     public Operation(OperationType type, TupleId tupleId, TupleDesc[] tupleDescs, ReadOnlySpan<byte> val){
    //         if (type != OperationType.Read && Util.IsEmpty(val)) {
    //             throw new ArgumentException("Writes must provide a non-null value");
    //         }
    //         Type = type;
    //         TupleID = tupleId;
    //         TupleDescs = tupleDescs;
    //         Value = val.ToArray();
    //     }
    //     public OperationType Type;
    //     public TupleId TupleID;
    //     public TupleDesc[] TupleDescs;
    //     public byte[] Value;


    //     public byte[] ToBytes(){
    //         using (MemoryStream m = new MemoryStream()) {
    //             using (BinaryWriter writer = new BinaryWriter(m)) {
    //                 writer.Write(BitConverter.GetBytes((int)Type));
    //                 writer.Write(TupleID.Key);
    //                 writer.Write(TupleID.TableHash);
    //                 writer.Write(TupleDescs.Count());
    //                 foreach (TupleDesc desc in TupleDescs){
    //                     writer.Write(desc.Attr);
    //                     writer.Write(desc.Size);
    //                 }
    //                 writer.Write(Value);
    //             }
    //             return m.ToArray();
    //         }
    //     }

    //     public static Operation FromBytes(byte[] data) {
    //         Operation op = new Operation();
    //         using (MemoryStream m = new MemoryStream(data)) {
    //             using (BinaryReader reader = new BinaryReader(m)) {
    //                 op.Type = (OperationType)reader.ReadInt32();

    //                 long key = reader.ReadInt64();
    //                 int tableHash = reader.ReadInt32();
    //                 op.TupleID = new TupleId(key, tableHash);

    //                 int tupleDescLength = reader.ReadInt32();
    //                 TupleDesc[] descs = new TupleDesc[tupleDescLength];
    //                 for (int i = 0; i < tupleDescLength; i++){
    //                     TupleDesc desc = new TupleDesc();
    //                     desc.Attr = reader.ReadInt64();
    //                     desc.Size = reader.ReadInt32();
    //                     descs[i] = desc;
    //                 }
    //             }
    //         }
    //         return op;
    //     }

    // }

    public readonly struct PrimaryKey{ //} : IEquatable<TupleId>{
        public readonly long Key1 = 0;
        public readonly long Key2 = 0;
        public readonly long Key3 = 0;
        public readonly long Key4 = 0;
        public readonly long Key5 = 0;
        public readonly long Key6 = 0;
        public readonly int Table;

        // public PrimaryKey (int t, params long[] keys){
        //     Table = t;
        //     for (int i = 0; i < keys.Length; i++){
        //         switch (i){
        //             case 0:
        //                 Key1 = keys[i];
        //                 break;
        //             case 1:
        //                 Key2 = keys[i];
        //                 break;
        //             case 2:
        //                 Key3 = keys[i];
        //                 break;
        //             case 3:
        //                 Key4 = keys[i];
        //                 break;
        //             case 4:
        //                 Key5 = keys[i];
        //                 break;
        //             case 5:
        //                 Key6 = keys[i];
        //                 break;
        //             default:
        //                 throw new ArgumentException("Too many keys");
        //         }
        //     }
        // }
        public PrimaryKey(int t, long k1, long k2, long k3, long k4, long k5, long k6){
            Table = t;
            Key1 = k1;
            Key2 = k2;
            Key3 = k3;
            Key4 = k4;
            Key5 = k5;
            Key6 = k6;
        }

        public PrimaryKey(int t, long k1, long k2, long k3, long k4, long k5){
            Table = t;
            Key1 = k1;
            Key2 = k2;
            Key3 = k3;
            Key4 = k4;
            Key5 = k5;
        }

        public PrimaryKey(int t, long k1, long k2, long k3, long k4){
            Table = t;
            Key1 = k1;
            Key2 = k2;
            Key3 = k3;
            Key4 = k4;
        }

        public PrimaryKey(int t, long k1, long k2, long k3){
            Table = t;
            Key1 = k1;
            Key2 = k2;
            Key3 = k3;
        }

        public PrimaryKey(int t, long k1, long k2){
            Table = t;
            Key1 = k1;
            Key2 = k2;
        }

        public PrimaryKey(int t, long k1){
            Table = t;
            Key1 = k1;
        }

        public override bool Equals(object o){
            if (o == null || GetType() != o.GetType()){
                return false;
            }
            PrimaryKey other = (PrimaryKey)o;
            if (Table != other.Table) return false;
            if (Key1 != other.Key1) return false;
            if (Key2 != other.Key2) return false;
            if (Key3 != other.Key3) return false;
            if (Key4 != other.Key4) return false;
            if (Key5 != other.Key5) return false;
            if (Key6 != other.Key6) return false;
            return true;
        }

        public override int GetHashCode(){
            int hash = 17;
            hash = hash * 31 + Key1.GetHashCode();
            hash = hash * 31 + Key2.GetHashCode();
            hash = hash * 31 + Key3.GetHashCode();
            hash = hash * 31 + Key4.GetHashCode();
            hash = hash * 31 + Key5.GetHashCode();
            hash = hash * 31 + Key6.GetHashCode();
            return hash * 31 + Table;
        }

        public static int SizeOf = sizeof(long) * 6 + sizeof(int);

        // public override bool Equals(object o){
        //     if (o == null || GetType() != o.GetType()){
        //         return false;
        //     }
        //     PrimaryKey other = (PrimaryKey)o;
        //     if (Table != other.Table) return false;
        //     if (Key1 != other.Key1) return false;
        //     if (Key2 != other.Key2) return false;
        //     if (Key3 != other.Key3) return false;
        //     if (Key4 != other.Key4) return false;
        //     if (Key5 != other.Key5) return false;
        //     if (Key6 != other.Key6) return false;
        //     return true;
        // }

        // public override int GetHashCode(){
        //     int hash = 17;
        //     foreach (long l in Keys){
        //         hash = hash * 31 + l.GetHashCode();
        //     }
        //     return hash * 31 + Table;
        // }

        public override string ToString(){
            return $"PK ({Key1}, {Key2}, {Key3}, {Key4}, {Key5}, {Key6}) Table {Table}";
        }

        public unsafe byte[] ToBytes(){
            byte[] arr = new byte[SizeOf];
            
            fixed (byte* b = arr) {
                var head = b;
                *(long*)head = Key1;
                head += sizeof(long);
                *(long*)head = Key2;
                head += sizeof(long);
                *(long*)head = Key3;
                head += sizeof(long);
                *(long*)head = Key4;
                head += sizeof(long);
                *(long*)head = Key5;
                head += sizeof(long);
                *(long*)head = Key6;
                head += sizeof(long);
                *(int*)head = Table;
            }
            return arr;
        }

        public static unsafe PrimaryKey FromBytes(byte[] data){
            PrimaryKey result;
            fixed (byte* b = data) {
                var head = b;
                long k1 = *(long*)head;
                head += sizeof(long);
                long k2 = *(long*)head;
                head += sizeof(long);
                long k3 = *(long*)head;
                head += sizeof(long);
                long k4 = *(long*)head;
                head += sizeof(long);
                long k5 = *(long*)head;
                head += sizeof(long);
                long k6 = *(long*)head;
                head += sizeof(long);
                int table = *(int*)head;
                result = new PrimaryKey(table, k1, k2, k3, k4, k5, k6);
                // long[] keys = new long[6];
                // for (int i = 0; i < keys.Length; i++){
                //     keys[i] = *(long*)head;
                //     head += sizeof(long);
                // }
                // result = new PrimaryKey(*(int*)head, keys);
            }
            return result;
        }

        // public bool Equals(TupleId o){
        //     return Key == o.Key && Attr == o.Attr && Table == o.Table;
        // }

        // public override bool Equals([NotNullWhen(true)] object o)
        // {
        //     if (o == null || GetType() != o.GetType())
        //     {
        //         return false;
        //     }
        //     return Equals((TupleId)o);
        // }

        // public override int GetHashCode(){
        //     return (int)Key + (int)Attr + Table.GetHashCode();
        // }
        
    }

    public struct TupleDesc {
        public TupleDesc(long attr, int size, int offset){
            Attr = attr;
            Size = size;
            Offset = offset;
        }
        public long Attr;
        public int Size;
        public int Offset;
        public static int SizeOf = sizeof(long) + sizeof(int) + sizeof(int);

        public override string ToString(){
            return $"(Attr:{Attr}, Size:{Size}, Offset:{Offset})";
        }

        public override bool Equals(object o){
            if (o == null || GetType() != o.GetType()){
                return false;
            }
            TupleDesc other = (TupleDesc)o;
            return Attr == other.Attr && Size == other.Size && Offset == other.Offset;
        }

        public override int GetHashCode(){
            return Attr.GetHashCode() + Size.GetHashCode() + Offset.GetHashCode();
        }

        public unsafe byte[] ToBytes(){
            byte[] arr = new byte[SizeOf];
            fixed (byte* b = arr) {
                var head = b;
                *(long*)head = Attr;
                head += sizeof(long);
                *(int*)head = Size;
                head += sizeof(int);
                *(int*)head = Offset;
            }
            return arr;
        }

        public static unsafe TupleDesc FromBytes(byte[] data){
            TupleDesc result;
            fixed (byte* b = data) {
                var head = b;
                long attr = *(long*)head;
                head += sizeof(long);
                int size = *(int*)head;
                head += sizeof(int);
                int offset = *(int*)head;
                result = new TupleDesc(attr, size, offset);
            }
            return result;
        }
    }

    // public struct KeyAttr {
    //     public KeyAttr(PrimaryKey key, long attr){
    //         Key = key;
    //         Attr = attr;
    //     }
    //     public PrimaryKey Key;
    //     public long Attr;
    //     public int Size => Key.Size + sizeof(long);

    //     public override string ToString(){
    //         return $"KA ({Key}, {Attr})";
    //     }

    //     public override bool Equals(object o){
    //         if (o == null || GetType() != o.GetType()){
    //             return false;
    //         }
    //         KeyAttr other = (KeyAttr)o;
    //         return Key.Equals(other.Key) && Attr == other.Attr;
    //     }

    //     public override int GetHashCode(){
    //         return Key.GetHashCode() + Attr.GetHashCode();
    //     }

    //     public unsafe byte[] ToBytes(){
    //         byte[] arr = new byte[Size];
    //         fixed (byte* b = arr) {
    //             var head = b;
    //             Key.ToBytes().CopyTo(new Span<byte>(head, Key.Size));
    //             head += Key.Size;
    //             *(long*)head = Attr;
    //         }
    //         return arr;
    //     }

    //     public static unsafe KeyAttr FromBytes(byte[] data) {
    //         KeyAttr result = new KeyAttr();

    //         fixed (byte* b = data) {
    //             var head = b;
    //             result.Key = PrimaryKey.FromBytes(new Span<byte>(head, data.Length - sizeof(long)).ToArray());
    //             head += result.Key.Size;
    //             result.Attr = *(long*)head;
    //         }
    //         return result;
    //     }
    // }

    // public class OCCComparer : IEqualityComparer<KeyAttr>
    // {
    //     public bool Equals(KeyAttr x, KeyAttr y)
    //     {
    //         return x.Key.Equals(y.Key);
    //     }

    //     public int GetHashCode(KeyAttr obj)
    //     {
    //         return obj.GetHashCode();
    //     }
    // }

    public class ByteArrayComparer : IEqualityComparer<byte[]> {
        public bool Equals(byte[] left, byte[] right) {
            if ( left == null || right == null ) {
            return left == right;
            }
            return left.SequenceEqual(right);
        }
        public int GetHashCode(byte[] key) {
            if (key == null)
            throw new ArgumentNullException("key");
            return key.Sum(b => b);
        }
    }

    public class Util {
        public static bool IsEmpty(ReadOnlySpan<byte> val){
            if (val.IsEmpty) {
                return true;
            }
            foreach (byte b in val)
            {
                if (b != 0)
                {
                    return false; // If any element is not 0, return false
                }
            }
            return true; // All elements are 0
        }

        public static bool IsTerminalStatus(TransactionStatus status){
            return status == TransactionStatus.Committed || status == TransactionStatus.Aborted;
        }

        public static int GetLength(byte[][] arr){
            int len = 0;
            foreach (byte[] a in arr){
                len += a.Length;
            }
            return len;
        }

        public static int CompareArrays<T>(IEnumerable<T> first, IEnumerable<T> second) where T : IComparable<T>
        {
            using (var firstEnum = first.GetEnumerator())
            using (var secondEnum = second.GetEnumerator())
            {
                while (firstEnum.MoveNext())
                {
                    if (!secondEnum.MoveNext())
                        return 1;

                    int cmp = firstEnum.Current.CompareTo(secondEnum.Current);
                    if (cmp != 0)
                        return cmp;
                }

                return secondEnum.MoveNext() ? -1 : 0;
            }
        }

        public static void Shuffle<T> (FastRandom rng, T[] array)
        {
            int n = array.Length;
            while (n > 1) 
            {
                int k = rng.Next(n--);
                T temp = array[n];
                array[n] = array[k];
                array[k] = temp;
            }
        }
    }

}