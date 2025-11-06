using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.core;

namespace TwoPhaseCommit;

public class RowRecord
{
    private enum LockMode { NONE, SHARED, EXCLUSIVE }
    private LockMode mode = LockMode.NONE;
    private long[] holders = new long[16];
    private int numSharedHolders = 0;
    private Queue<(long, LockMode, TaskCompletionSource<bool>)> waiters = new();

    public RowRecord()
    {
        for (int i = 0; i < holders.Length; i++)
            holders[i] = long.MaxValue;
    }

    private bool TryInsert(long holder)
    {
        for (int i = 0; i < holders.Length; i++)
        {
            if (holders[i] == long.MaxValue)
            {
                holders[i] = holder;
                return true;
            }
        }
        return false;
    }

    private void Remove(long holder)
    {
        for (int i = 0; i < holders.Length; i++)
        {
            if (holders[i] == holder)
            {
                holders[i] = long.MaxValue;
                return;
            }
        }
        // Should never happen
        Debug.Assert(false);
    }

    private bool CanWait(long waiter)
    {
        return holders.All(x => x > waiter) && waiters.All(x => x.Item1 > waiter);
    }

    private bool TryGrantLock(long txnId, LockMode requestedLock)
    {
        switch (requestedLock)
        {
            case LockMode.EXCLUSIVE:
                if (mode != LockMode.NONE) return false;
                var success = TryInsert(txnId);
                Debug.Assert(success);       
                mode = LockMode.EXCLUSIVE;
                return true;
            case LockMode.SHARED:
                if (mode == LockMode.EXCLUSIVE) return false;
                if (!TryInsert(txnId)) return false;
                numSharedHolders++;
                mode = LockMode.SHARED;
                return true;
            default:
                throw new Exception("Should never happen");
        }
    }

    private ValueTask<bool> TryAcquire(long txnId, LockMode requestedLock)
    {
        Debug.Assert(requestedLock != LockMode.NONE);
        lock (this)
        {
            if (waiters.Count == 0 && TryGrantLock(txnId, requestedLock))
                return ValueTask.FromResult(true);
            
            if (CanWait(txnId))
            {
                var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                waiters.Enqueue((txnId, requestedLock, tcs));
                return new ValueTask<bool>(tcs.Task);
            }
            return ValueTask.FromResult(false);
        }
    }

    public ValueTask<bool> TryAcquireExclusive(long txnId)
    {
        return TryAcquire(txnId, LockMode.EXCLUSIVE);
    }

    public void ReleaseExclusive(long txnId)
    {
        lock (this)
        {
            Debug.Assert(mode == LockMode.EXCLUSIVE);
            Remove(txnId);
            mode = LockMode.NONE;
            while (waiters.TryPeek(out var waiter))
            {
                if (!TryGrantLock(waiter.Item1, waiter.Item2)) break;
                waiters.Dequeue();
                waiter.Item3.SetResult(true);
            }
        }
    }
    
    public ValueTask<bool> TryAcquireShared(long txnId)
    {
       return TryAcquire(txnId, LockMode.SHARED);
    }

    public void ReleaseShared(long txnId)
    {
        lock (this)
        {
            Debug.Assert(mode == LockMode.SHARED);
            Remove(txnId);
            if (--numSharedHolders == 0)
                mode = LockMode.NONE;
            while (waiters.TryPeek(out var waiter))
            {
                if (!TryGrantLock(waiter.Item1, waiter.Item2)) break;
                waiters.Dequeue();
                waiter.Item3.SetResult(true);
            }
        }
    }
} 

public class Warehouse : RowRecord
{
    public byte wId;
    // public string wName;
    // public string wStreetAddress;
    // public string wCity;
    // public string wState;
    // public string wZip;
    // public double wTax;
    public double wYtd;
}

public class District : RowRecord
{
    public byte dId;
    public byte dWId;
    // public string dName;
    // public string dStreetAddress;
    // public string dCity;
    // public string dState;
    // public string dZip;
    // public int dTax;
    public double dYtd;
    public int dNextOrderId;
}

[StructLayout(LayoutKind.Explicit, Size = 2)]
public struct DistrictKey : IEquatable<DistrictKey>
{
    [FieldOffset(0)]
    private short word;
    [FieldOffset(0)]
    public byte dWId;
    [FieldOffset(1)]
    public byte dId;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public DistrictKey(byte dWId, byte dId)
    {
        this.dWId = dWId;
        this.dId = dId;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(DistrictKey other)
    {
        return word == other.word;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool Equals(object? obj)
    {
        return obj is DistrictKey other && Equals(other);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int GetHashCode()
    {
        return word.GetHashCode();
    }
}

public class Customer : RowRecord
{
    public int cID;
    public byte cDId;
    public byte cWId;
    // public string cFirstName;
    // public string cMiddle;
    // public string cLastName;
    // public string cStreet1;
    // public string cStreet2;
    // public string cCity;
    // public string cState;
    // public string cZip;
    // public string cPhone;
    // public DateTime cSince;
    // public string cCredit;
    // public double cCreditLim;
    // public double cDiscount;
    public double cBalance;
    public double cYtdPayment;
    public int cPaymentCnt;
    // public int cDeliveryCnt;
}

[StructLayout(LayoutKind.Explicit, Size = 8)]
public struct CustomerKey : IEquatable<CustomerKey>
{
    [FieldOffset(0)]
    private int word;
    [FieldOffset(0)]
    public byte cWId;
    [FieldOffset(1)]
    public byte cDId;
    [FieldOffset(2)]
    public int CId;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CustomerKey(byte cWId, byte cDId, int CId)
    {
        word = 0;
        this.cWId = cWId;
        this.cDId = cDId;
        this.CId = CId;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(CustomerKey other)
    {
        return word == other.word;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool Equals(object? obj)
    {
        return obj is CustomerKey other && Equals(other);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int GetHashCode()
    {
        return word.GetHashCode();
    }
}

public class Order : RowRecord 
{
    public int oId;
    public byte oDId;
    public byte oWId;
    public int oCId;
    public DateTime oEntryD;
    public int oCarrierId;
    public int oOLCnt;
    public int oAllLocal;
}

[StructLayout(LayoutKind.Explicit, Size = 16)]
public struct OrderKey : IEquatable<OrderKey>, IComparable<OrderKey>
{
    [FieldOffset(0)]
    public byte oWId;
    [FieldOffset(1)]
    public byte oDId;
    [FieldOffset(4)]
    public int oCId;
    [FieldOffset(8)]
    public int oId;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public OrderKey(byte oWId, byte oDId, int oCId, int oId)
    {
        this.oWId = oWId;
        this.oDId = oDId;
        this.oCId = oCId;
        this.oId = oId;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(OrderKey other)
    {
        return oWId == other.oWId && oDId == other.oDId && oCId == other.oCId && oId == other.oId;
    }

    public int CompareTo(OrderKey other)
    {
        // dictionary order: oWId, oDId, oCId, oId
        var wIdComparison = oWId.CompareTo(other.oWId);
        if (wIdComparison != 0) return wIdComparison;
        
        var dIdComparison = oDId.CompareTo(other.oDId);
        if (dIdComparison != 0) return dIdComparison;
        
        var cIdComparison = oCId.CompareTo(other.oCId);
        if (cIdComparison != 0) return dIdComparison;
        
        return oId.CompareTo(other.oId);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool Equals(object? obj)
    {
        return obj is OrderKey other && Equals(other);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int GetHashCode()
    {
        return HashCode.Combine(oWId, oDId, oCId, oId);
    }
}

public class OrderLine : RowRecord
{
    public int olOId;
    public byte olDId;
    public byte olWId;
    public byte olNumber;
    public int olIId;
    public byte olSupplyWId;
    public DateTime olDeliveryD;
    public int olQuantity;
    public double olAmount;
}

[StructLayout(LayoutKind.Explicit, Size = 8)]
public struct OrderLineKey : IEquatable<OrderLineKey>, IComparable<OrderLineKey>
{
    [FieldOffset(0)]
    private long word;
    [FieldOffset(0)]
    public byte olWId;
    [FieldOffset(1)]
    public byte olDId;
    [FieldOffset(2)]
    public int olOId;
    [FieldOffset(6)]
    public byte olNumber;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public OrderLineKey(byte olWId, byte olDId, int olOId, byte olNumber)
    {
        word = 0;
        this.olWId = olWId;
        this.olDId = olDId;
        this.olOId = olOId;
        this.olNumber = olNumber;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(OrderLineKey other)
    {
        return word == other.word;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool Equals(object? obj)
    {
        return obj is OrderLineKey other && Equals(other);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int GetHashCode()
    {
        return word.GetHashCode();
    }
    
    public int CompareTo(OrderLineKey other)
    {
        // dictionary order: olWId, olDId, olOId, olNumber
        var wIdComparison = olWId.CompareTo(other.olWId);
        if (wIdComparison != 0) return wIdComparison;

        var dIdComparison = olDId.CompareTo(other.olDId);
        if (dIdComparison != 0) return dIdComparison;

        var oIdComparison = olOId.CompareTo(other.olOId);
        if (oIdComparison != 0) return oIdComparison;

        return olNumber.CompareTo(other.olNumber);
    }
}

public class NewOrder : RowRecord
{
    public int noOId;
    public byte noDId;
    public byte noWId;
}

public class Item : RowRecord
{
    public int iId;
    // public string iName;
    public double iPrice;
    // public int iData;
}

public class Stock : RowRecord
{
    public int sIId;
    public byte sWId;
    public int sQuantity;
}

[StructLayout(LayoutKind.Explicit, Size = 8)]
public struct StockKey : IEquatable<StockKey>
{
    [FieldOffset(0)]
    private long word;
    [FieldOffset(0)]
    public byte sWId;
    [FieldOffset(4)]
    public int sIId;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public StockKey(byte sWId, int sIId)
    {
        word = 0;
        this.sWId = sWId;
        this.sIId = sIId;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(StockKey other)
    {
        return word == other.word;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool Equals(object? obj)
    {
        return obj is StockKey other && Equals(other);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int GetHashCode()
    {
        return word.GetHashCode();
    }
}

