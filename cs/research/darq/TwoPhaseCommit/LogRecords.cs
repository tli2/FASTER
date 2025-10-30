using System.Diagnostics;
using System.Runtime.InteropServices;
using FASTER.core;

namespace TwoPhaseCommit;

public enum LogRecordType : byte
{
    COMMIT,
    ABORT,
    INSERT_ORDER,
    INSERT_ORDER_LINE,
    UPDATE_NEXT_ORDER_ID,
    UPDATE_STOCK,
    UPDATE_WAREHOUSE_YTD,
    UPDATE_DISTRICT_YTD,
    UPDATE_CUSTOMER,
}

public interface ILogRecord : ILogEnqueueEntry
{
    public void Replay(TpccShard db);
}

[StructLayout(LayoutKind.Explicit, Size = 16)]
public struct AbortLogRecord : ILogRecord
{
    [FieldOffset(0)] public LogRecordType type;
    [FieldOffset(8)] public long transactionId;
    
    public int SerializedLength => 16;

    public void SerializeTo(Span<byte> dest)
    {
        MemoryMarshal.Write(dest, ref this);
    }
    
    public void Replay(TpccShard db)
    {
        if (db.activeTransactions.TryRemove(transactionId, out var txn))
        {
            txn.Undo();
            db.objectPool.Return(txn);       
        }
        else
            // Should not happen
            Debug.Assert(false);    
    }
}

[StructLayout(LayoutKind.Explicit, Size = 16)]
public struct CommitLogRecord : ILogRecord
{
    [FieldOffset(0)] public LogRecordType type;
    [FieldOffset(8)] public long transactionId;
    
    public int SerializedLength => 16;

    public void SerializeTo(Span<byte> dest)
    {
        MemoryMarshal.Write(dest, ref this);
    }
    
    public void Replay(TpccShard db)
    {
        if (db.activeTransactions.TryRemove(transactionId, out var txn))
        {
            db.objectPool.Return(txn);
        }
        else
            // Should not happen
            Debug.Assert(false);
    }
}

[StructLayout(LayoutKind.Explicit, Size = 40)]
public struct InsertOrderLogRecord : ILogRecord
{
    [FieldOffset(0)] public LogRecordType type;
    [FieldOffset(1)] public byte oDId;
    [FieldOffset(2)] public byte oWId;
    [FieldOffset(4)] public int oId;
    [FieldOffset(8)] public int oCId;
    [FieldOffset(12)] public int oCarrierId;
    [FieldOffset(16)] public int oOLCnt;
    [FieldOffset(20)] public int oAllLocal;
    [FieldOffset(24)] public long transactionId;
    [FieldOffset(32)] public DateTime oEntryD;

    public int SerializedLength => 40;

    public void SerializeTo(Span<byte> dest)
    {
        MemoryMarshal.Write(dest, ref this);
    }

    public void Replay(TpccShard db)
    {
        if (!db.activeTransactions.TryGetValue(transactionId, out var tx))
        {
            tx = db.objectPool.Checkout().Reset(transactionId);
            db.activeTransactions.TryAdd(transactionId, tx);
        }
        
        var order = new Order
        {
            oId = oId,
            oDId = oDId,
            oWId = oWId,
            oCId = oCId,
            oEntryD = oEntryD,
            oCarrierId = oCarrierId,
            oOLCnt = oOLCnt,
            oAllLocal = oAllLocal,
        };
        var key = new OrderKey(order.oWId, order.oDId, order.oCId, order.oId);
        tx.AddUndoAction(() => db.orders.TryRemove(key));
        // Locks are skipped for replay because we know there are no conflicts
        db.orders.TryAdd(key, order);
    }
}

// Log record for inserting a new OrderLine.
[StructLayout(LayoutKind.Explicit, Size = 44)]
public struct InsertOrderLineLogRecord : ILogRecord
{
    [FieldOffset(0)] public LogRecordType type;
    [FieldOffset(1)] public byte olDId;
    [FieldOffset(2)] public byte olWId;
    [FieldOffset(3)] public byte olNumber;
    [FieldOffset(4)] public byte olSupplyWId;
    [FieldOffset(8)] public long transactionId;
    [FieldOffset(16)] public DateTime olDeliveryD;
    [FieldOffset(24)] public double olAmount;
    [FieldOffset(32)] public int olIId;
    [FieldOffset(36)] public int olOId;
    [FieldOffset(40)] public int olQuantity;

    public int SerializedLength => 44;
    public void SerializeTo(Span<byte> dest) => MemoryMarshal.Write(dest, ref this);

    public void Replay(TpccShard db)
    {
        if (!db.activeTransactions.TryGetValue(transactionId, out var tx))
        {
            tx = db.objectPool.Checkout().Reset(transactionId);
            db.activeTransactions.TryAdd(transactionId, tx);
        }
        
        var orderLine = new OrderLine
        {
            olOId = olOId,
            olDId = olDId,
            olWId = olWId,
            olNumber = olNumber,
            olIId = olIId,
            olSupplyWId = olSupplyWId,
            olDeliveryD = olDeliveryD,
            olQuantity = olQuantity,
            olAmount = olAmount
        };
        var key = new OrderLineKey(orderLine.olWId, orderLine.olDId, orderLine.olOId, orderLine.olNumber);;
        tx.AddUndoAction(() => db.orderLines.TryRemove(key));
        // Locks are skipped for replay because we know there are no conflicts
        db.orderLines.TryAdd(new OrderLineKey(olWId, olDId, olOId, olNumber), orderLine);
    }
    
}

// Log record for updating the next available order ID for a district.
[StructLayout(LayoutKind.Explicit, Size = 16)]
public struct UpdateNextOrderIdLogRecord : ILogRecord
{
    [FieldOffset(0)] public LogRecordType type;
    [FieldOffset(1)] public byte wId;
    [FieldOffset(2)] public byte dId;
    [FieldOffset(8)] public long transactionId;

    public int SerializedLength => 16;
    public void SerializeTo(Span<byte> dest) => MemoryMarshal.Write(dest, ref this);

    public void Replay(TpccShard db)
    {
        if (!db.activeTransactions.TryGetValue(transactionId, out var tx))
        {
            tx = db.objectPool.Checkout().Reset(transactionId);
            db.activeTransactions.TryAdd(transactionId, tx);
        }
        var district = db.districts[new DistrictKey(wId, dId)];
        // Locks are skipped for replay because we know there are no conflicts
        district.dNextOrderId++;
        tx.AddUndoAction(() => district.dNextOrderId--);
    }
}

// Log record for updating stock quantity.
[StructLayout(LayoutKind.Explicit, Size = 20)]
public struct UpdateStockLogRecord : ILogRecord
{
    [FieldOffset(0)] public LogRecordType type;
    [FieldOffset(1)] public byte wId;
    [FieldOffset(4)] public int itemId;
    [FieldOffset(8)] public long transactionId;
    [FieldOffset(16)] public int newQuantity;
    
    public int SerializedLength => 20;
    public void SerializeTo(Span<byte> dest) => MemoryMarshal.Write(dest, ref this);

    public void Replay(TpccShard db)
    {
        if (!db.activeTransactions.TryGetValue(transactionId, out var tx))
        {
            tx = db.objectPool.Checkout().Reset(transactionId);
            db.activeTransactions.TryAdd(transactionId, tx);
        }
        var stock = db.stocks[new StockKey(wId, itemId)];
        var oldQuantity = stock.sQuantity;
        // Locks are skipped for replay because we know there are no conflicts
        stock.sQuantity = newQuantity;
        tx.AddUndoAction(() => stock.sQuantity = oldQuantity);
    }
}

// Log record for updating a warehouse's year-to-date balance.
[StructLayout(LayoutKind.Explicit, Size = 24)]
public struct UpdateWarehouseYtdLogRecord : ILogRecord
{
    [FieldOffset(0)] public LogRecordType type;
    [FieldOffset(1)] public byte wId;
    [FieldOffset(8)] public long transactionId;
    [FieldOffset(16)] public double newYtd;

    public int SerializedLength => 24;
    public void SerializeTo(Span<byte> dest) => MemoryMarshal.Write(dest, ref this);

    public void Replay(TpccShard db)
    {
        if (!db.activeTransactions.TryGetValue(transactionId, out var tx))
        {
            tx = db.objectPool.Checkout().Reset(transactionId);
            db.activeTransactions.TryAdd(transactionId, tx);
        }
        var warehouse = db.warehouses[wId];
        var oldYtd = warehouse.wYtd;
        // Locks are skipped for replay because we know there are no conflicts
        warehouse.wYtd = newYtd;
        tx.AddUndoAction(() => warehouse.wYtd = oldYtd);
    }
}

// Log record for updating a district's year-to-date balance.
[StructLayout(LayoutKind.Explicit, Size = 24)]
public struct UpdateDistrictYtdLogRecord : ILogRecord
{
    [FieldOffset(0)] public LogRecordType type;
    [FieldOffset(1)] public byte wId;
    [FieldOffset(2)] public byte dId;
    [FieldOffset(8)] public long transactionId;
    [FieldOffset(16)] public double newYtd;

    public int SerializedLength => 24;
    public void SerializeTo(Span<byte> dest) => MemoryMarshal.Write(dest, ref this);

    public void Replay(TpccShard db)
    {
        if (!db.activeTransactions.TryGetValue(transactionId, out var tx))
        {
            tx = db.objectPool.Checkout().Reset(transactionId);
            db.activeTransactions.TryAdd(transactionId, tx);
        }
        
        var district = db.districts[new DistrictKey(wId, dId)];
        var oldYtd = district.dYtd;
        district.dYtd = newYtd;
        tx.AddUndoAction(() => district.dYtd = oldYtd);
    }
}

// Log record for updating a customer's balance and payment history.
[StructLayout(LayoutKind.Explicit, Size = 24)]
public struct UpdateCustomerLogRecord : ILogRecord
{
    [FieldOffset(0)] public LogRecordType type;
    [FieldOffset(1)] public byte wId;
    [FieldOffset(2)] public byte dId;
    [FieldOffset(4)] public int cId;
    [FieldOffset(8)] public long transactionId;
    [FieldOffset(16)] public double amount;

    public int SerializedLength => 24;
    public void SerializeTo(Span<byte> dest) => MemoryMarshal.Write(dest, ref this);

    public void Replay(TpccShard db)
    {
        if (!db.activeTransactions.TryGetValue(transactionId, out var tx))
        {
            tx = db.objectPool.Checkout().Reset(transactionId);
            db.activeTransactions.TryAdd(transactionId, tx);
        }
        
        var customer = db.customers[new CustomerKey(wId, dId, cId)];
        customer.cBalance -= amount;
        customer.cYtdPayment += amount;
        customer.cPaymentCnt++;
        var amountLocal = amount;
        tx.AddUndoAction(() =>
        {
            customer.cBalance += amountLocal;
            customer.cYtdPayment -= amountLocal;
            customer.cPaymentCnt--;
        });

    }
}

public static class LogRecords
{
    public static unsafe void ApplyLogRecord(byte *b, TpccShard db)
    {
        var type = *(LogRecordType *)b;
        switch (type)
        {
            case LogRecordType.COMMIT:
                (*(CommitLogRecord*)b).Replay(db);
                break;
            case LogRecordType.ABORT:
                (*(AbortLogRecord*)b).Replay(db);
                break;
            case LogRecordType.INSERT_ORDER:
                (*(InsertOrderLogRecord*)b).Replay(db);
                break;
            case LogRecordType.INSERT_ORDER_LINE:
                (*(InsertOrderLineLogRecord*)b).Replay(db);
                break;
            case LogRecordType.UPDATE_NEXT_ORDER_ID:
                (*(UpdateNextOrderIdLogRecord*)b).Replay(db);
                break;
            case LogRecordType.UPDATE_STOCK:
                (*(UpdateStockLogRecord*)b).Replay(db);
                break;
            case LogRecordType.UPDATE_WAREHOUSE_YTD:
                (*(UpdateWarehouseYtdLogRecord*)b).Replay(db);
                break;
            case LogRecordType.UPDATE_DISTRICT_YTD:
                (*(UpdateDistrictYtdLogRecord*)b).Replay(db);
                break;
            case LogRecordType.UPDATE_CUSTOMER:
                (*(UpdateCustomerLogRecord*)b).Replay(db);
                break;
            default:
                throw new Exception("Unknown log record type");   
        }
    }
    
    public static CommitLogRecord CreateCommitRecord(long transactionId) => new() { type = LogRecordType.COMMIT, transactionId = transactionId };
    public static AbortLogRecord CreateAbortRecord(long transactionId) => new() { type = LogRecordType.ABORT, transactionId = transactionId };
    
    public static InsertOrderLogRecord CreateInsertOrderRecord(long transactionId, Order order) => new() 
    { 
        type = LogRecordType.INSERT_ORDER, 
        transactionId = transactionId, 
        oId = order.oId, 
        oDId = order.oDId, 
        oWId = order.oWId, 
        oCId = order.oCId, 
        oCarrierId = order.oCarrierId, 
        oOLCnt = order.oOLCnt, 
        oAllLocal = order.oAllLocal, 
        oEntryD = order.oEntryD 
    };

    public static InsertOrderLineLogRecord CreateInsertOrderLineRecord(long transactionId, OrderLine orderLine) => new()
    {
        type = LogRecordType.INSERT_ORDER_LINE,
        transactionId = transactionId,
        olOId = orderLine.olOId,
        olDId = orderLine.olDId,
        olWId = orderLine.olWId,
        olNumber = orderLine.olNumber,
        olIId = orderLine.olIId,
        olSupplyWId = orderLine.olSupplyWId,
        olDeliveryD = orderLine.olDeliveryD,
        olQuantity = orderLine.olQuantity,
        olAmount = orderLine.olAmount
    };
    
    public static UpdateNextOrderIdLogRecord CreateUpdateNextOrderIdRecord(long transactionId, byte wId, byte dId) => new()
    {
        type = LogRecordType.UPDATE_NEXT_ORDER_ID,
        transactionId = transactionId,
        wId = wId,
        dId = dId
    };

    public static UpdateStockLogRecord CreateUpdateStockRecord(long transactionId, byte wId, int itemId, int newQuantity) => new()
    {
        type = LogRecordType.UPDATE_STOCK,
        transactionId = transactionId,
        wId = wId,
        itemId = itemId,
        newQuantity = newQuantity
    };
    
    public static UpdateWarehouseYtdLogRecord CreateUpdateWarehouseYtdRecord(long transactionId, byte wId, double newYtd) => new()
    {
        type = LogRecordType.UPDATE_WAREHOUSE_YTD,
        transactionId = transactionId,
        wId = wId,
        newYtd = newYtd
    };
    
    public static UpdateDistrictYtdLogRecord CreateUpdateDistrictYtdRecord(long transactionId, byte wId, byte dId, double newYtd) => new()
    {
        type = LogRecordType.UPDATE_DISTRICT_YTD,
        transactionId = transactionId,
        wId = wId,
        dId = dId,
        newYtd = newYtd
    };
    
    public static UpdateCustomerLogRecord CreateUpdateCustomerRecord(long transactionId, byte wId, byte dId, int cId, double amount) => new()
    {
        type = LogRecordType.UPDATE_CUSTOMER,
        transactionId = transactionId,
        wId = wId,
        dId = dId,
        cId = cId,
        amount = amount
    };    
}