using System.Collections.Concurrent;
using System.Text;
using System.Diagnostics;
using SharpNeat.Utility;
using FASTER.common;

namespace DB {
public struct TpccConfig {
    public int NumWh;
    public int NumDistrict;
    public int NumCustomer;
    public int NumOrder;
    public int NumItem;
    public int NumStock;
    public int NewOrderCrossPartitionProbability;
    public int PaymentCrossPartitionProbability;
    public int PartitionsPerThread;

    public TpccConfig(
        int numWh = 2,
        int numDistrict = 10,
        int numCustomer = 3000,
        int numOrder = 3000,
        int numItem = 100000, 
        int numStock = 100000,
        int newOrderCrossPartitionProbability = 10, 
        int paymentCrossPartitionProbability = 15,
        int partitionsPerThread = 4
        ){
        NumWh = numWh;
        NumDistrict = numDistrict;
        NumCustomer = numCustomer;
        NumOrder = numOrder;
        NumItem = numItem;
        NumStock = numStock;
        NewOrderCrossPartitionProbability = newOrderCrossPartitionProbability;
        PaymentCrossPartitionProbability = paymentCrossPartitionProbability;
        PartitionsPerThread = partitionsPerThread;
    }

    public override string ToString()
    {
        return $"NumWh: {NumWh}, NumDistrict: {NumDistrict}, NumCustomer: {NumCustomer}, NumOrder: {NumOrder}, NumItem: {NumItem}, NumStock: {NumStock}, NewOrderCrossPartitionProbability: {NewOrderCrossPartitionProbability}, PaymentCrossPartitionProbability: {PaymentCrossPartitionProbability}, PartitionsPerThread: {PartitionsPerThread}";
    }
}

public struct Query {

    public int w_id;
    public int d_id;
    public int c_id;
    public int c_d_id;
    public int c_w_id;
    public float h_amount;
    public byte[] c_last;
    public int o_ol_cnt;
    public int[] ol_i_ids;
    public int[] ol_supply_w_id;
    public int[] ol_quantity;
    public bool isNewOrder;

    public Query(int w_id, int d_id, int c_id, int o_ol_cnt, int[] ol_i_ids, int[] ol_supply_w_id, int[] ol_quantity) {
        isNewOrder = true;
        this.w_id = w_id;
        this.d_id = d_id;
        this.c_id = c_id;
        this.o_ol_cnt = o_ol_cnt;
        this.ol_i_ids = ol_i_ids;
        this.ol_supply_w_id = ol_supply_w_id;
        this.ol_quantity = ol_quantity;
    }

    public Query(int w_id, int d_id, int c_id, int c_d_id, int c_w_id, float h_amount, byte[] c_last) {
        isNewOrder = false;
        this.w_id = w_id;
        this.d_id = d_id;
        this.c_id = c_id;
        this.c_d_id = c_d_id;
        this.c_w_id = c_w_id;
        this.h_amount = h_amount;
        this.c_last = c_last;
    }
}

// public struct NewOrderQuery : Query {
//     public int w_id;
//     public int d_id;
//     public int c_id;
//     public int o_ol_cnt;
//     public int[] ol_i_ids;
//     public int[] ol_supply_w_id;
//     public int[] ol_quantity;
//     public NewOrderQuery(int w_id, int d_id, int c_id, int o_ol_cnt, int[] ol_i_ids, int[] ol_supply_w_id, int[] ol_quantity){
//         this.w_id = w_id;
//         this.d_id = d_id;
//         this.c_id = c_id;
//         this.o_ol_cnt = o_ol_cnt;
//         this.ol_i_ids = ol_i_ids;
//         this.ol_supply_w_id = ol_supply_w_id;
//         this.ol_quantity = ol_quantity;
//     }

//     public unsafe byte[] ToBytes(){
//         byte[] bytes = new byte[4 + 4 + 4 + 4 + 4 + 4 + 4 * ol_i_ids.Length + 4 * ol_supply_w_id.Length + 4 * ol_quantity.Length];
//         fixed (byte* b = bytes){
//             *(int*)b = w_id;
//             *(int*)(b + 4) = d_id;
//             *(int*)(b + 8) = c_id;
//             *(int*)(b + 12) = o_ol_cnt;
//             *(int*)(b + 16) = ol_i_ids.Length;
//             for (int i = 0; i < ol_i_ids.Length; i++){
//                 *(int*)(b + 20 + i * 4) = ol_i_ids[i];
//             }
//             *(int*)(b + 20 + ol_i_ids.Length * 4) = ol_supply_w_id.Length;
//             for (int i = 0; i < ol_supply_w_id.Length; i++){
//                 *(int*)(b + 24 + ol_i_ids.Length * 4 + i * 4) = ol_supply_w_id[i];
//             }
//             *(int*)(b + 24 + ol_i_ids.Length * 4 + ol_supply_w_id.Length * 4) = ol_quantity.Length;
//             for (int i = 0; i < ol_quantity.Length; i++){
//                 *(int*)(b + 28 + ol_i_ids.Length * 4 + ol_supply_w_id.Length * 4 + i * 4) = ol_quantity[i];
//             }
//         }
//         return bytes;
//     }

//     public static unsafe NewOrderQuery FromBytes(byte[] bytes){
//         int w_id, d_id, c_id, o_ol_cnt;
//         int[] ol_i_ids, ol_supply_w_id, ol_quantity;
//         fixed (byte* b = bytes){
//             w_id = *(int*)b;
//             d_id = *(int*)(b + 4);
//             c_id = *(int*)(b + 8);
//             o_ol_cnt = *(int*)(b + 12);
//             int ol_i_ids_len = *(int*)(b + 16);
//             ol_i_ids = new int[ol_i_ids_len];
//             for (int i = 0; i < ol_i_ids_len; i++){
//                 ol_i_ids[i] = *(int*)(b + 20 + i * 4);
//             }
//             int ol_supply_w_id_len = *(int*)(b + 20 + ol_i_ids_len * 4);
//             ol_supply_w_id = new int[ol_supply_w_id_len];
//             for (int i = 0; i < ol_supply_w_id_len; i++){
//                 ol_supply_w_id[i] = *(int*)(b + 24 + ol_i_ids_len * 4 + i * 4);
//             }
//             int ol_quantity_len = *(int*)(b + 24 + ol_i_ids_len * 4 + ol_supply_w_id_len * 4);
//             ol_quantity = new int[ol_quantity_len];
//             for (int i = 0; i < ol_quantity_len; i++){
//                 ol_quantity[i] = *(int*)(b + 28 + ol_i_ids_len * 4 + ol_supply_w_id_len * 4 + i * 4);
//             }
//         }
//         return new NewOrderQuery(w_id, d_id, c_id, o_ol_cnt, ol_i_ids, ol_supply_w_id, ol_quantity);
//     }
// }

// public struct PaymentQuery {
//     public int w_id;
//     public int d_id;
//     public int c_id;
//     public int c_d_id;
//     public int c_w_id;
//     public float h_amount;
//     public string c_last;
//     public static int Size = 4 + 4 + 4 + 4 + 4 + 4 + 16;
//     public PaymentQuery(int w_id, int d_id, int c_id, int c_d_id, int c_w_id, float h_amount, string c_last){
//         this.w_id = w_id;
//         this.d_id = d_id;
//         this.c_id = c_id;
//         this.c_d_id = c_d_id;
//         this.c_w_id = c_w_id;
//         this.h_amount = h_amount;
//         this.c_last = c_last;
//     }

//     // public unsafe byte[] ToBytes(){
//     //     byte[] bytes = new byte[Size];
//     //     fixed (byte* b = bytes){
//     //         *(int*)b = w_id;
//     //         *(int*)(b + 4) = d_id;
//     //         *(int*)(b + 8) = c_id;
//     //         *(int*)(b + 12) = c_d_id;
//     //         *(int*)(b + 16) = c_w_id;
//     //         *(float*)(b + 20) = h_amount;
//     //         Encoding.ASCII.GetBytes(c_last).CopyTo(bytes, 24);
//     //     }
//     //     return bytes;
//     // }

//     // public static unsafe PaymentQuery FromBytes(byte[] bytes){
//     //     int w_id, d_id, c_id, c_d_id, c_w_id;
//     //     float h_amount;
//     //     string c_last;
//     //     fixed (byte* b = bytes){
//     //         w_id = *(int*)b;
//     //         d_id = *(int*)(b + 4);
//     //         c_id = *(int*)(b + 8);
//     //         c_d_id = *(int*)(b + 12);
//     //         c_w_id = *(int*)(b + 16);
//     //         h_amount = *(float*)(b + 20);
//     //         c_last = Encoding.ASCII.GetString(bytes, 24, 16);
//     //     }
//     //     return new PaymentQuery(w_id, d_id, c_id, c_d_id, c_w_id, h_amount, c_last);
//     // }

// }

/// <summary>
/// Adapted from https://github.com/SQLServerIO/TPCCBench/blob/master/TPCCDatabaseGenerator/TPCCGenData.cs
/// and coco
/// </summary>
public class TpccBenchmark : TableBenchmark {

    private static readonly FastRandom Frnd = new FastRandom();
    private static byte[] RandHold = Encoding.ASCII.GetBytes("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890");
    private static byte[] ZipRandHold = Encoding.ASCII.GetBytes("1234567890");
    private static byte SpaceAsByte = (byte)' ';
    private static byte[] ZeroAsBytes = BitConverter.GetBytes(0);
    private static byte[] BcAsBytes = { (byte)'B', (byte)'C' };
    private static byte[][] LastNames = new byte[][] {
        Encoding.ASCII.GetBytes("BAR"), Encoding.ASCII.GetBytes("OUGHT"), Encoding.ASCII.GetBytes("ABLE"), Encoding.ASCII.GetBytes("PRI"), Encoding.ASCII.GetBytes("PRES"),
        Encoding.ASCII.GetBytes("ESE"), Encoding.ASCII.GetBytes("ANTI"), Encoding.ASCII.GetBytes("CALLY"), Encoding.ASCII.GetBytes("ATION"), Encoding.ASCII.GetBytes("EING")
    };

    private static byte[] ORIGINAL = Encoding.ASCII.GetBytes("ORIGINAL");
    private static TupleDesc[] updateStockTds = new TupleDesc[] {
        new TupleDesc((int)TableField.S_QUANTITY, TpccSchema.fieldsToSchema[TableField.S_QUANTITY].Item1, 0),
        new TupleDesc((int)TableField.S_YTD, TpccSchema.fieldsToSchema[TableField.S_YTD].Item1, TpccSchema.fieldsToSchema[TableField.S_QUANTITY].Item1),
        new TupleDesc((int)TableField.S_ORDER_CNT, TpccSchema.fieldsToSchema[TableField.S_ORDER_CNT].Item1, TpccSchema.fieldsToSchema[TableField.S_QUANTITY].Item1 + TpccSchema.fieldsToSchema[TableField.S_YTD].Item1),
        new TupleDesc((int)TableField.S_REMOTE_CNT, TpccSchema.fieldsToSchema[TableField.S_REMOTE_CNT].Item1, TpccSchema.fieldsToSchema[TableField.S_QUANTITY].Item1 + TpccSchema.fieldsToSchema[TableField.S_YTD].Item1 + TpccSchema.fieldsToSchema[TableField.S_ORDER_CNT].Item1)
    };

    private static TupleDesc[] updateWarehouseTds = new TupleDesc[] {
        new TupleDesc((int)TableField.W_YTD, TpccSchema.fieldsToSchema[TableField.W_YTD].Item1, 0)
    };

    private static TupleDesc[] updateDistrictYtdTds = new TupleDesc[] {
        new TupleDesc((int)TableField.D_YTD, TpccSchema.fieldsToSchema[TableField.D_YTD].Item1, 0)
    };

    private static TupleDesc[] updateDistrictNextOIdTds = new TupleDesc[] {
        new TupleDesc((int)TableField.D_NEXT_O_ID, TpccSchema.fieldsToSchema[TableField.D_NEXT_O_ID].Item1, 0)
    };

    private static TupleDesc[] updateCustomerTds = new TupleDesc[] {
        new TupleDesc((int)TableField.C_BALANCE, TpccSchema.fieldsToSchema[TableField.C_BALANCE].Item1, 0),
        new TupleDesc((int)TableField.C_YTD_PAYMENT, TpccSchema.fieldsToSchema[TableField.C_YTD_PAYMENT].Item1, TpccSchema.fieldsToSchema[TableField.C_BALANCE].Item1),
        new TupleDesc((int)TableField.C_PAYMENT_CNT, TpccSchema.fieldsToSchema[TableField.C_PAYMENT_CNT].Item1, TpccSchema.fieldsToSchema[TableField.C_BALANCE].Item1 + TpccSchema.fieldsToSchema[TableField.C_YTD_PAYMENT].Item1),
        new TupleDesc((int)TableField.C_DATA, TpccSchema.fieldsToSchema[TableField.C_DATA].Item1, TpccSchema.fieldsToSchema[TableField.C_BALANCE].Item1 + TpccSchema.fieldsToSchema[TableField.C_YTD_PAYMENT].Item1 + TpccSchema.fieldsToSchema[TableField.C_PAYMENT_CNT].Item1)
    };

    private static byte[] emptyByteArr = new byte[0];

    Dictionary<TableType, string> tableDataFiles = new Dictionary<TableType, string> {
        {TableType.Warehouse, "data/warehouseData_{0}.bin"},
        {TableType.District, "data/districtData_{0}.bin"},
        {TableType.Customer, "data/customerData_{0}.bin"},
        {TableType.History, "data/historyData_{0}.bin"},
        {TableType.Order, "data/orderData_{0}.bin"},
        {TableType.NewOrder, "data/newOrderData_{0}.bin"},
        {TableType.Item, "data/itemData.bin"},
        {TableType.OrderLine, "data/orderLineData_{0}.bin"},
        {TableType.Stock, "data/stockData_{0}.bin"}
    };
    private int[][] ol_cnts;
    private long[][] entry_ds;

    public int PartitionId;
    private TpccConfig tpcCfg;
    private Dictionary<int, ShardedTable> tables;
    private ShardedTransactionManager txnManager;
    Query[] queries;
    private RpcClient rpcClient;
    internal SimpleObjectPool<byte[]> orderBytePool;
    internal SimpleObjectPool<byte[]> stockBytePool;
    internal SimpleObjectPool<byte[]> orderLineBytePool;
    internal SimpleObjectPool<byte[]> customerBytePool;
    internal SimpleObjectPool<byte[]> historyBytePool;
    CountdownEvent cde;
    internal int[] successCounts;
    // internal int[] abortCounts;

    public TpccBenchmark(int partitionId, TpccConfig tpcCfg, BenchmarkConfig cfg, Dictionary<int, ShardedTable> tables, ShardedTransactionManager txnManager) : base(cfg){
        System.Console.WriteLine("Init");
        PartitionId = partitionId;
        this.tpcCfg = tpcCfg;
        this.tables = tables;
        this.txnManager = txnManager;
        this.rpcClient = txnManager.GetRpcClient();
        orderBytePool = new SimpleObjectPool<byte[]>(() => new byte[tables[(int)TableType.Order].rowSize]);
        // assume larger
        int stockByteSize = TpccSchema.fieldsToSchema[TableField.S_YTD].Item1 + TpccSchema.fieldsToSchema[TableField.S_ORDER_CNT].Item1 + TpccSchema.fieldsToSchema[TableField.S_REMOTE_CNT].Item1 + TpccSchema.fieldsToSchema[TableField.S_QUANTITY].Item1;
        stockBytePool = new SimpleObjectPool<byte[]>(() => new byte[stockByteSize]);
        orderLineBytePool = new SimpleObjectPool<byte[]>(() => new byte[tables[(int)TableType.OrderLine].rowSize]);
        // assume larger
        int customerByteSize = TpccSchema.fieldsToSchema[TableField.C_BALANCE].Item1 + TpccSchema.fieldsToSchema[TableField.C_YTD_PAYMENT].Item1 + TpccSchema.fieldsToSchema[TableField.C_PAYMENT_CNT].Item1 + TpccSchema.fieldsToSchema[TableField.C_DATA].Item1;
        customerBytePool = new SimpleObjectPool<byte[]>(() => new byte[customerByteSize]);
        historyBytePool = new SimpleObjectPool<byte[]>(() => new byte[tables[(int)TableType.History].rowSize]);
        cde = new CountdownEvent(cfg.threadCount * cfg.perThreadDataCount);
        successCounts = new int[cfg.threadCount];
        // abortCounts = new int[cfg.threadCount];

        Debug.Assert(cfg.threadCount <= tpcCfg.NumWh, "Thread count must be less than number of warehouses");
        Debug.Assert(tpcCfg.PartitionsPerThread * cfg.threadCount == tpcCfg.NumWh, "Partitions per thread * thread count must equal number of warehouses");

        this.ol_cnts = new int[tpcCfg.NumDistrict][];
        for (int i = 0; i < tpcCfg.NumDistrict; i++){
            ol_cnts[i] = new int[tpcCfg.NumOrder];
        }
        this.entry_ds = new long[tpcCfg.NumDistrict][];
        for (int i = 0; i < tpcCfg.NumDistrict; i++){
            entry_ds[i] = new long[tpcCfg.NumOrder];
        }

        int numNewOrders = GenerateQueryData();

        stats = new BenchmarkStatistics($"TpccBenchmark", cfg, numNewOrders, cfg.datasetSize, tpcCfg);
        System.Console.WriteLine("Done init");
    }

    private Query GenerateNewOrderQuery(int w_id, int i){
        int rbk = Frnd.Next(1, 100);
        int o_ol_cnt = Frnd.Next(5, 15);
        int[] ol_i_ids = new int[o_ol_cnt];
        int[] ol_supply_w_id = new int[o_ol_cnt];
        int[] ol_quantity = new int[o_ol_cnt];
        for (int j = 0; j < o_ol_cnt; j++){
            // TODO: make not the same between machines
            ol_i_ids[j] = (i / cfg.perThreadDataCount) * (tpcCfg.NumItem / cfg.threadCount) + ((i * o_ol_cnt + j) % (tpcCfg.NumItem / cfg.threadCount)) + 1;
            // bool retry;
            // do {
            //     retry = false;
            //     ol_i_ids[j] = NonUniformRandom(8191, 1, 100000);
            //     for (int k = 0; k < j; k++){
            //         if (ol_i_ids[j] == ol_i_ids[k]){
            //             retry = true;
            //             break;
            //         }
            //     }
            // } while (retry);

            // if (j == o_ol_cnt - 1 && rbk == 1){
            //     ol_i_ids[j] = 0;
            // }

            ol_supply_w_id[j] = w_id;
            if (j == 0) {
                int x = Frnd.Next(1, 100);
                if (x <= tpcCfg.NewOrderCrossPartitionProbability) {
                    while (ol_supply_w_id[j] == w_id) {
                        ol_supply_w_id[j] = Frnd.Next(1, tpcCfg.NumWh + 1);
                    }
                }
            }
            ol_quantity[j] = Frnd.Next(1, 10);
        }
        return new Query(
            w_id,
            Frnd.Next(1, tpcCfg.NumDistrict + 1), 
            NonUniformRandom(1023, 1, 3000),
            o_ol_cnt,
            ol_i_ids,
            ol_supply_w_id,
            ol_quantity
        );
    }

    public Query GeneratePaymentQuery(int w_id, int i) {
        int d_id = Frnd.Next(1, tpcCfg.NumDistrict + 1);
        int c_w_id = w_id;
        int c_d_id;
        int x = Frnd.Next(1, 100);
        if (x <= tpcCfg.PaymentCrossPartitionProbability) {
            while (c_w_id == w_id) {
                c_w_id = Frnd.Next(1, tpcCfg.NumWh + 1);
            }
            c_d_id = Frnd.Next(1, tpcCfg.NumDistrict + 1);
        } else {
            c_d_id = d_id;
        }

        int y = Frnd.Next(1, 100);
        byte[] c_last;
        int c_id;
        if (y <= 60) {
            // c_last = RandLastName(0); // or testing small num
            c_last = RandLastName(NonUniformRandom(255, 0, 999));
            c_id = 0;
        } else {
            c_id = NonUniformRandom(1023, 1, 3000);
            c_last = null;
        }
        return new Query(
            w_id,
            d_id,
            c_id,
            c_d_id,
            c_w_id,
            Frnd.Next(1, 5000),
            c_last
        );
    }

    public int GenerateQueryData() {
        this.queries = new Query[cfg.datasetSize];
        int numNewOrders = 0;
        int partitionsPerMachine = tpcCfg.PartitionsPerThread * cfg.threadCount;
        for (int i = 0; i < queries.Length; i++){
            int thread_idx = i / cfg.perThreadDataCount;
            // int w_id = Frnd.Next((partitionId * tpcCfg.PartitionsPerThread) + 1, (partitionId * tpcCfg.PartitionsPerThread) + 1 + tpcCfg.PartitionsPerThread);
            int w_id = (PartitionId * partitionsPerMachine) + thread_idx * tpcCfg.PartitionsPerThread + (i % tpcCfg.PartitionsPerThread) + 1;
            // randomly assign NewOrder vs Payment
            if (Frnd.Next(1, 100) <= 50){
                numNewOrders++;
                queries[i] = GenerateNewOrderQuery(w_id, i);
            } else {
                queries[i] = GeneratePaymentQuery(w_id, i);
            }
        }
        return numNewOrders;
    }

    public unsafe void NewOrder(Query query, Action<bool> callback){
        TransactionContext ctx = txnManager.Begin();
        ReadOnlySpan<byte> warehouseRow = tables[(int)TableType.Warehouse].Read(new PrimaryKey((int)TableType.Warehouse, query.w_id), tables[(int)TableType.Warehouse].GetSchema(), ctx);
        PrimaryKey districtPk = new PrimaryKey((int)TableType.District, query.w_id, query.d_id);
        ReadOnlySpan<byte> districtRow = tables[(int)TableType.District].Read(districtPk, tables[(int)TableType.District].GetSchema(), ctx);
        ReadOnlySpan<byte> customerRow = tables[(int)TableType.Customer].Read(new PrimaryKey((int)TableType.Customer, query.w_id, query.d_id, query.c_id), tables[(int)TableType.Customer].GetSchema(), ctx);

        bool allLocal = true;
        for (int i = 0; i < query.o_ol_cnt; i++)
        {
            if (query.ol_i_ids[i] == 0) {
                txnManager.Abort(ctx, callback);
                return;
            }            
            if (query.ol_supply_w_id[i] != query.w_id) allLocal = false;
        }
        // update district with increment D_NEXT_O_ID
        ReadOnlySpan<byte> old_d_next_o_id_bytes = ExtractField(TableType.District, TableField.D_NEXT_O_ID, districtRow);
        int old_d_next_o_id = BitConverter.ToInt32(old_d_next_o_id_bytes);
        int new_d_next_o_id = old_d_next_o_id + 1;
        ReadOnlySpan<byte> new_d_next_o_id_bytes = new ReadOnlySpan<byte>(&new_d_next_o_id, sizeof(int));
        tables[(int)TableType.District].Update(ref districtPk, updateDistrictNextOIdTds, new_d_next_o_id_bytes, ctx);

        // insert into order and new order
        PrimaryKey newOrderPk = new PrimaryKey((int)TableType.NewOrder, query.w_id, query.d_id, old_d_next_o_id);
        PrimaryKey orderPk = new PrimaryKey((int)TableType.Order, query.w_id, query.d_id, old_d_next_o_id);
        byte[] insertOrderData = orderBytePool.Checkout();
        SetField(TableType.Order, TableField.O_C_ID, insertOrderData, new ReadOnlySpan<byte>(&query.c_id, sizeof(int)));
        long time = DateTime.Now.ToBinary();
        SetField(TableType.Order, TableField.O_ENTRY_D, insertOrderData, new ReadOnlySpan<byte>(&time, sizeof(long)));
        SetField(TableType.Order, TableField.O_CARRIER_ID, insertOrderData, ZeroAsBytes);
        SetField(TableType.Order, TableField.O_OL_CNT, insertOrderData, new ReadOnlySpan<byte>(&query.o_ol_cnt, sizeof(int)));
        SetField(TableType.Order, TableField.O_ALL_LOCAL, insertOrderData, new ReadOnlySpan<byte>(&allLocal, sizeof(bool)));
        bool success = tables[(int)TableType.Order].Insert(ref orderPk, insertOrderData, ctx);
        if (!success) {
            txnManager.Abort(ctx, callback);
            return;
        }
        success = tables[(int)TableType.NewOrder].Insert(ref newOrderPk, emptyByteArr, ctx);
        if (!success) {
            txnManager.Abort(ctx, callback);
            return;
        }

        float total_amount = 0;
        for (int i = 0; i < query.o_ol_cnt; i++)
        {
            ReadOnlySpan<byte> itemRow = tables[(int)TableType.Item].Read(new PrimaryKey((int)TableType.Item, query.ol_i_ids[i]), tables[(int)TableType.Item].GetSchema(), ctx);
            ReadOnlySpan<byte> stockRow = tables[(int)TableType.Stock].Read(new PrimaryKey((int)TableType.Stock, query.ol_supply_w_id[i], query.ol_i_ids[i]), tables[(int)TableType.Stock].GetSchema(), ctx);

            // update stock
            byte[] updateStockData = stockBytePool.Checkout();
            float i_price = BitConverter.ToSingle(ExtractField(TableType.Item, TableField.I_PRICE, itemRow));

            int s_quantity = BitConverter.ToInt32(ExtractField(TableType.Stock, TableField.S_QUANTITY, stockRow));
            if (s_quantity >= query.ol_quantity[i] + 10) s_quantity -= query.ol_quantity[i];
            else s_quantity += 91 - query.ol_quantity[i];
            new ReadOnlySpan<byte>(&s_quantity, sizeof(int)).CopyTo(updateStockData.AsSpan(updateStockTds[0].Offset));

            int s_ytd = BitConverter.ToInt32(ExtractField(TableType.Stock, TableField.S_YTD, stockRow)) + query.ol_quantity[i];
            new ReadOnlySpan<byte>(&s_ytd, sizeof(int)).CopyTo(updateStockData.AsSpan(updateStockTds[1].Offset));

            int s_order_cnt = BitConverter.ToInt32(ExtractField(TableType.Stock, TableField.S_ORDER_CNT, stockRow)) + 1;
            new ReadOnlySpan<byte>(&s_order_cnt, sizeof(int)).CopyTo(updateStockData.AsSpan(updateStockTds[2].Offset));

            PrimaryKey stockPk = new PrimaryKey((int)TableType.Stock, query.ol_supply_w_id[i], query.ol_i_ids[i]);
            if (query.ol_supply_w_id[i] != query.w_id)
            {
                int s_remote_cnt = BitConverter.ToInt32(ExtractField(TableType.Stock, TableField.S_REMOTE_CNT, stockRow)) + 1;
                new ReadOnlySpan<byte>(&s_remote_cnt, sizeof(int)).CopyTo(updateStockData.AsSpan(updateStockTds[3].Offset));
                tables[(int)TableType.Stock].Update(ref stockPk, updateStockTds, updateStockData, ctx);
            } else {
                tables[(int)TableType.Stock].Update(ref stockPk, updateStockTds[0..3], updateStockData, ctx);
            }

            // insert into order line
            float ol_amount = i_price * query.ol_quantity[i];
            byte[] updateOrderLineData = orderLineBytePool.Checkout();
            fixed (int* b = query.ol_i_ids){
                SetField(TableType.OrderLine, TableField.OL_I_ID, updateOrderLineData, new ReadOnlySpan<byte>(&b[i], sizeof(int)));
            }
            fixed (int* b = query.ol_supply_w_id){
                SetField(TableType.OrderLine, TableField.OL_SUPPLY_W_ID, updateOrderLineData, new ReadOnlySpan<byte>(&b[i], sizeof(int)));
            }
            SetField(TableType.OrderLine, TableField.OL_DELIVERY_D, updateOrderLineData, ZeroAsBytes);
            fixed (int* b = query.ol_quantity){
                SetField(TableType.OrderLine, TableField.OL_QUANTITY, updateOrderLineData, new ReadOnlySpan<byte>(&b[i], sizeof(int)));
            }
            SetField(TableType.OrderLine, TableField.OL_AMOUNT, updateOrderLineData, new ReadOnlySpan<byte>(&ol_amount, sizeof(float)));
            ReadOnlySpan<byte> distInfo = ExtractField(TableType.Stock, TableField.S_DIST_01 + query.d_id - 1, stockRow);
            SetField(TableType.OrderLine, TableField.OL_DIST_INFO, updateOrderLineData, distInfo);
            PrimaryKey orderLinePk = new PrimaryKey((int)TableType.OrderLine, query.w_id, query.d_id, new_d_next_o_id, i);
            success = tables[(int)TableType.OrderLine].Insert(ref orderLinePk, updateOrderLineData, ctx);
            if (!success) {
                txnManager.Abort(ctx, callback);
                return;
            }
            // update total_amount
            float c_discount = BitConverter.ToSingle(ExtractField(TableType.Customer, TableField.C_DISCOUNT, customerRow));
            float w_tax = BitConverter.ToSingle(ExtractField(TableType.Warehouse, TableField.W_TAX, warehouseRow));
            float d_tax = BitConverter.ToSingle(ExtractField(TableType.District, TableField.D_TAX, districtRow));
            total_amount += ol_amount * (1 - c_discount) * (1 + w_tax + d_tax);

            // should be fine since Insert and Update call AddToWriteset which makes a copy of the data? 
            stockBytePool.Return(updateStockData);
            orderLineBytePool.Return(updateOrderLineData);
        }
        txnManager.CommitWithCallback(ctx, callback);
        orderBytePool.Return(insertOrderData);
        return;
    }

    public unsafe void Payment(Query query, Action<bool> callback){
        TransactionContext ctx = txnManager.Begin();
        PrimaryKey warehousePk = new PrimaryKey((int)TableType.Warehouse, query.w_id);
        ReadOnlySpan<byte> warehouseRow = tables[(int)TableType.Warehouse].Read(warehousePk, tables[(int)TableType.Warehouse].GetSchema(), ctx);
        PrimaryKey districtPk = new PrimaryKey((int)TableType.District, query.w_id, query.d_id);
        ReadOnlySpan<byte> districtRow = tables[(int)TableType.District].Read(districtPk, tables[(int)TableType.District].GetSchema(), ctx);
        PrimaryKey customerPk;
        ReadOnlySpan<byte> customerRow;
        if (query.c_id == 0) {
            // TODO
            byte[] secondaryIndexKey = new byte[4+4+16];
            new ReadOnlySpan<byte>(&query.c_w_id, sizeof(int)).CopyTo(secondaryIndexKey);
            new ReadOnlySpan<byte>(&query.c_d_id, sizeof(int)).CopyTo(secondaryIndexKey.AsSpan(sizeof(int)));
            query.c_last.CopyTo(secondaryIndexKey.AsSpan(2 * sizeof(int)));
            (customerRow, customerPk) = tables[(int)TableType.Customer].ReadSecondary(secondaryIndexKey, tables[(int)TableType.Customer].GetSchema(), ctx);
        } else {
            customerPk = new PrimaryKey((int)TableType.Customer, query.c_w_id, query.c_d_id, query.c_id);
            customerRow = tables[(int)TableType.Customer].Read(customerPk, tables[(int)TableType.Customer].GetSchema(), ctx);
        }

        // standard tpcc write to w_ytd
        // update warehouse with increment W_YTD
        float w_ytd = BitConverter.ToSingle(ExtractField(TableType.Warehouse, TableField.W_YTD, warehouseRow)) + query.h_amount;
        tables[(int)TableType.Warehouse].Update(ref warehousePk, updateWarehouseTds, new ReadOnlySpan<byte>(&w_ytd, sizeof(float)), ctx);

        // update district with increment D_YTD
        float d_ytd = BitConverter.ToSingle(ExtractField(TableType.District, TableField.D_YTD, districtRow)) + query.h_amount;
        tables[(int)TableType.District].Update(ref districtPk, updateDistrictYtdTds, new ReadOnlySpan<byte>(&d_ytd, sizeof(float)), ctx);

        // update customer
        byte[] updateCustomerData = customerBytePool.Checkout();
        ReadOnlySpan<byte> c_credit = ExtractField(TableType.Customer, TableField.C_CREDIT, customerRow);
        float c_balance = BitConverter.ToSingle(ExtractField(TableType.Customer, TableField.C_BALANCE, customerRow)) - query.h_amount;
        float c_ytd_payment = BitConverter.ToSingle(ExtractField(TableType.Customer, TableField.C_YTD_PAYMENT, customerRow)) + query.h_amount;
        int c_payment_cnt = BitConverter.ToInt32(ExtractField(TableType.Customer, TableField.C_PAYMENT_CNT, customerRow)) + 1;
        new ReadOnlySpan<byte>(&c_balance, sizeof(float)).CopyTo(updateCustomerData.AsSpan(updateCustomerTds[0].Offset));
        new ReadOnlySpan<byte>(&c_ytd_payment, sizeof(float)).CopyTo(updateCustomerData.AsSpan(updateCustomerTds[1].Offset));
        new ReadOnlySpan<byte>(&c_payment_cnt, sizeof(int)).CopyTo(updateCustomerData.AsSpan(updateCustomerTds[2].Offset));
        if (c_credit.SequenceEqual(BcAsBytes))
        {
            ReadOnlySpan<byte> c_data_old = ExtractField(TableType.Customer, TableField.C_DATA, customerRow);
            int offset = updateCustomerTds[3].Offset;
            // TODO: need spaces between each; not according to spec but not used?
            new ReadOnlySpan<byte>(&query.c_id, sizeof(int)).CopyTo(updateCustomerData.AsSpan(offset));
            offset += sizeof(int);
            new ReadOnlySpan<byte>(&query.c_d_id, sizeof(int)).CopyTo(updateCustomerData.AsSpan(offset));
            offset += sizeof(int);
            new ReadOnlySpan<byte>(&query.c_w_id, sizeof(int)).CopyTo(updateCustomerData.AsSpan(offset));
            offset += sizeof(int);
            new ReadOnlySpan<byte>(&query.d_id, sizeof(int)).CopyTo(updateCustomerData.AsSpan(offset));
            offset += sizeof(int);
            new ReadOnlySpan<byte>(&query.w_id, sizeof(int)).CopyTo(updateCustomerData.AsSpan(offset));
            offset += sizeof(int);
            // TODO: need formatting; not according to spec but not used? 
            new ReadOnlySpan<byte>(&query.h_amount, sizeof(float)).CopyTo(updateCustomerData.AsSpan(offset));
            offset += sizeof(float); 
            c_data_old.Slice(0, 500 - offset).CopyTo(updateCustomerData.AsSpan(offset));
            tables[(int)TableType.Customer].Update(ref customerPk, updateCustomerTds, updateCustomerData, ctx);
        } else {
            tables[(int)TableType.Customer].Update(ref customerPk, updateCustomerTds[0..3], updateCustomerData, ctx);
        }

        // insert into history
        PrimaryKey historyPk = new PrimaryKey((int)TableType.History, query.w_id, query.d_id, query.c_w_id, query.c_d_id, query.c_id, DateTime.Now.ToBinary());
        byte[] insertHistoryData = historyBytePool.Checkout();
        SetField(TableType.History, TableField.H_AMOUNT, insertHistoryData, new ReadOnlySpan<byte>(&query.h_amount, sizeof(float)));
        // manually set field to avoid additional allocation
        (int _, int h_offset) = tables[(int)TableType.History].GetAttrMetadata((long)TableField.H_DATA);
        ExtractField(TableType.Warehouse, TableField.W_NAME, warehouseRow).CopyTo(insertHistoryData.AsSpan(h_offset, 10));
        ExtractField(TableType.District, TableField.D_NAME, districtRow).CopyTo(insertHistoryData.AsSpan(h_offset + 10, 10));
        bool success = tables[(int)TableType.History].Insert(ref historyPk, insertHistoryData, ctx);
        if (!success) {
            txnManager.Abort(ctx, callback);
            return;
        }
        txnManager.CommitWithCallback(ctx, callback);
        customerBytePool.Return(updateCustomerData);
        historyBytePool.Return(insertHistoryData);
    }

    override public void RunTransactions(){
        for (int i = 0; i < cfg.iterationCount; i++){
            // txnManager.Reset();
            // txnManager.Run();

            // new Thread(()=> rpcClient.PopulateTables(cfg, tpcCfg)).Start(); // populate tables in other machines
            // PopulateTables();
            // PopulateItemTable(tables[6], txnManager, 1);
            // Console.WriteLine($"done inserting");
            var opSw = Stopwatch.StartNew();
            // table and txnManager not used
            WorkloadMultiThreadedTransactions(tables[6], txnManager, cfg.ratio);
            cde.Wait();
            opSw.Stop();
            int txnAborts = cfg.datasetSize - successCounts.Sum();
            Console.WriteLine($"abort count {txnAborts}");
            long opMs = opSw.ElapsedMilliseconds;
            stats?.AddTransactionalResult((0, opMs, 0, txnAborts));
            // txnManager.Terminate();
        }

        stats?.ShowAllStats();
        stats?.SaveStatsToFile();
    }

    override protected internal int WorkloadSingleThreadedTransactions(Table table, TransactionManager txnManager, int thread_idx, double ratio)
    {
        Action<bool> incrementCount = (success) => {
            if (success) {
                Interlocked.Increment(ref successCounts[thread_idx]);
            //     Console.WriteLine($"Thread {thread_idx} success count now {successCounts[thread_idx]}");
            // } else {
            //     Interlocked.Increment(ref abortCounts[thread_idx]);
            //     Console.WriteLine($"Thread {thread_idx} failed count now {abortCounts[thread_idx]}");
            }
            cde.Signal();
        };
        for (int i = 0; i < cfg.perThreadDataCount; i += 1){
            int loc = i + (cfg.perThreadDataCount * thread_idx);
            if (queries[loc].isNewOrder){
                NewOrder(queries[loc], incrementCount);
            } else {
                Payment(queries[loc], incrementCount);
            }
        }
        // return abortCount;

        // cde.Wait();
        // return abortCount;
        return 0;
    }

    override protected internal int InsertSingleThreadedTransactions(Table table, TransactionManager txnManager, int thread_idx){
        int abortCount = 0;
        int perThreadDataCount = keys.Count() / cfg.insertThreadCount;
        int remainder = 0;
        // have the last thread handle the remaining data
        if (thread_idx == cfg.insertThreadCount - 1) {
            if (perThreadDataCount == 0) thread_idx = 0;
            remainder = keys.Count() % cfg.insertThreadCount;
        }
        // Console.WriteLine($"thread {thread_idx} writes from {(perThreadDataCount * thread_idx)} to {(perThreadDataCount * thread_idx) + perThreadDataCount + cfg.perTransactionCount - 1 + remainder}");
        for (int i = 0; i < perThreadDataCount + remainder; i += cfg.perTransactionCount){
            TransactionContext ctx = txnManager.Begin();
            for (int j = 0; j < cfg.perTransactionCount; j++) {
                int loc = i + j + (perThreadDataCount * thread_idx);
                if (loc >= keys.Count()) break;
                bool insertSuccess = table.Insert(ref keys[loc], values[loc], ctx);
                if (!insertSuccess) throw new Exception($"Failed to insert record {loc} {keys[loc]} for table {table.GetId()}");
            }
            var success = txnManager.Commit(ctx);
            if (!success){
                abortCount++;
            }
        }
        return abortCount;
    }

    public void GenerateTables(){
        GenerateItemData();
        for (int j = 0; j < tpcCfg.NumWh; j++) {
            int w_id = 1 + j;
            GenerateWarehouseData(w_id);
            GenerateCustomerData(w_id);
            GenerateDistrictData(w_id);
            GenerateHistoryData(w_id);
            GenerateOrderData(w_id);
            GenerateNewOrderData(w_id);
            GenerateOrderLineData(w_id);
            GenerateStockData(w_id);
        }
    }

    public void PopulateTables(){
        int partitionsPerMachine = tpcCfg.PartitionsPerThread * cfg.threadCount;
        for (int j = 0; j < partitionsPerMachine; j++) {
            int w_id = (PartitionId * partitionsPerMachine) + 1 + j;
            foreach (TableType tableType in Enum.GetValues(typeof(TableType)))
            {
                Console.WriteLine($"Start with populating {tableType} for {w_id}");
                switch (tableType) 
                {
                    case TableType.Customer:
                        PopulateCustomerTable(tables[(int)tableType], txnManager, w_id);
                        break;
                    case TableType.Item:
                        PopulateItemTable(tables[(int)tableType], txnManager, w_id);
                        break;
                    case TableType.Warehouse:
                    case TableType.District:
                    case TableType.History:
                    case TableType.NewOrder:
                    case TableType.Order:
                    case TableType.OrderLine:
                    case TableType.Stock:
                        PopulateTable(tables[(int)tableType], txnManager, String.Format(tableDataFiles[tableType], w_id));
                        break;
                    default:
                        throw new ArgumentException("Invalid table type");
                }
                Console.WriteLine($"Done with populating {tableType}");
            }
        }
        System.Console.WriteLine("done inserting");
    }

    public void PopulateTable(ShardedTable table, ShardedTransactionManager txnManager, string filename){
        LoadData(table, filename);
        InsertMultiThreadedTransactions(table, txnManager);
    }
    public void PopulateItemTable(ShardedTable table, ShardedTransactionManager txnManager, int w_id){
        using (var reader = new BinaryReader(File.Open(tableDataFiles[TableType.Item], FileMode.Open, FileAccess.Read, FileShare.Read))) {
            int numItems = reader.ReadInt32();
            values = new byte[numItems][];
            keys = new PrimaryKey[numItems];
            for (int i = 0; i < numItems; i++)
            {
                int pkLen = reader.ReadInt32();
                byte[] pkBytes = reader.ReadBytes(pkLen);
                byte[] data = reader.ReadBytes(table.rowSize);
                PrimaryKey pk = PrimaryKey.FromBytes(pkBytes);

                keys[i] = new PrimaryKey(pk.Table, w_id, pk.Key1);
                values[i] = data;
            }
        }
        InsertMultiThreadedTransactions(table, txnManager);
    }
    public void PopulateCustomerTable(ShardedTable table, ShardedTransactionManager txnManager, int w_id){
        using (var reader = new BinaryReader(File.Open(String.Format(tableDataFiles[TableType.Customer], w_id), FileMode.Open))) {
            int numEntries = reader.ReadInt32();
            keys = new PrimaryKey[numEntries];
            values = new byte[numEntries][];

            for (int i = 0; i < numEntries; i++)
            {
                int keyLen = reader.ReadInt32();
                byte[] pkBytes = reader.ReadBytes(keyLen);
                PrimaryKey pk = PrimaryKey.FromBytes(pkBytes);
                byte[] data = reader.ReadBytes(table.rowSize);
                
                keys[i] = pk;
                values[i] = data;
            }
            
            Dictionary<byte[], PrimaryKey> secondaryIndex = new Dictionary<byte[], PrimaryKey>(new ByteArrayComparer());
            int secondaryIndexCount = reader.ReadInt32();
            for (int i = 0; i < secondaryIndexCount; i++)
            {
                int keyLen = reader.ReadInt32();
                byte[] key = reader.ReadBytes(keyLen);
                int pkLen = reader.ReadInt32();
                byte[] pk = reader.ReadBytes(pkLen);
                secondaryIndex[key] = PrimaryKey.FromBytes(pk);           
            }
            table.AddSecondaryIndex(secondaryIndex, TpccSchema.customerBuildTempPk);
        }
        InsertMultiThreadedTransactions(table, txnManager);
    }

    private void LoadData(Table table, string filename){
        using (var reader = new BinaryReader(File.Open(filename, FileMode.Open))) {
            int numItems = reader.ReadInt32();
            keys = new PrimaryKey[numItems];
            values = new byte[numItems][];
            for (int i = 0; i < numItems; i++)
            {
                int pkLen = reader.ReadInt32();
                byte[] pkBytes = reader.ReadBytes(pkLen);
                byte[] data = reader.ReadBytes(table.rowSize);
                PrimaryKey pk = PrimaryKey.FromBytes(pkBytes);

                keys[i] = pk;
                values[i] = data;
            }
        }
    }
    public void GenerateWarehouseData(int w_id){
        Table table = tables[(int)TableType.Warehouse];
        using (var writer = new BinaryWriter(File.Open(String.Format(tableDataFiles[TableType.Warehouse], w_id), FileMode.Create))) {
            writer.Write(1);
            byte[] data = new byte[table.rowSize];
            Span<byte> span = new Span<byte>(data);
            
            int offset = 0;
            RandomByteString(6, 10).CopyTo(span); // W_NAME
            offset += 10;
            RandomByteString(10, 20).CopyTo(span.Slice(offset)); // W_STREET_1
            offset += 20;
            RandomByteString(10, 20).CopyTo(span.Slice(offset)); // W_STREET_2
            offset += 20;
            RandomByteString(10, 20).CopyTo(span.Slice(offset)); // W_CITY
            offset += 20;
            RandomByteString(2, 2).CopyTo(span.Slice(offset)); // W_STATE
            offset += 2;
            RandZip().CopyTo(span.Slice(offset)); // W_ZIP
            offset += 9;
            BitConverter.GetBytes(0.1000f).CopyTo(span.Slice(offset)); // W_TAX
            offset += 4;
            BitConverter.GetBytes(3000000.00f).CopyTo(span.Slice(offset)); // W_YTD
            PrimaryKey pk = new PrimaryKey(table.GetId(), w_id);
            // PK: W_ID

            byte[] pkBytes = pk.ToBytes();
            writer.Write(pkBytes.Length);
            writer.Write(pkBytes);
            writer.Write(data);
        }
    }
    public void GenerateDistrictData(int w_id){
        Table table = tables[(int)TableType.District];
        using (var writer = new BinaryWriter(File.Open(String.Format(tableDataFiles[TableType.District], w_id), FileMode.Create))) {
            writer.Write(tpcCfg.NumDistrict);
            for (int i = 1; i <= tpcCfg.NumDistrict; i++)
            {
                byte[] data = new byte[table.rowSize];
                Span<byte> span = new Span<byte>(data);
                
                int offset = 0;
                RandomByteString(6, 10).CopyTo(span.Slice(offset)); // D_NAME
                offset += 10;
                RandomByteString(10, 20).CopyTo(span.Slice(offset)); // D_STREET_1
                offset += 20;
                RandomByteString(10, 20).CopyTo(span.Slice(offset)); // D_STREET_2
                offset += 20;
                RandomByteString(10, 20).CopyTo(span.Slice(offset)); // D_CITY
                offset += 20;
                RandomByteString(2, 2).CopyTo(span.Slice(offset)); // D_STATE
                offset += 2;
                RandZip().CopyTo(span.Slice(offset)); // D_ZIP
                offset += 9;
                RandFloat(0, 2000, 10000).CopyTo(span.Slice(offset)); // D_TAX
                offset += 4;
                BitConverter.GetBytes(30000).CopyTo(span.Slice(offset)); // D_YTD
                offset += 4;
                BitConverter.GetBytes(tpcCfg.NumOrder + 1).CopyTo(span.Slice(offset)); // D_NEXT_O_ID
                PrimaryKey pk = new PrimaryKey(table.GetId(), w_id, i);
                // PK: D_W_ID, D_ID

                byte[] pkBytes = pk.ToBytes();
                writer.Write(pkBytes.Length);
                writer.Write(pkBytes);
                writer.Write(data);
            }
        }
    }
    public unsafe void GenerateCustomerData(int w_id){
        ConcurrentDictionary<byte[], PrimaryKey> secondaryIndex = new ConcurrentDictionary<byte[], PrimaryKey>(new ByteArrayComparer());
        // group rows by new index attribute 
        Dictionary<byte[], List<(PrimaryKey, byte[])>> groupByAttr = new Dictionary<byte[], List<(PrimaryKey, byte[])>>();
        using (var writer = new BinaryWriter(File.Open(String.Format(tableDataFiles[TableType.Customer], w_id), FileMode.Create))) {
            writer.Write(tpcCfg.NumDistrict * tpcCfg.NumCustomer);
            for (int i = 1; i <= tpcCfg.NumDistrict; i++)
            {
                for (int j = 1; j <= tpcCfg.NumCustomer; j++)
                {
                    Table table = tables[(int)TableType.Customer];
                    byte[] data = new byte[table.rowSize];
                    Span<byte> span = new Span<byte>(data);
                    
                    int offset = 0;
                    RandomByteString(8, 16).CopyTo(span.Slice(offset)); // C_FIRST
                    offset += 16;
                    new byte[]{(byte)'O', (byte)'E'}.CopyTo(span.Slice(offset)); // C_MIDDLE
                    offset += 2;
                    byte[] lastName = RandLastName(j <= 1000 ? j - 1 : NonUniformRandom(255, 0, 999));
                    lastName.CopyTo(span.Slice(offset)); // C_LAST
                    offset += 16;
                    RandomByteString(10, 20).CopyTo(span.Slice(offset)); // C_STREET_1
                    offset += 20;
                    RandomByteString(10, 20).CopyTo(span.Slice(offset)); // C_STREET_2
                    offset += 20;
                    RandomByteString(10, 20).CopyTo(span.Slice(offset)); // C_CITY
                    offset += 20;
                    RandomByteString(2, 2).CopyTo(span.Slice(offset)); // C_STATE
                    offset += 2;
                    RandZip().CopyTo(span.Slice(offset)); // C_ZIP
                    offset += 9;
                    RandomByteString(16, 16).CopyTo(span.Slice(offset)); // C_PHONE
                    offset += 16;
                    BitConverter.GetBytes(DateTime.Now.ToBinary()).CopyTo(span.Slice(offset)); // C_SINCE
                    offset += 8;
                    new byte[]{(byte)(Frnd.Next(0, 10) == 1 ? 'B' : 'G'), (byte)'C'}.CopyTo(span.Slice(offset)); // C_CREDIT
                    offset += 2;
                    BitConverter.GetBytes(50000).CopyTo(span.Slice(offset)); // C_CREDIT_LIM
                    offset += 4;
                    RandFloat(0, 5000, 10000).CopyTo(span.Slice(offset)); // C_DISCOUNT
                    offset += 4;
                    BitConverter.GetBytes(-10).CopyTo(span.Slice(offset)); // C_BALANCE
                    offset += 4;
                    BitConverter.GetBytes(10).CopyTo(span.Slice(offset)); // C_YTD_PAYMENT
                    offset += 4;
                    BitConverter.GetBytes(1).CopyTo(span.Slice(offset)); // C_PAYMENT_CNT
                    offset += 4;
                    BitConverter.GetBytes(0).CopyTo(span.Slice(offset)); // C_DELIVERY_CNT
                    offset += 4;
                    RandomByteString(300, 500).CopyTo(span.Slice(offset)); // C_DATA
                    PrimaryKey pk = new PrimaryKey(table.GetId(), w_id, i, j);
                    // PK: C_W_ID, C_D_ID, C_ID
                    byte[] pkBytes = pk.ToBytes();
                    writer.Write(pkBytes.Length);
                    writer.Write(pkBytes);
                    writer.Write(data);

                    byte[] key = BitConverter.GetBytes(w_id).Concat(BitConverter.GetBytes(i)).Concat(lastName).ToArray();
                    if (!groupByAttr.ContainsKey(key)){
                        groupByAttr[key] = new List<(PrimaryKey, byte[])>();
                    }
                    groupByAttr[key].Add((pk, data));
                    

                }

                foreach (var entry in groupByAttr){
                    List<(PrimaryKey, byte[])> sameLastNames = entry.Value;
                    // sort by C_FIRST
                    sameLastNames.Sort((a, b) => {
                        return Util.CompareArrays(a.Item2[0..16], b.Item2[0..16]);
                    });

                    secondaryIndex[entry.Key] = sameLastNames[(sameLastNames.Count - 1) / 2].Item1;
                }
            }

            // write secondary index to file
            writer.Write(secondaryIndex.Count);
            foreach (var entry in secondaryIndex){
                byte[] key = entry.Key;
                byte[] pk = entry.Value.ToBytes();
                writer.Write(key.Length);
                writer.Write(key);
                writer.Write(pk.Length);
                writer.Write(pk);
            }
        }
    }
    public void GenerateHistoryData(int w_id){
        Table table = tables[(int)TableType.History];
        using (var writer = new BinaryWriter(File.Open(String.Format(tableDataFiles[TableType.History], w_id), FileMode.Create))) {
            writer.Write(tpcCfg.NumDistrict * tpcCfg.NumCustomer);
            for (int i = 1; i <= tpcCfg.NumDistrict; i++)
            {
                for (int j = 1; j <= tpcCfg.NumCustomer; j++)
                {
                    byte[] data = new byte[table.rowSize];
                    Span<byte> span = new Span<byte>(data);
                    
                    int offset = 0;
                    BitConverter.GetBytes(10).CopyTo(span.Slice(offset)); // H_AMOUNT
                    offset += 4;
                    RandomByteString(12, 24).CopyTo(span.Slice(offset)); // H_DATA
                    PrimaryKey pk = new PrimaryKey(table.GetId(), w_id, i, w_id, i, j, DateTime.Now.ToBinary());
                    // PK: H_W_ID, H_D_ID, H_C_W_ID, H_C_D_ID, H_C_ID, H_DATE

                    byte[] pkBytes = pk.ToBytes();
                    writer.Write(pkBytes.Length);
                    writer.Write(pkBytes);
                    writer.Write(data);
                }
            }
        }
    }
    public void GenerateNewOrderData(int w_id){
        Table table = tables[(int)TableType.NewOrder];
        using (var writer = new BinaryWriter(File.Open(String.Format(tableDataFiles[TableType.NewOrder], w_id), FileMode.Create))) {
            writer.Write(tpcCfg.NumDistrict * (tpcCfg.NumOrder - 2100));
            for (int i = 1; i <= tpcCfg.NumDistrict; i++)
            {
                for (int j = 2101; j <= tpcCfg.NumOrder; j++)
                {
                    byte[] data = new byte[table.rowSize];
                    PrimaryKey pk = new PrimaryKey(table.GetId(), w_id, i, j);
                    // PK: NO_W_ID, NO_D_ID, NO_O_ID
                    byte[] pkBytes = pk.ToBytes();
                    writer.Write(pkBytes.Length);
                    writer.Write(pkBytes);
                    writer.Write(data);
                }
            }
        }
    }
    public void GenerateOrderData(int w_id){
        int[] cids = new int[tpcCfg.NumOrder];
        for (int i = 1; i <= tpcCfg.NumOrder; i++) {
            cids[i-1] = i;
        }
        
        Table table = tables[(int)TableType.Order];
        using (var writer = new BinaryWriter(File.Open(String.Format(tableDataFiles[TableType.Order], w_id), FileMode.Create))) {
            writer.Write(tpcCfg.NumDistrict * tpcCfg.NumOrder);
            for (int i = 1; i <= tpcCfg.NumDistrict; i++)
            {
                Util.Shuffle(Frnd, cids);
                for (int j = 1; j <= tpcCfg.NumOrder; j++)
                {
                    byte[] data = new byte[table.rowSize];
                    Span<byte> span = new Span<byte>(data);
                    
                    int offset = 0;
                    BitConverter.GetBytes(cids[j-1]).CopyTo(span.Slice(offset)); // O_C_ID
                    offset += 4;
                    BitConverter.GetBytes(DateTime.Now.ToBinary()).CopyTo(span.Slice(offset)); // O_ENTRY_D
                    offset += 8;
                    BitConverter.GetBytes(j < 2101 ? Frnd.Next(1,10) : 0).CopyTo(span.Slice(offset)); // O_CARRIER_ID
                    offset += 4;
                    int ol_cnt = Frnd.Next(5,15);
                    ol_cnts[i-1][j-1] = ol_cnt;
                    BitConverter.GetBytes(ol_cnt).CopyTo(span.Slice(offset)); // O_OL_CNT
                    offset += 4;
                    BitConverter.GetBytes(true).CopyTo(span.Slice(offset)); // O_ALL_LOCAL
                    PrimaryKey pk = new PrimaryKey(table.GetId(), w_id, i, j);
                    // PK: O_W_ID, O_D_ID, O_ID

                    byte[] pkBytes = pk.ToBytes();
                    writer.Write(pkBytes.Length);
                    writer.Write(pkBytes);
                    writer.Write(data);
                }
            }
        }
    }

    // assumes that order data has been generated
    public void GenerateOrderLineData(int w_id){
        Table table = tables[(int)TableType.OrderLine];
        using (var writer = new BinaryWriter(File.Open(String.Format(tableDataFiles[TableType.OrderLine], w_id), FileMode.Create))) {
            int count = ol_cnts.Sum(x => x.Sum(y => y));
            writer.Write(count);

            for (int i = 1; i <= tpcCfg.NumDistrict; i++)
            {
                for (int j = 1; j <= tpcCfg.NumOrder; j++)
                {
                    int olCnt = ol_cnts[i-1][j-1];
                    long oEntryD = entry_ds[i-1][j-1];

                    for (int k = 1; k <= olCnt; k++)
                    {
                        byte[] data = new byte[table.rowSize];
                        Span<byte> span = new Span<byte>(data);
                        
                        int offset = 0;
                        BitConverter.GetBytes(Frnd.Next(1,100000)).CopyTo(span.Slice(offset)); // OL_I_ID
                        offset += 4;
                        BitConverter.GetBytes(w_id).CopyTo(span.Slice(offset)); // OL_SUPPLY_W_ID
                        offset += 4;
                        BitConverter.GetBytes(j < 2101 ? oEntryD : 0).CopyTo(span.Slice(offset)); // OL_DELIVERY_D
                        offset += 8;
                        BitConverter.GetBytes(5).CopyTo(span.Slice(offset)); // OL_QUANTITY
                        offset += 4;
                        BitConverter.GetBytes(j < 2101 ? 0 : Frnd.Next(1, 999999) / 100f).CopyTo(span.Slice(offset)); // OL_AMOUNT
                        offset += 4;
                        RandomByteString(24, 24).CopyTo(span.Slice(offset)); // OL_DIST_INFO

                        PrimaryKey pk = new PrimaryKey(table.GetId(), w_id, i, j, k);
                        // PK: OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER

                        byte[] pkBytes = pk.ToBytes();
                        writer.Write(pkBytes.Length);
                        writer.Write(pkBytes);
                        writer.Write(data);
                    }
                }
            }
        }
    }

    private void GenerateItemData(){
        Table table = tables[(int)TableType.Item];
        using (var writer = new BinaryWriter(File.Open(tableDataFiles[TableType.Item], FileMode.Create))) {
            writer.Write(tpcCfg.NumItem);
            for (int i = 1; i <= tpcCfg.NumItem; i++)
            {
                byte[] data = new byte[table.rowSize];
                Span<byte> span = new Span<byte>(data);
                
                int offset = 0;
                BitConverter.GetBytes(Frnd.Next(1,10000)).CopyTo(span.Slice(offset)); // I_IM_ID
                offset += 4;
                RandomByteString(14, 24).CopyTo(span.Slice(offset)); // I_NAME
                offset += 24;
                BitConverter.GetBytes(Frnd.Next(1,100)).CopyTo(span.Slice(offset)); // I_PRICE
                offset += 4;
                byte[] i_data = RandomByteString(26, 50);
                int strLen = Encoding.ASCII.GetString(i_data).IndexOf(' ');
                if (Frnd.Next(1,10) == 1) {
                    int start = Frnd.Next(0, strLen - ORIGINAL.Length);
                    for (int j = 0; j < ORIGINAL.Length; j++) {
                        i_data[start + j] = ORIGINAL[j];
                    }
                }
                i_data.CopyTo(span.Slice(offset)); // I_DATA
                // PK: I_ID
                PrimaryKey pk = new PrimaryKey(table.GetId(), i);

                byte[] pkBytes = pk.ToBytes();
                writer.Write(pkBytes.Length);
                writer.Write(pkBytes);
                writer.Write(data);
            }
        }
    }

    public void GenerateStockData(int w_id) {
        Table table = tables[(int)TableType.Stock];
        using (var writer = new BinaryWriter(File.Open(String.Format(tableDataFiles[TableType.Stock], w_id), FileMode.Create))) {
            writer.Write(tpcCfg.NumStock);
            for (int i = 1; i <= tpcCfg.NumStock; i++)
            {
                byte[] data = new byte[table.rowSize];
                Span<byte> span = new Span<byte>(data);
                
                int offset = 0;
                BitConverter.GetBytes(Frnd.Next(10,100)).CopyTo(span.Slice(offset)); // S_QUANTITY
                offset += 4;
                RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_01
                offset += 24;
                RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_02
                offset += 24;
                RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_03
                offset += 24;
                RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_04
                offset += 24;
                RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_05
                offset += 24;
                RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_06
                offset += 24;
                RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_07
                offset += 24;
                RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_08
                offset += 24;
                RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_09
                offset += 24;
                RandomByteString(24, 24).CopyTo(span.Slice(offset)); // S_DIST_10
                offset += 24;
                BitConverter.GetBytes(0).CopyTo(span.Slice(offset)); // S_YTD
                offset += 4;
                BitConverter.GetBytes(0).CopyTo(span.Slice(offset)); // S_ORDER_CNT
                offset += 4;
                BitConverter.GetBytes(0).CopyTo(span.Slice(offset)); // S_REMOTE_CNT
                offset += 4;
                byte[] s_data = RandomByteString(26, 50);
                int strLen = Encoding.ASCII.GetString(s_data).IndexOf(' ');
                if (Frnd.Next(1,10) == 1) {
                    int start = Frnd.Next(0, strLen - ORIGINAL.Length);
                    for (int j = 0; j < ORIGINAL.Length; j++) {
                        s_data[start + j] = ORIGINAL[j];
                    }
                }
                s_data.CopyTo(span.Slice(offset)); // S_DATA
                PrimaryKey pk = new PrimaryKey(table.GetId(), w_id, i);
                // PK: S_W_ID, S_I_ID

                byte[] pkBytes = pk.ToBytes();
                writer.Write(pkBytes.Length);
                writer.Write(pkBytes);
                writer.Write(data);
            }
        }
    }

    private ReadOnlySpan<byte> ExtractField(TableType tableType, TableField field, ReadOnlySpan<byte> row) {
        (int size, int offset) = tables[(int)tableType].GetAttrMetadata((long)field);
        return row.Slice(offset, size);
    }

    private void SetField(TableType tableType, TableField field, byte[] row, ReadOnlySpan<byte> value) {
        (int size, int offset) = tables[(int)tableType].GetAttrMetadata((long)field);
        value.CopyTo(row.AsSpan(offset));
    }

    // private (byte[], TupleDesc[]) BuildUpdate(byte[] data, TupleDesc[] tds, TableType tableType, TableField field, byte[] value){
    //     int size = tables[(int)tableType].GetAttrMetadata((long)field).Item1;
    //     int offset = tds.Length == 0 ? 0 : tds[tds.Length - 1].Offset + tds[tds.Length - 1].Size;
    //     return (data.Concat(value).ToArray(), tds.Append(new TupleDesc((int)field, size, offset)).ToArray());
    // }
    private void PrintByteArray(byte[] arr){
        foreach (byte b in arr){
            Console.Write(b + " ");
        }
        Console.WriteLine();
    }
    
    /// <summary>
    /// Generates a random byte array with the given length, 
    /// padded with spaces until it reaches the maximum length 
    /// </summary>
    /// <param name="strMin">
    ///   minimum size of the string
    /// </param>
    /// <param name="strMax"></param>
    /// <returns>Random string</returns>
    private static byte[] RandomByteString(int strMin, int strMax)
    {
        byte[] randomString = new byte[strMax];
        int stringLen = Frnd.Next(strMin, strMax);
        for (int x = 0; x < strMax; ++x)
        {
            if (x < stringLen)
                randomString[x] = RandHold[Frnd.Next(0, 62)];
            else
                randomString[x] = SpaceAsByte;
        }

        return randomString;
    }

    /// <summary>
    /// Generates a random zip code byte array with the given length
    /// </summary>
    /// <returns>Random string</returns>
    private static byte[] RandZip()
    {
        byte[] holdZip = new byte[5];
        for (int x = 0; x < 4; ++x)
        {
            holdZip[x] = ZipRandHold[Frnd.Next(0, 9)];
        }
        for (int x = 4; x < 5; ++x)
        {
            holdZip[x] = (byte)'1';
        }

        return holdZip;
    }

    /// <summary>
    /// Generates a 8 byte array representation of a random float 
    /// </summary>
    /// <param name="min">min value random</param>
    /// <param name="max">max value random</param>
    /// <param name="divisor">divisor</param>
    /// <returns>8 byte</returns>
    private static byte[] RandFloat(int min, int max, int divisor){
        return BitConverter.GetBytes((float)Frnd.Next(min, max) / divisor);
    }

    /// <summary>
    /// Derived from coco's Random.h
    /// </summary>
    /// <param name="A"></param>
    /// <param name="C"></param>
    /// <param name="min"></param>
    /// <param name="max"></param>
    /// <returns></returns>
    private static int NonUniformRandom(int A, int min, int max){
        return ((Frnd.Next(0, A) | Frnd.Next(min, max)) % (max - min + 1)) + min;
    }

    private static byte[] RandLastName(int n){
        // int len = LastNames[n / 100].Length + LastNames[(n / 10) % 10].Length + LastNames[n % 10].Length;
        byte[] lastname = new byte[16];
        LastNames[n / 100].CopyTo(lastname, 0);
        LastNames[(n / 10) % 10].CopyTo(lastname, LastNames[n / 100].Length);
        LastNames[n % 10].CopyTo(lastname, LastNames[n / 100].Length + LastNames[(n / 10) % 10].Length);
        return lastname;
    }

}
}