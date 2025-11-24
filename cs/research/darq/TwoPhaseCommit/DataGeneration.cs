using System.Diagnostics;
using protobuf;

namespace TwoPhaseCommit;

/// <summary>
/// Holds TPC-C specification constants and the NURand generator.
/// </summary>
public static class TpccConstants
{
    public const int NUM_WAREHOUSES = 40;
    public const int NUM_ITEMS = 100000;
    public const int NUM_CUSTOMERS_PER_DISTRICT = 3000;
    public const int NUM_DISTRICTS_PER_WAREHOUSE = 10;
    public const int TXN_MAX_RETRY = 10;


    // NURand constants for specific fields
    private const int A_C_LAST = 255;
    private const int A_C_ID = 1023;
    private const int A_OL_I_ID = 8191;

    // Pre-calculated 'C' values for NURand.
    // In a fully compliant-spec, these are generated once.
    // We can just use pre-computed values or generate them statically.
    private static readonly int C_C_LAST = new Random().Next(0, A_C_LAST + 1);
    private static readonly int C_C_ID = new Random().Next(0, A_C_ID + 1);
    private static readonly int C_OL_I_ID = new Random().Next(0, A_OL_I_ID + 1);

    public static readonly double PAYMENT_REMOTE_PROB = 0.5;
    public static readonly double ORDER_ITEM_REMOTE_PROB = 0.2;
    

    // According to TPC-C standard
    public static int NonUniformRandom(int A, int x, int y, int C, Random rand)
    {
        int part1 = rand.Next(0, A + 1);
        int part2 = rand.Next(x, y + 1);
        return (((part1 | part2) + C) % (y - x + 1)) + x;
    }

    /// <summary>
    /// Gets a non-uniform random Customer ID.
    /// </summary>
    public static int GetCustomerId(Random rand)
    {
        return NonUniformRandom(A_C_ID, 1, NUM_CUSTOMERS_PER_DISTRICT, C_C_ID, rand);
    }

    /// <summary>
    /// Gets a non-uniform random Item ID.
    /// </summary>
    public static int GetItemId(Random rand)
    {
        // Note: invalid item ID is not simulated in this implementation
        return NonUniformRandom(A_OL_I_ID, 1, NUM_ITEMS, C_OL_I_ID, rand);
    }
}

/// <summary>
/// Pre-generates a TPC-C workload.
/// </summary>
public class TpccWorkloadGenerator
{
    public static List<List<Func<Task>>> GenerateWorkload(
        Dictionary<byte, TpccShardService.TpccShardServiceClient> clients, long numTransactions)
    {
        var workload = new List<List<Func<Task>>>();
        for (var i = 0; i < TpccConstants.NUM_WAREHOUSES; i++)
            workload.Add(new List<Func<Task>>());
        
        var rand = new Random();

        for (long i = 0; i < numTransactions; i++)
        {
            var homeWarehouse = (byte) rand.Next(0, TpccConstants.NUM_WAREHOUSES);

            // Find the correct client for this transaction's home warehouse
            TpccShardService.TpccShardServiceClient client = clients[homeWarehouse];
            var choice = rand.NextDouble();

            if (choice <= 0.49)
            {
                var req = GenerateNewOrderRequest(rand, homeWarehouse);
                // Capture the 'client' and 'req' in the lambda
                workload[homeWarehouse].Add(async () => { await client.NewOrderAsync(req); });
            }
            else if (choice <= 0.96)
            {
                var req = GeneratePaymentRequest(rand, homeWarehouse);
                workload[homeWarehouse].Add(async () => { await client.PaymentAsync(req); });
            }
            else
            {
                var req = GenerateOrderStatusRequest(rand, homeWarehouse);
                workload[homeWarehouse].Add(async () => { await client.OrderStatusAsync(req); });
            }
        }

        return workload;
    }

    private static NewOrderRequest GenerateNewOrderRequest(Random rand, int homeWarehouseId)
    {
        int w_id = homeWarehouseId;
        int d_id = rand.Next(0, TpccConstants.NUM_DISTRICTS_PER_WAREHOUSE);
        int c_id = TpccConstants.GetCustomerId(rand);
        int ol_cnt = rand.Next(5, 16); // 5 to 15 items

        var request = new NewOrderRequest
        {
            WId = w_id,
            DId = d_id,
            CId = c_id
        };

        for (int i = 0; i < ol_cnt; i++)
        {
            int item_id = TpccConstants.GetItemId(rand); // Always valid now
            int supply_w_id;

            // 1% of items are from a remote warehouse
            if (rand.NextDouble() <= TpccConstants.ORDER_ITEM_REMOTE_PROB && TpccConstants.NUM_WAREHOUSES > 1)
            {
                do
                {
                    supply_w_id = rand.Next(0, TpccConstants.NUM_WAREHOUSES);
                } while (supply_w_id == w_id);
            }
            else
            {
                supply_w_id = w_id;
            }

            request.Items.Add(new protobuf.OrderLine
            {
                ItemId = item_id,
                WSupplyingId = supply_w_id,
                Quantity = rand.Next(1, 11) // 1 to 10
            });
        }

        return request;
    }

    private static PaymentRequest GeneratePaymentRequest(Random rand, int homeWarehouseId)
    {
        int w_id = homeWarehouseId;
        int d_id = rand.Next(0, TpccConstants.NUM_DISTRICTS_PER_WAREHOUSE);
        int c_w_id, c_d_id;

        // 15% of payments are for a remote warehouse
        if (rand.NextDouble() <= TpccConstants.PAYMENT_REMOTE_PROB && TpccConstants.NUM_WAREHOUSES > 1)
        {
            do
            {
                c_w_id = rand.Next(0, TpccConstants.NUM_WAREHOUSES);
            } while (c_w_id == w_id);

            c_d_id = rand.Next(0, TpccConstants.NUM_DISTRICTS_PER_WAREHOUSE);
        }
        else
        {
            c_w_id = w_id;
            c_d_id = d_id;
        }

        int c_id = TpccConstants.GetCustomerId(rand);
        var amount = (rand.NextDouble() * 4999.0) + 1.0;

        return new PaymentRequest
        {
            WId = w_id,
            DId = d_id,
            CId = c_id,
            CwId = c_w_id,
            CdId = c_d_id,
            Amount = amount
        };
    }

    private static OrderStatusRequest GenerateOrderStatusRequest(Random rand, int homeWarehouseId)
    {
        int w_id = homeWarehouseId;
        int d_id = rand.Next(0, TpccConstants.NUM_DISTRICTS_PER_WAREHOUSE);
        int c_id = TpccConstants.GetCustomerId(rand);

        return new OrderStatusRequest
        {
            WId = w_id,
            DId = d_id,
            CId = c_id
        };
    }
    
    public static List<Item> GenerateItems(Random rand)
    {
        var result = new List<Item>();
        for (var i = 1; i < TpccConstants.NUM_ITEMS + 1; i++)
        {
            result.Add(new Item
            {
                iId = i,
                iPrice = rand.NextDouble() * 99.0 + 1,
            });
        }

        return result;
    }

    public static void GenerateShardData(TpccShard shard, LoadDataRequest request)
    {
        var rand = new Random(request.Seed);

        // Load the Item table which is read-only and replicated
        foreach (var i in request.Items)
            shard.items[i.IId] = new Item
            {
                iId = i.IId,
                iPrice = i.IPrice
            };

        // Generate data per warehouse
        foreach (byte wId in request.AssignedWarehouseIds)
        {
            // Generate Warehouse
            shard.warehouses[wId] = new Warehouse
            {
                wId = wId,
                wYtd = 300000.0
            };

            foreach (var i in shard.items.Keys)
                shard.stocks[new StockKey(wId, i)] = new Stock
                {
                    sIId = i,
                    sWId = wId,
                    sQuantity = rand.Next(10, 101)
                };

            for (byte d = 0; d < TpccConstants.NUM_DISTRICTS_PER_WAREHOUSE; d++)
            {
                shard.districts[new DistrictKey(wId, d)] = new District
                {
                    dId = d,
                    dWId = wId,
                    dYtd = 30000.0,
                    dNextOrderId = 1
                };
                
                for (int c = 1; c < TpccConstants.NUM_CUSTOMERS_PER_DISTRICT + 1; c++)
                {

                    shard.customers[new CustomerKey(wId, d, c)] = new Customer
                    {
                        cID = c,
                        cDId = d,
                        cWId = wId,
                        cBalance = -10.00,
                        cYtdPayment = 0.0,
                        cPaymentCnt = 0
                    };
                    
                }
            }
        }
    }
}