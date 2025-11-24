
using Orleans;

namespace TwoPhaseCommitOrleans;

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
    public static List<Func<Task>> GenerateWorkload(IClusterClient client, int numWarehouses, long numTransactions)
    {
        var workload = new List<Func<Task>>((int)numTransactions);
        var rand = new Random();

        for (long i = 0; i < numTransactions; i++)
        {
            var homeWarehouse = (byte) rand.Next(1, numWarehouses + 1);

            // Find the correct client for this transaction's home warehouse
            int choice = rand.Next(1, 101);
            
            if (choice <= 49)
                workload.Add(GenerateNewOrderRequest(client, rand, homeWarehouse, numWarehouses));
            else if (choice <= 96)
                workload.Add(GeneratePaymentRequest(client, rand, homeWarehouse, numWarehouses));
            else
                workload.Add(GeneratePaymentRequest(client, rand, homeWarehouse, numWarehouses));
        }

        return workload;
    }

    private static Func<Task> GenerateNewOrderRequest(IClusterClient client, Random rand, int homeWarehouseId, int numWarehouses)
    {
        int w_id = homeWarehouseId;
        int d_id = rand.Next(1, TpccConstants.NUM_DISTRICTS_PER_WAREHOUSE + 1);
        int c_id = TpccConstants.GetCustomerId(rand);
        int ol_cnt = rand.Next(5, 16); // 5 to 15 items
        var ols = new List<OrderLine>(ol_cnt);
        
        for (int i = 0; i < ol_cnt; i++)
        {
            int item_id = TpccConstants.GetItemId(rand); // Always valid now
            int supply_w_id;

            // 1% of items are from a remote warehouse
            if (rand.NextDouble() <= TpccConstants.ORDER_ITEM_REMOTE_PROB && numWarehouses > 1)
            {
                do
                {
                    supply_w_id = rand.Next(1, numWarehouses + 1);
                } while (supply_w_id == w_id);
            }
            else
            {
                supply_w_id = w_id;
            }

            ols.Add(new OrderLine
            {
                ItemId = item_id,
                SupplyWarehouseId = supply_w_id,
                Quantity = rand.Next(1, 11) // 1 to 10
            });
        }

        return async () => 
        {
            await client.GetGrain<INewOrderWorker>($"{w_id}-{d_id}-{c_id}").ExecuteAsync(w_id, d_id, c_id, ols);
        };
    }

    private static Func<Task> GeneratePaymentRequest(IClusterClient client, Random rand, int homeWarehouseId, int numWarehouses)
    {
        int w_id = homeWarehouseId;
        int d_id = rand.Next(1, TpccConstants.NUM_DISTRICTS_PER_WAREHOUSE + 1);
        int c_w_id, c_d_id;

        // 15% of payments are for a remote warehouse
        if (rand.NextDouble() <= TpccConstants.PAYMENT_REMOTE_PROB && numWarehouses > 1)
        {
            do
            {
                c_w_id = rand.Next(1, numWarehouses + 1);
            } while (c_w_id == w_id);

            c_d_id = rand.Next(1, TpccConstants.NUM_DISTRICTS_PER_WAREHOUSE + 1);
        }
        else
        {
            c_w_id = w_id;
            c_d_id = d_id;
        }

        int c_id = TpccConstants.GetCustomerId(rand);
        var amount = (rand.NextDouble() * 4999.0) + 1.0;

        return async () =>
        {
            await client.GetGrain<IPaymentWorker>($"{w_id}-{d_id}").ExecuteAsync(w_id, d_id, c_id, c_w_id, c_d_id, amount);
        };
    }

    private static Func<Task> GenerateOrderStatusRequest(IClusterClient client, Random rand, int homeWarehouseId)
    {
        int w_id = homeWarehouseId;
        int d_id = rand.Next(1, TpccConstants.NUM_DISTRICTS_PER_WAREHOUSE + 1);
        int c_id = TpccConstants.GetCustomerId(rand);

        return async () =>
        {
            await client.GetGrain<ICustomerGrain>($"{w_id}-{d_id}-{c_id}").OrderStatus();
        };
    }
    
    public static List<Item> GenerateItems(Random rand)
    {
        var result = new List<Item>();
        for (var i = 0; i < TpccConstants.NUM_ITEMS; i++)
        {
            result.Add(new Item
            {
                Id = i,
                Price = rand.NextDouble() * 99.0 + 1,
            });
        }

        return result;
    }
}