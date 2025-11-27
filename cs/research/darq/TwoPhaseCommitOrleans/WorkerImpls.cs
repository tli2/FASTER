using Orleans;
using Orleans.Concurrency;

namespace TwoPhaseCommitOrleans;

[StatelessWorker]
public class NewOrderWorker : Grain, INewOrderWorker
{
    private readonly IGrainFactory grains;

    public NewOrderWorker(IGrainFactory grains)
    {
        this.grains = grains;
    }

    public async Task<int> ExecuteAsync(int wId, int dId, int cId, List<OrderLine> orderLines)
    {
        var warehouse = grains.GetGrain<IWarehouseGrain>($"{wId}");
        await warehouse.GetState();

        var district = grains.GetGrain<IDistrictGrain>($"{wId}-{dId}"); 
        await district.GetState();
        var orderId = await district.NextOrderId();

        var customer = grains.GetGrain<ICustomerGrain>($"{wId}-{dId}-{cId}");
        await customer.SetLastOrder($"{wId}-{dId}-{orderId}");

        var order = new Order
        {
            Id = orderId,
            DistrictId = dId,
            WarehouseId = wId,
            CustomerId = cId,
            EntryDate = DateTime.Now,
        };

        for (var i = 0; i < orderLines.Count; i++ )
        {
            var ol = orderLines[i];
            var stock = grains.GetGrain<IStockGrain>($"{ol.SupplyWarehouseId}-{ol.ItemId}");
            await stock.ModifyStock(ol.Quantity);
            
            order.Lines.Add(new OrderLine
            {
                Number = i,
                ItemId = ol.ItemId,
                SupplyWarehouseId = ol.SupplyWarehouseId,
                Quantity = ol.Quantity,
                // Simplified price logic to avoid reading item table, which is replicated in baseline
                Amount = ol.Quantity * 10.0f,
            });
        }

        var orderGrain = grains.GetGrain<IOrderGrain>( $"{wId}-{dId}-{orderId}");
        await orderGrain.Create(order);

        return orderId;
    }
}

[StatelessWorker]
public class PaymentWorker : Grain, IPaymentWorker
{
    private readonly IGrainFactory grains;

    public PaymentWorker(IGrainFactory grains)
    {
        this.grains = grains;
    }


    public async Task ExecuteAsync(int wId, int dId, int cId, int cWId, int cDId, double amount)
    {
        var warehouse = grains.GetGrain<IWarehouseGrain>($"{wId}");
        await warehouse.AddYtdSales(amount);

        var district = grains.GetGrain<IDistrictGrain>($"{wId}-{dId}"); 
        await district.AddYtdSales(amount);

        var customer = grains.GetGrain<ICustomerGrain>($"{cWId}-{cDId}-{cId}");
        await customer.ProcessPayment(amount);
    }
}

[StatelessWorker]
public class BulkLoaderWorker : Grain, IBulkLoaderWorker
{
    private readonly IGrainFactory grains;
    
    public BulkLoaderWorker(IGrainFactory grains)
    {
        this.grains = grains;
    }
    
    [Transaction(TransactionOption.Suppress)]
    public async Task LoadData(int seed, List<int> assignedWarehouses, List<Item> items)
    {
        var rand = new Random(seed);

        var parallelOptions = new ParallelOptions {MaxDegreeOfParallelism = 8};
        
        foreach (var w in assignedWarehouses)
        {
            await grains.GetGrain<IWarehouseGrain>($"{w}")
                .Create(new Warehouse
                {
                    Id = w,
                    YtdSales = 300000.0
                });

            await Parallel.ForEachAsync(items, parallelOptions, async (i, _) =>
            {
                await grains.GetGrain<IStockGrain>($"{w}-{i.Id}").Create(new Stock
                {
                    ItemId = i.Id,
                    WarehouseId = w,
                    Quantity = rand.Next(10, 101)
                });
            });

            for (var d = 0; d < TpccConstants.NUM_DISTRICTS_PER_WAREHOUSE; d++)
            {
                await grains.GetGrain<IDistrictGrain>($"{w}-{d}").Create(new District
                {
                    Id = d,
                    WarehouseId = w,
                    YtdSales = 30000.0,
                    NextOrderId = 1
                });
                await Parallel.ForAsync(1, TpccConstants.NUM_CUSTOMERS_PER_DISTRICT + 1, parallelOptions, async (c, _) =>
                {
                    await grains.GetGrain<ICustomerGrain>($"{w}-{d}-{c}").Create(new Customer
                    {
                        Id = c,
                        DistrictId = d,
                        WarehouseId = w,
                        Balance = -10.00,
                    });
                });
            }
        }
    }
}