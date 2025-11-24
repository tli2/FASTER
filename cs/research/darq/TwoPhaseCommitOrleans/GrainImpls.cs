using Orleans;
using Orleans.Transactions.Abstractions;

namespace TwoPhaseCommitOrleans;

// Example of one Entity Grain. Others follow identical pattern.
public class WarehouseGrain : Grain, IWarehouseGrain
{
    private readonly ITransactionalState<Warehouse> state;

    public WarehouseGrain(
        [TransactionalState("warehouse", "TransactionStore")] ITransactionalState<Warehouse> state)
    {
        this.state = state;
    }
    
    public Task Create(Warehouse warehouse) => state.PerformUpdate(w =>
    {
        w.Id = warehouse.Id;
        w.YtdSales = warehouse.YtdSales;
    });

    public Task<Warehouse> GetState() => state.PerformRead(w => w);

    public Task AddYtdSales(double amount)
    {
        return state.PerformUpdate(w => 
        {
            w.YtdSales += amount;
        });
    }
}

public class DistrictGrain : Grain, IDistrictGrain
{
    private readonly ITransactionalState<District> state;

    public DistrictGrain(
        [TransactionalState("district", "TransactionStore")] ITransactionalState<District> state)
    {
        this.state = state;
    }

    public Task Create(District district) => state.PerformUpdate(d =>
    {
        d.Id = district.Id;
        d.WarehouseId = district.WarehouseId;
        d.YtdSales = district.YtdSales;
        d.NextOrderId = district.NextOrderId;
    });
    
    public Task<District> GetState() => state.PerformRead(d => d);

    public Task AddYtdSales(double amount)
    {
        return state.PerformUpdate(d => d.YtdSales += amount);
    }

    public Task<int> NextOrderId()
    {
        return state.PerformUpdate(d => 
        {
            int id = d.NextOrderId;
            d.NextOrderId++;
            return id;
        });
    }
}

public class CustomerGrain : Grain, ICustomerGrain
{
    private readonly ITransactionalState<Customer> state;
    private IGrainFactory grains;
    
    public CustomerGrain(
        IGrainFactory grains,
        [TransactionalState("customer", "TransactionStore")] ITransactionalState<Customer> state)
    {
        this.grains = grains;
        this.state = state;
    }
    
    public Task Create(Customer customer) => state.PerformUpdate(c =>
    {
        c.Id = customer.Id;
        c.WarehouseId = customer.WarehouseId;
        c.DistrictId = customer.DistrictId;
        c.Balance = customer.Balance;
        c.YtdPayment = customer.YtdPayment;
        c.PaymentCnt = customer.PaymentCnt;
    });
    
    public Task<Customer> GetState()
    {
        return state.PerformRead(c => c);
    }

    public Task ProcessPayment(double amount)
    {
        return state.PerformUpdate(c =>
        {
            c.Balance -= amount;
            c.YtdPayment += amount;
            c.PaymentCnt++;
        });
    }

    public async Task<Order> OrderStatus()
    {
        var lastOrder = await state.PerformRead(c => c.LastOrderId);
        if (lastOrder == null) return new Order(); // No orders (yet)
        return await grains.GetGrain<IOrderGrain>(lastOrder).GetState();
    }

    public Task SetLastOrder(string orderId)
    {
        return state.PerformUpdate(c => c.LastOrderId = orderId);
    }
}

public class StockGrain : Grain, IStockGrain
{
    private readonly ITransactionalState<Stock> state;
    
    public StockGrain(
        [TransactionalState("stock", "TransactionStore")] ITransactionalState<Stock> state)
    {
        this.state = state;
    }
    
    public Task Create(Stock stock) => state.PerformUpdate(s =>
    {
        s.ItemId = stock.ItemId;
        s.Quantity = stock.Quantity;
        s.WarehouseId = stock.WarehouseId;
    });
    
    public Task<Stock> GetState()
    {
        return state.PerformRead(s => s);
    }

    public Task ModifyStock(int quantity)
    {
        return state.PerformUpdate(s =>
        {
            s.Quantity -= s.Quantity > quantity + 10 ? quantity : quantity + 91;
        });
    }
}

public class OrderGrain : Grain, IOrderGrain
{
    private readonly ITransactionalState<Order> state;

    public OrderGrain(
        [TransactionalState("order", "TransactionStore")] ITransactionalState<Order> state)
    {
        this.state = state;
    }
    
    public Task Create(Order order)
    {
        return state.PerformUpdate(o =>
        {
            o.Id = order.Id;
            o.DistrictId = order.DistrictId;
            o.WarehouseId = order.WarehouseId;
            o.CustomerId = order.CustomerId;
            o.EntryDate = order.EntryDate;
            o.CarrierId = order.CarrierId;
            o.Lines = order.Lines;
        });
    }

    public Task<Order> GetState()
    {
        return state.PerformRead(o => o);
    }
}

