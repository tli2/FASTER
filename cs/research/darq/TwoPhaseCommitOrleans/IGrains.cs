using Orleans;

namespace TwoPhaseCommitOrleans;

[Alias("TwoPhaseCommitOrleans.interfaces.IWarehouseGrain")]
[TpccPlacement]
public interface IWarehouseGrain : IGrainWithStringKey
{
    [Transaction(TransactionOption.Supported)]
    [Alias("Create")]
    Task Create(Warehouse warehouse);
    
    [Transaction(TransactionOption.Join)]
    [Alias("GetState")]
    Task<Warehouse> GetState();
    
    [Transaction(TransactionOption.Join)]
    [Alias("AddYtdSales")]
    Task AddYtdSales(double amount);
}

[Alias("TwoPhaseCommitOrleans.interfaces.IDistrictGrain")]
[TpccPlacement]
public interface IDistrictGrain : IGrainWithStringKey // WarehouseId + DistrictId
{
    [Transaction(TransactionOption.Supported)]
    [Alias("Create")]
    Task Create(District district);
    
    [Transaction(TransactionOption.Join)]
    [Alias("GetState")]
    Task<District> GetState();
    
    [Transaction(TransactionOption.Join)]
    [Alias("AddYtdSales")]
    Task AddYtdSales(double amount);
    
    // Returns the ID for the new order and increments state
    [Transaction(TransactionOption.Join)]
    [Alias("GetNextOrderId")]
    Task<int> NextOrderId(); 
}

[Alias("TwoPhaseCommitOrleans.interfaces.ICustomerGrain")]
[TpccPlacement]
public interface ICustomerGrain : IGrainWithStringKey // Warehouse + District + CustomerId
{
    [Transaction(TransactionOption.Supported)]
    [Alias("Create")]
    Task Create(Customer customer);
    
    [Transaction(TransactionOption.Join)]
    [Alias("GetState")]
    Task<Customer> GetState();

    [Transaction(TransactionOption.Join)]
    [Alias("Payment")]
    Task ProcessPayment(double amount);
    
    [Transaction(TransactionOption.Create)]
    [Alias("OrderStatus")]
    Task<Order> OrderStatus();
    
    [Transaction(TransactionOption.Join)]
    [Alias("SetLastOrder")]
    Task SetLastOrder(string orderId);
}

[Alias("TwoPhaseCommitOrleans.interfaces.IStockGrain")]
[TpccPlacement]
public interface IStockGrain : IGrainWithStringKey // WarehouseId + ItemId
{
    [Transaction(TransactionOption.Supported)]
    [Alias("Create")]
    Task Create(Stock stock);
    
    [Transaction(TransactionOption.Join)]
    [Alias("GetState")]
    Task<Stock> GetState();

    [Transaction(TransactionOption.Join)]
    [Alias("ReduceStock")]
    Task ModifyStock(int quantity);
}

[Alias("TwoPhaseCommitOrleans.interfaces.IOrderGrain")]
[TpccPlacement]
public interface IOrderGrain : IGrainWithStringKey // Warehouse + District + OrderId
{
    [Transaction(TransactionOption.Join)]
    [Alias("Create")]
    Task Create(Order order);
    
    [Transaction(TransactionOption.Join)]
    [Alias("GetState")]
    Task<Order> GetState();
}

[Alias("TwoPhaseCommitOrleans.interfaces.INewOrderWorker")]
[TpccPlacement]
public interface INewOrderWorker : IGrainWithStringKey
{
    // Returns OrderId
    [Transaction(TransactionOption.Create)]
    [Alias("ExecuteAsync")]
    Task<int> ExecuteAsync(int wId, int dId, int cId, List<OrderLine> orderLines);
}

[Alias("TwoPhaseCommitOrleans.interfaces.IPaymentWorker")]
[TpccPlacement]
public interface IPaymentWorker : IGrainWithStringKey
{
    // Returns OrderId
    [Transaction(TransactionOption.Create)]
    [Alias("ExecuteAsync")]
    Task ExecuteAsync(int wId, int dId, int cId, int cWId, int cDId, double amount);
}

[Alias("TwoPhaseCommitOrleans.IBulkLoaderWorker")]
[TpccPlacement]
public interface IBulkLoaderWorker : IGrainWithStringKey
{
    [Transaction(TransactionOption.Create)]
    [Alias("LoadData")]
    Task LoadData(int seed, List<int> assignedWarehouses, List<Item> items);
}
