using Orleans;

namespace TwoPhaseCommitOrleans;

[GenerateSerializer]
public class Warehouse
{
    [Id(0)] public int Id { get; set; }
    // [Id(1)] public string Name { get; set; } = "";
    // [Id(2)] public string Street1 { get; set; } = "";
    // [Id(3)] public string Street2 { get; set; } = "";
    // [Id(4)] public string City { get; set; } = "";
    // [Id(5)] public string State { get; set; } = "";
    // [Id(6)] public string Zip { get; set; } = "";
    // [Id(7)] public double Tax { get; set; }
    [Id(8)] public double YtdSales { get; set; }
}

[GenerateSerializer]
public class District
{
    [Id(0)] public int Id { get; set; }
    [Id(1)] public int WarehouseId { get; set; }
    // [Id(2)] public string Name { get; set; } = "";
    // [Id(3)] public string Street1 { get; set; } = "";
    // [Id(4)] public string Street2 { get; set; } = "";
    // [Id(5)] public string City { get; set; } = "";
    // [Id(6)] public string State { get; set; } = "";
    // [Id(7)] public string Zip { get; set; } = "";
    // [Id(8)] public double Tax { get; set; }
    [Id(9)] public double YtdSales { get; set; }
    [Id(10)] public int NextOrderId { get; set; } // Critical for sequence generation
}

[GenerateSerializer]
public class Customer
{
    [Id(0)] public int Id { get; set; }
    [Id(1)] public int DistrictId { get; set; }
    [Id(2)] public int WarehouseId { get; set; }
    // [Id(3)] public string FirstName { get; set; } = "";
    // [Id(4)] public string MiddleName { get; set; } = "";
    // [Id(5)] public string LastName { get; set; } = "";
    // [Id(6)] public string Street1 { get; set; } = "";
    // [Id(7)] public string Street2 { get; set; } = "";
    // [Id(8)] public string City { get; set; } = "";
    // [Id(9)] public string State { get; set; } = "";
    // [Id(10)] public string Zip { get; set; } = "";
    // [Id(11)] public string Phone { get; set; } = "";
    // [Id(12)] public DateTime Since { get; set; }
    // [Id(13)] public string Credit { get; set; } = "";
    // [Id(14)] public double CreditLim { get; set; }
    // [Id(15)] public double Discount { get; set; }
    [Id(16)] public double Balance { get; set; }
    [Id(17)] public double YtdPayment { get; set; }
    [Id(18)] public int PaymentCnt { get; set; }
    // [Id(19)] public int DeliveryCnt { get; set; }
    // [Id(20)] public string Data { get; set; } = "";
    
    // Optimization: Pointer to last order to avoid scans
    [Id(21)] public string LastOrderId { get; set; }
}

[GenerateSerializer]
public class Order
{
    [Id(0)] public int Id { get; set; }
    [Id(1)] public int DistrictId { get; set; }
    [Id(2)] public int WarehouseId { get; set; }
    [Id(3)] public int CustomerId { get; set; }
    [Id(4)] public DateTime EntryDate { get; set; }
    [Id(5)] public int CarrierId { get; set; }
    [Id(8)] public List<OrderLine> Lines { get; set; } = new();
}

[GenerateSerializer]
public class OrderLine
{
    [Id(0)] public int Number { get; set; }
    [Id(1)] public int ItemId { get; set; }
    [Id(2)] public int SupplyWarehouseId { get; set; }
    [Id(3)] public DateTime? DeliveryDate { get; set; }
    [Id(4)] public int Quantity { get; set; }
    [Id(5)] public double Amount { get; set; }
    // [Id(6)] public string DistInfo { get; set; } = "";
}

// --- Stock ---
[GenerateSerializer]
public class Stock
{
    [Id(0)] public int ItemId { get; set; }
    [Id(1)] public int WarehouseId { get; set; }
    [Id(2)] public int Quantity { get; set; }
}

// --- Item (Read Only, usually cached) ---
[GenerateSerializer]
public class Item
{
    [Id(0)] public int Id { get; set; }
    // [Id(2)] public string Name { get; set; } = "";
    [Id(3)] public double Price { get; set; }
    // [Id(4)] public string Data { get; set; } = "";
}