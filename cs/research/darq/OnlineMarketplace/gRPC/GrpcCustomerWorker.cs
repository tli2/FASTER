using Common.Entities;
using Common.Http;
using Common.Requests;
using Common.Services;
using Common.Streaming;
using Common.Workers.Customer;
using Common.Workload;
using Common.Workload.CustomerWorker;
using Grpc.Core;
using Newtonsoft.Json;
using OnlineMarketplace.Proto;
using CartItem = OnlineMarketplace.Proto.CartItem;
using Customer = Common.Entities.Customer;
using Product = Common.Entities.Product;

namespace OnlineMarketplace.gRPC;

public class GrpcCustomerWorker : AbstractCustomerWorker
{
    private readonly CartService.CartServiceClient cartServiceClient;
    private readonly IDictionary<(int sellerId, int productId), Product> cartItems;
    private readonly ISet<string> tids;
    private static readonly int MAX_CHECKOUT_ATTEMPTS = 3;
    
    public GrpcCustomerWorker(ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer, CartService.CartServiceClient cartServiceClient, ILogger logger) : base(sellerService, numberOfProducts, config, customer, logger)
    {
        this.cartServiceClient = cartServiceClient;
        cartItems = new Dictionary<(int, int), Product>(config.minMaxNumItemsRange.max);
        tids = config.trackTids ? new HashSet<string>() : null;
    }

    protected override void AddItemsToCart()
    {
        int numberKeysToAddToCart = this.random.Next(this.config.minMaxNumItemsRange.min, this.config.minMaxNumItemsRange.max + 1);
        if (this.config.uniqueSeller)
        {
            int sellerId = this.sellerIdGenerator.Sample();
            // prevent cases where number of products per seller is lower than number of items in the cart
            numberKeysToAddToCart = Math.Min(this.numberOfProducts, numberKeysToAddToCart);
            while (this.cartItems.Count < numberKeysToAddToCart)
            {
                this.AddItem(sellerId);
            }
        } else {
            while (this.cartItems.Count < numberKeysToAddToCart)
            {
                int sellerId = this.sellerIdGenerator.Sample();
                this.AddItem(sellerId);
            }
        }
    }
    
    private void AddItem(int sellerId)
    {
        var product = this.sellerService.GetProduct(sellerId, this.productIdGenerator.Sample() - 1);
        if (!this.cartItems.TryAdd((sellerId, product.product_id), product)) return;
        var quantity = this.random.Next(this.config.minMaxQtyRange.min, this.config.minMaxQtyRange.max + 1);
        cartServiceClient.AddToCart(BuildCartItem(product, quantity));
    }
    
    private CartItem BuildCartItem(Product product, int quantity)
    {
        // define voucher from distribution
        float voucher = 0;
        int probVoucher = this.random.Next(0, 101);
        if (probVoucher <= this.config.voucherProbability)
        {
            voucher = product.price * 0.10f;
        }

        return new CartItem
        {
            SellerId = product.seller_id,
            ProductId = product.product_id,
            ProductName = product.name,
            UnitPrice = product.price,
            FreightValue = product.freight_value,
            Quantity = quantity,
            Voucher = voucher,
            Version = product.version
        };
    }

    protected override void SendCheckoutRequest(string tid)
    {

        DateTime sentTs;
        int attempt = 0;
        try
        {
            bool success = false;
            do {
                sentTs = DateTime.UtcNow;
                var checkoutResult = cartServiceClient.Checkout(BuildCheckoutPayload(tid));
                success = checkoutResult.Success;
            } while(!success && ++attempt < MAX_CHECKOUT_ATTEMPTS);
            // Clean up
            cartItems.Clear();
            if(success)
            {
                this.submittedTransactions.Add(new(tid, TransactionType.CUSTOMER_SESSION, sentTs));
            } else
            {
                this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.CUSTOMER_SESSION, this.customer.id, MarkStatus.ABORT, "cart"));
            }
        }
        catch (Exception e)
        {
            this.logger.LogError("Customer {0}: Exception: {1} Message: {2} ", customer.id, e.GetType().Name, e.Message);
            this.InformFailedCheckout();
        }
    }
    
    private CustomerCheckoutRequest BuildCheckoutPayload(string tid)
    {
        // define payment type randomly
        var typeIdx = this.random.Next(1, 4);
        var type = typeIdx > 2 ? PaymentType.CREDIT_CARD : typeIdx > 1 ? PaymentType.DEBIT_CARD : PaymentType.BOLETO;
        var installments = type == PaymentType.CREDIT_CARD ? this.random.Next(1, 11) : 0;

        return new CustomerCheckoutRequest
        {
            CustomerId = customer.id,
            FirstName = customer.first_name,
            LastName = customer.last_name,
            City = customer.city,
            Address = customer.address,
            Complement = customer.complement,
            State = customer.state,
            ZipCode = customer.zip_code,
            PaymentType = type.ToString(),
            CardNumber = customer.card_number,
            CardHolderName = customer.card_holder_name,
            CardExpiration = customer.card_expiration,
            CardSecurityNumber = customer.card_security_number,
            CardType = customer.card_type,
            Installments = installments,
            InstanceId = tid
        };
    }

    protected override void InformFailedCheckout()
    {
        cartServiceClient.SealCart(new SealCartRequest
        {
            CustomerId = customer.id
        });
    }
}