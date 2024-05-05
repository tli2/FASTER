using System.Threading.Tasks;
using FASTER.common;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Status = Grpc.Core.Status;

namespace FASTER.libdpr.gRPC
{
    public class DprServerInterceptor<TService> : Interceptor
    {
        private StateObject _stateObject;
        private ThreadLocalObjectPool<byte[]> serializationArrayPool;
        private int requestId;

        // For now, we require that the gRPC integration only works with RwLatchVersionScheme, which supports protected
        // blocks that start and end on different threads
        // TService is a parameter for DI to only create interceptors after the service is up 
        public DprServerInterceptor(StateObject stateObject, TService service)
        {
            this._stateObject = stateObject;
            serializationArrayPool = new ThreadLocalObjectPool<byte[]>(() => new byte[1 << 10]);
        }

        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request,
            ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation)
        {
            // TODO(Tianyu): Create Epoch Context specific to a request

            var header = context.RequestHeaders.GetValueBytes(DprMessageHeader.GprcMetadataKeyName);
            if (header != null)
            {
                // Speculative code path
                if (!await _stateObject.TryReceiveAndStartActionAsync(header))
                    // Use an error to signal to caller that this call cannot proceed
                    // TODO(Tianyu): add more descriptive exception information
                    throw new RpcException(Status.DefaultCancelled);
                var response = await continuation.Invoke(request, context);
                var buf = serializationArrayPool.Checkout();
                _stateObject.ProduceTagAndEndAction(buf);
                context.ResponseTrailers.Add(DprMessageHeader.GprcMetadataKeyName, buf);
                serializationArrayPool.Return(buf);
                return response;
            }
            else
            {
                // Non speculative code path
                _stateObject.StartLocalAction();
                var response = await continuation.Invoke(request, context);
                _stateObject.EndAction();
                // TODO(Tianyu): Allow custom version headers to avoid waiting on, say, a read into a committed value
                await _stateObject.NextCommit();
                return response;
            }
        }
    }
}