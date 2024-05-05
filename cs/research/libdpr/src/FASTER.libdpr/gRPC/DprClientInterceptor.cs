using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FASTER.common;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace FASTER.libdpr.gRPC
{
    public class DprClientInterceptor : Interceptor
    {
        private DprSession session;
        private ThreadLocalObjectPool<byte[]> serializationArrayPool;

        public DprClientInterceptor(DprSession session)
        {
            this.session = session;
            serializationArrayPool = new ThreadLocalObjectPool<byte[]>(() => new byte[1 << 10]);
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(TRequest request,
            ClientInterceptorContext<TRequest, TResponse> context,
            BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
        {
            // TODO(Tianyu): getting headers/trailers apparently unsupported in blocking version of the call
            throw new NotImplementedException();
        }

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(TRequest request,
            ClientInterceptorContext<TRequest, TResponse> context,
            AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        {
            var buffer = serializationArrayPool.Checkout();
            session.TagMessage(buffer);
            // TODO(Tianyu): Add logic to await for commit if crossing SU

            var headers = context.Options.Headers;
            if (headers == null)
            {
                // TODO(Tianyu): Is this object expensive?
                headers = new Metadata();
                var options = context.Options.WithHeaders(headers);
                context = new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, options);
            }

            // TODO(Tianyu): Why no span variant?
            headers.Add(DprMessageHeader.GprcMetadataKeyName, buffer);
            // TODO(Tianyu): Assuming it is ok now to return into object pool?
            serializationArrayPool.Return(buffer);

            var call = continuation(request, context);
            return new AsyncUnaryCall<TResponse>(HandleTrailer(call.ResponseAsync, call.GetTrailers),
                call.ResponseHeadersAsync, call.GetStatus, call.GetTrailers, call.Dispose);
        }


        private async Task<TResponse> HandleTrailer<TResponse>(Task<TResponse> inner, Func<Metadata> getTrailer)
            where TResponse : class
        {
            var result = await inner;
            var metadata = getTrailer();
            var header = metadata.GetValueBytes(DprMessageHeader.GprcMetadataKeyName);
            Debug.Assert(header != null);
            if (session.Receive(header)) return result;
            throw new RpcException(Status.DefaultCancelled);
        }
    }
}