using dse.services;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using protobuf.noint;

namespace microbench;

public class FasterKvReservationBackgroundServiceNoInt : BackgroundService
{
    private FasterKvReservationStateObject backend;

    private ThreadLocalObjectPool<ClientSession<Key, Value, int, bool, Empty, IFunctions<Key, Value, int, bool, Empty>>>
        sessions;

    private ILogger<FasterKvReservationBackgroundService> logger;
    private FasterKvReservationStartFile file;

    public FasterKvReservationBackgroundServiceNoInt(FasterKvReservationStateObject backend,
        FasterKvReservationStartFile file, ILogger<FasterKvReservationBackgroundService> logger)
    {
        this.backend = backend;
        this.file = file;
        this.logger = logger;
        sessions =
            new ThreadLocalObjectPool<
                ClientSession<Key, Value, int, bool, Empty, IFunctions<Key, Value, int, bool, Empty>>>(() =>
                this.backend.kv.NewSession(new ReserveFunctions()));
    }

    private void LoadFromFile(string filename)
    {
        using var reader = new StreamReader(filename);
        var s = sessions.Checkout();
        for (var line = reader.ReadLine(); line != null; line = reader.ReadLine())
        {
            var parts = line.Split(',');
            var offeringId = long.Parse(parts[0]);
            var entityId = long.Parse(parts[1]);
            var price = int.Parse(parts[2]);
            var count = int.Parse(parts[3]);

            var key = new Key(TableId.OFFERINGS, offeringId);
            var val = Value.CreateOffering(offeringId, entityId, price, count);
            var status = s.Upsert(ref key, ref val);
            // Not planning on running into larger-than-mem or other complex situations
            if (!status.IsCompletedSuccessfully) throw new NotImplementedException();
        }

        sessions.Return(s);
        backend.ForceCheckpoint();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogWarning("Faster service is starting...");
        backend.ConnectToCluster(out var restored);
        if (!restored && !file.file.Equals(""))
            LoadFromFile(file.file);

        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        logger.LogWarning("Faster service is stopping...");
    }

    public unsafe Task<ReservationResponseWithHeader> MakeReservation(ReservationRequestWithHeader request)
    {
        var dprHeader = stackalloc byte[DprMessageHeader.FixedLenSize];
        var headerSpan = new Span<byte>(dprHeader, DprMessageHeader.FixedLenSize);
        if (!backend.TryReceiveAndStartAction(request.DprHeader.Span))
            throw new DprSessionRolledBackException(backend.WorldLine());

        var s = sessions.Checkout();
        var offeringKey = new Key(TableId.OFFERINGS, request.OfferingId);
        var reservationCount = request.Count;
        var success = false;
        var status = s.RMW(ref offeringKey, ref reservationCount, ref success);
        // Not planning on running into larger-than-mem or other complex situations
        if (!status.IsCompletedSuccessfully) throw new NotImplementedException();
        if (!success)
        {
            sessions.Return(s);
            backend.ProduceTagAndEndAction(headerSpan);
            return Task.FromResult(new ReservationResponseWithHeader()
            {
                DprHeader = ByteString.CopyFrom(headerSpan),
                Ok = false
            });
        }

        var reservationsKey = new Key(TableId.RESERVATIONS, request.ReservationId);
        var reservationsEntry = Value.CreateReservation(request.ReservationId, request.OfferingId,
            request.CustomerId, request.Count);
        status = s.Upsert(ref reservationsKey, ref reservationsEntry);
        if (status.IsCanceled)
        {
            // this reservation is a duplicate, roll back earlier update
            reservationCount = -reservationCount;
            status = s.RMW(ref offeringKey, ref reservationCount, ref success);
            if (!status.IsCompletedSuccessfully) throw new NotImplementedException();
            sessions.Return(s);
            backend.ProduceTagAndEndAction(headerSpan);
            return Task.FromResult(new ReservationResponseWithHeader
            {
                DprHeader = ByteString.CopyFrom(headerSpan),
                Ok = false
            });
        }
        if (status.IsCompletedSuccessfully)
        {
            sessions.Return(s);
            backend.ProduceTagAndEndAction(headerSpan);
            return Task.FromResult(new ReservationResponseWithHeader
            {
                DprHeader = ByteString.CopyFrom(headerSpan),
                Ok = true
            });
        }
        throw new NotImplementedException();

    }
}

public class FasterKvReservationServiceNoInt : FasterKVReservationNoInterceptorService.FasterKVReservationNoInterceptorServiceBase
{
    private FasterKvReservationBackgroundServiceNoInt faster;

    public FasterKvReservationServiceNoInt(FasterKvReservationBackgroundServiceNoInt faster)
    {
        this.faster = faster;
    }

    public override Task<ReservationResponseWithHeader> MakeReservation(ReservationRequestWithHeader request, ServerCallContext context)
    {
        return faster.MakeReservation(request);
    }
}