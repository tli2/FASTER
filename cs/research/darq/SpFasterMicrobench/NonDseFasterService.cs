using dse.services;
using FASTER.common;
using FASTER.core;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using protobuf;

namespace microbench;

public class NonDseFasterBackgroundService : BackgroundService
{
    public FasterKV<Key, Value> kv;
    private Thread checkpointThread;
    private long checkpointInterval = 10;
    private ThreadLocalObjectPool<ClientSession<Key, Value, int, bool, Empty, IFunctions<Key, Value, int, bool, Empty>>>
        sessions;

    private ILogger<FasterKvReservationBackgroundService> logger;
    private FasterKvReservationStartFile file;
    
    public NonDseFasterBackgroundService(FasterKV<Key, Value> kv,
        FasterKvReservationStartFile file, ILogger<FasterKvReservationBackgroundService> logger)
    {
        this.kv = kv;
        this.file = file;
        this.logger = logger;
        sessions =
            new ThreadLocalObjectPool<
                ClientSession<Key, Value, int, bool, Empty, IFunctions<Key, Value, int, bool, Empty>>>(() =>
                this.kv.NewSession(new ReserveFunctions()));
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
        var task = kv.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);
        if (!task.IsCompleted)
            task.AsTask().GetAwaiter().GetResult();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Faster service is starting...");
        if (!file.file.Equals(""))
            LoadFromFile(file.file);
        checkpointThread = new Thread(() =>
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                kv.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver);
                Thread.Sleep((int)checkpointInterval);
            }
        });
        checkpointThread.Start();
        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        logger.LogInformation("Faster service is stopping...");
        checkpointThread.Join();
    }

    public Task<ReservationResponse> MakeReservation(ReservationRequest request)
    {
        var s = sessions.Checkout();
        try
        {
            var offeringKey = new Key(TableId.OFFERINGS, request.OfferingId);
            var reservationCount = request.Count;
            var success = false;
            var status = s.RMW(ref offeringKey, ref reservationCount, ref success);
            // Not planning on running into larger-than-mem or other complex situations
            if (!status.IsCompletedSuccessfully) throw new NotImplementedException();
            if (!success)
                return Task.FromResult(new ReservationResponse
                {
                    Ok = false
                });

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
                return Task.FromResult(new ReservationResponse
                {
                    Ok = false
                });
            }
            else if (status.IsCompletedSuccessfully)
            {
                return Task.FromResult(new ReservationResponse
                {
                    Ok = true
                });
            }
            else
                throw new NotImplementedException();
        }
        finally
        {
            sessions.Return(s);
        }
    }

    public Task<ReservationResponse> CancelReservation(ReservationRequest request)
    {
        var s = sessions.Checkout();
        try
        {
            var offeringKey = new Key(TableId.OFFERINGS, request.OfferingId);
            var reservationCount = request.Count;
            var reservationsKey = new Key(TableId.RESERVATIONS, request.ReservationId);

            var status = s.Delete(ref reservationsKey);
            if (status.NotFound)
            {
                return Task.FromResult(new ReservationResponse
                {
                    Ok = false
                });
            }

            // Add updates back to count
            reservationCount = -reservationCount;
            var success = false;
            status = s.RMW(ref offeringKey, ref reservationCount, ref success);
            if (!status.IsCompletedSuccessfully) throw new NotImplementedException();
            return Task.FromResult(new ReservationResponse
            {
                Ok = true
            });
        }
        finally
        {
            sessions.Return(s);
        }
    }

    public Task<AddOfferingResponse> AddOffering(AddOfferingRequest request)
    {
        var s = sessions.Checkout();
        try
        {
            var offeringKey = new Key(TableId.OFFERINGS, request.OfferingToAdd.OfferingId);
            var offeringEntry = Value.CreateOffering(request.OfferingToAdd.OfferingId, request.OfferingToAdd.EntityId,
                request.OfferingToAdd.Price, request.OfferingToAdd.RemainingCount);
            var status = s.Upsert(ref offeringKey, ref offeringEntry);
            if (status.IsCanceled)
                return Task.FromResult(new AddOfferingResponse
                {
                    Ok = false
                });
            // Not planning on running into larger-than-mem or other complex situations
            if (!status.IsCompletedSuccessfully) throw new NotImplementedException();
            return Task.FromResult(new AddOfferingResponse
            {
                Ok = true
            });
        }
        finally
        {
            sessions.Return(s);
        }
    }
}

public class NonDseReservationService : FasterKVReservationService.FasterKVReservationServiceBase
{
    private NonDseFasterBackgroundService faster;

    public NonDseReservationService(NonDseFasterBackgroundService faster)
    {
        this.faster = faster;
    }

    public override Task<ReservationResponse> MakeReservation(ReservationRequest request, ServerCallContext context)
    {
        return faster.MakeReservation(request);
    }

    public override Task<ReservationResponse> CancelReservation(ReservationRequest request, ServerCallContext context)
    {
        return faster.CancelReservation(request);
    }

    public override Task<AddOfferingResponse> AddOffering(AddOfferingRequest request, ServerCallContext context)
    {
        return faster.AddOffering(request);
    }
}