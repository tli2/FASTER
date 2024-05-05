using System.Collections.Concurrent;
using System.Diagnostics;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using protobuf;

namespace dse.services;

public enum TableId : byte
{
    OFFERINGS,
    RESERVATIONS
}

public struct Key : IFasterEqualityComparer<Key>
{
    internal ulong key;

    public Key(TableId table, long id)
    {
        // We reserve the first byte for table id 
        Debug.Assert(((ulong)id & 0xFF00000000000000L) == 0L);
        key = ((ulong)(byte)table << 56) | (ulong)id;
    }

    internal Key(ulong key)
    {
        this.key = key;
    }

    public TableId GetTable() => (TableId)((key & 0xFF00000000000000L) >> 56);

    public long GetId() => (long)(key & 0xFFFFFFFFFFFFFFL);

    public long GetHashCode64(ref Key k)
    {
        return Utility.GetHashCode((long)k.key);
    }

    public bool Equals(ref Key k1, ref Key k2)
    {
        return k1.key == k2.key;
    }
}

public struct Value
{
    private Key key;
    private long field1, field2, field3;

    private Value(Key key, long field1, long field2, long field3)
    {
        this.key = key;
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
    }

    public static Value CreateOffering(long offeringId, long entityId, int price, int remainingCount)
    {
        return new Value(new Key(TableId.OFFERINGS, offeringId), entityId, price, remainingCount);
    }

    public static Value CreateReservation(long reservationId, long offeringId, long customerId, int count)
    {
        return new Value(new Key(TableId.RESERVATIONS, reservationId), offeringId, customerId, count);
    }

    public bool TryDecrementCount(int by)
    {
        Debug.Assert(key.GetTable() == TableId.OFFERINGS);
        long currentCount = 0, decremented = 0;
        do
        {
            currentCount = field3;
            if (currentCount < by) return false;
            decremented = currentCount - by;
        } while (Interlocked.CompareExchange(ref field3, decremented, currentCount) != currentCount);

        return true;
    }
}

public class FasterKvReservationStartFile
{
    public string file;
}

public class FasterKvReservationStateObject : StateObject
{
    public FasterKV<Key, Value> kv;
    private Guid indexCheckpointToken = default;
    private ConcurrentDictionary<long, Guid> tokenMappings = new();

    public FasterKvReservationStateObject(FasterKV<Key, Value> kv,
        IVersionScheme versionScheme, DprWorkerOptions options) : base(versionScheme, options)
    {
        this.kv = kv;
    }

    public override void Dispose()
    {
    }

    public override void PerformCheckpoint(long version, ReadOnlySpan<byte> metadata, Action onPersist)
    {
        // TODO(Tianyu): Do something about index checkpoints
        Console.WriteLine($"Performing checkpoint for version {version}");
        Guid token;
        // If return is false, this means the previous checkpoint is still running and we should not advance more
        var success = kv.TryTakeDprStyleCheckpoint(version, metadata, onPersist, out token);
        Debug.Assert(success);
        tokenMappings[version] = token;
        Task.Run(() => kv.CompleteCheckpointAsync());
    }

    public override void RestoreCheckpoint(long version, out ReadOnlySpan<byte> metadata)
    {
        // TODO(Tianyu): Figure out how to do advanced non-blocking rollback
        tokenMappings.Clear();
        foreach (var guid in kv.CheckpointManager.GetLogCheckpointTokens())
        {
            using StreamReader s = new(new MemoryStream(kv.CheckpointManager.GetLogCheckpointMetadata(guid, null)));
            s.ReadLine(); // version
            s.ReadLine(); // checksum
            s.ReadLine(); // guid
            s.ReadLine(); // useSnapshotFile
            var checkpointVersion = long.Parse(s.ReadLine());
            tokenMappings[checkpointVersion] = guid;
        }

        kv.Recover(default, tokenMappings[version]);
        metadata = kv.CommitCookie;
    }

    public override void PruneVersion(long version)
    {
        if (!tokenMappings.TryRemove(version, out var guid)) return;
        kv.CheckpointManager.Purge(guid);
    }

    public override IEnumerable<Memory<byte>> GetUnprunedVersions()
    {
        return kv.CheckpointManager.GetLogCheckpointTokens().Select(guid =>
        {
            using StreamReader s = new(new MemoryStream(kv.CheckpointManager.GetLogCheckpointMetadata(guid, null)));
            s.ReadLine();
            s.ReadLine();
            s.ReadLine();
            s.ReadLine();
            s.ReadLine();
            s.ReadLine();
            s.ReadLine();
            s.ReadLine();
            s.ReadLine();
            s.ReadLine();
            s.ReadLine();
            s.ReadLine();
            s.ReadLine();
            s.ReadLine();
            s.ReadLine();
            var numSessions = int.Parse(s.ReadLine());
            // We don't use recoverable sessions in DPR version of Faster
            Debug.Assert(numSessions == 0);
            // Read object log segment offsets
            var numSegments = int.Parse(s.ReadLine());
            // We don't use object log in DPR version of Faster
            Debug.Assert(numSegments == 0);
            var cookie = s.ReadToEnd();
            var metadata = cookie.Length == 0 ? null : Convert.FromBase64String(cookie);
            return new Memory<byte>(metadata);
        });
    }
}

public class ReserveFunctions : FunctionsBase<Key, Value, int, bool, Empty>
{
    public override bool ConcurrentReader(ref Key key, ref int input, ref Value value, ref bool dst,
        ref ReadInfo readInfo)
    {
        // Should never be called
        throw new NotImplementedException();
    }

    public override bool SingleReader(ref Key key, ref int input, ref Value value, ref bool dst, ref ReadInfo readInfo)
    {
        // Should never be called
        throw new NotImplementedException();
    }

    public override bool ConcurrentWriter(ref Key key, ref int input, ref Value src, ref Value dst, ref bool output,
        ref UpsertInfo upsertInfo)
    {
        // Not allowed
        upsertInfo.Action = UpsertAction.CancelOperation;
        return false;
    }

    public override bool SingleWriter(ref Key key, ref int input, ref Value src, ref Value dst, ref bool output,
        ref UpsertInfo upsertInfo,
        WriteReason reason)
    {
        dst = src;
        return true;
    }

    public override bool InitialUpdater(ref Key key, ref int input, ref Value value, ref bool output,
        ref RMWInfo rmwInfo)
    {
        // Should not be called in workload
        throw new NotImplementedException();
    }

    public override bool CopyUpdater(ref Key key, ref int input, ref Value oldValue, ref Value newValue,
        ref bool output,
        ref RMWInfo rmwInfo)
    {
        newValue = oldValue;
        output = newValue.TryDecrementCount(input);
        return true;
    }

    public override bool InPlaceUpdater(ref Key key, ref int input, ref Value value, ref bool output,
        ref RMWInfo rmwInfo)
    {
        output = value.TryDecrementCount(input);
        return true;
    }
}

public class FasterKvReservationBackgroundService : BackgroundService
{
    private FasterKvReservationStateObject backend;

    private ThreadLocalObjectPool<ClientSession<Key, Value, int, bool, Empty, IFunctions<Key, Value, int, bool, Empty>>>
        sessions;

    private ILogger<FasterKvReservationBackgroundService> logger;
    private FasterKvReservationStartFile file;
    public FasterKvReservationBackgroundService(FasterKvReservationStateObject backend,
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
        logger.LogInformation("Faster service is starting...");
        backend.ConnectToCluster(out var restored);
        if (!restored && !file.file.Equals(""))
            LoadFromFile(file.file);

        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        logger.LogInformation("Faster service is stopping...");
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

public class FasterKvReservationService : FasterKVReservationService.FasterKVReservationServiceBase
{
    private FasterKvReservationBackgroundService faster;

    public FasterKvReservationService(FasterKvReservationBackgroundService faster)
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