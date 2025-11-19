using System.Collections.Concurrent;
using System.Net;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using protobuf;
using Temporalio.Activities;
using Temporalio.Workflows;
using Microsoft.Azure.Cosmos;
using Temporalio.Exceptions;

namespace TravelReservation;
// Represents the available inventory for an item
public class OfferingDocument
{
    [JsonProperty("partitionId")]
    public string PartitionId { get; set; }
    
    [JsonProperty("id")]
    public string Id { get; set; } // e.g., "offering-123"

    [JsonProperty("entityId")]
    public long EntityId { get; set; }
    
    [JsonProperty("price")]
    public int Price { get; set; }
    
    [JsonProperty("remainingCount")]
    public int RemainingCount { get; set; }
}

public class BenchmarkRunConfigDocument
{
    [JsonProperty("partitionId")]
    public string PartitionId { get; set; }
    
    [JsonProperty("id")]
    public string Id { get; set; } 
    
    [JsonProperty("runGuid")]
    public string RunGuid { get; set; }
}

// Represents a specific reservation made by a customer
public class ReservationDocument
{
    [JsonProperty("partitionId")]
    public string PartitionId { get; set; } // e.g., "offering-123"
    
    [JsonProperty("id")]
    public string Id { get; set; } // This is our idempotency key, e.g., the reservationId

    [JsonProperty("offeringId")]
    public string OfferingId { get; set; } // The Partition Key

    [JsonProperty("customerId")]
    public long CustomerId { get; set; }
    
    [JsonProperty("count")]
    public int Count { get; set; }
}

[Workflow]
public class TemporalReservationWorkflow
{
    [WorkflowRun]
    public async Task<bool> RunAsync(string workflowContent)
    {
        var toExecute = new List<ReservationRequest>();
        var split = workflowContent.Split(',');
        for (var i = 2; i < split.Length; i += 4)
        {
            toExecute.Add(new ReservationRequest
            {
                ReservationId = long.Parse(split[i]),
                OfferingId = long.Parse(split[i + 1]),
                CustomerId = long.Parse(split[i + 2]),
                Count = int.Parse(split[i + 3]),
            });
        }

        var compensations = new List<Func<Task>>();
        var activityOptions = new ActivityOptions
        {
            StartToCloseTimeout = TimeSpan.FromSeconds(30),
            RetryPolicy = new()
            {
                MaximumAttempts = 10,
            }
        };

        try
        {
            // 3. Execute each reservation sequentially
            for (var i = 0; i < toExecute.Count; i++)
            {
                var request = toExecute[i];
                var i1 = i;
                var success = await Workflow.ExecuteActivityAsync(
                    (TemporalReservationActivities acts) => acts.MakeReservationAsync(request, i1),
                    activityOptions);

                if (success)
                {
                    // If successful, add the "undo" operation to the front of our compensation list.
                    compensations.Add(() => Workflow.ExecuteActivityAsync(
                        (TemporalReservationActivities acts) => acts.CancelReservationAsync(request, i1),
                        activityOptions));
                }
                else
                {
                    Console.WriteLine($"Reservation failed for offering {request.OfferingId}, not enough inventory.");
                    foreach (var compensation in compensations)
                    {
                        // We execute the compensation tasks. We should use separate, more
                        // robust options for compensations to ensure they complete.
                        await compensation();
                    }

                    return false;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            return false;
        }

        return true;
    }
}

public class TemporalReservationActivities
{
    private readonly Container container;

    public TemporalReservationActivities(Container container)
    {
        this.container = container;
    }
    
    [Activity]
    public async Task<bool> MakeReservationAsync(ReservationRequest request, int serviceId)
    {
        try
        {
            ItemResponse<OfferingDocument> offeringResponse = await container.ReadItemAsync<OfferingDocument>(
                id: $"offering-{serviceId}-{request.OfferingId}",
                partitionKey: new PartitionKey($"{serviceId}-{request.OfferingId}"));
            if (offeringResponse.Resource.RemainingCount < request.Count) return false;

            var reservationDoc = new ReservationDocument
            {
                PartitionId = $"{serviceId}-{request.OfferingId}",
                Id = $"reservation-{serviceId}-{request.ReservationId}",
                OfferingId = $"offering-{serviceId}-{request.OfferingId}",
                CustomerId = request.CustomerId,
                Count = request.Count,
            };

            // 2. Create the batch with a conditional patch
            var batchOptions = new TransactionalBatchPatchItemRequestOptions { IfMatchEtag = offeringResponse.ETag };
            var batch = container.CreateTransactionalBatch(new PartitionKey($"{serviceId}-{request.OfferingId}"))
                .PatchItem(
                    id: $"offering-{serviceId}-{request.OfferingId}", 
                    patchOperations: new[] { PatchOperation.Increment("/remainingCount", -request.Count) },
                    requestOptions: batchOptions)
                .CreateItem(reservationDoc);
            
            using var batchResponse = await batch.ExecuteAsync();

            return batchResponse.IsSuccessStatusCode;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
        {
            // Retry E-TAG failure
            return await MakeReservationAsync(request, serviceId);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
        {
            // This means we are simply retrying a successful reservation
            return true;
        }
    }

    [Activity]
    public async Task CancelReservationAsync(ReservationRequest request, int serviceId)
    {
        try
        {
            // 1. Read both the reservation to get its count, and the offering to get its ETag
            ItemResponse<OfferingDocument> offeringResponse = await container.ReadItemAsync<OfferingDocument>(
                id: $"offering-{serviceId}-{request.OfferingId}",
                partitionKey: new PartitionKey($"{serviceId}-{request.OfferingId}"));

            // No need to read the reservation if we trust the input `request.Count`.
            // If we don't, we would read it here first.

            var offeringEtag = offeringResponse.ETag;

            // 2. Create a transactional batch with a conditional patch to increment inventory
            var batchOptions = new TransactionalBatchPatchItemRequestOptions { IfMatchEtag = offeringEtag };
            var batch = container.CreateTransactionalBatch(new PartitionKey($"{serviceId}-{request.OfferingId}"))
                .DeleteItem(id:  $"reservation-{serviceId}-{request.OfferingId}")
                .PatchItem(
                    id: $"offering-{serviceId}-{request.OfferingId}",
                    patchOperations: new[] { PatchOperation.Increment("/remainingCount", request.Count) },
                    requestOptions: batchOptions);
            await batch.ExecuteAsync();
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
        {
            // Retry E-TAG failure
            await CancelReservationAsync(request, serviceId);
        }
    }
}