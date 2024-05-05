using System.Text;
using MathNet.Numerics.Distributions;

namespace TravelReservation;

public class WorkloadGenerator
{
    private int numClients, numServices, numWorkflowsPerSecond, numSeconds, numOfferings;
    private string baseFileName;

    public WorkloadGenerator SetNumClients(int numClients)
    {
        this.numClients = numClients;
        return this;
    }

    public WorkloadGenerator SetNumServices(int numServices)
    {
        this.numServices = numServices;
        return this;
    }

    public WorkloadGenerator SetNumWorkflowsPerSecond(int numWorkflowsPerSecond)
    {
        this.numWorkflowsPerSecond = numWorkflowsPerSecond;
        return this;
    }

    public WorkloadGenerator SetNumSeconds(int numSeconds)
    {
        this.numSeconds = numSeconds;
        return this;
    }

    public WorkloadGenerator SetNumOfferings(int numOfferings)
    {
        this.numOfferings = numOfferings;
        return this;
    }

    public WorkloadGenerator SetBaseFileName(string baseFileName)
    {
        this.baseFileName = baseFileName;
        return this;
    }

    public void GenerateWorkloadTrace(Random random)
    {
        // Generate database
        // Over provision a to ensure we don't all abort
        var numOfferingsRequired = (int) Math.Ceiling(numClients * numWorkflowsPerSecond * numSeconds * 4.0 / numOfferings);
        for (var i = 0; i < numServices; i++)
        {
            using var writer = new StreamWriter($"{baseFileName}-service-{i}.csv");
            for (var j = 0; j < numOfferings; j++)
            {
                var builder = new StringBuilder();
                // offering id
                builder.Append(j);
                builder.Append(',');
                // entityId id
                builder.Append(random.NextInt64());
                builder.Append(',');
                // price
                builder.Append(random.Next(100, 300));
                builder.Append(',');
                // num reservable
                // TODO(Tianyu): Randomize a bit?
                builder.Append(numOfferingsRequired);
                writer.WriteLine(builder.ToString());
            }
        }

        // Generate workload
        var poisson = new Poisson(numWorkflowsPerSecond);
        for (var i = 0; i < numClients; i++)
        {
            List<long> requestTimestamps = new();
            for (var second = 0; second < numSeconds; second++)
            {
                var numRequests = poisson.Sample();
                for (var request = 0; request < numRequests; request++)
                    requestTimestamps.Add(1000 * second + random.Next(1000));
            }

            requestTimestamps.Sort();
            var uniqueIds = new Dictionary<long, byte>();
            using var writer = new StreamWriter($"{baseFileName}-client-{i}.csv");
            foreach (var timestamp in requestTimestamps)
            {
                var builder = new StringBuilder();
                // Issue time
                builder.Append(timestamp);
                builder.Append(',');
                // Workflow Id -- must ensure uniqueness
                long id;
                do
                {
                    id = random.NextInt64(0xFFFFFFFFFFFF) / numClients + i;
                } while (!uniqueIds.TryAdd(id, 0));
                builder.Append(id);
                
                for (var j = 0; j < numServices; j++)
                {
                    builder.Append(',');
                    // Reservation Id -- must ensure uniqueness
                    do
                    {
                        id = random.NextInt64(0xFFFFFFFFFFFF) / numClients + i;
                    } while (!uniqueIds.TryAdd(id, 0));

                    builder.Append(id);
                    builder.Append(',');
                    // offeringId
                    builder.Append(random.NextInt64(numOfferings));
                    builder.Append(',');
                    // customerId
                    builder.Append(random.NextInt64());
                    builder.Append(',');
                    // count 
                    // TODO(Tianyu): More than 1?
                    builder.Append(1);
                }

                writer.WriteLine(builder.ToString());
            }
        }
    }
}