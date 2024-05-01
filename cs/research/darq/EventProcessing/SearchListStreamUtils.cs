using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using dse.services;
using FASTER.libdpr;
using MathNet.Numerics.Distributions;
using Newtonsoft.Json;
using pubsub;
using StepRequest = FASTER.libdpr.StepRequest;

namespace EventProcessing;

public class SearchListStreamUtils
{
    public const string relevantSearchTerm = "fever";

    public const int WindowSizeMilli = 500;

    public const int numRegions = 8;

    public static int GetRegionCode(string ip)
    {
        return ip.Split(".").Select(int.Parse).Sum() % numRegions;
    }
}

public class SearchListDataGenerator
{
    private string outputFile;

    private double trendProb, relevantProb;

    // In number of searches
    private int avgTrendLength, stdTrendLength;
    private int searchTermLength;
    private int termsPerSecond;
    private int numSearchTerms;

    public SearchListDataGenerator SetOutputFile(string outputFile)
    {
        this.outputFile = outputFile;
        return this;
    }

    public SearchListDataGenerator SetSearchTermRelevantProb(double relevantProb)
    {
        this.relevantProb = relevantProb;
        return this;
    }

    public SearchListDataGenerator SetTrendParameters(double trendProb, int avgTrendLength, int stdTrendLength)
    {
        this.trendProb = trendProb;
        this.avgTrendLength = avgTrendLength;
        this.stdTrendLength = stdTrendLength;
        return this;
    }

    public SearchListDataGenerator SetThroughput(int termsPerSecond)
    {
        this.termsPerSecond = termsPerSecond;
        return this;
    }

    public SearchListDataGenerator SetSearchTermLength(int searchTermLength)
    {
        this.searchTermLength = searchTermLength;
        return this;
    }

    public SearchListDataGenerator SetNumSearchTerms(int numSearchTerms)
    {
        this.numSearchTerms = numSearchTerms;
        return this;
    }

    private static string GenerateRandomIp(Random random)
    {
        var component1 = random.Next(256);
        var component2 = random.Next(256);
        var component3 = random.Next(256);
        var component4 = random.Next(256);
        return $"{component1}.{component2}.{component3}.{component4}";
    }

    private string PopulateSearchTerm(Random random, StringBuilder builder, bool relevant)
    {
        builder.Clear();
        var length = searchTermLength;
        if (relevant)
        {
            builder.Append(SearchListStreamUtils.relevantSearchTerm);
            length -= SearchListStreamUtils.relevantSearchTerm.Length;
        }

        for (var i = 0; i < length; i++)
            // Generate ascii alphabets
            builder.Append((char)random.Next(97, 123));
        return builder.ToString();
    }

    private string[] BuildRegionReverseLookUpTable(Random random)
    {
        var result = new string[SearchListStreamUtils.numRegions];
        for (var i = 0; i < SearchListStreamUtils.numRegions; i++)
        {
            string ip;
            do
            {
                ip = GenerateRandomIp(random);
            } while (SearchListStreamUtils.GetRegionCode(ip) != i);

            result[i] = ip;
        }

        return result;
    }

    private void ComputeTrend(int numTermsGenerated, Random random, int[] trendTable)
    {
        for (var i = 0; i < SearchListStreamUtils.numRegions; i++)
        {
            if (trendTable[i] < numTermsGenerated) trendTable[i] = -1;
            if (trendTable[i] == -1 && random.NextDouble() < trendProb)
                trendTable[i] = i + (int)Math.Max(0, Normal.Sample(random, avgTrendLength, stdTrendLength));
        }
    }

    public void Generate()
    {
        var random = new Random();
        // Pre-populate some reverse-lookup table for regions;
        var regionTable = BuildRegionReverseLookUpTable(random);
        var trendTable = new int[SearchListStreamUtils.numRegions];

        try
        {
            File.Delete(outputFile);
        }
        catch (Exception)
        {
        }

        using var writer = new StreamWriter(new FileStream(outputFile, FileMode.Create));
        var builder = new StringBuilder();
        var jsonObject = new SearchListJson();
        var numSeconds = numSearchTerms / termsPerSecond;
        var searchesPerSecond = new int[numSeconds];
        Poisson.Samples(random, searchesPerSecond, termsPerSecond);
        for (var time = 0; time < numSeconds; time++)
        {
            var milliStep = 1000.0 / searchesPerSecond[time];
            for (var i = 0; i < searchesPerSecond[time]; i++)
            {
                ComputeTrend(i, random, trendTable);
                var region = random.Next(SearchListStreamUtils.numRegions);
                // triple the probability that we generate a relevant search term by 3 times if trending
                var prob = trendTable[region] == -1 ? relevantProb : relevantProb * 3;
                var relevant = random.NextDouble() < prob;
                jsonObject.SearchTerm = PopulateSearchTerm(random, builder, relevant);
                jsonObject.UserId = random.NextInt64();
                jsonObject.IP = regionTable[region];
                jsonObject.Timestamp = time * 1000 + (int)Math.Floor(milliStep * i);
                writer.WriteLine(JsonConvert.SerializeObject(jsonObject));
            }
        }
    }
}

public class SearchListDataLoader
{
    private string filename;
    private List<string> rawJsons = new();
    private List<long> timestamps = new();
    private SpPubSubServiceClient client;
    private int topicName;
    private Stopwatch stopwatch;
    
    public SearchListDataLoader(string filename, SpPubSubServiceClient client, int topicName, Stopwatch stopwatch)
    {
        this.filename = filename;
        this.client = client;
        this.topicName = topicName;
        this.stopwatch = stopwatch;
    }

    public int LoadData()
    {
        Console.WriteLine("Started loading json messages from file");
        rawJsons = File.ReadLines(filename).ToList();
        timestamps = rawJsons.Select(s => JsonConvert.DeserializeObject<SearchListJson>(s).Timestamp).ToList()!;
        Console.WriteLine($"Loading of {timestamps.Count} messages complete");
        return timestamps.Count;
    }

    public async Task SequentialIssue()
    {
        stopwatch.Start();
        var batched = new EnqueueRequest
        {
            ProducerId = -1,
            TopicId = topicName,
        };
        for (var i = 0; i < timestamps.Count; i++)
        {
            var time = timestamps[i];
            var currentTime = stopwatch.ElapsedMilliseconds;
            while (currentTime < time)
            {
                if (batched.Events.Count != 0)
                {
                    while (true)
                    {
                        try
                        {
                            await client.EnqueueEventsAsync(batched);
                            break;
                        }
                        catch (Exception e)
                        {
                            // Wait a bit so failures can recover
                            await Task.Delay(10);
                        }
                    }
                    batched = new EnqueueRequest
                    {
                        ProducerId = 0,
                        TopicId = topicName,
                    };
                }
                Thread.Yield();
                currentTime = stopwatch.ElapsedMilliseconds;
            }
            batched.SequenceNum = i;
            batched.Events.Add(rawJsons[i]);
            if (batched.Events.Count >= 1024)
            {
                while (true)
                {
                    try
                    {
                        await client.EnqueueEventsAsync(batched);
                        break;
                    }
                    catch (Exception e)
                    {
                        // Wait a bit so failures can recover
                        await Task.Delay(10);
                    }
                }
                batched = new EnqueueRequest
                {
                    ProducerId = -1,
                    TopicId = topicName,
                };
            }
        }
        var termination = new EnqueueRequest
        {
            ProducerId = -1,
            TopicId = topicName,
        };
        termination.Events.Add($"termination");
        while (true)
        {
            try
            {
                await client.EnqueueEventsAsync(termination);
                break;
            }
            catch (Exception e)
            {
                // Wait a bit so failures can recover
                await Task.Delay(10);
            }
        }
        Console.WriteLine("########## Finished publishing messages");
    }

    public async Task ParallelIssue(int degree = 128)
    {
        var semaphore = new SemaphoreSlim(degree, degree);
        stopwatch.Start();
        var batched = new EnqueueRequest
        {
            ProducerId = -1,
            TopicId = topicName,
        };
        for (var i = 0; i < timestamps.Count; i++)
        {
            var time = timestamps[i];
            var currentTime = stopwatch.ElapsedMilliseconds;
            while (currentTime < time)
            {
                if (batched.Events.Count != 0)
                {
                    var batched1 = batched;
                    await semaphore.WaitAsync();
                    _ = Task.Run(async () =>
                    {
                        while (true)
                        {
                            try
                            {
                                await client.EnqueueEventsAsync(batched1);
                                semaphore.Release();
                                return;
                            }
                            catch (Exception e)
                            {
                                // Wait a bit so failures can recover
                                await Task.Delay(10);
                            }
                        }
                    });
                    batched = new EnqueueRequest
                    {
                        ProducerId = -1,
                        TopicId = topicName,
                    };
                }
                Thread.Yield();
                currentTime = stopwatch.ElapsedMilliseconds;
            }
            batched.SequenceNum = i;
            batched.Events.Add(rawJsons[i]);
            if (batched.Events.Count >= 64)
            {
                await semaphore.WaitAsync();
                var now = stopwatch.ElapsedMilliseconds;
                var batched1 = batched;
                _ = Task.Run(async () =>
                {
                    while (true)
                    {
                        try
                        {
                            await client.EnqueueEventsAsync(batched1);
                            semaphore.Release();
                            return;
                        }
                        catch (Exception e)
                        {
                            // Wait a bit so failures can recover
                            await Task.Delay(10);
                        }
                    }
                });
                batched = new EnqueueRequest
                {
                    ProducerId = -1,
                    TopicId = topicName,
                };
            }
        }
        var termination = new EnqueueRequest
        {
            ProducerId = 0,
            SequenceNum = timestamps.Count,
            TopicId = topicName,
        };
        termination.Events.Add($"termination");
        while (true)
        {
            try
            {
                await client.EnqueueEventsAsync(termination);
                break;
            }
            catch (Exception e)
            {
                // Wait a bit so failures can recover
                await Task.Delay(10);
            }
        }
        Console.WriteLine("########## Finished publishing messages");
    }
}