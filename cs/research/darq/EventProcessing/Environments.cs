using Azure.Storage.Blobs;
using FASTER.core;
using FASTER.libdpr;

namespace EventProcessing;

public interface IEnvironment
{
    Dictionary<int, (int, string)> GetClusterMap();

    Task PublishResultsAsync(string fileName, MemoryStream bytes);
    
    int GetPubsubServicePort(int hostId);

    public FileBasedCheckpointManager GetDarqCheckpointManager(int topicId);

    public IDevice GetDarqDevice(int topicId);

    public string GetDprFinderConnString();

    public int GetDprFinderPort();

    public PingPongDevice GetDprFinderDevice();
}

public class LocalDebugEnvironment : IEnvironment
{
    private readonly Dictionary<int, (int, string)> clusterMap = new()
    {
        { 0, (0, "http://127.0.0.1:15721") },
        { 1, (0, "http://127.0.0.1:15721") },
        { 2, (0, "http://127.0.0.1:15721") },
        { 3, (0, "http://127.0.0.1:15721") }
    };

    public Dictionary<int, (int, string)> GetClusterMap()
    {
        return clusterMap;
    }
    

    public int GetPubsubServicePort(int hostId)
    {
        return 15721 + hostId;
    }

    public FileBasedCheckpointManager GetDarqCheckpointManager(int topicId)
    {
        var result = new FileBasedCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"D:\\darq{topicId}"), removeOutdated: false);
        result.PurgeAll();
        return result;
    }

    public IDevice GetDarqDevice(int topicId)
    {
        return new ManagedLocalStorageDevice($"D:\\darq{topicId}.log", deleteOnClose: true);
    }

    public string GetDprFinderConnString() => "http://127.0.0.1:15720";

    public int GetDprFinderPort() => 15720;

    public PingPongDevice GetDprFinderDevice()
    {
        var device1 = new LocalMemoryDevice(1 << 24, 1 << 24, 1);
        var device2 = new LocalMemoryDevice(1 << 24, 1 << 24, 1);
        return new PingPongDevice(device1, device2, true);
    }

    public Task PublishResultsAsync(string fileName, MemoryStream bytes)
    {
        Console.WriteLine($"Results for {fileName}:");
        var reader = new StreamReader(bytes);
        var text = reader.ReadToEnd();
        // Print to console
        Console.Write(text);
        return Task.CompletedTask;
    }
}

public class KubernetesLocalStorageEnvironment : IEnvironment
{
    private bool cleanStart;    
    private readonly Dictionary<int, (int, string)> clusterMap = new()
    {
        { 0, (0, "http://pubsub0.dse.svc.cluster.local:15721") },
        { 1, (1, "http://pubsub1.dse.svc.cluster.local:15721") },
        { 2, (0, "http://pubsub0.dse.svc.cluster.local:15721") },
        { 3, (1, "http://pubsub1.dse.svc.cluster.local:15721") }
    };

    public KubernetesLocalStorageEnvironment(bool cleanStart)
    {
        this.cleanStart = cleanStart;
    }

    public Dictionary<int, (int, string)> GetClusterMap()
    {
        return clusterMap;
    }

    public int GetPubsubServicePort(int hostId)
    {
        return 15721;
    }

    public FileBasedCheckpointManager GetDarqCheckpointManager(int topicId)
    {
        var result = new FileBasedCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"/mnt/plrs/darq{topicId}"), removeOutdated: false);
        if (cleanStart)
            result.PurgeAll();
        return result;
    }

    public IDevice GetDarqDevice(int topicId)
    {
        if (cleanStart)
            ManagedLocalStorageDevice.RemoveIfPresent($"/mnt/plrs/darq{topicId}.log");
        return new ManagedLocalStorageDevice($"/mnt/plrs/darq{topicId}.log");
    }

    public string GetDprFinderConnString() => "http://dprfinder.dse.svc.cluster.local:15721";

    public int GetDprFinderPort() => 15721;

    public PingPongDevice GetDprFinderDevice()
    {
        if (cleanStart)
        {
            ManagedLocalStorageDevice.RemoveIfPresent("/mnt/plrs/finder1");
            ManagedLocalStorageDevice.RemoveIfPresent("/mnt/plrs/finder2");
        }

        var device1 = new ManagedLocalStorageDevice("/mnt/plrs/finder1", recoverDevice: true);
        var device2 = new ManagedLocalStorageDevice("/mnt/plrs/finder2", recoverDevice: true);
        return new PingPongDevice(device1, device2, true);
    }

    public async Task PublishResultsAsync(string fileName, MemoryStream bytes)
    {
        var connString = Environment.GetEnvironmentVariable("AZURE_RESULTS_CONN_STRING");
        var blobServiceClient = new BlobServiceClient(connString);
        var blobContainerClient = blobServiceClient.GetBlobContainerClient("results");

        await blobContainerClient.CreateIfNotExistsAsync();
        var blobClient = blobContainerClient.GetBlobClient(fileName);

        await blobClient.UploadAsync(bytes, overwrite: true);
    }
}