using Azure.Storage.Blobs;
using FASTER.core;
using FASTER.libdpr;

namespace TwoPhaseCommit;

public interface IEnvironment
{
    public int GetNumShards();
    
    public string GetShardConnString(int index);

    public int GetShardPort(Options options);

    public FileBasedCheckpointManager GetShardCheckpointManager(Options options);

    public IDevice GetShardDevice(Options options);

    public string GetDprFinderConnString();

    public int GetDprFinderPort();

    public PingPongDevice GetDprFinderDevice();

    public Task PublishResultsAsync(string fileName, MemoryStream bytes);
}

public class LocalDebugEnvironment : IEnvironment
{
    public int GetNumShards() => 2;

    public string GetShardConnString(int index)
    {
        return $"http://127.0.0.1:{15722 + index}";
    }

    public int GetShardPort(Options options)
    {
        return 15722 + options.WorkerName;
    }

    public FileBasedCheckpointManager GetShardCheckpointManager(Options options)
    {
        var result = new FileBasedCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"./participant{options.WorkerName}"), removeOutdated: false);
        result.PurgeAll();
        return result;    
    }

    public IDevice GetShardDevice(Options options) => new NativeStorageDevice($"./participant{options.WorkerName}.log", deleteOnClose: false);

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
    public int GetNumShards() => 4;
    
    
    public string GetShardConnString(int index)
    {
        return $"http://participant{index}.dse.svc.cluster.local:15721";
    }

    public int GetShardPort(Options options)
    {
        return 15721;
    }

    public FileBasedCheckpointManager GetShardCheckpointManager(Options options)
    {
        var result = new FileBasedCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"/mnt/plrs/participant{options.WorkerName}"), removeOutdated: false);
        result.PurgeAll();
        return result;
    }

    public IDevice GetShardDevice(Options options)
    {
        NativeStorageDevice.RemoveIfPresent($"/mnt/plrs/participant{options.WorkerName}.log");
        return new NativeStorageDevice($"/mnt/plrs/participant{options.WorkerName}.log");
    }

    public string GetDprFinderConnString() => "http://dprfinder.dse.svc.cluster.local:15721";

    public int GetDprFinderPort() => 15721;

    public PingPongDevice GetDprFinderDevice()
    {
        NativeStorageDevice.RemoveIfPresent("/mnt/plrs/finder1");
        NativeStorageDevice.RemoveIfPresent("/mnt/plrs/finder2");

        var device1 = new NativeStorageDevice("/mnt/plrs/finder1");
        var device2 = new NativeStorageDevice("/mnt/plrs/finder2");
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