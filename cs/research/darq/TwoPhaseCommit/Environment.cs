using Azure.Storage.Blobs;
using FASTER.core;
using FASTER.libdpr;

namespace TwoPhaseCommit;

public interface IEnvironment
{
    public string GetCoordinatorConnString();

    public int GetCoordinatorPort(Options options);

    public FileBasedCheckpointManager GetCoordinatorCheckpointManager(Options options);

    public IDevice GetCoordinatorDevice(Options options);

    public string GetParticipantConnString(int index);

    public int GetParticipantPort(Options options);

    public FileBasedCheckpointManager GetParticipantCheckpointManager(Options options);

    public IDevice GetParticipantDevice(Options options);

    public string GetDprFinderConnString();

    public int GetDprFinderPort();

    public PingPongDevice GetDprFinderDevice();

    public Task PublishResultsAsync(string fileName, MemoryStream bytes);
}

public class LocalDebugEnvironment : IEnvironment
{
    public string GetCoordinatorConnString()
    {
        return $"http://127.0.0.1:15721";
    }

    public int GetCoordinatorPort(Options options)
    {
        return 15721;
    }

    public FileBasedCheckpointManager GetCoordinatorCheckpointManager(Options options)
    {
        var result = new FileBasedCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"D:\\coordinator{options.WorkerName}"), removeOutdated: false);
        result.PurgeAll();
        return result;
    }

    public IDevice GetCoordinatorDevice(Options options) =>
        new NativeStorageDevice($"D:\\coordinator{options.WorkerName}.log", deleteOnClose: false);

    public string GetParticipantConnString(int index)
    {
        return $"http://127.0.0.1:{15722 + index}";
    }

    public int GetParticipantPort(Options options)
    {
        return 15722 + options.WorkerName;
    }

    public FileBasedCheckpointManager GetParticipantCheckpointManager(Options options)
    {
        var result = new FileBasedCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"D:\\participant{options.WorkerName}"), removeOutdated: false);
        result.PurgeAll();
        return result;    
    }

    public IDevice GetParticipantDevice(Options options) => new NativeStorageDevice($"D:\\participant{options.WorkerName}.log", deleteOnClose: false);

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
    
    public KubernetesLocalStorageEnvironment(bool cleanStart)
    {
        this.cleanStart = cleanStart;
    }
    public string GetCoordinatorConnString()
    {
        return "http://coordinator.dse.svc.cluster.local:15721"; 
    }

    public int GetCoordinatorPort(Options options)
    {
        return 15721;
    }

    public FileBasedCheckpointManager GetCoordinatorCheckpointManager(Options options)
    {
        var result = new FileBasedCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"/mnt/plrs/coordinator{options.WorkerName}"), removeOutdated: false);
        if (cleanStart)
            result.PurgeAll();
        return result;
    }

    public IDevice GetCoordinatorDevice(Options options)
    {
        if (cleanStart)
            NativeStorageDevice.RemoveIfPresent($"/mnt/plrs/coordinator{options.WorkerName}.log");
        return new NativeStorageDevice($"/mnt/plrs/coordinator{options.WorkerName}.log");    
    }

    public string GetParticipantConnString(int index)
    {
        return $"http://participant{index}.dse.svc.cluster.local:15721";
    }

    public int GetParticipantPort(Options options)
    {
        return 15721;
    }

    public FileBasedCheckpointManager GetParticipantCheckpointManager(Options options)
    {
        var result = new FileBasedCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"/mnt/plrs/participant{options.WorkerName}"), removeOutdated: false);
        if (cleanStart)
            result.PurgeAll();
        return result;
    }

    public IDevice GetParticipantDevice(Options options)
    {
        if (cleanStart)
            NativeStorageDevice.RemoveIfPresent($"/mnt/plrs/participant{options.WorkerName}.log");
        return new NativeStorageDevice($"/mnt/plrs/participant{options.WorkerName}.log");
    }

    public string GetDprFinderConnString() => "http://dprfinder.dse.svc.cluster.local:15721";

    public int GetDprFinderPort() => 15721;

    public PingPongDevice GetDprFinderDevice()
    {
        if (cleanStart)
        {
            NativeStorageDevice.RemoveIfPresent("/mnt/plrs/finder1");
            NativeStorageDevice.RemoveIfPresent("/mnt/plrs/finder2");
        }

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