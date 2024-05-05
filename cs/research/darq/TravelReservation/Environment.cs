using Azure.Storage.Blobs;
using FASTER.core;
using FASTER.devices;
using FASTER.libdpr;

namespace TravelReservation;

public interface IEnvironment
{
    public string GetOrchestratorConnString();

    public int GetOrchestratorPort(Options options);

    public FileBasedCheckpointManager GetOrchestratorCheckpointManager(Options options);

    public IDevice GetOrchestratorDevice(Options options);

    public string GetServiceConnString(int index);

    public int GetServicePort(Options options);

    public FileBasedCheckpointManager GetServiceCheckpointManager(Options options);

    public IDevice GetServiceDevice(Options options);

    public string GetDprFinderConnString();

    public int GetDprFinderPort();

    public PingPongDevice GetDprFinderDevice();

    public Task PublishResultsAsync(string fileName, MemoryStream bytes);
}

public class LocalDebugEnvironment : IEnvironment
{
    private int roundRobin;
    public string GetOrchestratorConnString()
    {
        var port = roundRobin++ / 2 == 0 ? 15724 : 15725;
        return $"http://127.0.0.1:{port}";
    }

    public int GetOrchestratorPort(Options options)
    {
        return options.WorkerName + 15721;
    }

    public FileBasedCheckpointManager GetOrchestratorCheckpointManager(Options options)
    {
        var result = new FileBasedCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"D:\\orchestrators{options.WorkerName}"), removeOutdated: false);
        result.PurgeAll();
        return result;
    }

    public IDevice GetOrchestratorDevice(Options options) =>
        new ManagedLocalStorageDevice($"D:\\orchestator{options.WorkerName}.log", deleteOnClose: true);

    public string GetServiceConnString(int index)
    {
        return $"http://127.0.0.1:{15721 + index}";
    }

    public int GetServicePort(Options options)
    {
        return 15721 + options.WorkerName;
    }

    public FileBasedCheckpointManager GetServiceCheckpointManager(Options options)
    {
        var result = new FileBasedCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"D:\\service{options.WorkerName}"), removeOutdated: false);
        result.PurgeAll();
        return result;
    }

    public IDevice GetServiceDevice(Options options) =>
        new ManagedLocalStorageDevice($"D:\\service{options.WorkerName}.log", deleteOnClose: true);

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

    public string GetOrchestratorConnString() => "http://orchestrator.dse.svc.cluster.local:15721";

    public int GetOrchestratorPort(Options options) => 15721;

    public FileBasedCheckpointManager GetOrchestratorCheckpointManager(Options options)
    {
        var result = new FileBasedCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"/mnt/plrs/orchestrators{options.WorkerName}"), removeOutdated: false);
        if (cleanStart)
            result.PurgeAll();
        return result;
    }

    public IDevice GetOrchestratorDevice(Options options)
    {
        if (cleanStart)
            ManagedLocalStorageDevice.RemoveIfPresent($"/mnt/plrs/orchestrator{options.WorkerName}.log");
        return new ManagedLocalStorageDevice($"/mnt/plrs/orchestrator{options.WorkerName}.log");
    }

    public string GetServiceConnString(int index) => $"http://service{index}.dse.svc.cluster.local:15721";

    public int GetServicePort(Options options) => 15721;

    public FileBasedCheckpointManager GetServiceCheckpointManager(Options options)
    {
        var result = new FileBasedCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"/mnt/plrs/service{options.WorkerName}"), removeOutdated: false);
        if (cleanStart)
            result.PurgeAll();
        return result;
    }

    public IDevice GetServiceDevice(Options options)
    {
        if (cleanStart)
            ManagedLocalStorageDevice.RemoveIfPresent($"/mnt/plrs/service{options.WorkerName}.log");
        return new ManagedLocalStorageDevice($"/mnt/plrs/service{options.WorkerName}.log");
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

public class KubernetesCloudStorageEnvironment : IEnvironment
{
    private bool cleanStart;

    public KubernetesCloudStorageEnvironment(bool cleanStart)
    {
        this.cleanStart = cleanStart;
    }

    public string GetOrchestratorConnString() => "http://orchestrator.dse.svc.cluster.local:15721";

    public int GetOrchestratorPort(Options options) => 15721;

    public FileBasedCheckpointManager GetOrchestratorCheckpointManager(Options options)
    {
        var result = new FileBasedCheckpointManager(
            new AzureStorageNamedDeviceFactory(Environment.GetEnvironmentVariable("AZURE_CONN_STRING")),
            new DefaultCheckpointNamingScheme($"orchestrators/{options.WorkerName}/checkpoints"),
            removeOutdated: false);
        if (cleanStart)
            result.PurgeAll();
        return result;
    }

    public IDevice GetOrchestratorDevice(Options options)
    {
        var result = new AzureStorageDevice(Environment.GetEnvironmentVariable("AZURE_CONN_STRING"), "orchestrators",
            options.WorkerName.ToString(), "darq");
        if (cleanStart)
            result.PurgeAll();
        return result;
    }

    public string GetServiceConnString(int index) => $"http://service{index}.dse.svc.cluster.local:15721";

    public int GetServicePort(Options options) => 15721;

    public FileBasedCheckpointManager GetServiceCheckpointManager(Options options)
    {
        var result = new FileBasedCheckpointManager(
            new AzureStorageNamedDeviceFactory(Environment.GetEnvironmentVariable("AZURE_CONN_STRING")),
            new DefaultCheckpointNamingScheme($"services/{options.WorkerName}/checkpoints"), removeOutdated: false);
        if (cleanStart)
            result.PurgeAll();
        return result;
    }

    public IDevice GetServiceDevice(Options options)
    {
        var result = new AzureStorageDevice(Environment.GetEnvironmentVariable("AZURE_CONN_STRING"), "services",
            options.WorkerName.ToString(), "log");
        if (cleanStart)
            result.PurgeAll();
        return result;
    }

    public string GetDprFinderConnString() => "http://dprfinder.dse.svc.cluster.local:15721";

    public int GetDprFinderPort() => 15721;

    public PingPongDevice GetDprFinderDevice()
    {
        var device1 = new AzureStorageDevice(Environment.GetEnvironmentVariable("AZURE_CONN_STRING"), "dprfinder",
            "data", "1");
        var device2 = new AzureStorageDevice(Environment.GetEnvironmentVariable("AZURE_CONN_STRING"), "dprfinder",
            "data", "2");
        if (cleanStart)
        {
            device1.PurgeAll();
            device2.PurgeAll();
        }

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