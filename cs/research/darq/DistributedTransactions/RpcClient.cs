using Grpc.Net.Client;
using Google.Protobuf;
using FASTER.client;
using FASTER.libdpr;
using FASTER.darq;
using System.Collections.Concurrent;


namespace DB {
public abstract class RpcClient {
    protected long partitionId;
    protected Dictionary<long, GrpcChannel> clusterMap;

    public RpcClient(long partitionId, Dictionary<long, GrpcChannel> clusterMap){
        this.partitionId = partitionId;
        this.clusterMap = clusterMap;
    }

    public long GetId(){
        return partitionId;
    }
    public ReadOnlySpan<byte> Read(PrimaryKey key, TransactionContext ctx){
        var channel = GetServerChannel(key);
        var client = new TransactionProcessor.TransactionProcessorClient(channel);
        PbPrimaryKey pk = new PbPrimaryKey { Keys = {key.Key1, key.Key2, key.Key3, key.Key4, key.Key5, key.Key6}, Table = key.Table};

        var reply = client.Read(new ReadRequest { Key = pk, Tid = ctx.tid, PartitionId = partitionId});
        
        return reply.Value.ToByteArray();
    }

    public (byte[], PrimaryKey) ReadSecondary(PrimaryKey tempPk, byte[] key, TransactionContext ctx){
        var channel = GetServerChannel(tempPk);
        var client = new TransactionProcessor.TransactionProcessorClient(channel);

        var reply = client.ReadSecondary(
            new ReadSecondaryRequest { 
                Key = ByteString.CopyFrom(key),
                Table = tempPk.Table,
                Tid = ctx.tid,
                PartitionId = partitionId
            }
        );
        return (reply.Value.ToByteArray(), new PrimaryKey(tempPk.Table, reply.Key.Keys.ToArray()[0], reply.Key.Keys.ToArray()[1], reply.Key.Keys.ToArray()[2], reply.Key.Keys.ToArray()[3], reply.Key.Keys.ToArray()[4], reply.Key.Keys.ToArray()[5]));
    }

    public void PopulateTables(BenchmarkConfig cfg, TpccConfig tpccCfg){
        foreach (var entry in clusterMap){
            if (entry.Key == partitionId) continue;
            var channel = entry.Value;
            var client = new TransactionProcessor.TransactionProcessorClient(channel);
            var reply = client.PopulateTables(
                new PopulateTablesRequest {
                    Seed = cfg.seed,
                    Ratio = cfg.ratio,
                    ThreadCount = cfg.threadCount,
                    AttrCount = cfg.attrCount,
                    PerThreadDataCount = cfg.perThreadDataCount,
                    IterationCount = cfg.iterationCount,
                    PerTransactionCount = cfg.perTransactionCount,
                    NCommitterThreads = cfg.nCommitterThreads,
                    NumWh = tpccCfg.NumWh,
                    NumDistrict = tpccCfg.NumDistrict,
                    NumCustomer = tpccCfg.NumCustomer,
                    NumItem = tpccCfg.NumItem,
                    NumOrder = tpccCfg.NumOrder,
                    NumStock = tpccCfg.NumStock,
                    NewOrderCrossPartitionProbability = tpccCfg.NewOrderCrossPartitionProbability,
                    PaymentCrossPartitionProbability = tpccCfg.PaymentCrossPartitionProbability,
                    PartitionsPerThread = tpccCfg.PartitionsPerThread
                }
            );
            if (!reply.Success) throw new System.Exception("Failed to populate tables");
        }
    }


    /// <summary>
    /// Returns the appropriate channel to talk to correct shard
    /// </summary>
    /// <param name="key"></param>
    /// <returns>Null if key maps to itself, appropriate channel otherwise</returns>
    private GrpcChannel? GetServerChannel(PrimaryKey key){
        var id = HashKeyToDarqId(key);
        if (id == partitionId) return null;
        return clusterMap[id];
    }

    abstract public long HashKeyToDarqId(PrimaryKey key);

    public bool IsLocalKey(PrimaryKey key){
        if (key.Table == (int)TableType.Item) return true;
        return HashKeyToDarqId(key) == partitionId;
    }

    public int GetNumServers(){
        return clusterMap.Count();
    }
}

public class TpccRpcClient : RpcClient
{
    public TpccRpcClient(long partitionId, Dictionary<long, GrpcChannel> clusterMap) : base(partitionId, clusterMap)
    {
    }

    public override long HashKeyToDarqId(PrimaryKey key){
        // return partitionId;
        if (key.Table == (int)TableType.Item) return partitionId;
        return (key.Key1 - 1) / 4; // TODO: make it / partitionsPerThread * threadCount
    }
}

public class YcsbRpcClient : RpcClient
{
    public YcsbRpcClient(long partitionId, Dictionary<long, GrpcChannel> clusterMap) : base(partitionId, clusterMap)
    {
    }

    public override long HashKeyToDarqId(PrimaryKey key){
        // TODO: arbitrary for now, define some rules for how to map keys to servers
        // return key.Keys[0] % clusterMap.Count;
        return 0;
    }

}

}