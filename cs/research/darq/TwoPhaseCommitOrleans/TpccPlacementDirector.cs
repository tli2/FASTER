using Orleans;
using Orleans.Placement;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace TwoPhaseCommitOrleans;

public class TpccPlacementDirector : IPlacementDirector
{
    // This method is called whenever Orleans needs to place a new activation
    public Task<SiloAddress> OnAddActivation(
        PlacementStrategy strategy, 
        PlacementTarget target, 
        IPlacementContext context)
    {
        var silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();

        if (silos.Length == 0)
            throw new OrleansException("No compatible silos found for TPC-C placement.");

        var warehouseId = int.Parse(target.GrainIdentity.Key.ToString().Split("-")[0]);
        
        // Round-robin placement
        return Task.FromResult(silos[warehouseId % silos.Length]);
    }
}

[Serializable]
[GenerateSerializer]
public class TpccPlacementStrategy : PlacementStrategy
{
    // It is stateless, so we don't need fields.
    
}

// 2. The Attribute (The tag you put on the interface)
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, AllowMultiple = false)]
public class TpccPlacementAttribute : PlacementAttribute
{
    // Pass a new instance of the strategy to the base constructor
    public TpccPlacementAttribute() : base(new TpccPlacementStrategy())
    {
    }
}