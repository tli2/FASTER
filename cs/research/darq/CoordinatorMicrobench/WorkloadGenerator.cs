using FASTER.libdpr;

namespace microbench;

public interface IWorkloadGenerator
{
    void GenerateDependenciesOneRun(IList<DprWorkerId> workers, DprWorkerId me, long currentVersion, List<WorkerVersion> output);
}

public class UniformWorkloadGenerator : IWorkloadGenerator
{
    private Random rand = new();
    private double depProb;

    public UniformWorkloadGenerator(double depProb)
    {
        this.depProb = depProb;
    }

    public void GenerateDependenciesOneRun(IList<DprWorkerId> workers, DprWorkerId me,
        long currentVersion, List<WorkerVersion> output)
    {
        output.Clear();
        if (currentVersion != 1)
            output.Add(new WorkerVersion(me, currentVersion - 1));
        foreach (var worker in workers)
        {
            if (worker.Equals(me)) continue;

            if (rand.NextDouble() < depProb)
                output.Add(new WorkerVersion(worker, currentVersion));
        }
    }
}