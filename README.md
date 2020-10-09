# FASTER + DPR

Please see the master branch for documentation on FASTER itself. This branch contains a prototype 
implementation of D-FASTER, based on the DPR model. Most of the code is under
serverless, with the exception
of non-blocking rollback implementation, which is incorporated with FASTER itself.

To re-run the experiments, compile either `cs/DprMicrobench`,
`cs/ServerlessYcsb`, or `cs/YcsbClient`. Set up a cluster and replace the cluster information under `XXCoordinator`
with the cluster, and run the executable with option `worker` on each of the machines. From a different machine, run
the `coordinator` option to start a run with the  appropriate command-line arguments. In addition to this, some
constants can be tuned under `BenchmarkConsts` in the main file of each benchmark.

An example of this would be if one wishes to reproduce the failure experiment from section 6.5 on a smaller cluster of two
machines, one should replace `clsuterConfig` in `cs/YcsbServerClient/YcsbServerClient/YcsbCoordinator.cs` with the desired 
cluster, and set `BenchmarkConsts::kTriggerRecovery = true` in `cs/YcsbServerClient/YcsbServerClient/Program.cs`. Then, on
the two machines, run `<executable> -t worker -n 0` and `<executable> -t worker -n 1` respectively, with `-n` corresponding
to their position in the declaration. Then, from a third machine, run `<executable> -t coordinator -c 16 -s 16 -b 1024 -d uniform -r 50 -i 100 -w 1024`
to spawn an experiment. Documentation for the various command-line arguments are shown in `cs/YcsbServerClient/YcsbServerClient/Program.cs`.



