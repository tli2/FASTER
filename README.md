# FASTER + DPR

Please see the master branch for documentation on FASTER itself. This branch contains a prototype 
implementation of D-FASTER, based on the DPR model. Most of the code is under
serverless, with the exception
of non-blocking rollback implementation, which is incorporated with FASTER itself.

To re-run the experiments, compile either DprMicrobench,
ServerlessYcsb, or YcsbClient. Set up a cluster and replace the cluster information under `XXCoordinator` with the cluster, and run the executable with option
`worker` on each of the machines. From a different machine, run the `coordinator` option to start a run with the 
appropriate command-line arguments. In addition to this, some constants can be tuned under `BenchmarkConsts` in
the main file of each benchmark. 



