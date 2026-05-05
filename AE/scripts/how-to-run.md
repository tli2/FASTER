# How to run the experiments

Each script runs **one experimental condition**. Run it once per data point, varying the flags to sweep across conditions. Results are uploaded to Azure Blob Storage on completion.

## Setup

Fill in `AE/scripts/env.sh` with your connection strings (the file is created automatically on first run). Then source it or let each script source it via `common.sh`.

Required variables per experiment group:

| Variable | Required by |
|---|---|
| `AE_RESULTS_CONN_STRING` | all cluster experiments (1–8) |
| `AE_COSMOS_CONN_STRING` | exp 3 only |
| `AE_AZURE_TABLE_CONN_STRING` | exp 7 only |

Optional:
- `AE_NAMESPACE` — Kubernetes namespace (default: `dse`)

---

## Experiment 1 — TravelReservation latency (Fig 5a)

```
exp01-travel-latency.sh --speculative true|false --n-services N
```

Paper sweep: `--speculative true` and `--speculative false`, `--n-services 1` through `10`.


---

## Experiment 2 — TravelReservation throughput-latency (Fig 5b)

```
exp02-travel-throughput.sh --speculative true|false --wps WPS --window W
```

Paper sweep: `--wps` in `{100,200,…,1000}`, `--window` in `{16,32,64,128,256,512,1024}`, both speculative settings.

Output filenames:
- speculative: `workload-{WPS}wps-result-{W}-.txt`
- non-speculative: `workload-{WPS}wps-ns-result-{W}-.txt`

For the throughput-latency plot (Fig 5b), `plots.py` reads from the **fixed-wps=1000** sweep across window sizes:
```bash
# Speculative: windows [16,32,64,128,256,512,1024] at wps=1000
for W in 16 32 64 128 256 512 1024; do
    bash exp02-travel-throughput.sh --speculative true  --wps 1000 --window $W
done
# Non-speculative: windows [64,128,256,512,1024,2048] at wps=1000
for W in 64 128 256 512 1024 2048; do
    bash exp02-travel-throughput.sh --speculative false --wps 1000 --window $W
done
```

---

## Experiment 3 — Temporal baseline (Fig 5a/5b)

Deploy Temporal first (see README.md §3.5):
```
helm install temporal temporalio/temporal -n temporal -f AE/temporal-values.yaml
```

```
exp03-temporal-baseline.sh --n-services N      # latency panel (Fig 5a)
exp03-temporal-baseline.sh --wps WPS           # throughput panel (Fig 5b)
```

Paper sweep: latency `--n-services 1..10`; throughput `--wps 100..400`.

Output filenames:
- latency panel: `TravelReservation-latency-{N}-temporal-result.txt`
- throughput panel: `TravelReservation-thr-{WPS}-temporal-result-128-.txt`

**Note:** `plots.py`'s temporal throughput-latency series reads `temporal-thr-result-{w}-.txt`
(window-varying sweep at fixed high wps, using `output_filename=temporal-thr`). This data was
generated with a direct Helm chart invocation, not via exp03. To regenerate, run the
TravelReservationTemporal chart with `output_filename=temporal-thr` and `window` in
`[16, 32, 64, 128]`.

Tear down when done: `helm uninstall temporal -n temporal`

---

## Experiment 4 — EventProcessing latency (Fig 6)

```
exp04-event-latency.sh --speculative true|false --checkpoint-interval CHK
```

`CHK` is `10` (DSE, low checkpoint overhead) or `500` (baseline, high checkpoint overhead).
Optional: `--workload NAME` (default: `events-50k-long`, matches `EP_LATENCY_WORKLOAD` in `plots.py`).

Paper conditions:

```bash
bash exp04-event-latency.sh --speculative true  --checkpoint-interval 10
bash exp04-event-latency.sh --speculative false --checkpoint-interval 10
bash exp04-event-latency.sh --speculative false --checkpoint-interval 500
bash exp04-event-latency.sh --speculative true  --checkpoint-interval 500
```

---

## Experiment 5 — EventProcessing recovery (Fig 8)

```
exp05-event-recovery.sh --speculative true|false [--simulated-recovery] [--kill-at SECONDS]
```

| Flag | Effect |
|---|---|
| `--speculative true\|false` | DSE on or off (required) |
| `--simulated-recovery` | Use 4-node no-clean-start environment (in-process rollback variant) |
| `--kill-at SECONDS` | Kill a processor pod after N seconds (real failure variant) |

Paper variants:

```bash
# speculative-killed: DSE on, real pod kill at t=30s
bash exp05-event-recovery.sh --speculative true  --kill-at 30

# non-speculative-killed: DSE off, real pod kill at t=30s
bash exp05-event-recovery.sh --speculative false --kill-at 30

# speculative-simulated: DSE on, in-process rollback, 4-node recovery environment
bash exp05-event-recovery.sh --speculative true  --simulated-recovery
```

---

## Experiment 6 — TwoPhaseCommit throughput (Fig 7)

```
exp06-tpc-throughput.sh --speculative true|false --window W
```

Paper conditions (window values that saturate each variant):

```bash
bash exp06-tpc-throughput.sh --speculative true  --window 128
bash exp06-tpc-throughput.sh --speculative false --window 32
```

---

## Experiment 7 — TwoPhaseCommit Orleans baseline (Fig 7)

No tunable flags for the paper conditions. Just run:

```bash
bash exp07-tpc-orleans.sh
```

---

## Experiment 8 — TwoPhaseCommit recovery (Fig 9)

No tunable flags. Runs the speculative TPC-C variant with in-process failover:

```bash
bash exp08-tpc-recovery.sh
```

---

## Experiment 9 — SpFasterMicrobench (Fig 10)

Run on two separate machines (server and client). See the usage block printed by `--help` or inside the script.

```bash
# On server machine:
bash exp09-spfaster-microbench.sh server <mode>

# On client machine (sweeps outstanding-window sizes):
bash exp09-spfaster-microbench.sh client <mode> <server-host> <workload-trace>
```

Modes: `none` (vanilla FASTER), `noint` (DSE-manual), `int` (DSE-interceptor).

---

## Experiment 10 — StateObjectMicrobench (Fig 11)

Single machine, sweeps thread counts:

```bash
bash exp10-stateobject-microbench.sh
```

For a quick smoke test with a single condition, pass args directly through to the binary:

```bash
bash exp10-stateobject-microbench.sh -t 0 -n 4 -o 100000
```
