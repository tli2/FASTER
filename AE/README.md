# libDSE — OSDI '26 Artifact

This artifact accompanies *"Distributed Speculative Execution for Cloud
Applications"*. The cluster experiments (1–8) target Azure Kubernetes Service
(AKS); reproducing the results requires an equivalent cluster with
credentials set in `AE/scripts/env.sh`. The two microbenchmarks (9 and 10)
run on any 32-vCPU machine without Kubernetes.

---

## 1. Repository layout

```
FASTER/
├── Dockerfile                # Builds every experiment + bundles workloads
├── cs/                       # C# implementation
│   ├── src/                  # FasterKV + FasterLog (Microsoft open source)
│   └── research/
│       ├── libdpr/           # libDSE runtime — paper §4
│       └── darq/             # DARQ + every experiment driver
│           ├── TravelReservation/           # Exp 1, 2 (Fig 5)
│           ├── TravelReservationTemporal/   # Temporal baseline (Fig 5)
│           ├── EventProcessing/             # Exp 4, 5 (Fig 6, 8)
│           ├── TwoPhaseCommit/              # Exp 6, 8 (Fig 7, 9)
│           ├── TwoPhaseCommitOrleans/       # Orleans baseline (Fig 7)
│           ├── SpFasterMicrobench/          # Exp 9 (Fig 10)
│           ├── StateObjectMicrobench/       # Exp 10 (Fig 11)
│           └── <experiment>/{helm-workload,helm-storage}/
├── cc/                       # FASTER C++ implementation (not used in paper)
├── AE/
│   ├── README.md             # This file
│   ├── paper.pdf             # Paper PDF
│   ├── temporal-values.yaml  # Helm values for Temporal (exp 3 only)
│   └── scripts/              # Per-experiment runners (one condition each)
│       ├── common.sh         # sources env.sh; creates it on first run
│       ├── how-to-run.md     # Flag reference and example sweep loops
│       └── exp01-…sh … exp10-…sh
```

## 2. Paper figures → artifact mapping

| Paper figure | Project | Experiment |
|---|---|---|
| Fig 5a — TravelReservation latency vs #services | `cs/research/darq/TravelReservation` | 1 |
| Fig 5b — TravelReservation throughput-latency | `cs/research/darq/TravelReservation` | 2 |
| Fig 5  — Temporal baseline (both panels) | `cs/research/darq/TravelReservationTemporal` | 3 |
| Fig 6  — EventProcessing (c=10ms, c=500ms, bytes) | `cs/research/darq/EventProcessing` | 4 |
| Fig 7  — TwoPhaseCommit (spec / no-spec / Orleans) | `cs/research/darq/{TwoPhaseCommit,TwoPhaseCommitOrleans}` | 6, 7 |
| Fig 8  — EventProcessing recovery | `cs/research/darq/EventProcessing` | 5 |
| Fig 9  — TwoPhaseCommit recovery | `cs/research/darq/TwoPhaseCommit` | 8 |
| Fig 10 — DSE instrumentation overhead | `cs/research/darq/SpFasterMicrobench` | 9 |
| Fig 11 — DSE primitive thread scalability | `cs/research/darq/StateObjectMicrobench` | 10 |

## Getting Started Instructions

Verify the artifact builds and runs without any cloud infrastructure using the
StateObjectMicrobench (exp 10), which runs entirely on a single local machine.

**Prerequisite:** `dotnet` 9.0 SDK — see the table in §3.1 for install links.

```sh
git clone <this-repo> faster && cd faster
dotnet build cs/research/darq/darq.sln -c Release
bash AE/scripts/exp10-stateobject-microbench.sh -t 0 -n 4 -o 100000
```

Expected output within ~2 minutes: per-thread operation counts and latency statistics
(median, p95) printed to stdout. Non-zero throughput values confirm the binary is working.

---

## Detailed Instructions

The remaining sections cover full AKS cluster setup and end-to-end experiment execution.

---

## 3. Setup

### 3.1 Prerequisites

| Tool | Version | Notes |
|---|---|---|
| `az` (Azure CLI) | ≥ 2.50 | for `az login` |
| `kubectl` | ≥ 1.27 | matches AKS API |
| `helm` | v3.x | for chart deploys |
| `docker` | any recent | for image build |
| `dotnet` | 9.0 SDK | local builds + microbenchmarks |

### 3.2 AKS cluster

The paper's cluster: 10× `Standard_D8s_v3` (8 vCPU, 32 GB) labeled
`nodepool=dsebench` with premium LRS SSDs, plus a single-node system pool.
Adjust resource group / cluster name as needed:

```sh
RG=DSE
LOC=westus3
CLUSTER=dse-ae

az group create -n "$RG" -l "$LOC"

az aks create \
  --resource-group "$RG" \
  --name "$CLUSTER" \
  --location "$LOC" \
  --node-count 1 \
  --node-vm-size Standard_D2s_v3 \
  --enable-managed-identity \
  --generate-ssh-keys

az aks nodepool add \
  --resource-group "$RG" \
  --cluster-name "$CLUSTER" \
  --name dsebench \
  --node-count 10 \
  --node-vm-size Standard_D8s_v3 \
  --labels nodepool=dsebench \
  --node-osdisk-type Managed \
  --node-osdisk-size 128

az aks get-credentials --resource-group "$RG" --name "$CLUSTER"
kubectl create namespace dse
```

The Helm charts select pods to nodes labeled `nodepool=dsebench` and bind
PVCs against `storageClassName: premium-lrs`. If your cluster differs, edit
the chart templates.

Subsequent shells re-authenticate with:

```sh
az login
az aks get-credentials --resource-group "$RG" --name "$CLUSTER"
```

### 3.3 PersistentVolumeClaims

Each experiment ships a separate `helm-storage` chart that creates its PVCs.
Storage is intentionally one-time and persists across runs (recovery
experiments depend on it). Install once per experiment that needs persistence:

```sh
helm install pvc-tr  cs/research/darq/TravelReservation/helm-storage  -n dse
helm install pvc-ep  cs/research/darq/EventProcessing/helm-storage    -n dse
helm install pvc-tpc cs/research/darq/TwoPhaseCommit/helm-storage     -n dse
```

For exp 5 sub-figure (c) (`speculative-simulated`), provision 4 EventProcessing
worker PVCs instead of the default 2:

```sh
helm upgrade pvc-ep cs/research/darq/EventProcessing/helm-storage -n dse \
  --set 'workers[0].num=0' --set 'workers[1].num=1' \
  --set 'workers[2].num=2' --set 'workers[3].num=3'
```

### 3.4 Backing services

#### Azure Storage — working data + results

```sh
az storage account create -n dseworkdata -g "$RG" --sku Standard_LRS
az storage account create -n dseresults  -g "$RG" --sku Standard_LRS

AE_CONN_STRING=$(az storage account show-connection-string \
                   -n dseworkdata -g "$RG" -o tsv)
AE_RESULTS_CONN_STRING=$(az storage account show-connection-string \
                   -n dseresults  -g "$RG" -o tsv)
```

`AE_CONN_STRING` is used by exp 1, 2, 4, 5, 6, 8. `AE_RESULTS_CONN_STRING` is
used by all cluster experiments.

#### Cosmos DB — Temporal application state (exp 3 only)

```sh
az cosmosdb create -n dse-expr -g "$RG" \
  --kind GlobalDocumentDB \
  --locations regionName="$LOC" failoverPriority=0 isZoneRedundant=False

AE_COSMOS_CONN_STRING=$(az cosmosdb keys list \
  -n dse-expr -g "$RG" --type connection-strings \
  --query 'connectionStrings[0].connectionString' -o tsv)
```

#### Azure Managed Instance for Apache Cassandra (exp 3 only)

An ARM template for the paper's 3-node Cassandra cluster is at `AE/cassandra-arm.json`.
Deploy it into the same resource group as your AKS cluster so Cassandra and AKS share a VNet.
The AKS cluster (§3.2) must exist first.

```sh
# Get the AKS-managed VNet ID
AKS_VNET_ID=$(az network vnet list \
  --resource-group "MC_${RG}_${CLUSTER}_${LOC}" \
  --query "[0].id" -o tsv)

az deployment group create \
  --resource-group "$RG" \
  --template-file AE/cassandra-arm.json \
  --parameters \
    cassandraClusters_dsebench_cassandra_name=dsebench-cassandra \
    virtualNetworks_aks_vnet_externalid="$AKS_VNET_ID" \
    initialCassandraAdminPassword="<choose-a-password>"
```

The cluster is created in a stopped state (cost-saving). Start it before exp 3:

```sh
az managed-cassandra cluster start \
  --resource-group "$RG" --cluster-name dsebench-cassandra
```

Retrieve the node IPs and superuser password to populate `AE/temporal-values.yaml`
(paste IPs into every `hosts:` field, password into every `password:` field):

```sh
az managed-cassandra datacenter show \
  --resource-group "$RG" --cluster-name dsebench-cassandra \
  --data-center-name datacenter-1 \
  --query "properties.seedNodes[*].ipAddress" -o tsv

# Password is the value you set for initialCassandraAdminPassword above.
```

Stop the cluster between runs to avoid unnecessary charges:

```sh
az managed-cassandra cluster deallocate \
  --resource-group "$RG" --cluster-name dsebench-cassandra
```

Deploy Temporal:

```sh
kubectl create namespace temporal
helm repo add temporalio https://go.temporal.io/helm-charts
helm install temporal temporalio/temporal -n temporal -f AE/temporal-values.yaml
```

After exp 3 finishes:

```sh
helm uninstall temporal -n temporal
```

#### Azure Tables — Orleans cluster membership (exp 7 only)

Orleans transactional state for exp 7 is in-memory; the connection string is
needed only for the cluster-membership table.

```sh
az storage account create -n dseorleansmember -g "$RG" --sku Standard_LRS

AE_AZURE_TABLE_CONN_STRING=$(az storage account show-connection-string \
                   -n dseorleansmember -g "$RG" -o tsv)
```

### 3.5 env.sh

On first run, any experiment script auto-creates `AE/scripts/env.sh`
(gitignored) with labeled placeholders, then exits. Run any expNN script
once, paste the connection strings captured in §3.4 plus `AE_TRACES_URL`
(see §3.6), then re-run. Each runner reads only the variables it actually
uses, so unused fields can be left empty.

### 3.6 Container image

The Dockerfile at the repo root builds every experiment binary and bundles
the workload traces from `AE/workloads/` into the image. Trace files (~1.7 GB)
are hosted separately — set `AE_TRACES_URL` (in `env.sh` or your shell) first.

```sh
# Download workload traces if missing
if [ -z "$(ls -A AE/workloads/EventProcessing-latency/workloads 2>/dev/null)" ]; then
    curl -L --fail --progress-bar -o /tmp/traces.zip "$AE_TRACES_URL"
    unzip -o /tmp/traces.zip -d .
    rm /tmp/traces.zip
fi

docker login docker.io
docker build -t <your-image> .
docker push <your-image>
export AE_IMAGE=<your-image>
```

`AE_IMAGE` is required for all cluster experiments (1–8) and must be set in `env.sh`
before running them. Every chart's `values.yaml` has an empty image default; the correct
image is injected at deploy time via `AE_IMAGE`.

For local development and microbenchmarks, build the solutions directly:

```sh
dotnet build cs/research/libdpr/libdpr.sln -c Release
dotnet build cs/research/darq/darq.sln    -c Release
```

Both target .NET 9.0. Unit tests: `dotnet test cs/research/libdpr/test/FASTER.libdpr.test`.

### 3.7 Microbench host (exp 9 and 10)

A single 32-vCPU machine with no Kubernetes. The paper used
`Standard_D32s_v3`. On a fresh Ubuntu 22.04 box:

```sh
wget https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt-get update && sudo apt-get install -y dotnet-sdk-9.0 git

git clone <this-repo> faster && cd faster
dotnet build cs/research/darq/darq.sln -c Release
```

Exp 9 needs two boxes (server + client) on the same network; exp 10 is fully
single-host.

### 3.8 Tear-down

```sh
helm uninstall temporal -n temporal      # if exp 3 was run
az group delete -n "$RG" --yes --no-wait
```

`az group delete` removes the AKS cluster, all storage accounts, the Cosmos
DB account, and any other resources in the group.

## 4. Running each experiment

Each script runs **one experimental condition**; invoke once per data point
and vary flags to sweep. See **[`AE/scripts/how-to-run.md`](scripts/how-to-run.md)**
for the full flag reference and example sweep loops.

> **Exp 3** requires Temporal — see §3.4 to deploy and tear down.

| # | Script | Figure | Key flags | Env vars |
|---|---|---|---|---|
| 1 | `exp01-travel-latency.sh`         | Fig 5a | `--speculative`, `--n-services` | `AE_CONN_STRING`, `AE_RESULTS_CONN_STRING` |
| 2 | `exp02-travel-throughput.sh`      | Fig 5b | `--speculative`, `--wps`, `--window` | `AE_CONN_STRING`, `AE_RESULTS_CONN_STRING` |
| 3 | `exp03-temporal-baseline.sh`      | Fig 5 (both panels) | `--n-services` or `--wps` | `AE_RESULTS_CONN_STRING`, `AE_COSMOS_CONN_STRING` |
| 4 | `exp04-event-latency.sh`          | Fig 6 | `--speculative`, `--checkpoint-interval` | `AE_CONN_STRING`, `AE_RESULTS_CONN_STRING` |

> **Fig 5b Temporal series** (`temporal-thr-result-{w}-.txt`): the window-sweep data for
> the Temporal throughput-latency curve was generated by invoking the
> `TravelReservationTemporal` Helm chart directly with `output_filename=temporal-thr` and
> `window` in `[16, 32, 64, 128]`, not via `exp03`. See
> [`AE/scripts/how-to-run.md`](scripts/how-to-run.md) §Experiment 3 for the exact commands.
| 5 | `exp05-event-recovery.sh`         | Fig 8 | `--speculative`, `--kill-at`, `--simulated-recovery` | `AE_CONN_STRING`, `AE_RESULTS_CONN_STRING` |
| 6 | `exp06-tpc-throughput.sh`         | Fig 7 (DSE pair) | `--speculative`, `--window` | `AE_CONN_STRING`, `AE_RESULTS_CONN_STRING` |
| 7 | `exp07-tpc-orleans.sh`            | Fig 7 (in-memory) | none | `AE_RESULTS_CONN_STRING`, `AE_AZURE_TABLE_CONN_STRING` |
| 8 | `exp08-tpc-recovery.sh`           | Fig 9 | none | `AE_CONN_STRING`, `AE_RESULTS_CONN_STRING` |
| 9 | `exp09-spfaster-microbench.sh`    | Fig 10 | `server\|client`, mode `{none,noint,int}` | none (32-vCPU box) |
| 10 | `exp10-stateobject-microbench.sh` | Fig 11 | passthrough args to binary | none |

Each script prints output filenames on completion. Download all results:

```sh
az storage blob download-batch \
  --destination ./results \
  --source results \
  --connection-string "$AE_RESULTS_CONN_STRING"
```

## 5. Plotting

Install Python dependencies once:

```sh
pip install matplotlib numpy seaborn
```

Generate all figures into `AE/figures/`:

```sh
# Download results from Azure Blob into AE/data/ (cluster experiments 1–8)
source AE/scripts/env.sh
az storage blob download-batch \
  --destination AE/data \
  --source results \
  --connection-string "$AE_RESULTS_CONN_STRING"

# For exp 9 (local microbench): copy spfaster-*-summary-w*.txt and
# spfaster-*-latencies-w*.csv from the client machine into AE/data/ as well.

mkdir -p AE/figures
python3 AE/plots.py
```

**Pre-collected data coverage:** The `AE/data/` directory includes pre-collected results for
most experiments. Two sub-plots require additional data not included:

- **Fig 6c (bytes written):** `plot_events_bar_bytes()` needs `*-stats.csv` files from all
  four exp04 conditions (`--speculative true|false` × `--checkpoint-interval 10|500`). Run
  all four conditions and download results before plotting; `plots.py` silently skips this
  panel if the files are absent.

- **Fig 10 (DSE instrumentation overhead):** No pre-collected spfaster data is included.
  After running exp09, copy `spfaster-{none,noint,int}-summary-w*.txt` and
  `spfaster-{none,noint,int}-latencies-w*.csv` from the client machine into `AE/data/`.
  `plots.py` skips Fig 10 if these files are absent.
