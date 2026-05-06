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

## Prerequisites

| Tool | Version | Required for |
|---|---|---|
| `dotnet` | 9.0 SDK | All experiments (local builds + microbenchmarks) |
| `az` (Azure CLI) | ≥ 2.50 | Cluster experiments (1–8) |
| `kubectl` | ≥ 1.27 | Cluster experiments (1–8) |
| `helm` | v3.x | Cluster experiments (1–8) |
| `docker` | any recent | Cluster experiments (1–8) |
| Python 3 + `matplotlib`, `numpy`, `seaborn` | any recent | Plotting (§5) |

Install prerequisites:

**Linux (Ubuntu/Debian):**

```sh
# dotnet 9.0 SDK
wget https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt-get update && sudo apt-get install -y dotnet-sdk-9.0

# Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# docker
sudo apt-get install -y docker.io
sudo usermod -aG docker $USER   # log out and back in to apply

# Python plotting
pip install matplotlib numpy seaborn
```

**macOS:**

```sh
# dotnet 9.0 SDK
brew install --cask dotnet-sdk

# Azure CLI
brew install azure-cli

# kubectl
brew install kubectl

# helm
brew install helm

# docker (Docker Desktop)
brew install --cask docker

# Python plotting
pip install matplotlib numpy seaborn
```

Verify your environment:

```sh
dotnet --version
az --version
kubectl version --client
helm version
docker --version
python3 -c "import matplotlib, numpy, seaborn; print('ok')"
```

---

## Getting Started Instructions

Verify the artifact builds and runs without any cloud infrastructure using the
StateObjectMicrobench (exp 10), which runs entirely on a single local machine.

**Prerequisite:** `dotnet` 9.0 SDK — see the Prerequisites section above for install instructions.

```sh
dotnet build cs/research/darq/darq.sln -c Release
bash AE/scripts/exp10-stateobject-microbench.sh -t 0 -n 4 -o 100000
```

Expected output: a single floating-point number — the aggregate throughput in ops/s
(e.g. `3539823.0`). A non-zero value confirms the binary is working.

**Step 2 — Local TravelReservation end-to-end smoke test**:

```sh
bash AE/scripts/exp-local-travel.sh
```

Expected output: per-request latencies (ms) followed by throughput and average latency
printed to stdout. Non-zero throughput confirms the full DSE pipeline — DPR finder,
orchestrator, and reservation service — is working on the local machine.

---

## Detailed Instructions

The remaining sections cover full AKS cluster setup and end-to-end experiment execution.

---

## 3. Setup

### 3.1 AKS cluster

The paper's cluster: 10× `Standard_D8s_v3` (8 vCPU, 32 GB) labeled
`nodepool=dsebench` with premium LRS SSDs, plus a single-node system pool.
Adjust resource group / cluster name as needed:

```sh
RG=DSE
LOC=eastus
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

> **Environment variables:** `RG`, `LOC`, and `CLUSTER` are used throughout §3 and §4. They are
> not persisted automatically — either redeclare them at the top of every new shell session, or add
> them to your `~/.bashrc` / `~/.bash_profile` so they survive reboots:
>
> ```sh
> echo 'export RG=DSE' >> ~/.bashrc
> echo 'export LOC=eastus' >> ~/.bashrc
> echo 'export CLUSTER=dse-ae' >> ~/.bashrc
> source ~/.bashrc
> ```

### 3.2 PersistentVolumeClaims

Each cluster experiment uses a `helm-storage` chart to create PersistentVolumeClaims
(premium LRS SSDs) under `/mnt/plrs` inside each pod. **The experiment scripts
do not manage PVC lifecycle** — they only install and uninstall the workload pods.
Install each storage chart once before the first run of its experiment group and
leave it up for the duration of all runs; see §4.1, §4.4, and §4.6 for the
per-group install commands.

**Between normal runs, no manual PVC management is needed.** Experiments 1, 2,
4, 6, and 8 always wipe disk state at startup (`PurgeAll` / `RemoveIfPresent`),
so stale data from a prior run is never an issue.

**Final teardown** — PVCs do not need to be uninstalled manually; `az group
delete` (§3.7) removes them along with the entire cluster.

### 3.3 Backing services

#### Azure Storage — results

```sh
az storage account create -n dseresults  -g "$RG" --sku Standard_LRS

AE_RESULTS_CONN_STRING=$(az storage account show-connection-string \
                   -n dseresults  -g "$RG" -o tsv)
```

`AE_RESULTS_CONN_STRING` is used by all cluster experiments. Experiment state
(FasterKV/FasterLog logs and checkpoints) is stored on the premium LRS PVCs
provisioned in §3.2, not in Azure Blob Storage.

#### Cosmos DB — Temporal application state (exp 3 only)

```sh
az cosmosdb create -n dse-expr -g "$RG" \
  --kind GlobalDocumentDB \
  --locations regionName="$LOC" failoverPriority=0 isZoneRedundant=False

AE_COSMOS_CONN_STRING=$(az cosmosdb keys list \
  -n dse-expr -g "$RG" --type connection-strings \
  --query 'connectionStrings[0].connectionString' -o tsv)
```

> **Throughput provisioning:** The default serverless or low-RU provisioned throughput can cause
> Cosmos DB to throttle requests during exp 3, which will degrade latency and produce unreliable
> results. Before running exp 3, ensure the account (or its databases/containers) is provisioned
> with sufficient RU/s to avoid 429 throttling responses. The exact throughput required depends on
> your workload intensity; increase provisioned throughput if you observe 429 errors in the pod logs.

#### Azure Managed Instance for Apache Cassandra (exp 3 only)

An ARM template for the paper's 3-node Cassandra cluster is at `AE/cassandra-arm.json`.
Deploy it into the same resource group as your AKS cluster so Cassandra and AKS share a VNet.
The AKS cluster (§3.1) must exist first.

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

### 3.4 env.sh

On first run, any experiment script auto-creates `AE/scripts/env.sh`
(gitignored) with labeled placeholders, then exits. Run any expNN script
once, paste the connection strings captured in §3.3, then re-run. Each
runner reads only the variables it actually uses, so unused fields can be
left empty.

### 3.5 Container image

The Dockerfile at the repo root builds every experiment binary and bundles
the workload traces from `AE/workloads/` into the image. The trace archive
(~787 MB compressed / ~1.7 GB extracted) is hosted on Zenodo
(DOI: [10.5281/zenodo.20055042](https://doi.org/10.5281/zenodo.20055042))
and is not checked into this repository.

**Step 1 — download the workload traces:**

```sh
curl -L --fail --progress-bar \
    -o AE/workloads.zip \
    "https://zenodo.org/records/20055042/files/workloads.zip?download=1"
```

**Step 2 — unzip the archive:**

```sh
if [ -z "$(ls -A AE/workloads/EventProcessing-latency/workloads 2>/dev/null)" ]; then
    unzip -o AE/workloads.zip -d .
fi
```

**Step 3 — build and push the Docker image:**

```sh
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

### 3.6 Microbench host (exp 9 and 10)

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

### 3.7 Tear-down

```sh
helm uninstall temporal -n temporal      # if exp 3 was run
az group delete -n "$RG" --yes --no-wait
```

`az group delete` removes the AKS cluster, all storage accounts, the Cosmos
DB account, and any other resources in the group.

## 4. Running each experiment

Each script runs **one experimental condition**; invoke once per data point and vary flags to sweep.

### 4.1 — TravelReservation latency (Fig 5a)

**Script:** `exp01-travel-latency.sh` | **Env:** `AE_RESULTS_CONN_STRING`

Install PVCs once (shared by exp 1 and 2):

```sh
helm install   pvc-tr cs/research/darq/TravelReservation/helm-storage -n dse
# when done with all TravelReservation runs:
helm uninstall pvc-tr -n dse
```

Provisions `service{0..9}-pvc`, `orchestrator{10,11}-pvc`, `dprfinder-pvc` — covers all `--n-services 1..10` runs.

```
exp01-travel-latency.sh --speculative true|false --n-services N
```

Paper sweep: `--speculative true` and `--speculative false`, `--n-services 1` through `10`.

### 4.2 — TravelReservation throughput-latency (Fig 5b)

**Script:** `exp02-travel-throughput.sh` | **Env:** `AE_RESULTS_CONN_STRING`

Uses the same `pvc-tr` chart installed for exp 1.

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

### 4.3 — Temporal baseline (Fig 5a/5b)

**Script:** `exp03-temporal-baseline.sh` | **Env:** `AE_RESULTS_CONN_STRING`, `AE_COSMOS_CONN_STRING`

Deploy Temporal first (see §3.3):

```sh
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

For the throughput-latency curve (Fig 5b), sweep `--window` at a fixed high wps:

```bash
for W in 16 32 64 128; do
    bash exp03-temporal-baseline.sh --wps 400 --window $W
done
```

Tear down when done: `helm uninstall temporal -n temporal`

### 4.4 — EventProcessing latency (Fig 6)

**Script:** `exp04-event-latency.sh` | **Env:** `AE_RESULTS_CONN_STRING`

Install PVCs once (shared by exp 4 and 5):

```sh
helm install   pvc-ep cs/research/darq/EventProcessing/helm-storage -n dse
# when done with all EventProcessing runs:
helm uninstall pvc-ep -n dse
```

Provisions `pubsub{0..3}-pvc`, `dprfinder-pvc` — covers both exp 4 (2 workers) and exp 5 (4 workers).

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

### 4.5 — EventProcessing recovery (Fig 8)

**Script:** `exp05-event-recovery.sh` | **Env:** `AE_RESULTS_CONN_STRING`

Uses the same `pvc-ep` chart installed for exp 4. To reset the PVCs to a clean state between runs:

```sh
helm uninstall pvc-ep -n dse
helm install   pvc-ep cs/research/darq/EventProcessing/helm-storage -n dse
```

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

### 4.6 — TwoPhaseCommit throughput (Fig 7)

**Script:** `exp06-tpc-throughput.sh` | **Env:** `AE_RESULTS_CONN_STRING`

Install PVCs once (shared by exp 6 and 8):

```sh
helm install   pvc-tpc cs/research/darq/TwoPhaseCommit/helm-storage -n dse
# when done with all TwoPhaseCommit runs:
helm uninstall pvc-tpc -n dse
```

Provisions `participant{0..3}-pvc`, `dprfinder-pvc`.

```
exp06-tpc-throughput.sh --speculative true|false --window W
```

Paper conditions (window values that saturate each variant):

```bash
bash exp06-tpc-throughput.sh --speculative true  --window 128
bash exp06-tpc-throughput.sh --speculative false --window 32
```

### 4.7 — TwoPhaseCommit Orleans baseline (Fig 7)

**Script:** `exp07-tpc-orleans.sh` | **Env:** `AE_RESULTS_CONN_STRING`, `AE_AZURE_TABLE_CONN_STRING`

No tunable flags for the paper conditions:

```bash
bash exp07-tpc-orleans.sh
```

### 4.8 — TwoPhaseCommit recovery (Fig 9)

**Script:** `exp08-tpc-recovery.sh` | **Env:** `AE_RESULTS_CONN_STRING`

Uses the same `pvc-tpc` chart installed for exp 6.

No tunable flags. Runs the speculative TPC-C variant with in-process failover:

```bash
bash exp08-tpc-recovery.sh
```

### 4.9 — SpFasterMicrobench (Fig 10)

**Script:** `exp09-spfaster-microbench.sh` | **Env:** none (32-vCPU box, no Kubernetes)

Run on two separate machines (server and client):

```bash
# On server machine:
bash exp09-spfaster-microbench.sh server <mode>

# On client machine (sweeps outstanding-window sizes):
bash exp09-spfaster-microbench.sh client <mode> <server-host> <workload-trace>
```

Modes: `none` (vanilla FASTER), `noint` (DSE-manual), `int` (DSE-interceptor).

### 4.10 — StateObjectMicrobench (Fig 11)

**Script:** `exp10-stateobject-microbench.sh` | **Env:** none (single machine)

Sweeps thread counts automatically:

```bash
bash exp10-stateobject-microbench.sh
```

For a quick smoke test with a single condition, pass args directly through to the binary:

```bash
bash exp10-stateobject-microbench.sh -t 0 -n 4 -o 100000
```

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
