#!/usr/bin/env bash
# Local TravelReservation end-to-end smoke test.
#
# Runs entirely on the local machine — no Kubernetes, no Azure.
# Uses the pre-generated 30s/10wps trace in AE/workloads/TravelReservation-smoke/.
# Starts dprfinder / orchestrator / service in the background, then runs
# the client in the foreground.  Kills all background processes on exit.
#
# Prerequisites: darq.sln built in Release mode.
#   dotnet build cs/research/darq/darq.sln -c Release
#
# Usage:
#   bash AE/scripts/exp-local-travel.sh
#
# Expected output:
#   Results for local-smoke:
#   <one latency value per line, in ms>
#   Throughput: <req/s>
#   Average latency: <ms>
#   Latency std: <ms>

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT="$REPO_ROOT/cs/research/darq/TravelReservation"
WORKLOADS="$REPO_ROOT/AE/workloads/TravelReservation-smoke/workloads"

# Kill any leftover server processes and stale files from a previous crashed run.
# The DPR finder is in-memory: if it survives a crash it will remember committed
# versions that the freshly-purged checkpoints no longer have, causing restore
# failures on reconnect.  Always start from a completely clean slate.
pkill -KILL -f "TravelReservation" 2>/dev/null || true
pkill -KILL -f "dotnet run.*TravelReservation" 2>/dev/null || true
rm -f "$REPO_ROOT"/orchestrator*.log* "$REPO_ROOT"/service*.log* 2>/dev/null || true
rm -rf "$REPO_ROOT"/orchestrators1 "$REPO_ROOT"/service0 2>/dev/null || true

TMPDIR_LOCAL="$(mktemp -d)"

cleanup() {
    pkill -TERM -f "TravelReservation" 2>/dev/null || true
    sleep 1
    pkill -KILL -f "TravelReservation" 2>/dev/null || true
    rm -rf "$TMPDIR_LOCAL"
    rm -f "$REPO_ROOT"/orchestrator*.log* "$REPO_ROOT"/service*.log* 2>/dev/null || true
    rm -rf "$REPO_ROOT"/orchestrators1 "$REPO_ROOT"/service0 2>/dev/null || true
}
trap cleanup EXIT

echo "[local-travel] Starting dprfinder..."
dotnet run --project "$PROJECT" -c Release -- \
    -t dprfinder -e local \
    > "$TMPDIR_LOCAL/dprfinder.log" 2>&1 &

echo "[local-travel] Waiting for dprfinder to bind on :15720..."
until nc -z 127.0.0.1 15720 2>/dev/null; do sleep 0.5; done

echo "[local-travel] Starting orchestrator (worker 1)..."
dotnet run --project "$PROJECT" -c Release -- \
    -t orchestrator -e local -n 1 \
    > "$TMPDIR_LOCAL/orchestrator.log" 2>&1 &

echo "[local-travel] Starting reservation service (worker 0)..."
dotnet run --project "$PROJECT" -c Release -- \
    -t service -e local -n 0 -w "$WORKLOADS/smoke-service-0.csv" \
    > "$TMPDIR_LOCAL/service.log" 2>&1 &

echo "[local-travel] Waiting for orchestrator to bind on :15800..."
until nc -z 127.0.0.1 15800 2>/dev/null; do sleep 0.5; done

echo "[local-travel] Running client..."
dotnet run --project "$PROJECT" -c Release -- \
    -t client -e local \
    -w "$WORKLOADS/smoke-client-0.csv" \
    -m latency -o local-smoke
