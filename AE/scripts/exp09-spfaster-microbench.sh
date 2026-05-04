#!/usr/bin/env bash
# Experiment 9 — DSE instrumentation overhead (Fig 10).
#
# Single-machine microbench. Runs SpFasterMicrobench in either server or
# client mode against a 32-vCPU host (paper used Standard_D32s_v3).
#
# Usage:
#   On server box:
#     bash exp09-spfaster-microbench.sh server [none|noint|int]
#
#   On client box (separate machine):
#     bash exp09-spfaster-microbench.sh client <mode> <server-host> <workload-trace>
#
# The script does not orchestrate two boxes — run it on each host. It does
# sweep the outstanding-window size (-w) on the client side.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT="$REPO_ROOT/cs/research/darq/SpFasterMicrobench"

usage() {
    cat <<EOF >&2
usage:
  $0 server <mode>
  $0 client <mode> <server-host> <workload-trace> [output-file]

modes:
  none   vanilla FASTER, no DSE
  noint  DSE-manual (header processing in user code)
  int    DSE-interceptor (gRPC interceptor)
EOF
    exit 2
}

[ $# -ge 2 ] || usage
ROLE="$1"
MODE="$2"

case "$MODE" in
    none|noint|int) ;;
    *) echo "error: unknown mode '$MODE'" >&2; usage ;;
esac

case "$ROLE" in
    server)
        echo "[exp09] running server mode=$MODE"
        dotnet run --project "$PROJECT" -c Release -- -t server -m "$MODE"
        ;;
    client)
        [ $# -ge 4 ] || usage
        SERVER="$3"
        WORKLOAD="$4"
        OUT="${5:-spfaster-${MODE}-latencies.csv}"
        WINDOWS="${EXP09_WINDOWS:-1 2 4 8 16 32 64 128 256}"
        for W in $WINDOWS; do
            echo "[exp09] client mode=$MODE window=$W"
            dotnet run --project "$PROJECT" -c Release -- \
                -t client -m "$MODE" \
                -s "$SERVER" \
                -i "$WORKLOAD" \
                -w "$W" \
                -o "${OUT%.csv}-w${W}.csv" \
            | tee "${OUT%.csv}-summary-w${W}.txt"
        done
        echo "[exp09] complete; per-window files written to ${OUT%.csv}-w*.csv"
        echo "        copy spfaster-*-summary-w*.txt to AE/data/ alongside the latency CSVs"
        ;;
    *)
        usage
        ;;
esac
