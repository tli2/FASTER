#!/usr/bin/env bash
# Experiment 10 — DSE primitive thread scalability (Fig 11).
#
# Single-machine microbench. Sweeps thread count for each of the three
# action types and appends throughput numbers (ops/s on stdout) to one file
# per type.
#
#   -t 0  local-action
#   -t 1  send-receive
#   -t 2  detach-merge
#
# Output files (in CWD):
#   local-action-scalability.txt
#   send-receive-action-scalability.txt
#   detach-merge-action-scalability.txt

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT="$REPO_ROOT/cs/research/darq/StateObjectMicrobench"

# Allow a single-shot invocation (positional args passed straight through to
# the binary) for smoke testing: e.g. `exp10-…sh -t 0 -n 1 -o 1000`.
if [ $# -gt 0 ]; then
    echo "[exp10] passthrough: dotnet run -- $*"
    dotnet run --project "$PROJECT" -c Release -- "$@"
    exit 0
fi

THREADS="${EXP10_THREADS:-1 2 4 8 16 24 32}"
OPS="${EXP10_OPS:-1000000}"

declare -A LABEL_FOR=( [0]=local-action [1]=send-receive-action [2]=detach-merge-action )

for T in 0 1 2; do
    OUT="${LABEL_FOR[$T]}-scalability.txt"
    : > "$OUT"
    for N in $THREADS; do
        echo "[exp10] type=$T (${LABEL_FOR[$T]}) threads=$N"
        echo "# threads=$N" >> "$OUT"
        dotnet run --project "$PROJECT" -c Release -- \
            -t "$T" -n "$N" -o "$OPS" >> "$OUT"
    done
done

echo "[exp10] complete; results in:"
for T in 0 1 2; do
    echo "  ${LABEL_FOR[$T]}-scalability.txt"
done
