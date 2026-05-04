#!/usr/bin/env bash
# Experiment 8 — TwoPhaseCommit recovery (Fig 9).
#
# Same as exp06 with simulated failures injected (`failover=true`). The chart
# passes `-f true` to participants, which schedules in-process rollbacks. The
# resulting plot has red dots indicating aborted transactions during recovery.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common.sh"

require_env AE_CONN_STRING AE_RESULTS_CONN_STRING

CHART="$(chart_dir TwoPhaseCommit)"

log_step "exp08: TPC recovery (speculative, failover=true)"
run_release "tpc-fail" "$CHART" \
    --set "speculative=true" \
    --set "failover=true" \
    --set "window=128" \
    --set "tag=2pc-recovery" \
    --set "conn_string=$AE_CONN_STRING" \
    --set "results_conn_string=$AE_RESULTS_CONN_STRING"

log_step "exp08: complete"
echo "Results uploaded with filename:"
echo "  2pc-recovery-result-128-speculative.txt"
