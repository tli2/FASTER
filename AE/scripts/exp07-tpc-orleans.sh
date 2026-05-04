#!/usr/bin/env bash
# Experiment 7 — TwoPhaseCommit Orleans baseline (Fig 7, in-memory).
#
# 4 silos to mirror the 4-shard sharding in exp06. Backing transactional state
# is in-memory; AE_AZURE_TABLE_CONN_STRING is required only for Orleans
# cluster-membership.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common.sh"

require_env AE_RESULTS_CONN_STRING AE_AZURE_TABLE_CONN_STRING

CHART="$(chart_dir TwoPhaseCommitOrleans)"

log_step "exp07: TPC Orleans baseline"
run_release "tpco" "$CHART" \
    --set "num_silos=4" \
    --set "window=32" \
    --set "tag=2pc-results-orleans" \
    --set "azure_table_conn_string=$AE_AZURE_TABLE_CONN_STRING" \
    --set "results_conn_string=$AE_RESULTS_CONN_STRING"

log_step "exp07: complete"
echo "Results uploaded with filename:"
echo "  2pc-results-orleans.txt"
