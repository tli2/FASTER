#!/usr/bin/env bash
# Experiment 4 — EventProcessing latency (Fig 6), one run.
#
# Usage: exp04-event-latency.sh --speculative true|false --checkpoint-interval CHK
#
# CHK is 10 (low overhead) or 500 (high overhead baseline).
# Optional: --workload NAME  (default: events-50k-long, matches EP_LATENCY_WORKLOAD in plots.py)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common.sh"

require_env AE_RESULTS_CONN_STRING

SPECULATIVE=""
CHK=""
WORKLOAD="events-50k-long"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --speculative)          SPECULATIVE="$2"; shift 2 ;;
        --checkpoint-interval)  CHK="$2";         shift 2 ;;
        --workload)             WORKLOAD="$2";     shift 2 ;;
        *) echo "error: unknown flag '$1'" >&2; exit 2 ;;
    esac
done

[[ -n "$SPECULATIVE" ]] || { echo "error: --speculative required"         >&2; exit 2; }
[[ -n "$CHK"         ]] || { echo "error: --checkpoint-interval required" >&2; exit 2; }

CHART="$(chart_dir EventProcessing)"

log_step "exp04: checkpoint_interval=$CHK speculative=$SPECULATIVE workload=$WORKLOAD"
run_release "ep-c${CHK}-${SPECULATIVE}" "$CHART" \
    --set "workload=$WORKLOAD" \
    --set "checkpoint_interval=$CHK" \
    --set "tag=c${CHK}$([ "$SPECULATIVE" = true ] && echo spec || echo nospec)" \
    --set "processor_speculative=$SPECULATIVE" \
    --set "pubsub_speculative=$SPECULATIVE" \
    --set "results_conn_string=$AE_RESULTS_CONN_STRING"

log_step "exp04: complete"
echo "Result filename: EventProcessing-latency-${WORKLOAD}-results-c${CHK}$([ "$SPECULATIVE" = true ] && echo spec || echo nospec)-lat.csv"
