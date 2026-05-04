#!/usr/bin/env bash
# Experiment 6 — TwoPhaseCommit throughput (Fig 7), one run.
#
# Usage: exp06-tpc-throughput.sh --speculative true|false --window W

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common.sh"

require_env AE_CONN_STRING AE_RESULTS_CONN_STRING

SPECULATIVE=""
WINDOW=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --speculative)  SPECULATIVE="$2"; shift 2 ;;
        --window)       WINDOW="$2";      shift 2 ;;
        *) echo "error: unknown flag '$1'" >&2; exit 2 ;;
    esac
done

[[ -n "$SPECULATIVE" ]] || { echo "error: --speculative required" >&2; exit 2; }
[[ -n "$WINDOW"      ]] || { echo "error: --window required"      >&2; exit 2; }

TAG_SUFFIX="${SPECULATIVE/true/speculative}"
TAG_SUFFIX="${TAG_SUFFIX/false/default}"

CHART="$(chart_dir TwoPhaseCommit)"

log_step "exp06: speculative=$SPECULATIVE window=$WINDOW"
run_release "tpc-${TAG_SUFFIX}-${WINDOW}" "$CHART" \
    --set "speculative=$SPECULATIVE" \
    --set "window=$WINDOW" \
    --set "tag=2pc-results-${TAG_SUFFIX}-small" \
    --set "conn_string=$AE_CONN_STRING" \
    --set "results_conn_string=$AE_RESULTS_CONN_STRING"

log_step "exp06: complete"
echo "Result filename: 2pc-results-${TAG_SUFFIX}-small-result-${WINDOW}-${TAG_SUFFIX}.txt"
