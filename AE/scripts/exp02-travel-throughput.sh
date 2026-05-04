#!/usr/bin/env bash
# Experiment 2 — TravelReservation throughput-latency (Fig 5b), one run.
#
# Usage: exp02-travel-throughput.sh --speculative true|false --wps WPS --window W

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common.sh"

require_env AE_CONN_STRING AE_RESULTS_CONN_STRING

SPECULATIVE=""
WPS=""
WINDOW=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --speculative)  SPECULATIVE="$2"; shift 2 ;;
        --wps)          WPS="$2";         shift 2 ;;
        --window)       WINDOW="$2";      shift 2 ;;
        *) echo "error: unknown flag '$1'" >&2; exit 2 ;;
    esac
done

[[ -n "$SPECULATIVE" ]] || { echo "error: --speculative required" >&2; exit 2; }
[[ -n "$WPS"         ]] || { echo "error: --wps required"         >&2; exit 2; }
[[ -n "$WINDOW"      ]] || { echo "error: --window required"      >&2; exit 2; }

CHART="$(chart_dir TravelReservation)"
OVERLAY="$(overlay TravelReservation-thr "workload-3s.yaml")"

NS_SUFFIX=$([ "$SPECULATIVE" = false ] && echo "-ns" || echo "")

log_step "exp02: wps=$WPS window=$WINDOW speculative=$SPECULATIVE"
run_release "tr-thr-${SPECULATIVE}-${WPS}-${WINDOW}" "$CHART" \
    -f "$OVERLAY" \
    --set "workload=workload-${WPS}wps" \
    --set "tag=workload-${WPS}wps${NS_SUFFIX}" \
    --set "window=${WINDOW}" \
    --set "speculative=${SPECULATIVE}" \
    --set "conn_string=$AE_CONN_STRING" \
    --set "results_conn_string=$AE_RESULTS_CONN_STRING"

log_step "exp02: complete"
echo "Result filename in results blob:"
echo "  workload-${WPS}wps${NS_SUFFIX}-result-${WINDOW}-.txt"
