#!/usr/bin/env bash
# Experiment 1 — TravelReservation latency vs #services (Fig 5a), one run.
#
# Usage: exp01-travel-latency.sh --speculative true|false --n-services N

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common.sh"

require_env AE_RESULTS_CONN_STRING

SPECULATIVE=""
N_SERVICES=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --speculative)  SPECULATIVE="$2"; shift 2 ;;
        --n-services)   N_SERVICES="$2";  shift 2 ;;
        *) echo "error: unknown flag '$1'" >&2; exit 2 ;;
    esac
done

[[ -n "$SPECULATIVE" ]] || { echo "error: --speculative required" >&2; exit 2; }
[[ -n "$N_SERVICES"  ]] || { echo "error: --n-services required"  >&2; exit 2; }

CHART="$(chart_dir TravelReservation)"
OVERLAY="$(overlay TravelReservation-latency "workload-${N_SERVICES}s.yaml")"

log_step "exp01: n-services=$N_SERVICES speculative=$SPECULATIVE"
run_release "tr-lat-${SPECULATIVE}-${N_SERVICES}" "$CHART" \
    -f "$OVERLAY" \
    --set "speculative=${SPECULATIVE}" \
    --set "results_conn_string=$AE_RESULTS_CONN_STRING"

log_step "exp01: complete"
echo "Result filename in results blob:"
echo "  workload-${N_SERVICES}service-result-0-${SPECULATIVE/true/speculative}.txt"
