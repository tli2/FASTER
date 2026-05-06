#!/usr/bin/env bash
# Experiment 3 — Temporal baseline (Fig 5a/5b), one run.
#
# Usage:
#   Latency panel:              exp03-temporal-baseline.sh --n-services N
#   Throughput panel:           exp03-temporal-baseline.sh --wps WPS
#   Throughput-latency sweep:   exp03-temporal-baseline.sh --wps WPS --window W
#
# Exactly one of --n-services or --wps must be given.
# --window is optional and only valid with --wps; it selects the window-sweep
# variant that produces temporal-thr-result-{W}-.txt for Fig 5b.
#
# Prerequisites: Cassandra reachable, Temporal chart deployed, AE_COSMOS_CONN_STRING set.
# See README.md §3.5. Deploy Temporal with:
#   helm install temporal temporalio/temporal -n temporal -f AE/temporal-values.yaml
# Tear it down after:
#   helm uninstall temporal -n temporal

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common.sh"

require_env AE_RESULTS_CONN_STRING AE_COSMOS_CONN_STRING

N_SERVICES=""
WPS=""
WINDOW=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --n-services)  N_SERVICES="$2"; shift 2 ;;
        --wps)         WPS="$2";        shift 2 ;;
        --window)      WINDOW="$2";     shift 2 ;;
        *) echo "error: unknown flag '$1'" >&2; exit 2 ;;
    esac
done

if [[ -n "$N_SERVICES" && -n "$WPS" ]]; then
    echo "error: specify exactly one of --n-services or --wps" >&2; exit 2
fi
if [[ -z "$N_SERVICES" && -z "$WPS" ]]; then
    echo "error: one of --n-services or --wps required" >&2; exit 2
fi
if [[ -n "$WINDOW" && -z "$WPS" ]]; then
    echo "error: --window requires --wps" >&2; exit 2
fi

CHART="$(chart_dir TravelReservationTemporal)"

if [[ -n "$N_SERVICES" ]]; then
    OVERLAY="$(overlay TravelReservation-latency "workload-${N_SERVICES}s-temporal.yaml")"
    log_step "exp03 latency: N=$N_SERVICES"
    run_release "trt-lat-${N_SERVICES}" "$CHART" \
        -f "$OVERLAY" \
        --set "results_conn_string=$AE_RESULTS_CONN_STRING" \
        --set "cosmos_conn_string=$AE_COSMOS_CONN_STRING"
    log_step "exp03: complete"
    echo "Result filename: TravelReservation-latency-${N_SERVICES}-temporal-result.txt"
elif [[ -n "$WINDOW" ]]; then
    OVERLAY="$(overlay TravelReservation-thr "workload-${WPS}-temporal.yaml")"
    log_step "exp03 throughput window-sweep: wps=$WPS window=$WINDOW"
    run_release "trt-thr-${WPS}-w${WINDOW}" "$CHART" \
        -f "$OVERLAY" \
        --set "results_conn_string=$AE_RESULTS_CONN_STRING" \
        --set "cosmos_conn_string=$AE_COSMOS_CONN_STRING" \
        --set "output_filename=temporal-thr" \
        --set "window=$WINDOW"
    log_step "exp03: complete"
    echo "Result filename: temporal-thr-result-${WINDOW}-.txt"
else
    OVERLAY="$(overlay TravelReservation-thr "workload-${WPS}-temporal.yaml")"
    log_step "exp03 throughput: wps=$WPS"
    run_release "trt-thr-${WPS}" "$CHART" \
        -f "$OVERLAY" \
        --set "results_conn_string=$AE_RESULTS_CONN_STRING" \
        --set "cosmos_conn_string=$AE_COSMOS_CONN_STRING"
    log_step "exp03: complete"
    echo "Result filename: TravelReservation-thr-${WPS}-temporal-result.txt"
fi
