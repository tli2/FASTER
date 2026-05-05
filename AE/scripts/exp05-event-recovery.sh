#!/usr/bin/env bash
# Experiment 5 — EventProcessing recovery (Fig 8), one run.
#
# Usage: exp05-event-recovery.sh --speculative true|false [--simulated-recovery] [--kill-at SECONDS]
#
# Flags:
#   --speculative true|false   DSE on or off (required)
#   --simulated-recovery       Use 4-node no-clean-start environment for in-process rollback variant
#   --kill-at SECONDS          Kill a processor pod after SECONDS seconds (real failure variant)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common.sh"

require_env AE_RESULTS_CONN_STRING

SPECULATIVE=""
SIM_RECOVERY=false
KILL_AT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --speculative)        SPECULATIVE="$2";   shift 2 ;;
        --simulated-recovery) SIM_RECOVERY=true;  shift 1 ;;
        --kill-at)            KILL_AT="$2";        shift 2 ;;
        *) echo "error: unknown flag '$1'" >&2; exit 2 ;;
    esac
done

[[ -n "$SPECULATIVE" ]] || { echo "error: --speculative required" >&2; exit 2; }

LABEL="${SPECULATIVE}"
[[ "$SIM_RECOVERY" = true ]] && LABEL="${LABEL}-simrec"
[[ -n "$KILL_AT"          ]] && LABEL="${LABEL}-kill${KILL_AT}"

CHART="$(chart_dir EventProcessing)"
OVERLAY="$(overlay EventProcessing-recovery "values.yaml")"

log_step "exp05: speculative=$SPECULATIVE simulated_recovery=$SIM_RECOVERY kill_at=${KILL_AT:-none}"

trap 'clean_release "ep-rec-${LABEL}"' INT TERM
helm_install_release "ep-rec-${LABEL}" "$CHART" \
    -f "$OVERLAY" \
    --set "processor_speculative=$SPECULATIVE" \
    --set "pubsub_speculative=$SPECULATIVE" \
    --set "simulated_recovery=$SIM_RECOVERY" \
    --set "tag=recovery-${LABEL}" \
    --set "results_conn_string=$AE_RESULTS_CONN_STRING"

KILL_PID=""
if [[ -n "$KILL_AT" ]]; then
    (
        sleep "$KILL_AT"
        log_step "exp05: killing a processor pod at ${KILL_AT}s"
        kubectl get pods -n "$AE_NAMESPACE" -o name 2>/dev/null \
            | grep -E '/(filter|aggregate|detection)' \
            | head -1 \
            | xargs -r kubectl delete -n "$AE_NAMESPACE" || true
    ) &
    KILL_PID=$!
fi

wait_for_release_complete "ep-rec-${LABEL}" "30m"
[[ -n "$KILL_PID" ]] && wait "$KILL_PID" 2>/dev/null || true
clean_release "ep-rec-${LABEL}"
trap - INT TERM

log_step "exp05: complete"
echo "Result filename: recovery-1-${LABEL}-lat.csv"
