# Shared helpers for AE/scripts/expNN-*.sh runners.
# Source from each script:
#     SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#     source "$SCRIPT_DIR/common.sh"
#
# On first run, if AE/scripts/env.sh does not exist, this file creates it from
# the template below and exits with instructions. Fill in the connection strings
# and re-run your experiment.

# _write_env_template <dest>
# The heredoc here is the single authoritative list of every variable the
# experiment scripts consume.
_write_env_template() {
    local dest="$1"
    cat > "$dest" <<'ENVEOF'
# AE/scripts/env.sh — fill in the connection strings below, then re-run your experiment.
# This file is gitignored. Each runner sources it via common.sh on startup.

# ── Required by all cluster experiments (1–8) ───────────────────────────────
# Docker image built from this repo's Dockerfile and pushed to a registry.
# Build with: docker build -t <your-image> . && docker push <your-image>
export AE_IMAGE=""

# Azure Storage account used for result uploads.
export AE_RESULTS_CONN_STRING=""

# ── Required by experiment 3 only (Temporal baseline) ───────────────────────
# Cosmos DB connection string for Temporal application state.
export AE_COSMOS_CONN_STRING=""

# ── Required by experiment 7 only (Orleans baseline) ────────────────────────
# Azure Tables connection string for Orleans cluster-membership table.
export AE_AZURE_TABLE_CONN_STRING=""

# ── Required before running build-docker-image.sh ───────────────────────────
# URL of the externally-hosted workload traces zip (~1.7 GB).
export AE_TRACES_URL=""

# ── Optional tuning ─────────────────────────────────────────────────────────
# export AE_NAMESPACE="dse"     # Kubernetes namespace (default: dse)
# export AE_HELM_TIMEOUT="20m"  # per-release poll timeout (default: 20m)
ENVEOF
}

set -euo pipefail

AE_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AE_REPO_ROOT="$(cd "$AE_SCRIPT_DIR/../.." && pwd)"

# Source env.sh; create it from the template above if it does not yet exist.
if [ -f "$AE_SCRIPT_DIR/env.sh" ]; then
    # shellcheck disable=SC1091
    source "$AE_SCRIPT_DIR/env.sh"
else
    _write_env_template "$AE_SCRIPT_DIR/env.sh"
    echo "" >&2
    echo "  Created AE/scripts/env.sh — fill in your connection strings, then re-run." >&2
    echo "" >&2
    exit 1
fi

# Defaults — env.sh values override these only if env.sh sets them first.
: "${AE_NAMESPACE:=dse}"
: "${AE_HELM_TIMEOUT:=20m}"

log_step() {
    printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
}

require_env() {
    local missing=()
    for var in "$@"; do
        if [ -z "${!var:-}" ]; then
            missing+=("$var")
        fi
    done
    if [ "${#missing[@]}" -gt 0 ]; then
        echo "error: required environment variables are unset: ${missing[*]}" >&2
        echo "       fill in AE/scripts/env.sh and re-run" >&2
        exit 1
    fi
}

# helm_install_release <release> <chart-dir> [extra args...]
# Always installs into $AE_NAMESPACE. Caller passes -f overlay and any --set flags.
# Automatically injects --set "image=$AE_IMAGE" when AE_IMAGE is set (values.yaml default is empty).
helm_install_release() {
    local release="$1"
    local chart="$2"
    shift 2
    local image_set=()
    [ -n "${AE_IMAGE:-}" ] && image_set=( --set "image=$AE_IMAGE" )
    log_step "helm install $release ($chart)"
    helm install "$release" "$chart" -n "$AE_NAMESPACE" "${image_set[@]}" "$@"
}

# Convert helm-style duration string (e.g. "20m", "2h", "90s") to seconds.
_duration_to_seconds() {
    local d="$1"
    case "$d" in
        *h) echo $(( ${d%h} * 3600 )) ;;
        *m) echo $(( ${d%m} * 60  )) ;;
        *s) echo $(( ${d%s}        )) ;;
        *)  echo "$d" ;;
    esac
}

# wait_for_release_complete <release> [<timeout>]
# Polls every 15 s until every Job in the namespace reaches Complete, then
# returns 0. Returns 1 on timeout. Releases run one at a time and are torn down
# before the next starts, so "all Jobs in namespace" correctly identifies the
# current release's client Job(s).
wait_for_release_complete() {
    local release="$1"
    local timeout="${2:-$AE_HELM_TIMEOUT}"
    local timeout_secs
    timeout_secs="$(_duration_to_seconds "$timeout")"
    log_step "polling $release Jobs until complete (timeout $timeout)"
    # Give Kubernetes a moment to register Jobs before the first poll.
    sleep 2
    local deadline=$(( $(date +%s) + timeout_secs ))
    while true; do
        local total incomplete
        total=$(kubectl get jobs -n "$AE_NAMESPACE" --no-headers 2>/dev/null \
                | wc -l | tr -d ' ')
        incomplete=$(kubectl get jobs -n "$AE_NAMESPACE" --no-headers 2>/dev/null \
                     | grep -cv 'Complete' || true)
        if [ "$total" -gt 0 ] && [ "$incomplete" -eq 0 ]; then
            log_step "$release: all $total Job(s) complete"
            return 0
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            log_step "error: timed out waiting for $release after $timeout"
            return 1
        fi
        log_step "$release: $incomplete/$total Job(s) still running — next poll in 15s"
        sleep 15
    done
}

# clean_release <release>
# helm uninstall + wait for pods to disappear so the next iteration starts on a
# fresh namespace.
clean_release() {
    local release="$1"
    log_step "tearing down $release"
    helm uninstall "$release" -n "$AE_NAMESPACE" --wait || true
    # Pod templates don't carry app.kubernetes.io/instance, so poll namespace-wide.
    # Experiments run sequentially; the namespace should be empty between runs.
    local deadline=$(( $(date +%s) + 120 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local count
        count="$(kubectl get pods -n "$AE_NAMESPACE" \
                    --no-headers 2>/dev/null | wc -l | tr -d ' ')"
        if [ "$count" = "0" ]; then return 0; fi
        sleep 2
    done
    log_step "warning: pods in $AE_NAMESPACE did not clear within 120s; continuing"
}

# Convenience wrapper: install, wait, clean. Use when a script doesn't need
# any custom logic between phases.
run_release() {
    local release="$1"
    local chart="$2"
    shift 2
    # Trap so Ctrl-C still cleans up the release.
    trap 'clean_release "$release"' INT TERM
    helm_install_release "$release" "$chart" "$@"
    wait_for_release_complete "$release"
    clean_release "$release"
    trap - INT TERM
}

# Standard --set arg list for the connection strings shared by most charts.
# Echoes args; caller embeds in helm_install_release call.
common_helm_sets() {
    local sets=()
    [ -n "${AE_RESULTS_CONN_STRING:-}" ] && sets+=( --set "results_conn_string=$AE_RESULTS_CONN_STRING" )
    [ -n "${AE_IMAGE:-}" ]               && sets+=( --set "image=$AE_IMAGE" )
    printf '%s\n' "${sets[@]}"
}

# Path to a chart directory in the repo.
chart_dir() {
    local project="$1"
    echo "$AE_REPO_ROOT/cs/research/darq/$project/helm-workload"
}

# Path to an overlay file under AE/workloads/.
overlay() {
    local subdir="$1"
    local file="$2"
    echo "$AE_REPO_ROOT/AE/workloads/$subdir/configs/$file"
}
