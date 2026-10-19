#!/usr/bin/env bash

# Re-run an experiment script if it fails, repairing any node of the active
# deployment that has gone missing (e.g. a reclaimed AWS spot instance)
# before retrying. Only useful for an experiment script that itself skips
# the sweep points its output already covers -- this just re-invokes the
# same command, so without that a retry redoes the whole sweep rather than
# resuming it.
#
# Usage: ./run_resilient.sh <script> [args...]
#
# RESILIENT_MAX_ATTEMPTS overrides the retry cap (default 5).

DIR=$(dirname "${BASH_SOURCE[0]}")

source "${DIR}/utils.sh"

if [ $# -lt 1 ]; then
    echo "Usage: $0 <script> [args...]"
    exit 1
fi

max_attempts=${RESILIENT_MAX_ATTEMPTS:-5}

args=("$@")
has_resume=0
for a in "${args[@]}"; do
    [ "${a}" = "--resume" ] && has_resume=1
done

attempt=1
while true; do
    "${args[@]}"
    rc=$?
    [ ${rc} -eq 0 ] && exit 0

    if [ "${attempt}" -ge "${max_attempts}" ]; then
        error "run_resilient: '$*' failed after ${attempt} attempt(s) (exit ${rc}); giving up"
        exit "${rc}"
    fi

    attempt=$((attempt + 1))
    if [ "${has_resume}" -eq 0 ]; then
        args+=("--resume")
        has_resume=1
    fi
    error "run_resilient: '$*' failed (exit ${rc}); repairing the deployment and retrying (attempt ${attempt}/${max_attempts})"
    infra_repair || error "run_resilient: infra_repair reported a problem; retrying anyway"
done
