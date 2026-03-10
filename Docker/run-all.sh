#!/usr/bin/env bash

# Run all experiment scripts one after the other.
# By default, the --test flag is passed to each script.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

usage() {
    echo "Usage: $0 [--no-test]"
    echo "  --no-test  Disable the --test flag (run full experiments)."
}

test_flag="--test"
for arg in "$@"; do
    case "$arg" in
        --no-test)
            test_flag=""
            ;;
        *)
            echo "Unknown parameter: $arg"
            usage
            exit 1
            ;;
    esac
done

scripts=(
    "cdf.sh"
    "closed_economy.sh"
    "ephemeral.sh"
    "conflict.sh"
    "fault_tolerance.sh"
    "latency_throughput.sh"
    "swap.sh"
    "ycsb.sh"
)

for script in "${scripts[@]}"; do
    log "Running ${script} ${test_flag}..."
    bash "${DIR}/${script}" ${test_flag}
    if [ $? -ne 0 ]; then
        log "ERROR: ${script} failed. Aborting."
        exit 1
    fi
    log "${script} completed successfully."
done

log "All experiments completed successfully."
