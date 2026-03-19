#!/usr/bin/env bash

# Run all experiment scripts one after the other.
# By default, the --test flag is passed to each script.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

usage() {
    echo "Usage: $0 [--dry-run] [--no-test] [--protocols=LIST]"
    echo "  --dry-run        Skip the experiments; only draw plots using existing data."
    echo "  --no-test        Disable the --test flag (run full experiments)."
    echo "  --protocols=LIST Override the list of protocols to run (space-separated)."
}

dry_run=0
test_flag="--test"
protocols_flag=""
for arg in "$@"; do
    case "$arg" in
	--dry-run)
            dry_run=1
            ;;
        --no-test)
            test_flag=""
            ;;
        --protocols=*)
            protocols_flag="$arg"
            ;;
        *)
            echo "Unknown parameter: $arg"
            usage
            exit 1
            ;;
    esac
done

if [ "$dry_run" -eq 0 ]; then
    log "Pulling all Docker images from ${CONFIG_FILE}..."
    while IFS='=' read -r key value; do
        [[ -z "$key" || "$key" =~ ^[[:space:]]*# ]] && continue
        [[ "$key" =~ _image$ ]] || continue
        value=$(echo "$value" | xargs)
        log "Pulling image: ${value}"
        docker pull "${value}"
        if [ $? -ne 0 ]; then
            log "ERROR: Failed to pull image '${value}'. Aborting."
            exit 1
        fi
    done < "${CONFIG_FILE}"
    log "All Docker images pulled successfully."
fi

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
    if [ "$dry_run" -eq 0 ]; then	
	log "Running ${script} ${test_flag} ${protocols_flag}..."
	bash "${DIR}/${script}" ${test_flag} ${protocols_flag}
	if [ $? -ne 0 ]; then
            log "ERROR: ${script} failed. Aborting."
            exit 1
	fi
	log "${script} completed successfully."
    else
	bash "${DIR}/${script}" --dry-run
    fi
done

log "All experiments completed successfully."
