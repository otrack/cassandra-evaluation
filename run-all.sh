#!/usr/bin/env bash

# Run all experiment scripts one after the other.
# By default, the --test flag is passed to each script.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

usage() {
    echo "Usage: $0 [--dry-run] [--no-test] [--protocols=LIST]"
    echo "  --dry-run        Skip the experiments; only draw plots using existing data."
    echo "  --no-test        Disable the --test flag (run full experiments)."
    echo "  --protocols=LIST Override the list of protocols to run (comma-separated)."
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

scripts=(
    "ephemeral.sh"
    "fault_tolerance.sh"
    "latency_throughput.sh"
    "cdf.sh"
    "closed_economy.sh"
    "conflict.sh"
    "swap.sh"
    "ycsb.sh"
)

# Not every experiment can run every protocol.  The transactional workloads
# need multi-key atomicity, which only Accord, CockroachDB and Tiga provide,
# and the fault-tolerance experiment needs a leader to slow down and kill.
# Names must match the first column of protocols.csv.
DEFAULT_PROTOCOLS="accord cockroachdb-opt swiftpaxos-paxos swiftpaxos-epaxos swiftpaxos-curp cassandra-paxos tiga"
TRANSACTIONAL_PROTOCOLS="accord cockroachdb-opt tiga"
FAULT_TOLERANCE_PROTOCOLS="accord cockroachdb-opt"

protocols_for() {
    case "$1" in
        closed_economy.sh|swap.sh) echo "${TRANSACTIONAL_PROTOCOLS}" ;;
        fault_tolerance.sh)        echo "${FAULT_TOLERANCE_PROTOCOLS}" ;;
        *)                         echo "${DEFAULT_PROTOCOLS}" ;;
    esac
}

# Fail now rather than twenty minutes into a sweep.
for p in ${DEFAULT_PROTOCOLS} ${TRANSACTIONAL_PROTOCOLS} ${FAULT_TOLERANCE_PROTOCOLS}; do
    if ! awk -F',' -v want="${p}" 'NR>1 && $1==want {found=1} END {exit !found}' "${DIR}/protocols.csv"; then
        error "unknown protocol '${p}': not a row of protocols.csv"
        exit 1
    fi
done

for script in "${scripts[@]}"; do
    if [ "$dry_run" -eq 0 ]; then	
	# An explicit --protocols on the command line overrides the per-script sets.
	script_protocols="${protocols_flag:---protocols=$(protocols_for "${script}" | tr ' ' ',')}"
	log "Running ${script} ${test_flag} ${script_protocols}..."
	bash "${DIR}/${script}" ${test_flag} ${script_protocols}
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
