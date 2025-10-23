#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

source ${DIR}/functions_ycsb.sh

source ${DIR}/functions_cassandra.sh

# Function to print usage
print_usage() {
    echo "Usage: $0 <protocol> <number_of_threads> <min_loop> <max_loop> <workload_type> <workload> <record_count> <operation_count> <do_create_and_load> <do_clean_up>"
    echo "Example: $0 ONE 10 3 3 site.ycsb.workloads.CoreWorkload a 1 1 1 1"
    exit 1
}

# Function to emulate geo-distributed latency
emulate_latency() {
    local node_count=$1
    python3 emulate_latency.py "$node_count"
    if [ $? -ne 0 ]; then
        error "Failed to add latency emulation."
        exit 1
    fi
}

# Main script
if [ $# -lt 10 ]; then
    print_usage
fi

protocol=$1
nthreads=$2
min_loop=$3
max_loop=$4
workload_type=$5
workload=$6
record_count=$7
operation_count=$8
do_create_and_load=$9
do_clean_up=${10}

# Everything after the 10th arg becomes EXTRA_YCSB_OPTS (may be empty)
# Preserve exact quoting/spacing by using "$@" expansion slice.
if [ $# -gt 10 ]; then
  # EXTRA_YCSB_OPTS is an array-like expansion used later; keep it as a positional expansion slice
  # Note: "${@:11}" preserves each extra arg as its own word.
  EXTRA_YCSB_OPTS=( "${@:11}" )
else
  EXTRA_YCSB_OPTS=()
fi

log "Running ${workload_type} ${workload^^} for ${min_loop}-${max_loop} node(s)..."

# Create cluster and load YCSB (if needed)
if [ $do_create_and_load == "1" ];
then
    log "Starting ${protocol} cluster with ${min_loop} node(s)..."
    cassandra_start_cluster "${min_loop}" "$protocol"

    node_count=$(cassandra_get_node_count)
    hosts=$(cassandra_get_all_ips "${node_count}")
    port=9042

    debug "node_count:${node_count}"
    debug "hosts:${hosts}"
    debug "port:${port}"

    if [ -z "$hosts" ]; then
	echo "Failed to retrieve the IP addresses."
	exit 1
    fi

    output_file="${LOGDIR}/$(echo "${protocol}")_${min_loop}_load_$workload.dat"
    cassandra_run_ycsb "load" "$workload_type" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "$output_file" "$nthreads" "${EXTRA_YCSB_OPTS[@]}"

    log "Emulating latency for ${min_loop} node(s)..."
    emulate_latency "${min_loop}"    
fi

# Loop from min_loop to max_loop
for ((i=min_loop; i<=max_loop; i++)); do

    node_count=$(cassandra_get_node_count)
    hosts=$(cassandra_get_all_ips "${node_count}")
    port=9042

    if [ -z "$hosts" ]; then
	echo "Failed to retrieve the IP addresses."
	exit 1
    fi
    
    output_file="${LOGDIR}/$(echo "${protocol}")_${i}_run_$workload.dat"
    cassandra_run_ycsb "run" "$workload_type" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "$output_file" "$nthreads" "${EXTRA_YCSB_OPTS[@]}"

    if [ $i -lt $max_loop ];
    then
      	log "Adding a new node to the cluster..."
        cassandra_add_node "$protocol"
    fi
    
done

if [ $do_clean_up == "1" ];
then
    cassandra_cleanup_cluster
fi
