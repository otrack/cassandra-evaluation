#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

source ${DIR}/functions_ycsb.sh

source ${DIR}/functions_cassandra.sh

# Function to print usage
print_usage() {
    echo "Usage: $0 <protocol> <number_of_threads> <min_loop> <max_loop> <workload_type> <workload> <record_count> <operation_count> <output_file> <do_create_and_load> <do_clean_up>"
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
if [ $# -lt 11 ]; then
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
output_file=$9
do_create_and_load=${10}
do_clean_up=${11}

if [ $# -gt 11 ]; then
  EXTRA_YCSB_OPTS=( "${@:12}" )
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

    cassandra_run_ycsb "load" "$workload_type" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "${output_file}.load" "$nthreads" "${EXTRA_YCSB_OPTS[@]}"

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
    
    cassandra_run_ycsb "run" "$workload_type" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "${output_file}" "$nthreads" "${EXTRA_YCSB_OPTS[@]}"

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
