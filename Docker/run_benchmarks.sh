#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

source ${DIR}/functions_ycsb.sh

source ${DIR}/functions_cassandra.sh

# Function to print usage
print_usage() {
    echo "Usage: $0 <protocol> <number_of_threads> <node_count> <workload_type> <workload> <record_count> <operation_count> <output_file> <do_create_and_load> <do_clean_up>"
    echo "Example: $0 ONE 10 3 site.ycsb.workloads.CoreWorkload a 1 1 1 1"
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
node_count=$3
workload_type=$4
workload=$5
record_count=$6
operation_count=$7
output_file=$8
do_create_and_load=$9
do_clean_up=${10}

if [ $# -gt 10 ]; then
  EXTRA_YCSB_OPTS=( "${@:11}" )
else
  EXTRA_YCSB_OPTS=()
fi

log "Running ${workload_type} ${workload^^} for ${node_count} node(s)..."

# Create cluster and load YCSB (if needed)
if [ $do_create_and_load == "1" ];
then
    log "Starting ${protocol} cluster with ${node_count} node(s)..."
    cassandra_start_cluster "${node_count}" "$protocol"

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

    cassandra_run_ycsb "load" "$workload_type" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "${output_file}".load "$nthreads" "${EXTRA_YCSB_OPTS[@]}"

    log "Emulating latency for ${node_count} node(s)..."
    emulate_latency "${node_count}"    
fi

node_count=$(cassandra_get_node_count)
hosts=$(cassandra_get_all_ips "${node_count}")
port=9042

if [ -z "$hosts" ]; then
    echo "Failed to retrieve the IP addresses."
    exit 1
fi

cassandra_run_ycsb "run" "$workload_type" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "${output_file}" "$nthreads" "${EXTRA_YCSB_OPTS[@]}"

if [ $do_clean_up == "1" ];
then
    cassandra_cleanup_cluster
fi
