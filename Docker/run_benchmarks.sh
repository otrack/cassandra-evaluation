#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

# Function to print usage
print_usage() {
    echo "Usage: $0 <protocol> <number_of_threads> <min_loop> <max_loop> <workload> <record_count> <operation_count> <do_create_and_load>"
    echo "Example: $0 ONE 10 accord 3 10 a"
    exit 1
}

# Function to start Cassandra cluster
start_cluster() {
    local node_count=$1
    local protocol=$2
    python3 start_cassandra_data_centers.py "$node_count" "$protocol"
    if [ $? -ne 0 ]; then
        error "Failed to start Cassandra cluster with $node_count node(s)."
        exit 1
    fi
}

# Function to add a new Cassandra node
add_node() {
    local mode=$1
    python3 create_new_node.py "$mode"
    if [ $? -ne 0 ]; then
        error "Failed to add new Cassandra node."
        exit 1
    fi
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


# Function to load YCSB workload
load_ycsb_workload() {
    local consistency_level=$1
    local nthreads=$2
    local transaction_mode=$3
    local filename=$4
    local workload=$5
    local record_count=$6
    local operation_count=$7
    local max_retries=3
    local retry_count=0
    local success=false

    while [ $retry_count -lt $max_retries ]; do
        ./load_ycsb.sh "$consistency_level" "$nthreads" "$transaction_mode" "$filename" "$workload" "$record_count" "$operation_count"
        if [ $? -eq 0 ]; then
            success=true
            break
        else
            error "load_ycsb.sh failed. Retrying... ($((retry_count+1))/$max_retries)"
            retry_count=$((retry_count+1))
        fi
    done

    if [ "$success" = false ]; then
        error "load_ycsb.sh failed after $max_retries attempts. Exiting."
        exit 1
    fi
}

# Function to run YCSB benchmark
run_ycsb_benchmark() {
    local consistency_level=$1
    local nthreads=$2
    local filename=$3
    local workload=$4
    local record_count=$5
    local operation_count=$6
    ./run_ycsb.sh "$consistency_level" "$nthreads" "$filename" "$workload" "$record_count" "$operation_count"
    if [ $? -ne 0 ]; then
        error "YCSB benchmark failed."
        exit 1
    fi
}

# Main script
if [ $# -ne 8 ]; then
    print_usage
fi

protocol=$1
nthreads=$2
min_loop=$3
max_loop=$4
workload=$5
record_count=$6
operation_count=$7
do_create_and_load=$8

# Create cluster and load YCSB if needed
if [ $do_create_and_load == "1" ];
then
    log "Starting Cassandra cluster with ${min_loop} node(s)..."
    start_cluster "${min_loop}" "$protocol"

    log "Loading YCSB for ${min_loop} node(s)..."
    load_ycsb_workload "$protocol" "$nthreads" "${LOGDIR}/$(echo "${protocol}")_${min_loop}_load_$workload.dat" "$workload" "$record_count" "$operation_count"

    log "Emulating latency for ${min_loop} node(s)..."
    emulate_latency "${min_loop}"    
fi

# Loop from min_loop to max_loop
for ((i=min_loop; i<=max_loop; i++)); do

    log "Running YCSB benchmark ${workload^^} for $i node(s)..."
    run_ycsb_benchmark "$protocol" "$nthreads" "${LOGDIR}/$(echo "${protocol}")_${i}_run_$workload.dat" "$workload" "$record_count" "$operation_count"

    if [ $i -lt $max_loop ];
    then
	log "Adding a new Cassandra node to the cluster..."
        add_node "$protocol"
    fi
    
done

