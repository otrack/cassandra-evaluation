#!/bin/bash

# Function to print usage
print_usage() {
    echo "Usage: $0 <consistency_level> <number_of_threads> <serial_protocol> <min_loop> <max_loop> <workload> <record_count> <operation_count>"
    echo "Example: $0 ONE 10 accord 3 10 a"
    exit 1
}

# Function to start Cassandra cluster
start_cluster() {
    local node_count=$1
    local mode=$2
    echo "Starting Cassandra cluster with $node_count node(s)..."
    python3 start_cassandra_data_centers.py "$node_count" "$mode"
    if [ $? -ne 0 ]; then
        echo "Failed to start Cassandra cluster with $node_count node(s)."
        exit 1
    fi
}

# Function to add a new Cassandra node
add_node() {
    local mode=$1
    echo "Adding new Cassandra node to the cluster..."
    python3 create_new_node.py "$mode"
    if [ $? -ne 0 ]; then
        echo "Failed to add new Cassandra node."
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
            echo "load_ycsb.sh failed. Retrying... ($((retry_count+1))/$max_retries)"
            retry_count=$((retry_count+1))
        fi
    done

    if [ "$success" = false ]; then
        echo "load_ycsb.sh failed after $max_retries attempts. Exiting."
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
    echo "Running YCSB benchmark..."
    ./run_ycsb.sh "$consistency_level" "$nthreads" "$filename" "$workload" "$record_count" "$operation_count"
    if [ $? -ne 0 ]; then
        echo "YCSB benchmark failed."
        exit 1
    fi
}

# Function to clean up Cassandra cluster
cleanup_cluster() {
    echo "Cleaning up Cassandra cluster..."
    python3 cleanup_cassandra_cluster.py
    if [ $? -ne 0 ]; then
        echo "Failed to clean up Cassandra cluster."
        exit 1
    fi
}

# Main script
if [ $# -ne 8 ]; then
    print_usage
fi

consistency_level=$1
nthreads=$2
mode=$3
min_loop=$4
max_loop=$5
workload=$6
record_count=$7
operation_count=$8

# Determine transaction mode
if [ "$mode" == "accord" ] && [ "$consistency_level" == "SERIAL" ]; then 
    transaction_mode="full"
else
    transaction_mode="bruh"
fi

# Loop from min_loop to max_loop
for ((i=min_loop; i<=max_loop; i++)); do
    if [ $i -eq $min_loop ]; then
        start_cluster "$i" "$mode"
    else
        add_node "$mode"
    fi

    echo "Loading YCSB workload for $i node(s)..."
    load_ycsb_workload "$consistency_level" "$nthreads" "$transaction_mode" "$(echo "${consistency_level,,}")_${i}_nodes_${mode}_load_$workload.txt" "$workload" "$record_count" "$operation_count"

    echo "Running YCSB benchmark for $i node(s)..."
    run_ycsb_benchmark "$consistency_level" "$nthreads" "$(echo "${consistency_level,,}")_${i}_nodes_${mode}_run_$workload.txt" "$workload" "$record_count" "$operation_count"

    echo "Completed iteration for $i node(s)."
done

cleanup_cluster

echo "All iterations completed."
