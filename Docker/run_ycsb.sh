#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/functions_ycsb.sh

# Main script
if [ $# -ne 6 ]; then
    echo "Usage: $0 <consistency_level> <number_of_threads> <output_file> <workload> <record_count> <operation_count>"
    echo "Example: $0 ONE 1 results.txt a"
    exit 1
fi

consistency_level=$1
nthreads=$2
output_file=$3
workload="workloads/workload$4"
recordcount=$5
operationcount=$6

# Load data and write performance results to the output file
run_ycsb "run" "$ycsb_dir" "$workload" "$hosts" "$port" "$recordcount" "$operationcount" "$consistency_level" "$output_file" "$nthreads"

# Simulate a node crash after 2 minutes
# stop_container_after_delay "cassandra-node2" 90
