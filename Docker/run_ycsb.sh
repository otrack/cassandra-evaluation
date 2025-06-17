#!/bin/bash

# Function to get the IP address of a container
get_container_ip() {
    container_name=$1
    ip_address=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$container_name")
    echo "$ip_address"
}

# Function to get the IP addresses of all Cassandra nodes
get_all_cassandra_ips() {
    network_name=$1
    node_count=$2
    ips=""
    for i in $(seq 1 $node_count); do
        container_name="cassandra-node$i"
        ip=$(get_container_ip "$container_name")
        if [ -n "$ip" ]; then
            ips="$ips,$ip"
        fi
    done
    # Remove leading comma
    ips=${ips#,}
    echo "$ips"
}

# Function to get the number of Cassandra nodes
get_node_count() {
    i=1
    while true; do
        container_name="cassandra-node$i"
        ip=$(get_container_ip "$container_name")
        if [ -z "$ip" ]; then
            break
        fi
        i=$((i + 1))
    done
    echo $((i - 1))
}

# Function to run YCSB
run_ycsb() {
    action=$1
    ycsb_dir=$2
    workload=$3
    hosts=$4
    recordcount=$5
    operationcount=$6
    consistency_level=$7
    output_file=$8
    threads=$9
    debug="JAVA_OPTS=\"-Dorg.slf4j.simpleLogger.defaultLogLevel=debug\"" # comment out to have debug on
    cmd="${debug} $ycsb_dir/bin/ycsb.sh $action cassandra-cql -p hosts=$hosts -P $ycsb_dir/$workload -p cassandra.writeconsistencylevel=$consistency_level -p cassandra.readconsistencylevel=$consistency_level -p recordcount=$recordcount -p operationcount=$operationcount -threads $nthreads -s"

    eval "$cmd" | tee "$output_file"
    if [ $? -eq 0 ]; then
        echo "YCSB $action completed successfully."
    else
        echo "Error running YCSB $action."
        exit 1
    fi
}

# Function to stop a container after a delay
stop_container_after_delay() {
    container_name=$1
    delay=$2
    (
        sleep "$delay"
        docker stop "$container_name"
        echo "Stopped container '$container_name' after $delay seconds."
    ) &
}

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
ycsb_dir="/home/otrack/Implementation/YCSB"
network_name="cassandra-network"

node_count=$(get_node_count)
# hosts=$(get_all_cassandra_ips "$network_name" "$node_count")
hosts=$(get_container_ip "cassandra-node$node_count")
if [ -z "$hosts" ]; then
    echo "Failed to retrieve the IP addresses."
    exit 1
fi

# Load data and write performance results to the output file
run_ycsb "run" "$ycsb_dir" "$workload" "$hosts" "$recordcount" "$operationcount" "$consistency_level" "$output_file" "$nthreads"

# Simulate a node crash after 2 minutes
# stop_container_after_delay "cassandra-node2" 90
