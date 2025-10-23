#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

# Function to start Cassandra cluster
cassandra_start_cluster() {
    local node_count=$1
    local protocol=$2
    python3 start_cassandra_data_centers.py "$node_count" "$protocol"
    if [ $? -ne 0 ]; then
        error "Failed to start Cassandra cluster with $node_count node(s)."
        exit 1
    fi
}

# Function to add a new Cassandra node
cassandra_add_node() {
    local mode=$1
    python3 create_new_node.py "$mode"
    if [ $? -ne 0 ]; then
        error "Failed to add new Cassandra node."
        exit 1
    fi
}

# Function to clean up Cassandra cluster
cassandra_cleanup_cluster() {
    log "Cleaning up Cassandra cluster..."
    python3 cleanup_cassandra_cluster.py
    if [ $? -ne 0 ]; then
        error "Failed to clean up Cassandra cluster."
        exit 1
    fi
}

# Function to get the IP addresses of all Cassandra nodes
cassandra_get_all_ips() {
    node_count=$1
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
cassandra_get_node_count() {
    i=1
    while true; do
        container_name="cassandra-node$i"
	ip=$(get_container_ip "$container_name")
        if [ -z "$ip" ]; then
            break # FIXME
        fi
        i=$((i + 1))
    done
    echo $((i - 1))
}
