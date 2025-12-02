#!/usr/bin/env bash

CASSANDRA_DIR=$(dirname "${BASH_SOURCE[0]}")

cassandra_start_cluster() {
    local node_count=$1
    local protocol=$2
    python3 ${CASSANDRA_DIR}/start_cassandra_data_centers.py "$node_count" "$protocol"
    if [ $? -ne 0 ]; then
        error "Failed to start Cassandra cluster with $node_count node(s)."
        exit 1
    fi
}

cassandra_add_node() {
    local mode=$1
    python3 ${CASSANDRA_DIR}/create_new_node.py "$mode"
    if [ $? -ne 0 ]; then
        error "Failed to add new Cassandra node."
        exit 1
    fi
}

cassandra_cleanup_cluster() {
    log "Cleaning up Cassandra cluster..."
    python3 ${CASSANDRA_DIR}/cleanup_cassandra_cluster.py
    if [ $? -ne 0 ]; then
        error "Failed to clean up Cassandra cluster."
        exit 1
    fi
}

cassandra_get_hosts() {
    node_count=$1
    ips=""
    for i in $(seq 1 $node_count); do
        container_name=$(config "node_name")${i}
        ip=$(get_container_ip "$container_name")
        if [ -n "$ip" ]; then
            ips="$ips,$ip"
        fi
    done
    # Remove leading comma
    ips=${ips#,}
    echo "$ips"
}

cassandra_get_node_count() {
    i=1
    while true; do
	container_name=$(config "node_name")${i}
	ip=$(get_container_ip "$container_name")
        if [ -z "$ip" ]; then
            break # FIXME
        fi
        i=$((i + 1))
    done
    echo $((i - 1))
}

cassandra_get_port() {
    local port=9042
    echo ${port}
}
