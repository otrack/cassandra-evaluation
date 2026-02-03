#!/usr/bin/env bash

COCKROACHDB_DIR=$(dirname "${BASH_SOURCE[0]}")

cockroachdb_start_cluster() {
    if [ $# -ne 2 ]; then
        echo "usage: node_count protocol"
        exit -1
    fi
    local node_count=$1
    local protocol=$2
    local image=$(config cockroachdb_image)
    local network=$(config "network_name")
    
    log "Starting CockroachDB cluster with ${node_count} node(s)..."
    
    # Start the first node (which initializes the cluster)
    local first_node=$(config "node_name")1
    start_container ${image} ${first_node} "initialized new cluster" ${LOGDIR}/cockroachdb_node1.log \
        --rm -d --network ${network} --cap-add=NET_ADMIN --cap-add=NET_RAW \
        cockroach start --insecure --advertise-addr=${first_node} --join=${first_node} || {
        error "Failed to start first CockroachDB node"
        return 1
    }
    
    # Initialize the cluster
    local first_ip=$(get_container_ip ${first_node})
    sleep 2
    docker exec ${first_node} ./cockroach init --insecure --host=${first_node} || {
        error "Failed to initialize CockroachDB cluster"
        return 2
    }
    log "CockroachDB cluster initialized on ${first_node}"
    
    # Start remaining nodes
    for i in $(seq 2 $node_count); do
        local container_name=$(config "node_name")${i}
        start_container ${image} ${container_name} "initialized new node" ${LOGDIR}/cockroachdb_node${i}.log \
            --rm -d --network ${network} --cap-add=NET_ADMIN --cap-add=NET_RAW \
            cockroach start --insecure --advertise-addr=${container_name} --join=${first_node} || {
            error "Failed to start CockroachDB node ${i}"
            return 3
        }
    done
    
    log "CockroachDB cluster started successfully with ${node_count} node(s)"
}

cockroachdb_cleanup_cluster() {
    log "Cleaning up CockroachDB cluster..."
    local node_count=$(cockroachdb_get_node_count)
    for i in $(seq 1 $node_count); do
        container_name=$(config "node_name")${i}
        stop_container ${container_name} || {
            error "Failed to stop CockroachDB node ${i}"
            return 1
        }
    done
}

cockroachdb_get_hosts() {
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

cockroachdb_get_node_count() {
    i=1
    while true; do
        container_name=$(config "node_name")${i}
        ip=$(get_container_ip "$container_name")
        if [ -z "$ip" ]; then
            break
        fi
        i=$((i + 1))
    done
    echo $((i - 1))
}

cockroachdb_get_port() {
    local port=26257
    echo ${port}
}
