#!/usr/bin/env bash

COCKROACHDB_DIR=$(dirname "${BASH_SOURCE[0]}")

cockroachdb_start_cluster() {
    if [ $# -ne 2 ]; then
        echo "usage: node_count protocol"
        exit 1
    fi
    local node_count=$1
    local protocol=$2
    local image=$(config cockroachdb_image)
    local network=$(config "network_name")
    local resource_limits
    local resource_limits=$(get_resource_limits)
    local max_mem_gb=$(echo ${resource_limits} | awk '{for(i=1;i<=NF;i++) if($i=="--memory") {v=$(i+1); printf "%.0f\n", (tolower(v)~/m/ ? (v+0)/1024 : (v+0))}}')
    
    log "Starting CockroachDB cluster with ${node_count} node(s)..."
    
    # Start the first node (which initializes the cluster)
    local first_node=$(config "node_name")1
    # Note: Using "--" to separate Docker options from container command
    start_container ${image} ${first_node} "initial startup completed" ${LOGDIR}/cockroachdb_node1.log \
        --rm -d --network ${network} --cap-add=NET_ADMIN --cap-add=NET_RAW ${resource_limits} \
        -- start --insecure --store=type=mem,size=${max_mem_gb}GB --join=${first_node} || {
        error "Failed to start first CockroachDB node"
        return 1
    }
    
    # Initialize the cluster
    local first_ip=$(get_container_ip ${first_node})
    docker exec ${first_node} ./cockroach init --insecure --host=${first_node} || {
        error "Failed to initialize CockroachDB cluster"
        return 2
    }
    log "CockroachDB cluster initialized on ${first_node}"
    
    # Start remaining nodes
    for i in $(seq 2 $node_count); do
        local container_name=$(config "node_name")${i}
        # Note: Using "--" to separate Docker options from container command
        start_container ${image} ${container_name} "nodeID" ${LOGDIR}/cockroachdb_node${i}.log \
            --rm -d --network ${network} --cap-add=NET_ADMIN --cap-add=NET_RAW ${resource_limits} \
            -- start --insecure --store=type=mem,size=${max_mem_gb}GB --join=${first_ip} || {
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
    local node_count=$1
    local ips=""
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
    local i=1
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

cockroachdb_get_leaders() {
    # Returns all containers that hold at least one leaseholder for a range of
    # the usertable.  The lease_holder column in crdb_internal.ranges is a
    # node-ID (1-based integer) which maps directly to the container name.
    local container="$(config "node_name")1"
    local node_ids
    node_ids=$(docker exec "${container}" cockroach sql --insecure --format=csv \
        -e "SELECT DISTINCT lease_holder FROM crdb_internal.ranges WHERE table_name = 'usertable';" \
        2>/dev/null | tail -n +2 | tr -d '[:space:]')
    for node_id in ${node_ids}; do
        echo "$(config "node_name")${node_id}"
    done
}
