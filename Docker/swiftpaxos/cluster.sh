#!/usr/bin/env bash

SWIFTPAXOS_DIR=$(dirname "${BASH_SOURCE[0]}")

swiftpaxos_start_cluster() {
    if [ $# -ne 2 ]; then
        echo "usage: node_count protocol"
        exit -1
    fi
    local node_count=$1
    local protocol=$2
    image=$(config swiftpaxos_image)
    local resource_limits
    resource_limits=$(get_resource_limits)
    # Start master
    start_container ${image} "swiftpaxos-master" "waiting for ${node_count} replicas" ${LOGDIR}/swiftpaxos_master.log --rm -d --network $(config "network_name") ${resource_limits} -e NSERVERS=${node_count} -e TYPE=master || {
        error "Failed to start master"
        return 1
    }
    maddr=$(get_container_ip swiftpaxos-master)
    # Start remaining nodes
    for i in $(seq 1 $node_count); do
	if [[ "$i" == "$node_count" ]]; then
	    message="Node list"
	else
	    message="Server starting"
	fi
        container_name=$(config "node_name")$i
	start_container ${image} ${container_name} "${message}" ${LOGDIR}/swiftpaxos_node${i}.log --rm -d --network $(config "network_name") --cap-add=NET_ADMIN --cap-add=NET_RAW ${resource_limits} -e PROTOCOL=${protocol} -e NSERVERS=${node_count} -e TYPE=server -e MADDR=${maddr} || {
            error "Failed to start server $i"
            return 2
	}
    done        
}

swiftpaxos_cleanup_cluster() {
    log "Cleaning up Swiftpaxos cluster..."    
    stop_container "swiftpaxos-master"|| {
        error "Failed to stop master"
        return 1
    }
    local node_count=$(swiftpaxos_get_node_count)    
    for i in $(seq 1 $node_count); do
	container_name=$(config "node_name")$i
	stop_container ${container_name} || {
            error "Failed to start server $i"
            return 2
	}
    done	
}

swiftpaxos_get_hosts() {
    container_name="swiftpaxos-master"
    echo $(get_container_ip "$container_name")
}

swiftpaxos_get_node_count() {
    i=1
    while true; do
	container_name=$(config "node_name")$i
	ip=$(get_container_ip "$container_name")
        if [ -z "$ip" ]; then
            break # FIXME
        fi
        i=$((i + 1))
    done
    echo $((i - 1))
}


swiftpaxos_get_port() {
    local port=7087 # master
    echo ${port}
}
