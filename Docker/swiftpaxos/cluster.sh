#!/usr/bin/env bash

SWIFTPAXOS_DIR=$(dirname "${BASH_SOURCE[0]}")

swiftpaxos_start_cluster() {
    if [ $# -ne 2 ]; then
        echo "usage: node_count protocol"
        exit -1
    fi
    local node_count=$1
    local protocol=$(echo "$2" | awk -F- '{print $2}')
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
	start_container ${image} ${container_name} "${message}" ${LOGDIR}/${protocol}_node${i}.log --rm -d --network $(config "network_name") --cap-add=NET_ADMIN --cap-add=NET_RAW ${resource_limits} -e PROTOCOL=${protocol} -e NSERVERS=${node_count} -e TYPE=server -e THRIFTY=false -e MADDR=${maddr} || {
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

swiftpaxos_get_leaders() {
    # For the paxos sub-protocol, find the leader by scanning container logs for
    # the message "I am the leader".  For other sub-protocols (epaxos, curp, …)
    # there is no single leader; fall back to database-node1.
    local sub_protocol
    sub_protocol=$(echo "$1" | awk -F- '{print $2}')
    if [ "${sub_protocol}" = "paxos" ]; then
        local node_count
        node_count=$(swiftpaxos_get_node_count)
        for i in $(seq 1 "${node_count}"); do
            local container_name
            container_name="$(config "node_name")${i}"
            if docker logs --tail 1000 "${container_name}" 2>&1 | grep -q "I am the leader"; then
                echo "${container_name}"
                return
            fi
        done
    fi
    # Default: node 3
    echo "$(config "node_name")3"
}
