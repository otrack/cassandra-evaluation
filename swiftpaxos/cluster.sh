#!/usr/bin/env bash

SWIFTPAXOS_DIR=$(dirname "${BASH_SOURCE[0]}")

swiftpaxos_start_cluster() {
    if [ $# -lt 2 ]; then
        echo "usage: num_dcs protocol [nodes_per_dc]"
        exit -1
    fi
    local nodes_per_dc=${3:-$(config nodesperdc)}
    if [ "${nodes_per_dc:-1}" -gt 1 ]; then
        error "swiftpaxos protocol does not support nodesperdc > 1 (current nodesperdc=${nodes_per_dc})."
        exit 1
    fi
    local num_dcs=$1
    local protocol=$(echo "$2" | awk -F- '{print $2}')
    image=$(config swiftpaxos_image)
    local resource_limits
    resource_limits=$(get_resource_limits)
    # Start master
    start_container ${image} "swiftpaxos-master" "waiting for ${num_dcs} replicas" ${LOGDIR}/swiftpaxos_master.log --rm -d --network $(config "network_name") ${resource_limits} -e NSERVERS=${num_dcs} -e TYPE=master || {
        error "Failed to start master"
        return 1
    }
    maddr=$(get_container_ip swiftpaxos-master)
    # Start remaining nodes
    for i in $(seq 1 $num_dcs); do
	if [[ "$i" == "$num_dcs" ]]; then
	    message="Node list"
	else
	    message="Server starting"
	fi
        location=$(get_location $i ${LOCATIONS_FILE})
        container_name="${location}1"
	start_container ${image} ${container_name} "${message}" ${LOGDIR}/${protocol}_node${i}.log --rm -d --network $(config "network_name") --cap-add=NET_ADMIN --cap-add=NET_RAW ${resource_limits} -e PROTOCOL=${protocol} -e NSERVERS=${num_dcs} -e TYPE=server -e THRIFTY=false -e MADDR=${maddr} || {
            error "Failed to start server $i"
            return 2
	}
    done        
}

swiftpaxos_cleanup_cluster() {
    log "Cleaning up Swiftpaxos cluster..."    
    stop_container "swiftpaxos-master" || true
    local num_dcs=${1:-$(swiftpaxos_get_num_dcs)}
    [ -z "$num_dcs" ] || [ "$num_dcs" -eq 0 ] && num_dcs=5
    for i in $(seq 1 $num_dcs); do
        location=$(get_location $i ${LOCATIONS_FILE})
        container_name="${location}1"
        if container_exists "$container_name"; then
            stop_container "${container_name}" || true
        fi
        if container_exists "database-node${i}"; then
            stop_container "database-node${i}" || true
        fi
        if container_exists "ycsb-${i}"; then
            stop_container "ycsb-${i}" || true
        fi
    done	
    if container_exists "ycsb"; then
        stop_container "ycsb" || true
    fi
}

swiftpaxos_get_hosts() {
    container_name="swiftpaxos-master"
    echo $(get_container_ip "$container_name")
}

swiftpaxos_get_num_dcs() {
    i=1
    while true; do
        location=$(get_location $i ${LOCATIONS_FILE})
        if [ -z "$location" ]; then
            break
        fi
        container_name="${location}1"
        if ! node_is_up "$container_name"; then
            break
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
    # there is no single leader; fall back to nearest DC node.
    local sub_protocol
    sub_protocol=$(echo "$1" | awk -F- '{print $2}')
    local num_dcs=$(swiftpaxos_get_num_dcs)
    if [ "${sub_protocol}" = "paxos" ]; then
        for i in $(seq 1 "${num_dcs}"); do
            location=$(get_location $i ${LOCATIONS_FILE})
            container_name="${location}1"
            if dlogs "${container_name}" --tail 1000 2>&1 | grep -q "I am the leader"; then
                echo "${container_name}"
                return
            fi
        done
    fi
    # Default: node 3
    location=$(get_location 3 ${LOCATIONS_FILE})
    echo "${location}1"
}
