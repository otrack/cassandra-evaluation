#!/usr/bin/env bash

SWIFTPAXOS_DIR=$(dirname "${BASH_SOURCE[0]}")

swiftpaxos_start_cluster() {
    if [ $# -lt 2 ]; then
        echo "usage: num_dcs protocol [nodes_per_dc]"
        exit -1
    fi
    local num_dcs=$1
    local protocol=$(echo "$2" | awk -F- '{print $2}')
    local nodes_per_dc=${3:-$(config nodesperdc)}
    nodes_per_dc=${nodes_per_dc:-1}
    # Janus is sharded: shard j is made of the j-th replica of every DC.  The
    # other protocols replicate everything, with a single replica per DC.
    local shard_args=()
    if [ "${protocol}" = "janus" ]; then
        shard_args=(-e REPLICASPERSHARD=${num_dcs})
    elif [ "${nodes_per_dc}" -gt 1 ]; then
        error "swiftpaxos protocol ${protocol} does not support nodesperdc > 1 (current nodesperdc=${nodes_per_dc})."
        exit 1
    fi
    local nservers=$((num_dcs * nodes_per_dc))
    swiftpaxos_cleanup_cluster "${num_dcs}" "${nodes_per_dc}" >/dev/null 2>&1 || true
    image=$(config swiftpaxos_image)
    local resource_limits
    resource_limits=$(get_resource_limits)
    # Start master
    start_container ${image} "swiftpaxos-master" "waiting for ${nservers} replicas" ${LOGDIR}/swiftpaxos_master.log --rm -d --network $(config "network_name") ${resource_limits} -e NSERVERS=${nservers} -e TYPE=master -e PROTOCOL=${protocol} || {
        error "Failed to start master"
        return 1
    }
    maddr=$(get_container_ip swiftpaxos-master)
    # Start the replicas one at a time, shard by shard: the master numbers the
    # replicas in their registration order, and replicas [s*k, (s+1)*k) form
    # shard s, where k is the shard size.
    local index=0
    for j in $(seq 1 $nodes_per_dc); do
        for i in $(seq 1 $num_dcs); do
            if [[ "$index" == "$((nservers - 1))" ]]; then
                message="Node list"
            else
                message="Server starting"
            fi
            location=$(get_location $i ${LOCATIONS_FILE})
            container_name="${location}${j}"
            local logfile=${LOGDIR}/${protocol}_node${i}.log
            if [ "$j" -gt 1 ]; then
                logfile=${LOGDIR}/${protocol}_node${i}_${j}.log
            fi
            local extra_args=() container_cmd=()
            if infra_is_real; then
                # The image's default entrypoint (bin/run.sh) self-detects its
                # advertise address from the host's default-route interface,
                # which under --network host (used for every real provider) is
                # always the instance's private IP -- e.g. on AWS the public IP
                # is NAT'd at the Internet Gateway and never locally bound, so
                # that address is unreachable from another region/host. Bypass
                # run.sh and invoke the binary directly with the real,
                # peer-reachable address instead.
                local server_addr
                server_addr=$(infra_host_ip "$(node_index_of "${container_name}")")
                extra_args=(--entrypoint /swiftpaxos/bin/swiftpaxos)
                container_cmd=(-- -run server -config base.conf -protocol "${protocol}" -thrifty false -nservers "${nservers}" -maddr "${maddr}" -addr "${server_addr}" -alias "${server_addr}")
                if [ "${protocol}" = "janus" ]; then
                    container_cmd+=(-replicasPerShard "${num_dcs}")
                fi
            fi
            start_container ${image} ${container_name} "${message}" ${logfile} --rm -d --network $(config "network_name") --cap-add=NET_ADMIN --cap-add=NET_RAW ${resource_limits} "${extra_args[@]}" -e PROTOCOL=${protocol} -e NSERVERS=${nservers} -e TYPE=server -e THRIFTY=false -e MADDR=${maddr} "${shard_args[@]}" "${container_cmd[@]}" || {
                error "Failed to start server ${container_name}"
                return 2
            }
            swiftpaxos_wait_registration ${index} || return 3
            index=$((index + 1))
        done
    done
}

# Waits until the master registered replica number $1.
swiftpaxos_wait_registration() {
    local start_time
    start_time=$(date +%s)
    local timeout=${START_CONTAINER_TIMEOUT:-90}
    until dlogs swiftpaxos-master 2>&1 | grep -q -- "node $1 \["; do
        if (( $(date +%s) - start_time >= timeout )); then
            error "Timeout (${timeout}s) waiting for the registration of replica $1 at the master"
            return 1
        fi
        sleep 0.2
    done
}

swiftpaxos_cleanup_cluster() {
    log "Cleaning up Swiftpaxos cluster..."    
    stop_container "swiftpaxos-master" || true
    local num_dcs=${1:-$(swiftpaxos_get_num_dcs)}
    [ -z "$num_dcs" ] || [ "$num_dcs" -eq 0 ] && num_dcs=5
    local nodes_per_dc=${2:-$(config nodesperdc)}
    nodes_per_dc=${nodes_per_dc:-1}
    for i in $(seq 1 $num_dcs); do
        location=$(get_location $i ${LOCATIONS_FILE})
        for j in $(seq 1 $nodes_per_dc); do
            container_name="${location}${j}"
            if container_exists "$container_name"; then
                stop_container "${container_name}" || true
            fi
        done
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
        # Past the last location, get_location fails: the end of the walk
        location=$(get_location $i ${LOCATIONS_FILE} 2>/dev/null)
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
