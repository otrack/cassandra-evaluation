#!/usr/bin/env bash

CASSANDRA_DIR=$(dirname "${BASH_SOURCE[0]}")

cassandra_start_cluster() {
    local num_dcs=$1
    local protocol=$2
    local nodes_per_dc=${3:-$(config nodesperdc)}
    if printf '%s\n' "$protocol" | grep -wF -q -- "cassandra"; then
        protocol=$(echo "${protocol}" | awk -F- '{print $2}')
    fi
    cassandra_cleanup_cluster >/dev/null 2>&1 || true
    python3 ${CASSANDRA_DIR}/start_cassandra_data_centers.py "$num_dcs" "$protocol" "$nodes_per_dc"
    if [ $? -ne 0 ]; then
        error "Failed to start Cassandra cluster with $num_dcs DC(s)."
        exit 1
    fi

    local global_node_id=1
    for i in $(seq 1 $num_dcs); do
        local city=$(get_location $i ${LOCATIONS_FILE})
        for k in $(seq 1 $nodes_per_dc); do
            local container_name="${city}${k}"
            local log_file=${LOGDIR}/${protocol}_node${global_node_id}.log
            dlogs $container_name -f > ${log_file} 2>&1 &
            global_node_id=$((global_node_id + 1))
        done
    done
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
    local num_dcs=$1
    local nodes_per_dc=${2:-$(config nodesperdc)}
    local ips=""
    for i in $(seq 1 $num_dcs); do
        local city=$(get_location $i ${LOCATIONS_FILE})
        for k in $(seq 1 $nodes_per_dc); do
            local container_name="${city}${k}"
            local ip=$(get_container_ip "$container_name")
            if [ -n "$ip" ]; then
                ips="$ips,$ip"
            fi
        done
    done
    ips=${ips#,}
    echo "$ips"
}

cassandra_get_node_count() {
    local num_dcs=0
    for i in $(seq 1 15); do
        local city=$(get_location $i ${LOCATIONS_FILE} 2>/dev/null)
        [ -z "$city" ] && continue
        if node_is_up "${city}1"; then
            num_dcs=$((num_dcs + 1))
        fi
    done
    echo $num_dcs
}

cassandra_get_port() {
    local port=9042
    echo ${port}
}

cassandra_get_dc() {
    local container=$1
    dinspect "$container" -f '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null \
        | grep '^CASSANDRA_DC=' | cut -d'=' -f2
}

cassandra_get_leaders() {
    local first_city=$(get_location 1 ${LOCATIONS_FILE})
    echo "${first_city}1"
}
