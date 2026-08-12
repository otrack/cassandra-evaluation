#!/usr/bin/env bash

COCKROACHDB_DIR=$(dirname "${BASH_SOURCE[0]}")

cockroachdb_start_cluster() {
    if [ $# -lt 2 ]; then
        echo "usage: num_dcs protocol [nodes_per_dc]"
        exit 1
    fi
    local num_dcs=$1
    local protocol=$2
    local nodes_per_dc=${3:-$(config nodesperdc)}
    local image=$(config cockroachdb_image)
    local network=$(config "network_name")
    local resource_limits=$(get_resource_limits)
    local max_mem_gb=$(echo ${resource_limits} | awk '{for(i=1;i<=NF;i++) if($i=="--memory") {v=$(i+1); printf "%.0f\n", (tolower(v)~/m/ ? (v+0)/1024 : (v+0))}}')
    
    log "Starting CockroachDB cluster with ${num_dcs} DC(s) x ${nodes_per_dc} node(s)/DC..."
    cockroachdb_cleanup_cluster >/dev/null 2>&1 || true

    local first_city=$(get_location 1 ${LOCATIONS_FILE})
    local first_node="${first_city}1"

    # 1. Start the very first node
    start_container ${image} ${first_node} "initial startup completed" ${LOGDIR}/cockroachdb_node1.log \
        --rm -d --network ${network} -p 8080:8080 --cap-add=NET_ADMIN --cap-add=NET_RAW ${resource_limits} \
        -- start --insecure --store=type=mem,size=${max_mem_gb}GB --join=${first_node} --locality=region=${first_city},zone=1 || {
        error "Failed to start first CockroachDB node ${first_node}"
        return 1
    }

    local first_ip=$(get_container_ip ${first_node})
    dexec ${first_node} ./cockroach init --insecure --host=${first_node} || {
        error "Failed to initialize CockroachDB cluster"
        return 2
    }
    log "CockroachDB cluster initialized on ${first_node}"

    # 2. Start remaining nodes across all DCs
    local global_node_id=1
    for i in $(seq 1 $num_dcs); do
        local city=$(get_location $i ${LOCATIONS_FILE})
        for k in $(seq 1 $nodes_per_dc); do
            if [ $i -eq 1 ] && [ $k -eq 1 ]; then
                global_node_id=$((global_node_id + 1))
                continue
            fi
            local container_name="${city}${k}"
            start_container ${image} ${container_name} "nodeID" ${LOGDIR}/cockroachdb_node${global_node_id}.log \
                --rm -d --network ${network} --cap-add=NET_ADMIN --cap-add=NET_RAW ${resource_limits} \
                -- start --insecure --store=type=mem,size=${max_mem_gb}GB --join=${first_ip} --locality=region=${city},zone=${k} || {
                error "Failed to start CockroachDB node ${container_name}"
                return 3
            }
            global_node_id=$((global_node_id + 1))
        done
    done

    log "CockroachDB cluster started successfully with ${num_dcs} DCs x ${nodes_per_dc} node(s)/DC"
}

cockroachdb_cleanup_cluster() {
    log "Cleaning up CockroachDB cluster..."
    for i in $(seq 1 15); do
        local city=$(get_location $i ${LOCATIONS_FILE} 2>/dev/null)
        [ -z "$city" ] && continue
        for k in $(seq 1 8); do
            local container_name="${city}${k}"
            stop_container ${container_name} >/dev/null 2>&1 || true
        done
    done
}

cockroachdb_get_hosts() {
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

cockroachdb_get_node_count() {
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

cockroachdb_get_port() {
    echo 26257
}

cockroachdb_get_leaders() {
    local first_city=$(get_location 1 ${LOCATIONS_FILE})
    echo "${first_city}1"
}

cockroachdb_fix_lease_holder() {
    if [ $# -lt 1 ] || [ $# -gt 2 ]; then
        echo "usage: cockroachdb_fix_lease_holder num_dcs [best]"
        exit 1
    fi
    local num_dcs=$1
    local best=${2:-true}
    local latencies_csv="${LOCATIONS_FILE}"

    local chosen_city
    chosen_city=$(python3 - "${num_dcs}" "${latencies_csv}" "${best}" <<'PYEOF'
import csv, math, sys

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

num_dcs       = int(sys.argv[1])
latencies_csv = sys.argv[2]
use_best      = sys.argv[3].lower() == 'true'
majority      = num_dcs // 2 + 1

locations = []
with open(latencies_csv, newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        locations.append((float(row['lat']), float(row['lon']), row['loc'].strip().strip('"')))
        if len(locations) >= num_dcs:
            break

chosen_city = None
chosen_cost = float('inf') if use_best else float('-inf')
for i, (lat1, lon1, loc) in enumerate(locations):
    dists = sorted(
        haversine(lat1, lon1, lat2, lon2)
        for j, (lat2, lon2, _) in enumerate(locations) if i != j
    )
    needed   = majority - 1
    farthest = dists[needed - 1] if 0 < needed <= len(dists) else 0.0
    if use_best:
        if farthest < chosen_cost:
            chosen_cost = farthest
            chosen_city = loc
    else:
        if farthest > chosen_cost:
            chosen_cost = farthest
            chosen_city = loc

print(chosen_city)
PYEOF
)

    if [ -z "${chosen_city}" ]; then
        error "cockroachdb_fix_lease_holder: failed to determine location"
        return 1
    fi

    log "Pinning CockroachDB lease holder to ${chosen_city}..."
    local first_city=$(get_location 1 ${LOCATIONS_FILE})
    local container="${first_city}1"
    local stmt="ALTER TABLE usertable CONFIGURE ZONE USING constraints = '{+region=${chosen_city}: 1}', lease_preferences = '[[\"+region=${chosen_city}\"]]';"
    dexec "${container}" cockroach sql --insecure -e "${stmt}"
}
