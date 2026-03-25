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
    local city=$(cat ${DIR}/latencies.csv | head -n 2 | tail -n 1 | awk -F, '{print $3}')
    # Note: Using "--" to separate Docker options from container command
    start_container ${image} ${first_node} "initial startup completed" ${LOGDIR}/cockroachdb_node1.log \
        --rm -d --network ${network} -p 8080:8080 --cap-add=NET_ADMIN --cap-add=NET_RAW ${resource_limits} \
        -- start --insecure --store=type=mem,size=${max_mem_gb}GB --join=${first_node} --locality=region=${city},zone=1 || {
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
	city=$(cat ${DIR}/latencies.csv | head -n $((i+1)) | tail -n 1 | awk -F, '{print $3}')
        # Note: Using "--" to separate Docker options from container command
        start_container ${image} ${container_name} "nodeID" ${LOGDIR}/cockroachdb_node${i}.log \
            --rm -d --network ${network} --cap-add=NET_ADMIN --cap-add=NET_RAW ${resource_limits} \
            -- start --insecure --store=type=mem,size=${max_mem_gb}GB --join=${first_ip} --locality=region=${city},zone=1 || {
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
    local container="$(config "node_name")1"
    local node_ids
    node_ids=$(docker exec "${container}" cockroach sql --insecure --format=csv \
        -e "SELECT DISTINCT lease_holder FROM [SHOW RANGES FROM TABLE usertable WITH DETAILS];" \
        2>/dev/null | tail -n +2 | sed 's/[[:space:]]//g')
    for node_id in ${node_ids}; do
        echo "$(config "node_name")${node_id}"
    done
}

# cockroachdb_fix_lease_holder <node_count>
#
# Pin the CockroachDB lease holder of "usertable" to the node whose location
# minimises the round-trip latency to a Raft majority quorum.  The optimal
# leader is the one for which the distance to the (majority-1)-th nearest peer
# is smallest (i.e. the farthest node in the cheapest majority quorum is as
# close as possible).  Geographic distances and the fiber-optic latency model
# are the same as those used in distance.py / emulate_latency.py.
cockroachdb_fix_lease_holder() {
    if [ $# -ne 1 ]; then
        echo "usage: cockroachdb_fix_lease_holder node_count"
        exit 1
    fi
    local node_count=$1
    local latencies_csv="${COCKROACHDB_DIR}/../latencies.csv"

    log "Computing optimal CockroachDB lease holder for ${node_count} nodes..."

    # Inline Python: find the city whose round-trip to majority quorum is minimal.
    # Arguments are passed via sys.argv so the heredoc can remain single-quoted
    # (no accidental bash variable expansion inside the Python text).
    local best_city
    best_city=$(python3 - "${node_count}" "${latencies_csv}" <<'PYEOF'
import csv, math, sys

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

node_count    = int(sys.argv[1])
latencies_csv = sys.argv[2]
majority      = node_count // 2 + 1   # Raft majority quorum size

locations = []
with open(latencies_csv, newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        locations.append((float(row['lat']), float(row['lon']), row['loc']))
        if len(locations) >= node_count:
            break

best_city = None
best_cost = float('inf')
for i, (lat1, lon1, loc) in enumerate(locations):
    # Distances from node i to every other node, sorted ascending
    dists = sorted(
        haversine(lat1, lon1, lat2, lon2)
        for j, (lat2, lon2, _) in enumerate(locations) if i != j
    )
    # The leader commits when (majority-1) other nodes ACK.
    # Cost = distance to the farthest of those needed nodes.
    needed  = majority - 1
    farthest = dists[needed - 1] if 0 < needed <= len(dists) else 0.0
    if farthest < best_cost:
        best_cost = farthest
        best_city = loc

print(best_city)
PYEOF
)

    if [ -z "${best_city}" ]; then
        error "cockroachdb_fix_lease_holder: failed to determine best location"
        return 1
    fi

    log "Pinning CockroachDB lease holder to ${best_city}..."
    local container
    container="$(config "node_name")1"
    local stmt="ALTER TABLE usertable CONFIGURE ZONE USING lease_preferences = '[[\"+region=${best_city}\"]]';"
    docker exec "${container}" cockroach sql --insecure -e "${stmt}"
    if [ $? -ne 0 ]; then
        error "cockroachdb_fix_lease_holder: ALTER TABLE failed for region=${best_city}"
        return 1
    fi
    log "CockroachDB lease holder pinned to ${best_city}"
}
