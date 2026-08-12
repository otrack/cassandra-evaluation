#!/usr/bin/env bash

TIGA_DIR=$(realpath "$(dirname "${BASH_SOURCE[0]}")")

tiga_start_cluster() {
    if [ $# -lt 2 ]; then
        echo "usage: node_count protocol [nodes_per_dc]"
        exit -1
    fi
    local num_dcs=$1
    local protocol=$2
    local nodes_per_dc=${3:-$(config nodesperdc)}

    if [[ "$protocol" == tiga-* ]]; then
        protocol="${protocol#tiga-}"
    fi

    local image=$(config tiga_image)
    local resource_limits=$(get_resource_limits)
    tiga_cleanup_cluster >/dev/null 2>&1 || true

    # 1. Generate config-ycsb.yml dynamically using python
    python3 -c "
import sys, yaml, os, csv
num_dcs = int(sys.argv[1])
nodes_per_dc = int(sys.argv[2])
tiga_dir = sys.argv[3]
latencies_csv = sys.argv[4]

locations = []
lat_lons = []
with open(latencies_csv, newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        locations.append(row['loc'].strip().strip('\"'))
        lat_lons.append((float(row['lat']), float(row['lon'])))
        if len(locations) >= num_dcs:
            break

def haversine(lat1, lon1, lat2, lon2):
    import math
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon1 - lon2)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

best_leader_idx = 0
min_total_owd = float('inf')
for i in range(num_dcs):
    lat1, lon1 = lat_lons[i]
    total_owd = 0
    for j in range(num_dcs):
        if i == j: continue
        lat2, lon2 = lat_lons[j]
        dist = haversine(lat1, lon1, lat2, lon2)
        total_owd += int(dist / 204)
    if total_owd < min_total_owd:
        min_total_owd = total_owd
        best_leader_idx = i

base_config_path = os.path.join(tiga_dir, 'config-ycsb.yml')
if not os.path.exists(base_config_path):
    base_config_path = os.path.abspath(os.path.join(tiga_dir, '../../../Tiga/config-ycsb.yml'))
with open(base_config_path, 'r') as f:
    config = yaml.safe_load(f)

shards = []
host_map = {}
process_map = {}
initial_bounds = []
bound_caps = []

for j in range(nodes_per_dc):
    shard_servers = []
    for i in range(num_dcs):
        server_name = f'janus-lan-server-{i*100 + j:04d}'
        port = 10000 + j
        shard_servers.append(f'{server_name}:{port}')
        city = locations[i]
        container_name = f'{city}{j+1}'
        host_map[server_name] = container_name
        process_map[server_name] = server_name
        initial_bounds.append(100)
        bound_caps.append(400000)
    shards.append(shard_servers)

designate_replica = [best_leader_idx] * nodes_per_dc

proxy_name = 'janus-lan-proxy-0000'
host_map[proxy_name] = 'localhost'
process_map[proxy_name] = proxy_name

config['site']['server'] = shards
config['host'] = host_map
config['process'] = process_map
config['server_initial_bound'] = initial_bounds
config['server_bound_cap'] = bound_caps
config['designate_replica_id'] = designate_replica
config['preventive'] = True

with open(f'{tiga_dir}/config-ycsb.yml', 'w') as f:
    yaml.dump(config, f, default_flow_style=False)
" "$num_dcs" "$nodes_per_dc" "${TIGA_DIR}" "${DIR}/latencies.csv"

    log "Generated dynamic config file in ${TIGA_DIR}/config-ycsb.yml for ${num_dcs} DCs x ${nodes_per_dc} nodes/DC"

    # 2. Start replica containers
    local global_node_id=1
    for i in $(seq 1 $num_dcs); do
        local city=$(get_location $i ${DIR}/latencies.csv)
        for k in $(seq 1 $nodes_per_dc); do
            local server_name=$(printf "janus-lan-server-%04d" $(( (i-1) * 100 + (k-1) )))
            local container_name="${city}${k}"

            local staged_config
            staged_config=$(infra_stage_file "$(node_index_of "${container_name}")" \
                                             "${TIGA_DIR}/config-ycsb.yml" /tmp/config-ycsb.yml)

            start_container ${image} ${container_name} "started on" ${LOGDIR}/${protocol}_node${global_node_id}.log \
                --rm -d --network $(config "network_name") --cap-add=NET_ADMIN --cap-add=NET_RAW ${resource_limits} \
                -v ${staged_config}:/app/config/config-ycsb.yml \
                -e PROTOCOL=${protocol} \
                -e SERVER_NAME=${server_name} \
                -e CONFIG_PATH=/app/config/config-ycsb.yml || {
                error "Failed to start tiga/calvin/detock/janus server ${container_name}"
                return 2
            }
            global_node_id=$((global_node_id + 1))
        done
    done
}

tiga_cleanup_cluster() {
    log "Cleaning up Tiga cluster..."
    local num_dcs=$(config node_count 2>/dev/null || echo 5)
    local nodes_per_dc=$(config nodesperdc)
    for i in $(seq 1 15); do
        local city=$(get_location $i ${DIR}/latencies.csv 2>/dev/null)
        [ -z "$city" ] && continue
        for k in $(seq 1 8); do
            local container_name="${city}${k}"
            dstop ${container_name} >/dev/null 2>&1 || true
        done
    done
}

tiga_get_hosts() {
    local num_dcs=$1
    local nodes_per_dc=${2:-$(config nodesperdc)}
    local ips=""
    for i in $(seq 1 $num_dcs); do
        local city=$(get_location $i ${DIR}/latencies.csv)
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

tiga_get_node_count() {
    local num_dcs=0
    for i in $(seq 1 15); do
        local city=$(get_location $i ${DIR}/latencies.csv 2>/dev/null)
        [ -z "$city" ] && continue
        if node_is_up "${city}1"; then
            num_dcs=$((num_dcs + 1))
        fi
    done
    echo $num_dcs
}

tiga_get_port() {
    echo 10000
}

tiga_get_leaders() {
    local num_dcs=$(tiga_get_node_count)
    [ -z "$num_dcs" ] || [ "$num_dcs" -eq 0 ] && num_dcs=3
    local best_city=$(python3 -c "
import sys, csv, math
num_dcs = int(sys.argv[1])
latencies_csv = sys.argv[2]
locations = []
lat_lons = []
with open(latencies_csv, newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        locations.append(row['loc'].strip().strip('\"'))
        lat_lons.append((float(row['lat']), float(row['lon'])))
        if len(locations) >= num_dcs:
            break

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon1 - lon2)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

best_leader_idx = 0
min_total_owd = float('inf')
for i in range(num_dcs):
    lat1, lon1 = lat_lons[i]
    total_owd = 0
    for j in range(num_dcs):
        if i == j: continue
        lat2, lon2 = lat_lons[j]
        total_owd += int(haversine(lat1, lon1, lat2, lon2) / 204)
    if total_owd < min_total_owd:
        min_total_owd = total_owd
        best_leader_idx = i

print(locations[best_leader_idx])
" "$num_dcs" "${DIR}/latencies.csv")
    echo "${best_city}1"
}
