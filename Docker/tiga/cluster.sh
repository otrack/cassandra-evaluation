#!/usr/bin/env bash

TIGA_SERVICE_DIR=$(realpath "$(dirname "${BASH_SOURCE[0]}")")

tiga_start_cluster() {
    if [ $# -ne 2 ]; then
        echo "usage: node_count protocol"
        exit -1
    fi
    local node_count=$1
    local protocol=$2

    if [[ "$protocol" == tiga-* ]]; then
        protocol="${protocol#tiga-}"
    fi

    local image=$(config tiga_image)
    local resource_limits=$(get_resource_limits)

    # 1. Generate config-ycsb.yml dynamically using python
    python3 -c "
import sys, yaml, os
node_count = int(sys.argv[1])
node_name_prefix = sys.argv[2]
tiga_service_dir = sys.argv[3]
base_config_path = os.path.abspath(os.path.join(tiga_service_dir, '../../../Tiga/config-ycsb.yml'))
with open(base_config_path, 'r') as f:
    config = yaml.safe_load(f)
servers = []
host_map = {}
process_map = {}
initial_bounds = []
bound_caps = []
designate_replica = []
for i in range(1, node_count + 1):
    server_name = f'janus-lan-server-{(i-1)*100:04d}'
    port = i * 10000
    servers.append(f'{server_name}:{port}')
    container_name = f'{node_name_prefix}{i}'
    host_map[server_name] = container_name
    process_map[server_name] = server_name
    initial_bounds.append(100)
    bound_caps.append(400000)
    designate_replica.append(i - 1)
proxy_name = 'janus-lan-proxy-0000'
host_map[proxy_name] = 'localhost'
process_map[proxy_name] = proxy_name
config['site']['server'] = [servers]
config['host'] = host_map
config['process'] = process_map
config['server_initial_bound'] = initial_bounds
config['server_bound_cap'] = bound_caps
config['designate_replica_id'] = designate_replica
config['preventive'] = True
with open(f'{tiga_service_dir}/config-ycsb.yml', 'w') as f:
    yaml.dump(config, f, default_flow_style=False)
" "$node_count" "$(config "node_name")" "${TIGA_SERVICE_DIR}"

    log "Generated dynamic config file in ${TIGA_SERVICE_DIR}/config-ycsb.yml"

    # 2. Start replica containers
    for i in $(seq 1 $node_count); do
        local server_name=$(printf "janus-lan-server-%04d" $(( (i-1) * 100 )))
        local container_name=$(config "node_name")$i

        start_container ${image} ${container_name} "started on" ${LOGDIR}/${protocol}_node${i}.log \
            --rm -d --network $(config "network_name") ${resource_limits} \
            -v ${TIGA_SERVICE_DIR}/config-ycsb.yml:/app/config/config-ycsb.yml \
            -e PROTOCOL=${protocol} \
            -e SERVER_NAME=${server_name} \
            -e CONFIG_PATH=/app/config/config-ycsb.yml || {
            error "Failed to start tiga/calvin/detock/janus server $i"
            return 2
        }
    done
}

tiga_cleanup_cluster() {
    log "Cleaning up Tiga cluster..."
    local node_count=$(tiga_get_node_count)
    for i in $(seq 1 $node_count); do
        local container_name=$(config "node_name")$i
        stop_container ${container_name} || {
            error "Failed to stop server $i"
            return 2
        }
    done
}

tiga_get_hosts() {
    local node_count=$1
    container_name="$(config "node_name")1"
    echo $(get_container_ip "$container_name")
}

tiga_get_node_count() {
    i=1
    while true; do
        container_name=$(config "node_name")$i
        ip=$(get_container_ip "$container_name")
        if [ -z "$ip" ]; then
            break
        fi
        i=$((i + 1))
    done
    echo $((i - 1))
}

tiga_get_port() {
    echo 10000
}
