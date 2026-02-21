#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/cassandra/cluster.sh
source ${DIR}/cassandra/ycsb.sh
source ${DIR}/swiftpaxos/cluster.sh
source ${DIR}/cockroachdb/cluster.sh
source ${DIR}/cockroachdb/ycsb.sh

start_network() {
    local network_name=$(config network_name)

    if [ -z "$network_name" ]; then
        error "network_name not set in config"
        return 1
    fi

    if docker network inspect "$network_name" >/dev/null 2>&1; then
        debug "Network '${network_name}' already exists."
        return 0
    fi

    if docker network create --driver bridge "$network_name" >/dev/null 2>&1; then
        debug "Created network '${network_name}'."
        return 0
    else
        error "Failed to create network '${network_name}'."
        return 2
    fi
}

stop_network() {
    local network_name=$(config network_name)

    if [ -z "$network_name" ]; then
        error "network_name not set in config"
        return 1
    fi

    if ! docker network inspect "$network_name" >/dev/null 2>&1; then
        debug "Network ${network_name} not found."
        return 0
    fi

    if docker network rm "$network_name" >/dev/null 2>&1; then
        debug "Removed network: ${network_name}"
        return 0
    else
        error "Failed to remove network: ${network_name}"
        return 2
    fi
}

emulate_latency() {
    local node_count=$1
    python3 emulate_latency.py "$node_count"
    if [ $? -ne 0 ]; then
        error "Failed to add latency emulation."
        exit 1
    fi
}

run_ycsb() {
    if [ $# -lt 12 ]; then
	echo "Usage: $0 <action> <workload_type> <workload> <hosts> <port> <recordcount> <operation_count> <protocol> <output_file> <threads> <container_name> <network_adapter> [extra_ycsb_options]"
	echo "Example: $0 load site.ycsb.CoreWorkload a 127.0.0.1,127.0.0.2 8080 1 1 QUORUM results.txt 100 ycsb database-node1"
	exit 1
    fi

    local action=$1
    local workload_type=$2
    local workload=$3
    local hosts=$4
    local port=$5
    local recordcount=$6
    local operationcount=$7
    local protocol=$8
    local output_file=$9
    local threads=${10}
    local container_name=${11}
    local nearby_database=${12}
    local ycsb_threads=${threads}

    if [ "$action" == "load" ]; then
        ycsb_threads=1
    fi
    
    # capture any extra arguments (13th onward) and prepare a safely quoted string
    shift 12
    local extra_opts=( "$@" )
    local extra_opts_str=""
    if [ ${#extra_opts[@]} -gt 0 ]; then
      for o in "${extra_opts[@]}"; do
        # printf %q produces a shell-escaped representation; safe to append to the command string
        extra_opts_str+=" $(printf '%q' "$o")"
      done
    fi
    log ${extra_opts_str[@]}

    local docker_args="--rm -d --network container:${nearby_database} --env-file=${output_file%.dat}.docker"
    
    if [ "$action" == "load" ];
    then

	if printf '%s\n' "$protocol" | grep -wF -q -- "swiftpaxos";
	then
	    local leaderless="false"
	    local fast="false"
	    if printf '%s\n' "$protocol" | grep -wF -q -- "epaxos";
	    then
		leaderless="true"
		fast="true"
	    fi
	elif printf '%s\n' "$protocol" | grep -wF -q -- "cockroachdb";
	then
	    cockroachdb_create_usertable
	else
	    # cassandra
	    # Determine transaction mode
	    local transaction_mode="bruh"
	    if [ "$protocol" == "accord" ]; then 
		transaction_mode="full"
	    fi

	    # Create the keyspace if it doesn't exist
	    cassandra_create_keyspace 3600 "$node_count"

	    # Create the usertable if it doesn't exist
	    cassandra_create_usertable 3600 "$transaction_mode" "$node_count"
	fi
    fi

    local ycsb_image=$(config ycsb_image)
    
    local ycsb_client="swiftpaxos"
    if printf '%s\n' "$protocol" | grep -wF -q -- "swiftpaxos";
    then
	extra_opts_str+=" -p maddr=${hosts} \
-p mport=${port} \
-p verbose=false \
-p leaderless=${leaderless} \
-p fast=${fast}"
    elif printf '%s\n' "$protocol" | grep -wF -q -- "cockroachdb";
    then
	# CockroachDB using JDBC (PostgreSQL wire protocol)
	# Empty password is intentional - CockroachDB runs in insecure mode for testing
	ycsb_client="jdbc"
	hosts=$(get_container_ip ${nearby_database})
	local jdbc_url="jdbc:postgresql://${hosts}:${port}/defaultdb?sslmode=disable"
	extra_opts_str+=" -p db.driver=org.postgresql.Driver \
-p db.url=${jdbc_url} \
-p db.user=root \
-p db.passwd="
    else
	# cassandra
	ycsb_client="cassandra-cql"
	local consistency_level="ONE"
	if [ "$protocol" == "accord" ] || [ "$protocol" == "paxos" ];
	then
	    consistency_level="SERIAL"
	elif [ "$protocol" == "quorum" ];
	then
	    consistency_level="QUORUM"
	fi
	hosts=$(get_container_ip ${nearby_database})
	extra_opts_str+=" -p hosts=$hosts -p port=$port \
-p cassandra.writeconsistencylevel=$consistency_level \
-p cassandra.readconsistencylevel=$consistency_level"
    fi

    # adjust debug level below
    echo -e "JAVA_OPTS=-Dorg.slf4j.simpleLogger.defaultLogLevel=info\n\
YCSB_COMMAND=${action}\n\
YCSB_BINDING=${ycsb_client}\n\
YCSB_WORKLOAD=/ycsb/workloads/workload${workload}\n\
YCSB_RECORDCOUNT=${recordcount}\n\
YCSB_OPERATIONCOUNT=${operationcount}\n\
YCSB_THREADS=${ycsb_threads}\n\
YCSB_OPTS=-s -p workload=${workload_type} ${debug} -p workload=${workload_type} -p measurementtype=hdrhistogram -p hdrhistogram.fileoutput=false -p hdrhistogram.percentiles=$(seq -s, 1 100) ${extra_opts_str}" > ${output_file%.dat}.docker
    
    start_container ${ycsb_image} ${container_name} "Starting test" ${output_file} ${docker_args}

    if [ $? -eq 0 ]; then
        log "YCSB $action launched successfully."
    else
        log "Error launching YCSB $action."
        exit 1
    fi
}

run_benchmark() {    
    if [ $# -lt 10 ]; then
	echo "Usage: $0 <protocol> <number_of_threads> <node_count> <workload_type> <workload> <record_count> <operation_count> <output_file> <do_create_and_load> <do_clean_up>"
	echo "Example: $0 ONE 10 3 site.ycsb.workloads.CoreWorkload a 1 1 1 1"
	exit 1
    fi

    init_logdir

    log "run_benchmark using args: $@"
    
    protocol=$1
    nthreads=$2
    node_count=$3
    workload_type=$4
    workload=$5
    record_count=$6
    operation_count=$7
    output_file=$8
    do_create_and_load=$9
    do_clean_up=${10}

    if [ $# -gt 10 ]; then
	EXTRA_YCSB_OPTS=( "${@:11}" )
    else
	EXTRA_YCSB_OPTS=()
    fi

    pref=cassandra
    if printf '%s\n' "$protocol" | grep -wF -q -- "swiftpaxos"; then	
	pref=swiftpaxos
    elif printf '%s\n' "$protocol" | grep -wF -q -- "cockroachdb"; then
	pref=cockroachdb
    fi   

    log "Running ${workload_type} ${workload^^} for ${node_count} node(s)..."

    # Create cluster and load YCSB (if needed)    
    if [ $do_create_and_load == "1" ];
    then	
	log "Starting ${protocol} deployment with ${node_count} node(s)..."
	start_network
	${pref}_start_cluster "${node_count}" "$protocol"

	node_count=$(${pref}_get_node_count)
	hosts=$(${pref}_get_hosts "${node_count}")
	port=$(${pref}_get_port)

	debug "node_count:${node_count}"
	debug "hosts:${hosts}"
	debug "port:${port}"

	if [ -z "$hosts" ]; then
		echo "Failed to retrieve the IP addresses."
		exit 1
	fi

	nearby_database=$(config "node_name")1
	run_ycsb "load" "$workload_type" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "${output_file%.dat}.load" "$nthreads" "ycsb" "${nearby_database}" "${EXTRA_YCSB_OPTS[@]}"
	wait_container "ycsb"

	log "Emulating latency for ${node_count} node(s)..."
	emulate_latency "${node_count}"    
    fi

    node_count=$(${pref}_get_node_count)
    hosts=$(${pref}_get_hosts "${node_count}")
    port=$(${pref}_get_port)

    if [ -z "$hosts" ]; then
        echo "Failed to retrieve the IP addresses."
        exit 1
    fi

    for i in $(seq 1 1 ${node_count});
    do
	nearby_database=$(config "node_name")$i
	location=$(get_location $i ${DIR}/latencies.csv)
	log ${location}

	# FIXME move this elsewhere
	EXTRA_YCSB_OPTS2=("${EXTRA_YCSB_OPTS[@]}")
	if [ "${workload_type}" == "site.ycsb.workloads.ConflictWorkload" ]; 
	then
	    EXTRA_YCSB_OPTS2+=("-p")
	    EXTRA_YCSB_OPTS2+=("conflict.shift=$(( (record_count / node_count) * (i - 1) ))")
	fi

	run_ycsb "run" "$workload_type" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "${output_file%.dat}_${location}.dat" "$nthreads" "ycsb-${i}" "${nearby_database}" "${EXTRA_YCSB_OPTS2[@]}"
    done
    
    for i in $(seq 1 1 ${node_count});
    do
	wait_container "ycsb-${i}"
    done

    if [ $do_clean_up == "1" ];
    then
        ${pref}_cleanup_cluster ${node_count}
	stop_network
    fi
}

# Example usage
# run_benchmark paxos 1 3 site.ycsb.workloads.CoreWorkload a 1000 1000 /tmp/log 1 0
# run_benchmark swiftpaxos-paxos 1 3 site.ycsb.workloads.CoreWorkload a 1000 1000 /tmp/log 1 1
# run_benchmark swiftpaxos-paxos 12 3 site.ycsb.workloads.CoreWorkload a 1000 100 /tmp/log 1 1
