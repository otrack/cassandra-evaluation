#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/cassandra/cluster.sh
source ${DIR}/cassandra/ycsb.sh
source ${DIR}/swiftpaxos/cluster.sh

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
    if [ $# -lt 10 ]; then
	echo "Usage: $0 <action> <workload_type> <workload> <hosts> <port> <recordcount> <operation_count> <protocol> <output_file> <threads>"
	echo "Example: $0 load site.ycsb.CoreWorkload a 127.0.0.1,127.0.0.2 8080 1 1 QUORUM results.txt 100"
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
    
    # capture any extra arguments (11th onward) and prepare a safely quoted string
    shift 10
    local extra_opts=( "$@" )
    local extra_opts_str=""
    if [ ${#extra_opts[@]} -gt 0 ]; then
      for o in "${extra_opts[@]}"; do
        # printf %q produces a shell-escaped representation; safe to append to the command string
        extra_opts_str+=" $(printf '%q' "$o")"
      done
    fi
    
    local ycsb_dir=$(config ycsb_dir)
    
    local hdr_file=output_file.hdr

    if [ "$action" == "load" ];
    then

	if printf '%s\n' "$protocol" | grep -wF -q -- "swiftpaxos";
	then
	    # nothing to do
	    true
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


    local ycsb_client="swiftpaxos"
    if printf '%s\n' "$protocol" | grep -wF -q -- "swiftpaxos";
    then
	# nothing to do
	extra_opts_str+="-p maddr=${hosts} -p mport=${port} \
	-p verbose=false"
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
	extra_opts_str+="-p hosts=$hosts -p port=$port \
	-p cassandra.writeconsistencylevel=$consistency_level \
	-p cassandra.readconsistencylevel=$consistency_level"
    fi


    # debug="JAVA_OPTS=\"-Dorg.slf4j.simpleLogger.defaultLogLevel=debug\"" # comment out to have debug on
    cmd="${debug} $ycsb_dir/bin/ycsb.sh $action $ycsb_client \
    -p workload=$workload_type \
    -P ${ycsb_dir}/workloads/workload${workload} \
    -p recordcount=$recordcount \
    -p operationcount=$operationcount \
    -p measurementtype=hdrhistogram \
    -p hdrhistogram.fileoutput=false \
    -p hdrhistogram.output.path=${DIR}/${hdr_file} \
    -p hdrhistogram.percentiles=$(seq -s, 1 100) \
    ${extra_opts_str} \
    -threads $nthreads -s"

    eval "$cmd" | tee "$output_file"
    if [ $? -eq 0 ]; then
        log "YCSB $action completed successfully."
    else
        log "Error running YCSB $action."
        exit 1
    fi
}

run_benchmark() {    
    if [ $# -lt 10 ]; then
	echo "Usage: $0 <protocol> <number_of_threads> <node_count> <workload_type> <workload> <record_count> <operation_count> <output_file> <do_create_and_load> <do_clean_up>"
	echo "Example: $0 ONE 10 3 site.ycsb.workloads.CoreWorkload a 1 1 1 1"
	exit 1
    fi

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

	run_ycsb "load" "$workload_type" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "${output_file}".load "$nthreads" "${EXTRA_YCSB_OPTS[@]}"

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

    run_ycsb "run" "$workload_type" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "${output_file}" "$nthreads" "${EXTRA_YCSB_OPTS[@]}"

    if [ $do_clean_up == "1" ];
    then
        ${pref}_cleanup_cluster ${node_count}
	stop_network
    fi
}

# Example usage
# run_benchmark paxos 1 3 site.ycsb.workloads.CoreWorkload a 1000 1000 /tmp/log 1 1
run_benchmark swiftpaxos-paxos 1 3 site.ycsb.workloads.CoreWorkload a 1000 1000 /tmp/log 0 0

