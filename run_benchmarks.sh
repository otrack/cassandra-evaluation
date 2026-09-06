#!/usr/bin/env bash

# Helper scripts
DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/swiftpaxos/cluster.sh
source ${DIR}/cassandra/cluster.sh
source ${DIR}/cassandra/ycsb.sh
source ${DIR}/cockroachdb/cluster.sh
source ${DIR}/cockroachdb/ycsb.sh
source ${DIR}/tiga/cluster.sh

start_network() {
    local network_name=$(config "network_name")

    # A real provider gives every node its own machine and its own network
    # namespace, so there is no shared bridge to create -- but shaping left on
    # a machine by an interrupted run would silently distort this one.
    if infra_is_real; then
        infra_reset_network
        return 0
    fi

    # Check if network already exists
    if docker network inspect "${network_name}" >/dev/null 2>&1; then
        log "Docker network '${network_name}' already exists."
        return 0
    fi

    log "Creating Docker network '${network_name}'..."
    if docker network create --driver bridge "${network_name}"; then
        log "Docker network '${network_name}' created successfully."
        return 0
    else
        error "Failed to create Docker network '${network_name}'."
        return 1
    fi
}

stop_network() {
    local network_name=$(config "network_name")

    if infra_is_real; then
        return 0
    fi

    # Check if network exists
    if ! docker network inspect "${network_name}" >/dev/null 2>&1; then
        log "Docker network '${network_name}' does not exist, nothing to stop."
        return 0
    fi

    log "Removing Docker network '${network_name}'..."
    if docker network rm "${network_name}"; then
        log "Docker network '${network_name}' removed successfully."
        return 0
    else
        error "Failed to remove network: ${network_name}"
        return 2
    fi
}

emulate_latency() {
    local num_dcs=$1
    local nodes_per_dc=${2:-$(config nodesperdc)}
    python3 emulate_latency.py "$num_dcs" "$nodes_per_dc"
    if [ $? -ne 0 ]; then
        error "Failed to add latency emulation."
        exit 1
    fi
}

run_ycsb() {
    if [ $# -lt 13 ]; then
	echo "Usage: $0 <action> <workload_type> <workload> <hosts> <port> <recordcount> <operation_count> <protocol> <replication_factor> <output_file> <threads> <container_name> <network_adapter> [extra_ycsb_options]"
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
    local replication_factor=$9
    local output_file=${10}
    local threads=${11}
    local container_name=${12}
    local nearby_database=${13}
    local ycsb_threads=${threads}

    local norm_protocol="$protocol"
    if [[ "$norm_protocol" == tiga-* ]]; then
        norm_protocol="${norm_protocol#tiga-}"
    fi

    if [ "$action" == "load" ]; then
        ycsb_threads=1 # FIXME CRDB parallel load is failing
    fi
    
    shift 13
    local extra_opts=( "$@" )

    if [ "$action" == "load" ]; then
        local filtered_opts=()
        local i=0
        while [ $i -lt ${#extra_opts[@]} ]; do
            if [ "${extra_opts[$i]}" == "-p" ] && [ $((i+1)) -lt ${#extra_opts[@]} ] && \
               { [[ "${extra_opts[$((i+1))]}" == maxexecutiontime=* ]] || [[ "${extra_opts[$((i+1))]}" == db.tracing=* ]]; }; then
                i=$((i+2))
            else
                filtered_opts+=("${extra_opts[$i]}")
                i=$((i+1))
            fi
        done
        extra_opts=("${filtered_opts[@]}")
    fi

    if ! printf '%s\n' "$protocol" | grep -wF -q -e "cockroachdb" -e "accord"; then
        local filtered_opts=()
        local i=0
        while [ $i -lt ${#extra_opts[@]} ]; do
            if [ "${extra_opts[$i]}" == "-p" ] && [ $((i+1)) -lt ${#extra_opts[@]} ] && [[ "${extra_opts[$((i+1))]}" == db.tracing=* ]]; then
                i=$((i+2))
            else
                filtered_opts+=("${extra_opts[$i]}")
                i=$((i+1))
            fi
        done
        extra_opts=("${filtered_opts[@]}")
    fi

    local extra_opts_str=""
    if [ ${#extra_opts[@]} -gt 0 ]; then
      for o in "${extra_opts[@]}"; do
        extra_opts_str+=" $(printf '%q' "$o")"
      done
    fi
    log ${extra_opts_str[@]}

    # Cap the load generator.
    #
    # The replicas are sized from `machine`, but the YCSB containers used to run
    # unbounded, so the client JVM saw every core of the host.  That is not just
    # untidy: the Cassandra binding sizes its connection pools from
    # Runtime.availableProcessors(), for local *and* remote datacentres alike, so
    # on a 96-core machine each client opened ~96 channels per node -- a few
    # hundred of them across the emulated WAN -- and their handshakes timed out
    # against the driver's 500ms init-query budget.  The same run on a 12-core
    # box was healthy.  Capping the container keeps the client identical
    # whatever the host is.
    local ycsb_cpus=$(config ycsb_cpus)
    local cpu_limit=""
    if [ -n "${ycsb_cpus}" ]; then
        cpu_limit=" --cpus ${ycsb_cpus}"
    fi

    # --env-file is read by the Docker client, so it needs no staging even when
    # the daemon lives on another machine.
    local docker_args="--rm -d --security-opt apparmor=unconfined${cpu_limit} --network container:${nearby_database} --env-file=${output_file%.dat}.docker"
    if printf '%s\n' "$norm_protocol" | grep -wF -q -e "tiga" -e "calvin" -e "detock" -e "janus"; then
        # Bind-mounts, on the other hand, resolve on the daemon's filesystem.
        local staged_config
        staged_config=$(infra_stage_file "$(node_index_of "${nearby_database}")" \
                                         "${DIR}/tiga/config-ycsb.yml" /tmp/config-ycsb.yml)
        docker_args+=" -v ${staged_config}:/ycsb/config-ycsb.yml"
    fi
    
    if [ "$action" == "load" ];
    then
	if printf '%s\n' "$norm_protocol" | grep -wF -q -e "swiftpaxos" -e "tiga" -e "calvin" -e "detock" -e "janus";
	then
	    true
	elif printf '%s\n' "$protocol" | grep -wF -q -- "cockroachdb";
	then
	    cockroachdb_create_usertable 10 "$replication_factor" "${num_dcs:-3}" "$workload_type"
	else
	    local transaction_mode="bruh"
	    if [ "$protocol" == "accord" ]; then 
		transaction_mode="full"
	    fi
	    local nodes_per_dc=$(config nodesperdc)
	    cassandra_create_keyspace 3600 "${num_dcs:-3}" "$replication_factor" "${nodes_per_dc}"
	    cassandra_create_usertable 3600 "$transaction_mode" "${num_dcs:-3}" "$workload_type"
	fi
    fi

    # Bytes per record.  The swap workload moves S of these in each direction,
    # so S varies the coordination cost and the data volume together; changing
    # this separates them.  Note it also changes the dataset size, and with it
    # the memory pressure on the replicas.
    local fieldlength=$(config fieldlength)
    fieldlength=${fieldlength:-4000}

    local ycsb_image=$(config ycsb_image)
    local ycsb_client="swiftpaxos"

    if printf '%s\n' "$protocol" | grep -wF -q -- "swiftpaxos";
    then       
	local leaderless="false"
	local fast="false"
	if printf '%s\n' "$protocol" | grep -wF -q -- "epaxos";
	then
	    leaderless="true"
	    fast="false"
	elif printf '%s\n' "$protocol" | grep -wF -q -- "curp";
	then
	    leaderless="true"
	    fast="false"
	fi	
	extra_opts_str+=" -p maddr=${hosts} \
-p mport=${port} \
-p verbose=false \
-p leaderless=${leaderless} \
-p fast=${fast}"
    elif printf '%s\n' "$protocol" | grep -wF -q -- "cockroachdb";
    then
	ycsb_client="jdbc"
	local primary_host
	primary_host=$(get_container_ip ${nearby_database})
	local jdbc_url="jdbc:postgresql://${primary_host}:${port}/defaultdb?cockroachdb=true&sslmode=disable"
	IFS=',' read -ra host_array <<< "$hosts"
	for h in "${host_array[@]}"; do
	    if [ "$h" != "$primary_host" ]; then
		jdbc_url+=",jdbc:postgresql://${h}:${port}/defaultdb?cockroachdb=true&sslmode=disable"
		break
	    fi
	done
	extra_opts_str+=" -p db.driver=org.postgresql.Driver \
-p db.url=${jdbc_url} \
-p db.user=root \
-p db.passwd="
    elif printf '%s\n' "$norm_protocol" | grep -wF -q -e "tiga" -e "calvin" -e "detock" -e "janus"; then
	ycsb_client="tiga"
	extra_opts_str+=" -p tiga.config=/ycsb/config-ycsb.yml -p tiga.mode=${norm_protocol}"
    else
	ycsb_client="cassandra-cql"
	local consistency_level="ONE"
	if printf '%s\n' "$protocol" | grep -wF -q -- "cassandra";
	then
	    protocol=$(echo "$protocol" | awk -F- '{print $2}')
	fi
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

    local java_opts="-Dorg.slf4j.simpleLogger.defaultLogLevel=info"

    # --cpus alone should be enough (the JVM derives availableProcessors() from
    # the cgroup quota), but state it outright so that the pool sizing cannot
    # depend on container-detection quirks.
    if [ -n "${ycsb_cpus}" ]; then
        java_opts+=" -XX:ActiveProcessorCount=${ycsb_cpus}"
    fi

    # Raise one logger to debug without rebuilding the client.  Set
    # ycsb_debug_logger in exp.config (it survives across runs and is visible
    # where the other knobs are), or override it for a single run with
    #   YCSB_DEBUG_LOGGER=site.ycsb.db.CassandraCQLClient ./swap.sh --protocols=accord
    #
    # The YCSB clients bind slf4j-simple, which reads a per-logger level from
    # org.slf4j.simpleLogger.log.<logger>.  Failures the client swallows behind
    # `if (logger.isDebugEnabled())` -- the exception from a swap, in
    # particular -- then appear in the run log with their stack trace.
    local debug_logger="${YCSB_DEBUG_LOGGER:-$(config ycsb_debug_logger)}"
    if [ -n "${debug_logger}" ]; then
        java_opts+=" -Dorg.slf4j.simpleLogger.log.${debug_logger}=debug"
        log "YCSB debug logging enabled for ${debug_logger}"
    fi

    if ! printf '%s\n' "$norm_protocol" | grep -wF -q -e "tiga" -e "calvin" -e "detock" -e "janus"; then
        java_opts+=" -Ddatastax-java-driver.advanced.request.trace.attempts=100 -Ddatastax-java-driver.advanced.request.trace.interval=100ms"
    fi

    echo -e "JAVA_OPTS=${java_opts}\n\
YCSB_COMMAND=${action}\n\
YCSB_BINDING=${ycsb_client}\n\
YCSB_WORKLOAD=/ycsb/workloads/workload${workload}\n\
YCSB_RECORDCOUNT=${recordcount}\n\
YCSB_OPERATIONCOUNT=${operationcount}\n\
YCSB_THREADS=${ycsb_threads}\n\
YCSB_OPTS=-s -p core_workload_insertion_retry_limit=10 -p fieldcount=1 -p fieldlength=${fieldlength} -p workload=${workload_type} -p workload=${workload_type} -p measurementtype=hdrhistogram -p hdrhistogram.fileoutput=false -p hdrhistogram.percentiles=$(seq -s, 1 100) ${extra_opts_str}" > ${output_file%.dat}.docker
    
    start_container ${ycsb_image} ${container_name} "Starting" ${output_file} ${docker_args}

    if [ $? -eq 0 ]; then
        log "YCSB $action launched successfully."
    else
        log "Error launching YCSB $action."
        exit 1
    fi
}

run_benchmark() {    
    if [ $# -lt 11 ]; then
	echo "Usage: $0 <protocol> <number_of_threads> <num_dcs> <replication_factor> <workload_type> <workload> <record_count> <operation_count> <output_file> <do_create_and_load> <do_clean_up> [EXTRA_YCSB_OPTS...]"
	exit 1
    fi

    init_logdir
    log "run_benchmark using args: $@"
    
    protocol=$1
    nthreads=$2
    dc_count=$3
    replication_factor=$4
    workload_type=$5
    workload=$6
    record_count=$7
    operation_count=$8
    output_file=$9
    do_create_and_load=${10}
    do_clean_up=${11}
    nodes_per_dc=$(config nodesperdc)

    if [ $# -gt 11 ]; then
	EXTRA_YCSB_OPTS=( "${@:12}" )
    else
	EXTRA_YCSB_OPTS=()
    fi

    pref=cassandra
    if printf '%s\n' "$protocol" | grep -wF -q -- "swiftpaxos"; then	
	pref=swiftpaxos
	if [ "${nodes_per_dc:-1}" -gt 1 ]; then
	    error "swiftpaxos protocol does not support nodesperdc > 1 (current nodesperdc=${nodes_per_dc})."
	    exit 1
	fi
    elif printf '%s\n' "$protocol" | grep -wF -q -- "cockroachdb"; then
	pref=cockroachdb
    elif printf '%s\n' "$protocol" | grep -wF -q -e "tiga" -e "calvin" -e "detock" -e "janus"; then
	pref=tiga
    fi   

    log "Running ${workload_type} ${workload^^} for ${dc_count} DC(s) with ${nodes_per_dc} node(s)/DC..."

    if [ $do_create_and_load == "1" ];
    then	
	log "Starting ${protocol} deployment with ${dc_count} DC(s)..."
	start_network
	${pref}_start_cluster "${dc_count}" "$protocol" "${nodes_per_dc}"

	num_dcs=$(${pref}_get_num_dcs)
	hosts=$(${pref}_get_hosts "${num_dcs}" "${nodes_per_dc}")
	port=$(${pref}_get_port)

	if [ -z "$hosts" ]; then
		echo "Failed to retrieve IP addresses."
		exit 1
	fi

	first_dc=$(get_location 1 ${LOCATIONS_FILE})
	nearby_database="${first_dc}1"
	run_ycsb "load" "$workload_type" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "$replication_factor" "${output_file%.dat}.load" "$nthreads" "ycsb" "${nearby_database}" "${EXTRA_YCSB_OPTS[@]}"
	wait_container "ycsb"

	log "Emulating latency for ${num_dcs} DC(s) with ${nodes_per_dc} node(s)/DC..."
	emulate_latency "${num_dcs}" "${nodes_per_dc}"
    fi

    num_dcs=$(${pref}_get_num_dcs)
    hosts=$(${pref}_get_hosts "${num_dcs}" "${nodes_per_dc}")
    port=$(${pref}_get_port)

    if [ -z "$hosts" ]; then
        echo "Failed to retrieve IP addresses."
        exit 1
    fi

    for i in $(seq 1 1 ${num_dcs});
    do
        location=$(get_location $i ${LOCATIONS_FILE})
        nearby_database="${location}1"
        log ${location}

        EXTRA_YCSB_OPTS2=("${EXTRA_YCSB_OPTS[@]}")
        if [ "${workload_type}" == "site.ycsb.workloads.ConflictWorkload" ]; 
        then
            EXTRA_YCSB_OPTS2+=("-p")
            EXTRA_YCSB_OPTS2+=("conflict.shift=$(( (record_count / num_dcs) * (i - 1) ))")
        fi

        run_ycsb "run" "$workload_type" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "$replication_factor" "${output_file%.dat}_${location}.dat" "$nthreads" "ycsb-${i}" "${nearby_database}" "${EXTRA_YCSB_OPTS2[@]}"
    done
    
    for i in $(seq 1 1 ${num_dcs});
    do
        wait_container "ycsb-${i}"
    done

    local fast_path_script="${DIR}/${pref}/${pref}_fast_path.sh"

    local fast_ratio_sum=0
    local medium_ratio_sum=0
    local slow_ratio_sum=0
    local ephemeral_ratio_sum=0
    local ratio_count=0

    for i in $(seq 1 1 ${num_dcs}); do
        location=$(get_location $i ${LOCATIONS_FILE})
        container_name="${location}1"
        fp_output=$("${fast_path_script}" "${container_name}" 2>/dev/null) || true

        fp_fast=$(echo "$fp_output" | grep "^Fast ratio:" | awk '{print $3}')
        fp_medium=$(echo "$fp_output" | grep "^Medium ratio:" | awk '{print $3}')
        fp_slow=$(echo "$fp_output" | grep "^Slow ratio:" | awk '{print $3}')
        fp_ephemeral=$(echo "$fp_output" | grep "^Ephemeral ratio:" | awk '{print $3}')

        fp_fast=${fp_fast:-0}
        fp_medium=${fp_medium:-0}
        fp_slow=${fp_slow:-0}
        fp_ephemeral=${fp_ephemeral:-0}

        fast_ratio_sum=$(awk "BEGIN {printf \"%.4f\", ${fast_ratio_sum} + ${fp_fast}}")
        medium_ratio_sum=$(awk "BEGIN {printf \"%.4f\", ${medium_ratio_sum} + ${fp_medium}}")
        slow_ratio_sum=$(awk "BEGIN {printf \"%.4f\", ${slow_ratio_sum} + ${fp_slow}}")
        ephemeral_ratio_sum=$(awk "BEGIN {printf \"%.4f\", ${ephemeral_ratio_sum} + ${fp_ephemeral}}")
        ratio_count=$((ratio_count + 1))
    done

    if [ $ratio_count -gt 0 ]; then
        {
            echo "Fast ratio: $(awk "BEGIN {printf \"%.4f\", ${fast_ratio_sum} / ${ratio_count}}")"
            echo "Medium ratio: $(awk "BEGIN {printf \"%.4f\", ${medium_ratio_sum} / ${ratio_count}}")"
            echo "Slow ratio: $(awk "BEGIN {printf \"%.4f\", ${slow_ratio_sum} / ${ratio_count}}")"
            echo "Ephemeral ratio: $(awk "BEGIN {printf \"%.4f\", ${ephemeral_ratio_sum} / ${ratio_count}}")"
        } > "${output_file%.dat}_fast_path_ratio.dat"
    fi

    if [ $do_clean_up == "1" ];
    then
        ${pref}_cleanup_cluster ${num_dcs}
        stop_network
    fi
}

stop_benchmark() {
    if [ $# -lt 2 ]; then
	echo "Usage: $0 <protocol> <dc_count>"
	exit 1
    fi

    protocol=$1
    dc_count=$2

    pref=cassandra
    if printf '%s\n' "$protocol" | grep -wF -q -- "swiftpaxos"; then	
	pref=swiftpaxos
    elif printf '%s\n' "$protocol" | grep -wF -q -- "cockroachdb"; then
	pref=cockroachdb
    elif printf '%s\n' "$protocol" | grep -wF -q -e "tiga" -e "calvin" -e "detock" -e "janus"; then
	pref=tiga
    fi
    
    ${pref}_cleanup_cluster ${dc_count}
    stop_network
}
