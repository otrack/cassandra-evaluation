#!/usr/bin/env bash

# Helper scripts
DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/swiftpaxos/cluster.sh
source ${DIR}/cassandra/cluster.sh
source ${DIR}/cassandra/ycsb.sh
source ${DIR}/cassandra/profiler.sh
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

# Warm-up of the YCSB run phase, in seconds: WARMUP_EXECUTION_TIME if set
# (the --test mode of the experiment scripts sets it to 0), else exp.config.
warmup_execution_time() {
    local value="${WARMUP_EXECUTION_TIME:-$(config warmupexecutiontime)}"
    echo "${value:-0}"
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
        ycsb_threads=100 # FIXME CRDB parallel load is failing
    fi
    
    shift 13
    local extra_opts=( "$@" )

    if [ "$action" == "load" ]; then
        local filtered_opts=()
        local i=0
        while [ $i -lt ${#extra_opts[@]} ]; do
            if [ "${extra_opts[$i]}" == "-p" ] && [ $((i+1)) -lt ${#extra_opts[@]} ] && \
               { [[ "${extra_opts[$((i+1))]}" == maxexecutiontime=* ]] || [[ "${extra_opts[$((i+1))]}" == db.tracing=* ]] || [[ "${extra_opts[$((i+1))]}" == tiga.openloop.* ]]; }; then
                i=$((i+2))
            else
                filtered_opts+=("${extra_opts[$i]}")
                i=$((i+1))
            fi
        done
        extra_opts=("${filtered_opts[@]}")
    fi

    # Warm up every run phase, unless the caller sets its own warm-up.
    if [ "$action" == "run" ] && ! printf '%s\n' "${extra_opts[@]}" | grep -q '^warmupexecutiontime='; then
        extra_opts+=("-p" "warmupexecutiontime=$(warmup_execution_time)")
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
    if printf '%s\n' "$norm_protocol" | grep -wF -q -e "tiga" -e "calvin" -e "detock"; then
        # Bind-mounts, on the other hand, resolve on the daemon's filesystem.
        local staged_config
        staged_config=$(infra_stage_file "$(node_index_of "${nearby_database}")" \
                                         "${DIR}/tiga/config-ycsb.yml" /tmp/config-ycsb.yml)
        docker_args+=" -v ${staged_config}:/ycsb/config-ycsb.yml"
    fi
    
    if [ "$action" == "load" ];
    then
	if printf '%s\n' "$norm_protocol" | grep -wF -q -e "swiftpaxos" -e "tiga" -e "calvin" -e "detock";
	then
	    true
	elif printf '%s\n' "$protocol" | grep -wF -q -- "cockroachdb";
	then
	    cockroachdb_create_usertable 10 "$replication_factor" "${num_dcs:-3}" "$workload_type" "$recordcount"
	else
	    local transaction_mode="bruh"
	    if [ "$protocol" == "accord" ]; then 
		transaction_mode="full"
	    fi
	    cassandra_create_keyspace 3600 "${num_dcs:-3}" "$replication_factor"
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
	elif printf '%s\n' "$protocol" | grep -wF -q -- "janus";
	then
	    # the closest replica coordinates the transactions
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
	# With several nodes per DC, spread the clients over the gateways of
	# their DC: one JDBC "shard" (';'-separated) per local node, each with
	# the same backup as above.
	local nodes_per_dc
	nodes_per_dc=$(config nodesperdc)
	if [ "${nodes_per_dc:-1}" -gt 1 ]; then
	    local backup_url="${jdbc_url#*,}"
	    [ "${backup_url}" == "${jdbc_url}" ] && backup_url=""
	    local dc_prefix="${nearby_database%1}"
	    local k gateway
	    for k in $(seq 2 "${nodes_per_dc}"); do
		gateway=$(get_container_ip "${dc_prefix}${k}")
		[ -z "${gateway}" ] && continue
		jdbc_url+=";jdbc:postgresql://${gateway}:${port}/defaultdb?cockroachdb=true&sslmode=disable"
		[ -n "${backup_url}" ] && jdbc_url+=",${backup_url}"
	    done
	fi
	extra_opts_str+=" -p db.driver=org.postgresql.Driver \
-p db.url=${jdbc_url} \
-p db.user=root \
-p db.passwd="
    elif printf '%s\n' "$norm_protocol" | grep -wF -q -e "tiga" -e "calvin" -e "detock"; then
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

    if ! printf '%s\n' "$norm_protocol" | grep -wF -q -e "tiga" -e "calvin" -e "detock"; then
        java_opts+=" -Ddatastax-java-driver.advanced.request.trace.attempts=100 -Ddatastax-java-driver.advanced.request.trace.interval=100ms"
    fi

    # By default the Java driver (4.x) completes a PREPARE only once every
    # node has prepared the statement too, so the first execution of each
    # statement waits for a round trip to the farthest replica.  The client
    # opens a fresh session for the measured phase, after warmup, and prepares
    # lazily, so that round trip lands in the first measured operations: on
    # AWS it lifted the first-interval maximum by ~225-325ms (Osasco's 709ms
    # against ~380ms server-side).  The client only routes to its local node,
    # which prepares the statement itself; any other node re-prepares on an
    # UNPREPARED response.
    if [ "${ycsb_client}" == "cassandra-cql" ]; then
        java_opts+=" -Ddatastax-java-driver.advanced.prepared-statements.prepare-on-all-nodes=false"
    fi

    echo -e "JAVA_OPTS=${java_opts}\n\
YCSB_COMMAND=${action}\n\
YCSB_BINDING=${ycsb_client}\n\
YCSB_WORKLOAD=/ycsb/workloads/workload${workload}\n\
YCSB_RECORDCOUNT=${recordcount}\n\
YCSB_OPERATIONCOUNT=${operationcount}\n\
YCSB_THREADS=${ycsb_threads}\n\
YCSB_OPTS=-s -p core_workload_insertion_retry_limit=10 -p fieldcount=1 -p fieldlength=${fieldlength} -p workload=${workload_type} -p workload=${workload_type} -p measurementtype=hdrhistogram -p hdrhistogram.fileoutput=false -p hdrhistogram.percentiles=$(seq -s, 1 100) ${extra_opts_str}" > ${output_file%.dat}.docker
    
    # Synchronised start (run_benchmark sets YCSB_START_AT for the run phase):
    # the container waits in _ycsb_start_at_script until then, so the launcher only
    # waits for that script's first line, not for YCSB itself.
    local wait_msg="Starting"
    local container_cmd=()
    if [ "$action" == "run" ] && [ -n "${YCSB_START_AT:-}" ]; then
        if _ycsb_sync_start "${ycsb_image}" "${container_name}"; then
            docker_args+="${YCSB_SYNC_ARGS}"
            container_cmd=( -- "${YCSB_SYNC_CMD[@]}" )
            echo "YCSB_START_AT=${YCSB_START_AT}" >> ${output_file%.dat}.docker
            wait_msg="Scheduled YCSB start"
        else
            error "Could not set up a synchronised start for ${container_name}: it starts as soon as it is launched"
        fi
    fi

    start_container ${ycsb_image} ${container_name} "${wait_msg}" ${output_file} ${docker_args} "${container_cmd[@]}"

    if [ $? -eq 0 ]; then
        log "YCSB $action launched successfully."
    else
        log "Error launching YCSB $action."
        exit 1
    fi
}

# The YCSB clients' entrypoint for a synchronised start: wait until
# YCSB_START_AT (epoch seconds), then exec the image's own entrypoint and
# command, passed as the arguments.  Every client is launched well before that
# time, so they all start within a few milliseconds of each other however long
# launching them one after another took (on AWS ~10s each through the remote
# Docker contexts); the hosts' clocks are NTP/chrony synchronised.  Its first
# line is the launcher's readiness message (see run_ycsb).  POSIX sh, as the
# image's shell is unknown.  Kept here rather than in a file of its own, so it
# cannot go missing from a checkout.
_ycsb_start_at_script() {
    cat <<'SCRIPT'
t=${YCSB_START_AT:-0}
now=$(date +%s)
echo "Scheduled YCSB start at ${t} (in $((t - now))s)"
if [ "${t}" -gt "${now}" ]; then
    if [ $((t - now)) -gt 1 ]; then
        sleep $((t - now - 1))
    fi
    while [ "$(date +%s)" -lt "${t}" ]; do
        sleep 0.01 2>/dev/null || sleep 1
    done
elif [ "${t}" -gt 0 ]; then
    echo "WARNING: launched $((now - t))s after the scheduled start; raise ycsb.start_delay"
fi
echo "YCSB starting at $(date -u +%H:%M:%S.%3N 2>/dev/null || date -u +%H:%M:%S) UTC"
exec "$@"
SCRIPT
}

# _ycsb_sync_start <image> <container_name>: the docker arguments (YCSB_SYNC_ARGS)
# and command (YCSB_SYNC_CMD) that run the image's own entrypoint and command
# through _ycsb_start_at_script.  The image's entrypoint and command are read
# from the daemon that will run the container (pulling the image there first
# if need be).  The script is passed inline (sh -c '<script>' ycsb-start-at
# <entrypoint> <cmd>...), so nothing has to exist on the daemon's machine.
_ycsb_sync_start() {
    local image=$1 cname=$2 idx spec
    idx=$(node_index_of "${cname}")
    local fmt='{{json .Config.Entrypoint}}{{"\n"}}{{json .Config.Cmd}}'
    if ! spec=$(d "${cname}" image inspect --format "${fmt}" "${image}" 2>/dev/null); then
        dpull "${idx}" "${image}" >/dev/null 2>&1
        spec=$(d "${cname}" image inspect --format "${fmt}" "${image}" 2>/dev/null) || return 1
    fi
    local original=() a
    while IFS= read -r a; do
        original+=("${a}")
    done < <(printf '%s\n' "${spec}" | python3 -c '
import json, sys
for line in sys.stdin:
    line = line.strip()
    if line and line != "null":
        for a in json.loads(line):
            print(a)')
    [ ${#original[@]} -gt 0 ] || return 1
    YCSB_SYNC_ARGS=" --entrypoint sh"
    YCSB_SYNC_CMD=( -c "$(_ycsb_start_at_script)" ycsb-start-at "${original[@]}" )
}

# The delay between launching the first YCSB client of a run phase and the
# moment all of them start (ycsb.start_delay, YCSB_START_DELAY overrides it;
# 0 = start each as soon as it is launched).  Unset: 90s on real machines,
# where launching one client through its remote Docker context took ~10s, and
# 15s in simulation.
ycsb_start_delay() {
    local v="${YCSB_START_DELAY:-$(config ycsb.start_delay)}"
    if [ -z "${v}" ]; then
        if infra_is_real; then v=90; else v=15; fi
    fi
    echo "${v}"
}

# benchmark_already_done <output_file_base_without_.dat> <num_dcs>
#
# True when every per-location file a run_benchmark call for
# <output_file_base>.dat would produce (see the run-phase loop below) already
# exists and reached YCSB's own completion marker, so a resumed sweep can
# skip re-running that point instead of overwriting a result that's already
# there. A run cut short mid-flight (e.g. by a reclaimed node) leaves a
# shorter file with no such marker, so it is correctly treated as not done.
benchmark_already_done() {
    local base=$1 num_dcs=$2 i location f
    for i in $(seq 1 "${num_dcs}"); do
        location=$(get_location "${i}" "${LOCATIONS_FILE}" 2>/dev/null) || return 1
        f="${base}_${location}.dat"
        [ -f "${f}" ] || return 1
        grep -q '^\[OVERALL\], Throughput(ops/sec),' "${f}" || return 1
    done
    return 0
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
	if [ "${nodes_per_dc:-1}" -gt 1 ] && [ "$protocol" != "swiftpaxos-janus" ]; then
	    error "swiftpaxos protocol does not support nodesperdc > 1 (current nodesperdc=${nodes_per_dc})."
	    exit 1
	fi
    elif printf '%s\n' "$protocol" | grep -wF -q -- "cockroachdb"; then
	pref=cockroachdb
    elif printf '%s\n' "$protocol" | grep -wF -q -e "tiga" -e "calvin" -e "detock"; then
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
	if [ "${num_dcs}" -ne "${dc_count}" ]; then
		error "${protocol}: only ${num_dcs}/${dc_count} DC(s) came up; cluster formation failed partway through"
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
    if [ "${num_dcs}" -ne "${dc_count}" ]; then
        # Cluster formation stalled partway through (e.g. a replica's
        # registration wasn't observed in time) and silently left a
        # smaller deployment running. Measuring that would produce a
        # result file that looks complete but isn't comparable to the
        # other DCs' sweep points; fail loudly instead so run_resilient.sh
        # repairs the deployment and retries this point.
        error "${protocol}: only ${num_dcs}/${dc_count} DC(s) came up; cluster formation failed partway through"
        exit 1
    fi

    # Bracket the run phase only: the capture should cover what YCSB measures.
    local profile=0
    local profile_tag profile_dir
    if [ "${pref}" == "cassandra" ] && cassandra_profiler_enabled; then
        profile=1
        profile_tag=$(basename "${output_file%.dat}")
        # Outside the experiment's own log directory, which the parsers glob.
        profile_dir="${LOGDIR}/profiles/$(basename "$(dirname "${output_file}")")"
        cassandra_profiler_start "${num_dcs}" "${nodes_per_dc}" "${profile_tag}"
    fi

    # Launched concurrently, not one DC after another: each run_ycsb call
    # only blocks until its own container reports "Starting" (early in its
    # life, long before any measurement happens), so a sequential loop here
    # staggers every DC's actual measured window by however long the launches
    # ahead of it took -- observed up to ~36s across 5 regions against a 60s
    # maxexecutiontime, meaning the "all regions concurrently loaded" state
    # a multi-region run is supposed to capture only existed for a fraction
    # of most DCs' own window. run_ycsb/start_container only touch per-DC
    # resources (container name, output/.docker/log files), so nothing here
    # is shared across the parallel calls.
    #
    # Concurrent launching alone does not make the containers' own startup
    # (image pull, JVM warm-up) line up, so each client also waits in
    # _ycsb_start_at_script until a shared future moment (YCSB_START_AT)
    # before beginning the run: launching them one after another on AWS used
    # to spread their starts over ~40s, more than the 30s warmup, so each
    # client's cold start landed in the others' measured windows.
    local -a run_pids=()
    local start_delay
    start_delay=$(ycsb_start_delay)
    YCSB_START_AT=""
    if [ "${start_delay:-0}" -gt 0 ] 2>/dev/null && [ "${num_dcs}" -gt 1 ]; then
        YCSB_START_AT=$(( $(date +%s) + start_delay ))
        log "YCSB clients start together at $(date -u -d "@${YCSB_START_AT}" +%H:%M:%S 2>/dev/null || date -u -r "${YCSB_START_AT}" +%H:%M:%S) UTC (in ${start_delay}s)"
    else
        start_delay=0
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

        run_ycsb "run" "$workload_type" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "$replication_factor" "${output_file%.dat}_${location}.dat" "$nthreads" "ycsb-${i}" "${nearby_database}" "${EXTRA_YCSB_OPTS2[@]}" &
        run_pids+=($!)
    done
    wait "${run_pids[@]}"

    # A run is bounded by its warm-up plus maxexecutiontime, so a client still alive well past
    # that is stuck, not slow.  Cap the wait so one wedged client cannot hang
    # the whole sweep; the others keep their results and the parser drops the
    # missing one.  The load phase above is deliberately left unbounded: it is
    # single-threaded over recordcount rows and legitimately takes far longer.
    if [ -n "${YCSB_START_AT}" ] && [ "$(date +%s)" -ge "${YCSB_START_AT}" ]; then
        error "Launching the YCSB clients took longer than ycsb.start_delay (${start_delay}s): the last ones started late; raise it"
    fi
    YCSB_START_AT=""

    local client_timeout
    client_timeout=$(config ycsb.client.timeout)
    client_timeout=$(( ${client_timeout:-300} + start_delay ))

    local stuck=0
    for i in $(seq 1 1 ${num_dcs});
    do
        wait_container "ycsb-${i}" "${client_timeout}" || stuck=$((stuck + 1))
    done
    if [ ${stuck} -gt 0 ]; then
        error "${stuck} YCSB client(s) had to be stopped after ${client_timeout}s"
    fi

    if [ ${profile} -eq 1 ]; then
        cassandra_profiler_stop "${num_dcs}" "${nodes_per_dc}" "${profile_tag}" "${profile_dir}"
    fi

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
    elif printf '%s\n' "$protocol" | grep -wF -q -e "tiga" -e "calvin" -e "detock"; then
	pref=tiga
    fi
    
    ${pref}_cleanup_cluster ${dc_count}
    stop_network
}
