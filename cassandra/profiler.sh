#!/usr/bin/env bash

# async-profiler captures on the Cassandra replicas.
#
# The replicas are started with async-profiler loaded (see
# start_cassandra_data_centers.py), and with accord.debug_execution so that
# DebugExecution brackets each Accord task and executor critical section with
# a one.profiler.Span.  In async-profiler 4.5, Span.start() returns 0 unless a
# recording is running, and the resulting profiler.Span events exist only in
# JFR, so the capture here is always JFR and is bounded to the YCSB *run*
# phase: the load phase and the cluster bootstrap would otherwise dominate the
# recording.
#
# DebugExecution uses Span.endIfProfiled(), which keeps a span only if the
# profiler took a sample of the ending thread while the span was open.  The
# recorded spans are therefore a sample, biased towards long ones -- right for
# finding examples of outliers, wrong for counting them (the DebugExecution
# warnings in the node logs do that).  Which spans survive depends on the
# sampling engines; see cassandra_profiler_options.
#
# The session is driven through `nodetool profile execute` rather than
# `nodetool profile start`: start only accepts one event from a fixed list,
# no interval/threshold options, and refuses to run when
# kernel.perf_event_paranoid > 1 -- which is the default on most Docker
# hosts, and not something a container can change.  execute needs
# cassandra.async_profiler.unsafe_mode=true, which the node is started with.
#
# Captures are copied off the nodes before run_benchmark returns: the
# containers are started with auto_remove, so anything left inside them is
# gone at cleanup.

CASSANDRA_PROFILER_DIR_IN_CONTAINER=/tmp/async-profiler

# 0/1; CASSANDRA_PROFILER=1 in the environment overrides exp.config for one run.
cassandra_profiler_enabled() {
    local v="${CASSANDRA_PROFILER:-$(config cassandra.profiler)}"
    [ "${v}" == "1" ] || [ "${v}" == "true" ]
}

# async-profiler options for the session, without the start/file/format parts.
#
# - ctimer samples CPU with per-thread CPU-time timers.  It needs no
#   privileges, whereas cpu (perf_events) needs kernel.perf_event_paranoid
#   <= 1 on the host, and a container cannot lower that.  CAP_PERFMON does not
#   help: the image's entrypoint (cassandra-docker-library) runs `exec gosu
#   cassandra`, which clears the capability before the JVM starts.  What
#   ctimer loses is the kernel frames.
# - wall is what keeps off-CPU spans: ctimer never samples a thread that is
#   parked or blocked, so without wall a span spent waiting is never recorded.
#   Measured against the bundled 4.5 with 300 idle threads: ctimer alone kept
#   0% of parked spans; adding wall=10ms kept ~90% of 200ms waits and ~40% of
#   50ms waits.  Adding nobatch kept all of them, but its wall samples are then
#   recorded as jdk.ExecutionSample, mixed in with the CPU samples, and the
#   file grew ~20x.
# - The span timestamps come from JFR's own clock (the TSC); they need no
#   privileges either.
cassandra_profiler_options() {
    local opts="${CASSANDRA_PROFILER_OPTIONS:-$(config cassandra.profiler.options)}"
    echo "${opts:-event=ctimer,interval=1ms,wall=10ms,lock=1ms}"
}

# Report the host settings that decide what perf_events can do, and warn when
# the options ask for perf_events but the host will not allow them.  The values
# are the host's: these sysctls are not namespaced.
_cassandra_profiler_check_perf() {
    local node=$1 opts=$2
    local paranoid kptr
    paranoid=$(dexec "${node}" cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null | tr -d '[:space:]')
    kptr=$(dexec "${node}" cat /proc/sys/kernel/kptr_restrict 2>/dev/null | tr -d '[:space:]')
    log "Host kernel.perf_event_paranoid=${paranoid:-?} kernel.kptr_restrict=${kptr:-?}"

    case ",${opts}," in
        *",event=cpu,"*|*",event=cpu-clock,"*|*",event=cycles,"*|*",event=cache-misses,"*) ;;
        *) return 0 ;;
    esac
    if [ -n "${paranoid}" ] && [ "${paranoid}" -gt 1 ] 2>/dev/null; then
        error "The options use perf_events, but kernel.perf_event_paranoid=${paranoid} on the host: CPU samples will be missing or degraded."
        error "  Use event=ctimer, or have the host set kernel.perf_event_paranoid=1 (and kernel.kptr_restrict=0 for kernel symbols)."
    elif [ -n "${kptr}" ] && [ "${kptr}" != "0" ]; then
        error "kernel.kptr_restrict=${kptr} on the host: kernel frames will be unsymbolised."
    fi
}

# All replica container names, in the order cassandra_get_hosts uses.
cassandra_profiler_nodes() {
    local num_dcs=$1
    local nodes_per_dc=${2:-$(config nodesperdc)}
    for i in $(seq 1 ${num_dcs}); do
        local city=$(get_location $i ${LOCATIONS_FILE})
        for k in $(seq 1 ${nodes_per_dc}); do
            echo "${city}${k}"
        done
    done
}

_cassandra_nodetool_profile() {
    local container=$1; shift
    # JVM_OPTS in the container carries the server heap flags; nodetool
    # would inherit them (cf. wait_for_nodetool_status).
    dexec "${container}" env JVM_OPTS='' nodetool profile "$@"
}

# cassandra_profiler_start <num_dcs> <nodes_per_dc> <tag>
cassandra_profiler_start() {
    local num_dcs=$1 nodes_per_dc=$2 tag=$3
    local opts
    opts=$(cassandra_profiler_options)

    log "Starting async-profiler on Cassandra nodes (${opts})"
    _cassandra_profiler_check_perf "$(cassandra_profiler_nodes "${num_dcs}" "${nodes_per_dc}" | head -1)" "${opts}"
    local c pids=()
    for c in $(cassandra_profiler_nodes "${num_dcs}" "${nodes_per_dc}"); do
        (
            local file="${CASSANDRA_PROFILER_DIR_IN_CONTAINER}/${tag}_${c}.jfr"
            # docker exec runs as root, the server as cassandra.
            dexec "${c}" mkdir -p -m 1777 "${CASSANDRA_PROFILER_DIR_IN_CONTAINER}" >/dev/null 2>&1
            local out
            if ! out=$(_cassandra_nodetool_profile "${c}" execute "start,${opts},jfr,file=${file}" 2>&1); then
                error "async-profiler did not start on ${c}: ${out}"
                if printf '%s' "${out}" | grep -q -e "not enabled" -e "not permitted"; then
                    error "  the node was not started with the profiler properties; was cassandra.profiler set before the cluster was created?"
                fi
                exit 1
            fi
            debug "async-profiler on ${c}: ${out}"
        ) &
        pids+=($!)
    done
    # Started concurrently so that the per-node windows line up to within a
    # nodetool invocation rather than drifting by one per node.
    #
    # Wait for these jobs only: a bare `wait` also waits for every other
    # background job of the shell, which includes the `dlogs -f` followers
    # cassandra_start_cluster leaves running for the lifetime of the cluster.
    wait "${pids[@]}"
}

# cassandra_profiler_stop <num_dcs> <nodes_per_dc> <tag> <dest_dir>
cassandra_profiler_stop() {
    local num_dcs=$1 nodes_per_dc=$2 tag=$3 dest_dir=$4
    mkdir -p "${dest_dir}"

    log "Stopping async-profiler on Cassandra nodes; captures go to ${dest_dir}"
    local c pids=()
    for c in $(cassandra_profiler_nodes "${num_dcs}" "${nodes_per_dc}"); do
        (
            local file="${CASSANDRA_PROFILER_DIR_IN_CONTAINER}/${tag}_${c}.jfr"
            local out
            if ! out=$(_cassandra_nodetool_profile "${c}" execute "stop" 2>&1); then
                error "async-profiler did not stop cleanly on ${c}: ${out}"
            fi
            # The node address, not the host's, so `d` picks the right daemon
            # on a real deployment.
            if d "${c}" cp "${c}:${file}" "${dest_dir}/" >/dev/null 2>&1; then
                dexec "${c}" rm -f "${file}" >/dev/null 2>&1
            else
                error "No async-profiler capture could be copied from ${c}:${file}"
            fi
            # The JVM's own GC log (-Xlog:gc,safepoint,... in jvm-server.options).
            # Its "Reaching safepoint" times are the time-to-safepoint, which
            # samples can only infer, and it goes when the container does.  It
            # covers the JVM's whole life, not just this run.
            local gclogs="${dest_dir}/${tag}_${c}.gc.tgz"
            if ! dexec "${c}" sh -c 'cd "${CASSANDRA_HOME:-/opt/cassandra}/logs" && tar czf - gc.log*' > "${gclogs}" 2>/dev/null; then
                rm -f "${gclogs}"
                debug "No GC log found on ${c}"
            fi
            # A Cassandra build without Span support fails as soon as a task
            # runs with accord.debug_execution=true, not when the profiler
            # starts, so it would otherwise surface only as a broken run.
            if dlogs "${c}" 2>&1 | grep -q -m1 "one/profiler/Span"; then
                error "${c} logged a failure involving one.profiler.Span: the async-profiler jar in the image lacks it"
            fi
        ) &
        pids+=($!)
    done
    # Not a bare `wait`: see cassandra_profiler_start.
    wait "${pids[@]}"
}
