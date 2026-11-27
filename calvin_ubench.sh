#!/usr/bin/env bash

# Calvin micro-benchmark (Thomson et al., SIGMOD 2012, Section 6.2, Figure 5).
# The records are split into partitions, one per node of a data center (the
# machines of the paper), each with a small pool of "hot" records and a large
# pool of "cold" ones.  A single-partition transaction reads 10 records of its
# home partition, one of them hot; a multipartition transaction reads 5
# records, one of them hot, on each of 2 partitions.  It checks that the sum of
# their counters is non-negative, and if so increments each counter.  The
# contention index (CI) is the fraction of the hot records of a partition that
# a transaction accesses there, i.e. 1/(number of hot records per partition).
# As in Figure 5, the experiment considers low (CI=0.0001) and high (CI=0.01)
# contention, and 1M records per partition.
#
# The experiment has two phases (--phase; only the saturation one by default):
#  - saturation: at a fixed number of nodes per data center (--nodesperdc,
#    default: 1, i.e. 1 partition), on 5 data centers (3 with --test), for each contention index, it increases the
#    number of clients per site by a factor of 1.5 (as latency_throughput.sh
#    does) to plot the latency against the throughput, and stops once both of
#    them degrade (the system is saturated).  The number of clients of the
#    peak throughput is saved in results/calvin_ubench_peak.csv.
#    With --ramp, a single run per protocol and contention index goes through
#    the numbers of clients as stages (calvin.ramp of the workload, which runs
#    the first n threads in the stage of n clients), instead of one run each.
#  - scale: for each number of nodes per data center (--scale), contention
#    index and proportion of multipartition transactions (--mp), it runs the
#    peak number of clients of the saturation phase, scaled by the number of
#    nodes, and 1.5 times as many, to plot the throughput against the number of
#    nodes (Figure 5).
#
# The partitions of the workload follow the placement of the records by each
# system: Cassandra/Accord nodes own one token each (cassandra.fixed_tokens),
# usertable is split into one range per node for CockroachDB
# (cockroachdb.partitions), and Tiga shards the records by itself.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

usage() {
    echo "Usage: $0 [--dry-run] [--test] [--no-pull] [--resume] [--phase=PHASE] [--protocols=LIST] [--nodesperdc=N]"
    echo "          [--ci=LIST] [--clients=LIST] [--max-clients=N] [--records=N] [--scale=LIST] [--mp=LIST]"
    echo "          [--scale-clients=N] [--check-invariant[=true|false]] [--ramp] [--ramp-step=S] [--ramp-skip=S] [--ramp-delay=S] [--pareto] [--latency=pN]"
    echo "  --dry-run          Skip the experiment run; only parse existing data."
    echo "  --resume           Keep the logs of earlier runs and skip the points they completed (what"
    echo "                     run_resilient.sh adds on a retry). Meant for the same arguments as the"
    echo "                     interrupted invocation: a point is recognised by its protocol, nodes per"
    echo "                     DC, contention index, multipartition proportion and clients only;"
    echo "                     a point is rerun unless its logs record the Docker images configured now."
    echo "  --test             Small and short runs (20000 records per partition, 10s, 2 to 10"
    echo "                     clients/site, 1 and 2 nodes/DC), with containers right-sized to fit"
    echo "                     this machine."
    echo "  --no-pull          Use the local Docker images, without pulling them first."
    echo "  --phase=PHASE      saturation, scale or all (default: saturation)."
    echo "  --protocols=LIST   Override the list of protocols to run (comma-separated)."
    echo "  --nodesperdc=N     Nodes per DC of the saturation phase (default: 1, on 5 DCs; 3 with --test)."
    echo "  --ci=LIST          Contention indexes to run (comma-separated, default: 0.0001,0.01)."
    echo "  --clients=LIST     Clients per site of the saturation phase (comma-separated, default:"
    echo "                     16, 24, 36, ..., multiplying by 1.5 up to --max-clients)."
    echo "  --max-clients=N    Largest number of clients per site of the default sweep (default: 2048, 1024 with --ramp)."
    echo "  --records=N        Number of records per partition (default: 1000000)."
    echo "  --ramp             Saturation phase: sweep the numbers of clients (--clients, --max-clients) as the"
    echo "                     stages of one run per protocol and contention index, rather than one run each."
    echo "                     The sweep cannot stop early: it goes up to the largest number of clients."
    echo "  --ramp-step=S      Seconds per stage of --ramp (default: 30)."
    echo "  --ramp-delay=S     Seconds between the end of the warm-up and the start of the first stage, which all the"
    echo "                     clients start together (default: 30); it must cover their set-up."
    echo "  --pareto           Plot only the Pareto frontier of each curve, not every point (default: every point)."
    echo "  --latency=pN       Latency percentile of the saturation plot (default: p50, e.g. p99)."
    echo "  --ramp-skip=S      Seconds at the start of each stage left out of its results (default: 5)."
    echo "  --scale=LIST       Nodes per DC of the scale phase (comma-separated, default: 1,2,4)."
    echo "  --mp=LIST          Proportions of multipartition transactions of the scale phase"
    echo "                     (comma-separated, default: 0.1,1.0; the saturation phase uses the first)."
    echo "  --scale-clients=N  Clients per site and per node of the scale phase, instead of the peak"
    echo "                     of the saturation phase."
    echo "  --check-invariant  Read the records of each transaction and check that the sum of their"
    echo "                     counters is non-negative before incrementing them (default:"
    echo "                     calvin.checkinvariant in exp.config, false). Skips swiftpaxos-janus,"
    echo "                     which cannot evaluate the check."
}

dry_run=0
test_run=0
no_pull=0
resume=0
invocation_args=("$@")
phase="saturation"
protocols_override=""
nodesperdc_override=""
ci_override=""
clients_override=""
max_clients_override=""
records_override=""
scale_override=""
mp_override=""
scale_clients=""
ramp=0
ramp_step=30
ramp_skip=5
ramp_delay=30
pareto=0
latency=p50
check_invariant=$(config calvin.checkinvariant)
for arg in "$@"; do
    case "$arg" in
        --dry-run)
            dry_run=1
            ;;
        --test)
            test_run=1
            ;;
        --no-pull)
            no_pull=1
            ;;
        --resume)
            resume=1
            ;;
        --phase=*)
            phase="${arg#*=}"
            ;;
        --protocols=*)
            protocols_override=$(echo "${arg#*=}" | tr ',' ' ')
            ;;
        --nodesperdc=*|--nodes-per-dc=*)
            nodesperdc_override="${arg#*=}"
            ;;
        --ci=*)
            ci_override=$(echo "${arg#*=}" | tr ',' ' ')
            ;;
        --clients=*)
            clients_override=$(echo "${arg#*=}" | tr ',' ' ')
            ;;
        --max-clients=*)
            max_clients_override="${arg#*=}"
            ;;
        --records=*)
            records_override="${arg#*=}"
            ;;
        --scale=*)
            scale_override=$(echo "${arg#*=}" | tr ',' ' ')
            ;;
        --mp=*)
            mp_override=$(echo "${arg#*=}" | tr ',' ' ')
            ;;
        --scale-clients=*)
            scale_clients="${arg#*=}"
            ;;
        --ramp)
            ramp=1
            ;;
        --ramp-step=*)
            ramp_step="${arg#*=}"
            ;;
        --ramp-skip=*)
            ramp_skip="${arg#*=}"
            ;;
        --pareto|-pareto)
            pareto=1
            ;;
        --latency=*)
            latency="${arg#*=}"
            ;;
        --ramp-delay=*)
            ramp_delay="${arg#*=}"
            ;;
        --check-invariant)
            check_invariant=true
            ;;
        --check-invariant=*)
            check_invariant="${arg#*=}"
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown parameter: $arg"
            usage
            exit 1
            ;;
    esac
done

case "${phase}" in
    saturation|scale|all) ;;
    *)
        echo "Unknown phase: ${phase}"
        usage
        exit 1
        ;;
esac

check_invariant=${check_invariant:-false}
case "${check_invariant}" in
    true|false) ;;
    *)
        echo "Invalid --check-invariant / calvin.checkinvariant: ${check_invariant} (true or false)"
        usage
        exit 1
        ;;
esac

SATURATION_LOGDIR=${LOGDIR}/calvin_ubench
SCALE_LOGDIR=${LOGDIR}/calvin_ubench/scale
PEAK_FILE=${RESULTSDIR}/calvin_ubench_peak.csv
mkdir -p ${SATURATION_LOGDIR} ${SCALE_LOGDIR}
mkdir -p ${RESULTSDIR}

workload_type="site.ycsb.workloads.CalvinWorkload"
workload="calvin"
# The transactional systems whose YCSB client executes the transaction atomically
protocols="accord cockroachdb-opt tiga swiftpaxos-janus"
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi
# The writes of a one-shot transaction cannot depend on its reads: with the
# check, the YCSB client of Janus reports every transaction as not implemented.
if [ "${check_invariant}" == "true" ]; then
    for p in ${protocols}; do
        [[ "$p" == swiftpaxos-* ]] && log "Skipping ${p}: it cannot check the invariant (--check-invariant)"
    done
    protocols=$(for p in ${protocols}; do [[ "$p" == swiftpaxos-* ]] || echo "$p"; done | tr '\n' ' ')
fi
log "Calvin micro-benchmark: invariant check ${check_invariant}"

# Parameters of the Calvin paper: 10 records per transaction, one of them hot
# per partition (calvin.txnsize and calvin.hotfraction keep the defaults of the
# workload), a contention index of 0.0001 (low) or 0.01 (high), 1M records per
# partition, 10% or 100% multipartition transactions spanning 2 partitions.
ci_values="0.0001 0.01"
mp_values="0.1 1.0"
scale_values="1 2 4"
# The number of clients per site starts at min_clients and is multiplied by 1.5
# up to max_clients, unless --clients gives the list explicitly.
min_clients=16
max_clients=2048
client_counts=""
records=1000000
dcs=5
replication_factor=${dcs}
ops_per_thread=0

original_machine=$(config machine)
original_maxexecutiontime=$(config maxexecutiontime)
original_fix_lh=$(config "cockroachdb.fix_lease_holder")
original_nodesperdc=$(config "nodesperdc")
original_fixed_tokens=$(config "cassandra.fixed_tokens")
original_crdb_partitions=$(config "cockroachdb.partitions")

# Sets <key> to <value> in exp.config, adding it when missing.
set_config() {
    local key=$1 value=$2
    local pattern="^${key//./\\.}="
    if grep -q "${pattern}" "${CONFIG_FILE}"; then
        sed -i "s/${pattern}.*/${key}=${value}/" "${CONFIG_FILE}"
    else
        echo "${key}=${value}" >> "${CONFIG_FILE}"
    fi
}

restore_settings() {
    set_config machine "${original_machine}"
    set_config maxexecutiontime "${original_maxexecutiontime}"
    set_config cockroachdb.fix_lease_holder "${original_fix_lh}"
    set_config nodesperdc "${original_nodesperdc}"
    set_config cassandra.fixed_tokens "${original_fixed_tokens:-0}"
    set_config cockroachdb.partitions "${original_crdb_partitions:-0}"
}
trap restore_settings EXIT

# 1 node, hence 1 partition, per DC, with and without --test
saturation_nodesperdc=${nodesperdc_override:-1}

if [ "$test_run" -eq 1 ]; then
    dcs=3
    replication_factor=${dcs}
    # The hot pool of a partition has 1/CI = 10000 records at CI=0.0001, and
    # its cold pool must still provide 9 records per transaction.
    records=1000000
    WARMUP_EXECUTION_TIME=10
    min_clients=16
    max_clients=128
    scale_values="2"
    set_config maxexecutiontime 30
fi

[ -n "$ci_override" ] && ci_values="$ci_override"
[ -n "$mp_override" ] && mp_values="$mp_override"
[ -n "$scale_override" ] && scale_values="$scale_override"
[ -n "$clients_override" ] && client_counts="$clients_override"
# A ramp cannot stop at saturation, and its threads all exist from the start
[ "$ramp" -eq 1 ] && [ "$test_run" -eq 0 ] && max_clients=1024
[ -n "$max_clients_override" ] && max_clients="$max_clients_override"
if [ -z "$client_counts" ]; then
    c=${min_clients}
    while [ ${c} -le ${max_clients} ]; do
        client_counts="${client_counts} ${c}"
        c=$(( (c * 3 + 1) / 2 ))
    done
fi
[ -n "$records_override" ] && records="$records_override"
saturation_mp=$(echo ${mp_values} | awk '{print $1}')

maxexecutiontime=$(config maxexecutiontime)

# Prints the throughput (tx/s), summed over the data centers, and the average
# latency (ms), averaged over the data centers, of the run logged in $1.
global_tput_latency() {
    local output_file=$1
    local total_tput=0 total_latency=0 dc_count=0
    for i in $(seq 1 ${dcs}); do
        local dc dc_file dc_tput dc_lat
        dc=$(get_location ${i} ${LOCATIONS_FILE})
        dc_file="${output_file%.dat}_${dc}.dat"
        [ -f "${dc_file}" ] || continue
        dc_tput=$(awk -F',' '/^\[OVERALL\], Throughput\(ops\/sec\),/{t=$3; gsub(/[[:space:]]/,"",t); print int(t+0.5); exit}' "${dc_file}")
        dc_lat=$(grep -v CLEANUP "${dc_file}" | grep -v FAILED | awk -F',' '/AverageLatency\(us\)/{lat=$3; gsub(/[[:space:]]/,"",lat); if(lat+0>max) max=lat+0} END{print int(max/1000)}')
        total_tput=$(( total_tput + ${dc_tput:-0} ))
        total_latency=$(( total_latency + ${dc_lat:-0} ))
        dc_count=$(( dc_count + 1 ))
    done
    if [ "${dc_count}" -gt 0 ]; then
        echo "${total_tput} $(( total_latency / dc_count ))"
    else
        echo "0 0"
    fi
}

# The partitioner of the Calvin workload matching how protocol $1 places the
# records on the nodes of a data center.
calvin_partitioner() {
    case "$1" in
        accord|cassandra-*|paxos|quorum|one) echo "murmur3" ;;
        cockroachdb*) echo "range" ;;
        tiga*|calvin|detock) echo "tiga" ;;
        swiftpaxos-janus) echo "swiftpaxos" ;;
        *) echo "mod" ;;
    esac
}

# Prepares exp.config to deploy protocol $1 with $2 nodes per data center.
configure_deployment() {
    local proto=$1 npd=$2
    if [[ "$proto" == "cockroachdb-opt" ]]; then
        set_config cockroachdb.fix_lease_holder true
    elif [[ "$proto" == "cockroachdb-bad" ]]; then
        set_config cockroachdb.fix_lease_holder bad
    else
        set_config cockroachdb.fix_lease_holder false
    fi
    set_config nodesperdc "${npd}"
    # One token, or one range, per node: the records of partition k are on the
    # k-th node of each data center
    if [ "${npd}" -gt 1 ]; then
        set_config cassandra.fixed_tokens 1
        set_config cockroachdb.partitions "${npd}"
    else
        set_config cassandra.fixed_tokens "${original_fixed_tokens:-0}"
        set_config cockroachdb.partitions 0
    fi
    if [ "$test_run" -eq 1 ]; then
        compute_test_machine "${dcs}" "${npd}"
    fi
}

# The log of the run of protocol $1 with $2 nodes per data center, contention
# index $3, a proportion $4 of multipartition transactions and $5 clients per
# site, in directory $6; $7 is 1 for a point of a sweep (default) and 2 for a
# ramp.  Named after these parameters rather than the time of the run, so that
# --resume finds it again and a retried point overwrites its own partial logs
# and profiles.  parse_ycsb_to_csv.sh wants digits in that field, and reads the
# parameters from the log itself.
calvin_output_file() {
    local proto=$1 npd=$2 ci=$3 mp=$4 clients=$5 logdir=$6 mode=${7:-1}
    local token
    token=$(awk -v m="${mode}" -v n="${npd}" -v ci="${ci}" -v mp="${mp}" -v c="${clients}" \
        'BEGIN { printf "%d%02d%09d%04d%05d", m, n, ci * 1e8 + 0.5, mp * 1000 + 0.5, c }')
    echo "${logdir}/${proto}_${dcs}_${workload}_${token}.dat"
}

# The Docker images (exp.config key=value lines, sorted) a run of protocol $1
# uses: run_calvin records them next to each completed point's logs, in
# <point>.images, so that --resume only skips a point that the images
# configured now produced.
point_images() {
    local key keys
    keys=$(_image_keys_for_protocols "$1")
    [ -n "${keys}" ] || keys=$(grep -oE '^[^#=[:space:]]+_image' "${CONFIG_FILE}")
    for key in ${keys}; do
        echo "${key}=$(config "${key}" | xargs)"
    done | sort
}

# With --resume, whether the run of calvin_output_file's parameters ($1..$7)
# already completed in every data center, with the images configured now; if
# so, sets run_tput, run_latency and run_output_file from its logs, as
# run_calvin would have.
calvin_resume_point() {
    [ "${resume}" -eq 1 ] || return 1
    local f
    f=$(calvin_output_file "$@")
    benchmark_already_done "${f%.dat}" "${dcs}" || return 1
    if ! cmp -s "${f%.dat}.images" <(point_images "$1"); then
        log "Rerunning $1 (CI=$3, mp=$4, $2 node(s)/DC, $5 clients/site): ${f} was not recorded as produced by $(point_images "$1" | tr '\n' ' ')"
        return 1
    fi
    run_output_file=${f}
    read -r run_tput run_latency <<< "$(global_tput_latency ${f})"
    log "Skipping $1 (CI=$3, mp=$4, $2 node(s)/DC, $5 clients/site): already done (${f})"
}

# Records the peak, $3 clients per site for $4 tx/s, of protocol $1 at contention
# index $2 in PEAK_FILE, in place of any earlier one.
set_peak() {
    awk -F',' -v p="$1" -v ci="$2" 'NR == 1 || !($1 == p && $2 + 0 == ci + 0)' "${PEAK_FILE}" > "${PEAK_FILE}.tmp" \
        && mv "${PEAK_FILE}.tmp" "${PEAK_FILE}"
    echo "$1,$2,$3,$4" >> "${PEAK_FILE}"
}

# Runs protocol $1 with $2 nodes per data center, contention index $3, a
# proportion $4 of multipartition transactions and $5 clients per site, logging
# in directory $6; the first run of a deployment ($7=1) creates and loads it;
# $8 as for calvin_output_file.
# Sets run_tput and run_latency to the throughput and latency of the run, and run_output_file to its log.
#
# Exits when any data center's client did not complete: a run that lost a
# client (a node reclaimed mid-run, say) would otherwise be summed into a
# throughput that looks low rather than broken, and steer the saturation
# search with it.  run_resilient.sh then repairs the deployment and resumes
# from this point.
run_calvin() {
    local proto=$1 npd=$2 ci=$3 mp=$4 clients=$5 logdir=$6 create=$7 mode=${8:-1}
    local output_file
    output_file=$(calvin_output_file ${proto} ${npd} ${ci} ${mp} ${clients} ${logdir} ${mode})
    run_benchmark ${proto} ${clients} ${dcs} ${replication_factor} ${workload_type} ${workload} \
        $((records * npd)) $((clients * ops_per_thread)) ${output_file} ${create} 0 \
        -p calvin.contentionindex=${ci} \
        -p calvin.partitions=${npd} \
        -p calvin.partitioner=$(calvin_partitioner ${proto}) \
        -p calvin.mpproportion=${mp} \
        -p calvin.checkinvariant=${check_invariant} \
        -p maxexecutiontime=${maxexecutiontime} \
        "${run_calvin_extra[@]}"
    run_output_file=${output_file}
    if ! benchmark_already_done "${output_file%.dat}" "${dcs}"; then
        error "${proto} (CI=${ci}, mp=${mp}, ${npd} node(s)/DC, ${clients} clients/site) did not complete in every DC (${output_file%.dat}_*.dat); stopping"
        error "  Resume with: ./run_resilient.sh $0 ${invocation_args[*]} --resume"
        exit 1
    fi
    point_images ${proto} > "${output_file%.dat}.images"
    read -r run_tput run_latency <<< "$(global_tput_latency ${output_file})"
}

# Extra arguments of the YCSB run of run_calvin (the ramp)
run_calvin_extra=()

# The saturation phase of protocol $1 with --ramp: one run per contention index
# whose stages are the numbers of clients per site, instead of one run each.
ramp_run() {
    local p=$1 ci=$2 create=$3
    local list max stages
    list=$(echo ${client_counts} | tr ' ' ',')
    max=$(echo ${client_counts} | tr ' ' '\n' | tail -1)
    stages=$(echo ${client_counts} | wc -w)
    local saved_maxexecutiontime=${maxexecutiontime}
    # The run's clock starts before the first stage does (ramp_delay, the set-up of the clients): the stages, that
    # delay, and one more step of margin
    maxexecutiontime=$(( stages * ramp_step + ramp_delay + ramp_step ))
    # The ramp outlasts the default timeout on a stuck client (ycsb.client.timeout): allow for the
    # warm-up and the run, plus the usual margin
    local margin
    margin=$(config ycsb.client.timeout)
    export YCSB_CLIENT_TIMEOUT=$(( maxexecutiontime + $(warmup_execution_time) + ${margin:-300} ))
    export YCSB_RAMP_DELAY=${ramp_delay}
    run_calvin_extra=(-p calvin.ramp=${list} -p calvin.rampstep=${ramp_step} -p calvin.rampskip=${ramp_skip})
    # The run is bounded by maxexecutiontime (ops_per_thread is 0: no operation count)
    run_calvin ${p} ${saturation_nodesperdc} ${ci} ${saturation_mp} ${max} ${SATURATION_LOGDIR} ${create} 2
    run_calvin_extra=()
    unset YCSB_CLIENT_TIMEOUT YCSB_RAMP_DELAY
    maxexecutiontime=${saved_maxexecutiontime}
}

saturation_phase() {
    [ -f "${PEAK_FILE}" ] || echo "protocol,ci,clients,tput" > ${PEAK_FILE}
    for p in ${protocols}
    do
        configure_deployment ${p} ${saturation_nodesperdc}

        # The data set does not depend on the contention index nor on the
        # number of clients: deploy and load it once per protocol, for its
        # first run that --resume does not skip.
        cluster_started=0
        for ci in ${ci_values}
        do
            prev_latency=-1
            prev_throughput=-1
            peak_clients=0
            peak_throughput=-1
            if [ "${ramp}" -eq 1 ]; then
                local ramp_max
                ramp_max=$(echo ${client_counts} | tr ' ' '\n' | tail -1)
                if ! calvin_resume_point ${p} ${saturation_nodesperdc} ${ci} ${saturation_mp} ${ramp_max} ${SATURATION_LOGDIR} 2; then
                    ramp_run ${p} ${ci} $(( 1 - cluster_started ))
                    cluster_started=1
                fi
                local dc_files=()
                for i in $(seq 1 ${dcs}); do
                    dc_files+=("${run_output_file%.dat}_$(get_location ${i} ${LOCATIONS_FILE}).dat")
                done
                # The stages of the run, summed over the data centers
                while read -r stage clients tput latency; do
                    log "Ramp ${p} (CI=${ci}, ${clients} clients/site): ${tput} tx/s, median ${latency}ms"
                    if [ "${tput}" -gt "${peak_throughput}" ]; then
                        peak_throughput=${tput}
                        peak_clients=${clients}
                    fi
                done < <(python3 ${DIR}/ramp_to_csv.py --summary "${dc_files[@]}")
                log "Peak of ${p} (CI=${ci}, ${saturation_nodesperdc} node(s)/DC): ${peak_throughput} tx/s with ${peak_clients} clients/site"
                set_peak ${p} ${ci} ${peak_clients} ${peak_throughput}
                continue
            fi
            for clients in ${client_counts}
            do
                if ! calvin_resume_point ${p} ${saturation_nodesperdc} ${ci} ${saturation_mp} ${clients} ${SATURATION_LOGDIR}; then
                    run_calvin ${p} ${saturation_nodesperdc} ${ci} ${saturation_mp} ${clients} ${SATURATION_LOGDIR} $(( 1 - cluster_started ))
                    cluster_started=1
                fi
                tput=${run_tput}
                latency=${run_latency}

                if [ "${tput}" -gt "${peak_throughput}" ]; then
                    peak_throughput=${tput}
                    peak_clients=${clients}
                fi

                # Stop when both latency and throughput degrade wrt. the previous
                # number of clients (Pareto front)
                if [ "${prev_latency}" -ge 0 ] && [ "${latency}" -gt "${prev_latency}" ] && [ "${tput}" -lt "${prev_throughput}" ]; then
                    log "Pareto front reached for ${p} (CI=${ci}): latency ${latency}ms > ${prev_latency}ms and throughput ${tput} < ${prev_throughput} tx/s, stopping client increase"
                    break
                fi
                prev_latency=${latency}
                prev_throughput=${tput}
            done
            log "Peak of ${p} (CI=${ci}, ${saturation_nodesperdc} node(s)/DC): ${peak_throughput} tx/s with ${peak_clients} clients/site"
            set_peak ${p} ${ci} ${peak_clients} ${peak_throughput}
        done

        [ "${cluster_started}" -eq 1 ] && stop_benchmark ${p} ${dcs}
    done
}

# The clients per site of the scale phase for protocol $1, contention index $2
# and $3 nodes per data center: the peak of the saturation phase, scaled by
# the number of nodes.
scale_clients_for() {
    local proto=$1 ci=$2 npd=$3
    local per_node
    if [ -n "${scale_clients}" ]; then
        per_node=${scale_clients}
    else
        local peak
        peak=$(awk -F',' -v p="${proto}" -v ci="${ci}" '$1 == p && $2 + 0 == ci + 0 {print $3}' ${PEAK_FILE} 2>/dev/null | tail -1)
        if [ -z "${peak}" ] || [ "${peak}" -le 0 ]; then
            error "No peak number of clients for ${proto} (CI=${ci}) in ${PEAK_FILE}: run the saturation phase or pass --scale-clients"
            exit 1
        fi
        per_node=$(( (peak + saturation_nodesperdc - 1) / saturation_nodesperdc ))
    fi
    echo $(( per_node * npd ))
}

scale_phase() {
    for p in ${protocols}
    do
        if [[ "$p" == swiftpaxos* && "$p" != swiftpaxos-janus ]]; then
            log "Skipping ${p} in the scale phase (a single node per DC)"
            continue
        fi
        for npd in ${scale_values}
        do
            configure_deployment ${p} ${npd}
            cluster_started=0
            for ci in ${ci_values}
            do
                for mp in ${mp_values}
                do
                    base=$(scale_clients_for ${p} ${ci} ${npd}) || exit 1
                    for clients in ${base} $(( (base * 3 + 1) / 2 ))
                    do
                        if ! calvin_resume_point ${p} ${npd} ${ci} ${mp} ${clients} ${SCALE_LOGDIR}; then
                            run_calvin ${p} ${npd} ${ci} ${mp} ${clients} ${SCALE_LOGDIR} $(( 1 - cluster_started ))
                            cluster_started=1
                        fi
                        log "Scale ${p} (CI=${ci}, mp=${mp}, ${npd} node(s)/DC, ${clients} clients/site): ${run_tput} tx/s, ${run_latency}ms"
                    done
                    # Without multipartition transactions, a single partition
                    # gives the same run whatever the proportion
                    [ "${npd}" -eq 1 ] && break
                done
            done
            [ "${cluster_started}" -eq 1 ] && stop_benchmark ${p} ${dcs}
        done
    done
}

# Appends what this invocation runs -- its arguments, the images and the
# repository revision -- to ${LOGDIR}/calvin_ubench/invocations.txt, so that the
# logs say which build produced them.  A --resume with other arguments than
# the invocation it continues is warned about: its points are recognised by
# their protocol, nodes, CI, mp and clients only, so it would keep runs made
# under other settings (records, --check-invariant, the ramp's...).
record_invocation() {
    local f="${SATURATION_LOGDIR}/invocations.txt" args previous
    args=$(for a in "${invocation_args[@]}"; do [ "${a}" = "--resume" ] || printf '%s ' "${a}"; done)
    if [ "${resume}" -eq 1 ] && [ -f "${f}" ]; then
        previous=$(grep '^args: ' "${f}" | tail -1 | cut -c7-)
        if [ "${previous}" != "${args}" ]; then
            error "--resume with other arguments than the run it resumes:"
            error "  now:    ${args}"
            error "  before: ${previous}"
            error "  completed points are kept whatever the other settings were."
        fi
    fi
    {
        echo "## $(date -u +%Y-%m-%dT%H:%M:%SZ)$([ "${resume}" -eq 1 ] && echo ' (resume)')"
        echo "args: ${args}"
        echo "revision: $(git -C "${DIR}" rev-parse --short HEAD 2>/dev/null)$(git -C "${DIR}" diff --quiet HEAD 2>/dev/null || echo ' (modified)')"
        grep -E '^[^#]*_image=|^cassandra\.profiler(\.alloc|\.options)?=|^cassandra\.jvm_opts=' "${CONFIG_FILE}"
    } >> "${f}"
}

# Without --resume, drops the logs of earlier runs of the protocols and phases
# this invocation runs, before anything that can fail: run_resilient.sh retries
# a failed attempt with --resume, which must find only this invocation's points.
clear_earlier_logs() {
    [ "${resume}" -eq 1 ] && return 0
    local p
    for p in ${protocols}; do
        [ "${phase}" != "scale" ] && rm -f ${SATURATION_LOGDIR}/*${p}*
        [ "${phase}" != "saturation" ] && rm -f ${SCALE_LOGDIR}/*${p}*
    done
    return 0
}

if [ "$dry_run" -eq 0 ]; then
    clear_earlier_logs
    record_invocation
    [ "$no_pull" -eq 0 ] && pull_images
    if [ "${phase}" != "scale" ]; then
        saturation_phase
    fi
    if [ "${phase}" != "saturation" ]; then
        scale_phase
    fi
fi

debug "Parsing results..."
# The logs of a ramp (calvin.ramp) hold one result per stage ([RAMP] lines), the others one per run: parse
# each kind with its parser, whether or not this invocation has --ramp (it may only re-parse, with --dry-run)
ramp_logs=$(grep -l '^\[RAMP\]' ${SATURATION_LOGDIR}/*.dat 2>/dev/null)
other_logs=$(ls ${SATURATION_LOGDIR}/*.dat 2>/dev/null | grep -v -x -F -f <(echo "${ramp_logs}" | sed '/^$/d') )
{
    ${DIR}/parse_ycsb_to_csv.sh ${other_logs}
    [ -n "${ramp_logs}" ] && python3 ${DIR}/ramp_to_csv.py ${ramp_logs} | tail -n +2
} > ${RESULTSDIR}/calvin_ubench.csv
${DIR}/parse_ycsb_to_csv.sh \
    $(ls ${SCALE_LOGDIR}/*.dat 2>/dev/null) \
    > ${RESULTSDIR}/calvin_ubench_scale.csv

debug "Plotting..."
# The number of records loaded (YCSB's recordcount, over all the partitions),
# for the caption, when every saturation run used the same one
record_counts=$(grep -ohs -m1 'recordcount=[0-9]*' ${SATURATION_LOGDIR}/*.dat | sort -u)
[ "$(echo "${record_counts}" | grep -c .)" -eq 1 ] && records_arg="--records=${record_counts#*=}"
python3 ${DIR}/calvin_ubench.py $([ "${pareto}" -eq 1 ] && echo --pareto) --latency=${latency} ${records_arg} ${RESULTSDIR}/calvin_ubench.csv ${RESULTSDIR}/calvin_ubench.tex \
    ${RESULTSDIR}/calvin_ubench_scale.csv ${RESULTSDIR}/calvin_ubench_scale.tex

for job in calvin_ubench calvin_ubench_scale; do
    [ -f "${RESULTSDIR}/${job}.tex" ] || continue
    pdflatex -interaction nonstopmode -jobname=${job} -output-directory=${RESULTSDIR} \
    "\documentclass{article}\
 \usepackage{pgfplots}\
 \usepackage{tikz}\
 \usepackage{amssymb}\
 \usepackage{wasysym}\
 \usepackage{xspace}\
 \newcommand{\Accord}{\textsc{Entente}\xspace}\
 \usetikzlibrary{decorations.pathreplacing,positioning,automata,calc}\
 \usetikzlibrary{shapes,arrows}\
 \usepgflibrary{shapes.symbols}\
 \usetikzlibrary{shapes.symbols}\
 \usetikzlibrary{patterns}\
 \usetikzlibrary{matrix, positioning, pgfplots.groupplots}\
 \pgfplotsset{compat=1.17}\
 \begin{document}\
 \thispagestyle{empty}\centering\input{${job}.tex}\
 \end{document}" > /dev/null
done
