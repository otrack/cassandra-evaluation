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
# The experiment has two phases:
#  - saturation: at a fixed number of nodes per data center (--nodesperdc,
#    default from exp.config), for each contention index, it increases the
#    number of clients per site by a factor of 1.5 (as latency_throughput.sh
#    does) to plot the latency against the throughput, and stops once both of
#    them degrade (the system is saturated).  The number of clients of the
#    peak throughput is saved in results/calvin_ubench_peak.csv.
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
    echo "Usage: $0 [--dry-run] [--test] [--no-pull] [--phase=PHASE] [--protocols=LIST] [--nodesperdc=N]"
    echo "          [--ci=LIST] [--clients=LIST] [--max-clients=N] [--records=N] [--scale=LIST] [--mp=LIST]"
    echo "          [--scale-clients=N]"
    echo "  --dry-run          Skip the experiment run; only parse existing data."
    echo "  --test             Small and short runs (20000 records per partition, 10s, 2 to 10"
    echo "                     clients/site, 1 and 2 nodes/DC), with containers right-sized to fit"
    echo "                     this machine."
    echo "  --no-pull          Use the local Docker images, without pulling them first."
    echo "  --phase=PHASE      saturation, scale or all (default: all)."
    echo "  --protocols=LIST   Override the list of protocols to run (comma-separated)."
    echo "  --nodesperdc=N     Nodes per DC of the saturation phase (default from exp.config)."
    echo "  --ci=LIST          Contention indexes to run (comma-separated, default: 0.0001,0.01)."
    echo "  --clients=LIST     Clients per site of the saturation phase (comma-separated, default:"
    echo "                     16, 24, 36, ..., multiplying by 1.5 up to --max-clients)."
    echo "  --max-clients=N    Largest number of clients per site of the default sweep (default: 2048)."
    echo "  --records=N        Number of records per partition (default: 1000000)."
    echo "  --scale=LIST       Nodes per DC of the scale phase (comma-separated, default: 1,2,4)."
    echo "  --mp=LIST          Proportions of multipartition transactions of the scale phase"
    echo "                     (comma-separated, default: 0.1,1.0; the saturation phase uses the first)."
    echo "  --scale-clients=N  Clients per site and per node of the scale phase, instead of the peak"
    echo "                     of the saturation phase."
}

dry_run=0
test_run=0
no_pull=0
phase="all"
protocols_override=""
nodesperdc_override=""
ci_override=""
clients_override=""
max_clients_override=""
records_override=""
scale_override=""
mp_override=""
scale_clients=""
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

SATURATION_LOGDIR=${LOGDIR}/calvin_ubench
SCALE_LOGDIR=${LOGDIR}/calvin_ubench/scale
PEAK_FILE=${RESULTSDIR}/calvin_ubench_peak.csv
mkdir -p ${SATURATION_LOGDIR} ${SCALE_LOGDIR}
mkdir -p ${RESULTSDIR}

workload_type="site.ycsb.workloads.CalvinWorkload"
workload="calvin"
# The transactional systems whose YCSB client implements checkAndIncrement
protocols="accord cockroachdb-opt tiga"
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi

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
nodes=3
replication_factor=3
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

saturation_nodesperdc=${original_nodesperdc:-1}
[ -n "$nodesperdc_override" ] && saturation_nodesperdc="$nodesperdc_override"

if [ "$test_run" -eq 1 ]; then
    # The hot pool of a partition has 1/CI = 10000 records at CI=0.0001, and
    # its cold pool must still provide 9 records per transaction.
    records=20000
    WARMUP_EXECUTION_TIME=0
    min_clients=2
    max_clients=10
    scale_values="1 2"
    set_config maxexecutiontime 10
fi

[ -n "$ci_override" ] && ci_values="$ci_override"
[ -n "$mp_override" ] && mp_values="$mp_override"
[ -n "$scale_override" ] && scale_values="$scale_override"
[ -n "$clients_override" ] && client_counts="$clients_override"
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
    for i in $(seq 1 ${nodes}); do
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
        tiga*|calvin|detock|janus) echo "tiga" ;;
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
        compute_test_machine "${nodes}" "${npd}"
    fi
}

# Runs protocol $1 with $2 nodes per data center, contention index $3, a
# proportion $4 of multipartition transactions and $5 clients per site, logging
# in directory $6; the first run of a deployment ($7=1) creates and loads it.
# Sets run_tput and run_latency to the throughput and latency of the run.
run_calvin() {
    local proto=$1 npd=$2 ci=$3 mp=$4 clients=$5 logdir=$6 create=$7
    local ts output_file
    ts=$(date +%Y%m%d%H%M%S%N)
    output_file="${logdir}/${proto}_${nodes}_${workload}_${ts}.dat"
    run_benchmark ${proto} ${clients} ${nodes} ${replication_factor} ${workload_type} ${workload} \
        $((records * npd)) $((clients * ops_per_thread)) ${output_file} ${create} 0 \
        -p calvin.contentionindex=${ci} \
        -p calvin.partitions=${npd} \
        -p calvin.partitioner=$(calvin_partitioner ${proto}) \
        -p calvin.mpproportion=${mp} \
        -p maxexecutiontime=${maxexecutiontime}
    read -r run_tput run_latency <<< "$(global_tput_latency ${output_file})"
}

saturation_phase() {
    echo "protocol,ci,clients,tput" > ${PEAK_FILE}
    for p in ${protocols}
    do
        rm -f ${SATURATION_LOGDIR}/*${p}*
        configure_deployment ${p} ${saturation_nodesperdc}

        # The data set does not depend on the contention index nor on the
        # number of clients: deploy and load it once per protocol.
        do_create_and_load=1
        for ci in ${ci_values}
        do
            prev_latency=-1
            prev_throughput=-1
            peak_clients=0
            peak_throughput=-1
            for clients in ${client_counts}
            do
                run_calvin ${p} ${saturation_nodesperdc} ${ci} ${saturation_mp} ${clients} ${SATURATION_LOGDIR} ${do_create_and_load}
                do_create_and_load=0
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
            echo "${p},${ci},${peak_clients},${peak_throughput}" >> ${PEAK_FILE}
        done

        stop_benchmark ${p} ${nodes}
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
        if [[ "$p" == swiftpaxos* ]]; then
            log "Skipping ${p} in the scale phase (a single node per DC)"
            continue
        fi
        rm -f ${SCALE_LOGDIR}/*${p}*
        for npd in ${scale_values}
        do
            configure_deployment ${p} ${npd}
            do_create_and_load=1
            for ci in ${ci_values}
            do
                for mp in ${mp_values}
                do
                    base=$(scale_clients_for ${p} ${ci} ${npd}) || exit 1
                    for clients in ${base} $(( (base * 3 + 1) / 2 ))
                    do
                        run_calvin ${p} ${npd} ${ci} ${mp} ${clients} ${SCALE_LOGDIR} ${do_create_and_load}
                        do_create_and_load=0
                        log "Scale ${p} (CI=${ci}, mp=${mp}, ${npd} node(s)/DC, ${clients} clients/site): ${run_tput} tx/s, ${run_latency}ms"
                    done
                    # Without multipartition transactions, a single partition
                    # gives the same run whatever the proportion
                    [ "${npd}" -eq 1 ] && break
                done
            done
            stop_benchmark ${p} ${nodes}
        done
    done
}

if [ "$dry_run" -eq 0 ]; then
    [ "$no_pull" -eq 0 ] && pull_images
    if [ "${phase}" != "scale" ]; then
        saturation_phase
    fi
    if [ "${phase}" != "saturation" ]; then
        scale_phase
    fi
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh \
    $(ls ${SATURATION_LOGDIR}/*.dat 2>/dev/null) \
    > ${RESULTSDIR}/calvin_ubench.csv
${DIR}/parse_ycsb_to_csv.sh \
    $(ls ${SCALE_LOGDIR}/*.dat 2>/dev/null) \
    > ${RESULTSDIR}/calvin_ubench_scale.csv

debug "Plotting..."
python3 ${DIR}/calvin_ubench.py ${RESULTSDIR}/calvin_ubench.csv ${RESULTSDIR}/calvin_ubench.tex \
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
