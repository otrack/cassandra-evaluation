#!/usr/bin/env bash

# Calvin micro-benchmark (Thomson et al., SIGMOD 2012, Section 6.2, Figure 5).
# Each transaction reads 10 records, one of them taken from a small pool of "hot"
# records and the others from the large pool of "cold" ones, checks that the sum
# of their counters is non-negative, and if so increments each counter.  The
# contention index (CI) is the fraction of the hot records a transaction
# accesses, i.e. 1/(number of hot records).  As in Figure 5, the experiment
# considers low (CI=0.0001) and high (CI=0.01) contention.  For each contention
# index, it increases the number of clients per site by a factor of 1.5 (as
# latency_throughput.sh does) to plot the latency against the throughput, and
# stops once both of them degrade (the system is saturated).

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

usage() {
    echo "Usage: $0 [--dry-run] [--test] [--protocols=LIST] [--nodesperdc=N] [--ci=LIST] [--clients=LIST] [--max-clients=N] [--records=N]"
    echo "  --dry-run        Skip the experiment run; only parse existing data."
    echo "  --test           Small and short runs (20000 records, 10s, 2 to 10 clients/site),"
    echo "                   with containers right-sized to fit this machine."
    echo "  --protocols=LIST Override the list of protocols to run (comma-separated)."
    echo "  --nodesperdc=N   Override number of nodes per DC (default from exp.config)."
    echo "  --ci=LIST        Contention indexes to run (comma-separated, default: 0.0001,0.01)."
    echo "  --clients=LIST   Clients per site to sweep (comma-separated, default: 16, 24, 36, ...,"
    echo "                   multiplying by 1.5 up to --max-clients)."
    echo "  --max-clients=N  Largest number of clients per site of the default sweep (default: 2048)."
    echo "  --records=N      Number of records (default: 1000000)."
}

dry_run=0
test_run=0
protocols_override=""
nodesperdc_override=""
ci_override=""
clients_override=""
max_clients_override=""
records_override=""
for arg in "$@"; do
    case "$arg" in
        --dry-run)
            dry_run=1
            ;;
        --test)
            test_run=1
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

mkdir -p ${LOGDIR}/calvin_ubench
mkdir -p ${RESULTSDIR}/calvin_ubench

workload_type="site.ycsb.workloads.CalvinWorkload"
workload="calvin"
# The transactional systems whose YCSB client implements checkAndIncrement
protocols="accord cockroachdb-opt tiga"
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi

# Parameters of the Calvin paper: 10 records per transaction, one of them hot
# (calvin.txnsize and calvin.hotfraction keep the defaults of the workload), a
# contention index of 0.0001 (low) or 0.01 (high), 1M records.
ci_values="0.0001 0.01"
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

restore_settings() {
    sed -i "s/^machine=.*/machine=${original_machine}/" "${CONFIG_FILE}"
    sed -i "s/^maxexecutiontime=.*/maxexecutiontime=${original_maxexecutiontime}/" "${CONFIG_FILE}"
    sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=${original_fix_lh}/" "${CONFIG_FILE}"
    sed -i "s/^nodesperdc=.*/nodesperdc=${original_nodesperdc}/" "${CONFIG_FILE}"
}
trap restore_settings EXIT

if [ -n "$nodesperdc_override" ]; then
    sed -i "s/^nodesperdc=.*/nodesperdc=${nodesperdc_override}/" "${CONFIG_FILE}"
fi

if [ "$test_run" -eq 1 ]; then
    # The hot pool has 1/CI = 10000 records at CI=0.0001, and the cold pool
    # must still provide 9 records per transaction.
    records=20000
    min_clients=2
    max_clients=10
    compute_test_machine "${nodes}"
    sed -i "s/^maxexecutiontime=.*/maxexecutiontime=10/" "${CONFIG_FILE}"
fi

[ -n "$ci_override" ] && ci_values="$ci_override"
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

if [ "$dry_run" -eq 0 ]; then
    pull_images

    for p in ${protocols}
    do
        if [[ "$p" == "cockroachdb-opt" ]]; then
            sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=true/" "${CONFIG_FILE}"
        elif [[ "$p" == "cockroachdb-bad" ]]; then
            sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=bad/" "${CONFIG_FILE}"
        else
            sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=false/" "${CONFIG_FILE}"
        fi

        rm -f ${LOGDIR}/calvin_ubench/*${p}*

        # The data set does not depend on the contention index nor on the
        # number of clients: deploy and load it once per protocol.
        do_create_and_load=1
        for ci in ${ci_values}
        do
            prev_latency=-1
            prev_throughput=-1
            for clients in ${client_counts}
            do
                ts=$(date +%Y%m%d%H%M%S%N)
                output_file="${LOGDIR}/calvin_ubench/${p}_${nodes}_${workload}_${ts}.dat"

                run_benchmark ${p} ${clients} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((clients * ops_per_thread)) ${output_file} ${do_create_and_load} 0 -p calvin.contentionindex=${ci} -p maxexecutiontime=${maxexecutiontime}

                do_create_and_load=0

                read -r tput latency <<< "$(global_tput_latency ${output_file})"

                # Stop when both latency and throughput degrade wrt. the previous
                # number of clients (Pareto front)
                if [ "${prev_latency}" -ge 0 ] && [ "${latency}" -gt "${prev_latency}" ] && [ "${tput}" -lt "${prev_throughput}" ]; then
                    log "Pareto front reached for ${p} (CI=${ci}): latency ${latency}ms > ${prev_latency}ms and throughput ${tput} < ${prev_throughput} tx/s, stopping client increase"
                    break
                fi
                prev_latency=${latency}
                prev_throughput=${tput}
            done
        done

        stop_benchmark ${p} ${nodes}
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh \
    $(ls ${LOGDIR}/calvin_ubench/*.dat 2>/dev/null) \
    > ${RESULTSDIR}/calvin_ubench.csv

debug "Plotting..."
python3 ${DIR}/calvin_ubench.py ${RESULTSDIR}/calvin_ubench.csv ${RESULTSDIR}/calvin_ubench.tex

pdflatex -interaction nonstopmode -jobname=calvin_ubench -output-directory=${RESULTSDIR} \
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
 \thispagestyle{empty}\centering\input{calvin_ubench.tex}\
 \end{document}" > /dev/null
