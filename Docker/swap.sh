#!/usr/bin/env bash

# Swap workload experiment.
# This workload atomically swaps S items per transaction.
# The parameter S varies from 3 to 8, and the experiment measures total throughput.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh
source ${DIR}/cassandra/cassandra_breakdown.sh

usage() {
    echo "Usage: $0 [--dry-run] [--test] [--protocols=LIST]"
    echo "  --dry-run        Skip the experiment run; only draw plots using existing data."
    echo "  --test           Use a 60s run time and right-size containers to fit this machine."
    echo "  --protocols=LIST Override the list of protocols to run (space-separated)."
}

dry_run=0
test_run=0
protocols_override=""
for arg in "$@"; do
    case "$arg" in
        --dry-run)
            dry_run=1
            ;;
        --test)
            test_run=1
            ;;
        --protocols=*)
            protocols_override="${arg#*=}"
            ;;
        *)
            echo "Unknown parameter: $arg"
            usage
            exit 1
            ;;
    esac
done

mkdir -p ${LOGDIR}/swap
mkdir -p ${RESULTSDIR}/swap

workload_type="site.ycsb.workloads.SwapWorkload"
workload="sw" # workloads/workloadsw
protocols="accord cockroachdb" # only backends that support transactions
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi
nodes=7
replication_factor=3
records=$(config records)
single_client_threads=1    # 1 thread/DC for tracing and breakdown collection
multi_client_threads=50    # 50 threads/DC for throughput comparison (no tracing)
# ops_per_thread=0 means operationcount=0 (unlimited); run duration is controlled by maxexecutiontime
ops_per_thread=0
s_values=$(seq 3 8)

if [ "$test_run" -eq 1 ]; then
    original_machine=$(config machine)
    original_maxexecutiontime=$(config maxexecutiontime)
    restore_test_settings() {
        sed -i "s/^machine=.*/machine=${original_machine}/" "${CONFIG_FILE}"
        sed -i "s/^maxexecutiontime=.*/maxexecutiontime=${original_maxexecutiontime}/" "${CONFIG_FILE}"
    }
    trap restore_test_settings EXIT
    compute_test_machine "${nodes}"
    sed -i "s/^maxexecutiontime=.*/maxexecutiontime=60/" "${CONFIG_FILE}"
fi
maxexecutiontime=$(config maxexecutiontime)

if [ "$dry_run" -eq 0 ]; then
    # Write CSV header for breakdown results
    echo "protocol,S,city,fast_commit,slow_commit,commit,ordering,execution" > ${RESULTSDIR}/swap/breakdown.csv

    # Compute cities list once (nodes is fixed)
    cities_list=""
    for i in $(seq 1 ${nodes}); do
        loc=$(get_location $i ${DIR}/latencies.csv)
        cities_list="${cities_list} ${loc}"
    done

    # ---- Phase 1: single-client runs (1 thread/DC) – tracing enabled, breakdown collected ----
    for p in ${protocols}
    do
        # clean prior logs
        rm -f ${LOGDIR}/swap/*${p}*

        do_create_and_load=1
        for s in ${s_values}
        do
            ts=$(date +%Y%m%d%H%M%S%N)
            output_file="${LOGDIR}/swap/${p}_${nodes}_${workload}_${ts}.dat"

            # Enable tracing for CockroachDB so breakdown data can be collected
            tracing_opts=()
            if [[ "$p" == cockroachdb* ]]; then
                tracing_opts=("-p" "db.tracing=true")
            fi

            # Run without cleanup; cluster is stopped manually after breakdown
            run_benchmark ${p} ${single_client_threads} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((single_client_threads * ops_per_thread)) ${output_file} ${do_create_and_load} 0 "${tracing_opts[@]}" -p swap.s=${s} -p maxexecutiontime=${maxexecutiontime}

            # Compute performance breakdown for this S value
            if [[ "$p" == cockroachdb* ]]; then
                tmp_logdir=$(mktemp -d)
                for i in $(seq 1 ${nodes}); do
                    loc=$(get_location $i ${DIR}/latencies.csv)
                    src="${output_file%.dat}_${loc}.dat"
                    if [ -f "${src}" ]; then
                        cp "${src}" "${tmp_logdir}/${p}_${nodes}_${workload}_${ts}_${loc}.dat"
                    fi
                done
                python3 ${DIR}/cockroachdb/cockroachdb_breakdown.py \
                    ${p} ${tmp_logdir} ${workload} ${nodes} ${cities_list} | \
                    awk -F',' -v s="${s}" -v proto="${p}" '{print proto "," s "," $0}' >> ${RESULTSDIR}/swap/breakdown.csv
                rm -rf "${tmp_logdir}"
            elif [ "$p" == "accord" ]; then
                compute_breakdown ${nodes} accord | \
                    awk -F',' '{
                        # Field mapping from cassandra_breakdown.sh output:
                        # $1=city, $2=fast_commit, $3=slow_commit, $4=commit, $5=ordering, $6=execution
                        # ordering is 0 for accord (commit serves as the ordering phase)
                        print $1","$2","$3","$4",0,"$5
                    }' | \
                    awk -F',' -v s="${s}" '{print "accord," s "," $0}' >> ${RESULTSDIR}/swap/breakdown.csv
            fi

            do_create_and_load=0
        done

        # Clean up cluster after all S values for this protocol
        stop_benchmark ${p} ${nodes}
    done

    # ---- Phase 2: multi-client runs (50 threads/DC) – no tracing, no breakdown ----
    mkdir -p ${LOGDIR}/swap_multi
    for p in ${protocols}
    do
        # clean prior multi-client logs for this protocol
        rm -f ${LOGDIR}/swap_multi/*${p}*

        do_create_and_load=1
        for s in ${s_values}
        do
            ts=$(date +%Y%m%d%H%M%S%N)
            output_file="${LOGDIR}/swap_multi/${p}_${nodes}_${workload}_${ts}.dat"

            run_benchmark ${p} ${multi_client_threads} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((multi_client_threads * ops_per_thread)) ${output_file} ${do_create_and_load} 0 -p swap.s=${s} -p maxexecutiontime=${maxexecutiontime}

            do_create_and_load=0
        done

        # Clean up cluster after all S values for this protocol
        stop_benchmark ${p} ${nodes}
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh \
    $(ls ${LOGDIR}/swap/*.dat 2>/dev/null) \
    $(ls ${LOGDIR}/swap_multi/*.dat 2>/dev/null) \
    > ${RESULTSDIR}/swap.csv

debug "Plotting..."
python3 ${DIR}/swap.py ${RESULTSDIR}/swap.csv ${RESULTSDIR}/swap/breakdown.csv ${RESULTSDIR}/swap.tex

pdflatex -jobname=swap -output-directory=${RESULTSDIR} \
"\documentclass{article}\
 \usepackage{pgfplots}\
 \usepackage{tikz}\
 \usetikzlibrary{decorations.pathreplacing,positioning,automata,calc}\
 \usetikzlibrary{shapes,arrows}\
 \usepgflibrary{shapes.symbols}\
 \usetikzlibrary{shapes.symbols}\
 \usetikzlibrary{patterns}\
 \usetikzlibrary{matrix, positioning, pgfplots.groupplots}\
 \pgfplotsset{compat=1.17}\
 \begin{document}\
 \thispagestyle{empty}\centering\input{swap.tex}\
 \end{document}" > /dev/null
