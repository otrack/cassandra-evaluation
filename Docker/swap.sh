#!/usr/bin/env bash

# Swap workload experiment.
# This workload atomically swaps S items per transaction.
# The parameter S varies from 1 to 8, and the experiment measures total throughput.
# Both 1 and 50 clients/site are evaluated; tracing is enabled in all runs so that
# a per-(clients,S) performance breakdown can always be collected.

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
client_counts="1 50"       # thread counts per DC to evaluate
# ops_per_thread=0 means operationcount=0 (unlimited); run duration is controlled by maxexecutiontime
ops_per_thread=0
s_values=$(seq 1 8)

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
    pull_images
    # Write CSV header for breakdown results (clients column added after S)
    echo "protocol,S,clients,city,fast_commit,slow_commit,commit,ordering,execution" > ${RESULTSDIR}/swap/breakdown.csv

    # Compute cities list once (nodes is fixed)
    cities_list=""
    for i in $(seq 1 ${nodes}); do
        loc=$(get_location $i ${DIR}/latencies.csv)
        cities_list="${cities_list} ${loc}"
    done

    # Unified loop over client counts and protocols.
    # Tracing is enabled in every run so a breakdown can always be collected.
    # Accord must be stopped between client counts because its internal metrics
    # are cumulative and are not reset during the lifetime of a server.
    for clients in ${client_counts}
    do
        for p in ${protocols}
        do
            # clean prior logs for this protocol
            rm -f ${LOGDIR}/swap/*${p}*

            do_create_and_load=1
            for s in ${s_values}
            do
                ts=$(date +%Y%m%d%H%M%S%N)
                output_file="${LOGDIR}/swap/${p}_${nodes}_${workload}_${ts}.dat"

                # Always enable tracing so breakdown data can be collected
                tracing_opts=()
                if [[ "$p" == cockroachdb* ]]; then
                    tracing_opts=("-p" "db.tracing=true")
                fi

                run_benchmark ${p} ${clients} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((clients * ops_per_thread)) ${output_file} ${do_create_and_load} 0 "${tracing_opts[@]}" -p swap.s=${s} -p maxexecutiontime=${maxexecutiontime}

                # Compute performance breakdown for this (clients, S) value
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
                        awk -F',' -v s="${s}" -v c="${clients}" -v proto="${p}" '{print proto "," s "," c "," $0}' >> ${RESULTSDIR}/swap/breakdown.csv
                    rm -rf "${tmp_logdir}"
                elif [ "$p" == "accord" ]; then
                    compute_breakdown ${nodes} accord | \
                        awk -F',' -v s="${s}" -v c="${clients}" '{print "accord," s "," c "," $0}' >> ${RESULTSDIR}/swap/breakdown.csv
                fi

                do_create_and_load=0
            done

            # Stop the cluster after all S values for this (clients, protocol) combination.
            # This is required for Accord to reset its internal metrics before the next
            # client-count iteration; the cluster is restarted via do_create_and_load=1 above.
            stop_benchmark ${p} ${nodes}
        done
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh \
    $(ls ${LOGDIR}/swap/*.dat 2>/dev/null) \
    > ${RESULTSDIR}/swap.csv

debug "Plotting..."
python3 ${DIR}/swap.py ${RESULTSDIR}/swap.csv ${RESULTSDIR}/swap/breakdown.csv ${RESULTSDIR}/swap.tex

pdflatex -jobname=swap -output-directory=${RESULTSDIR} \
"\documentclass{article}\
 \usepackage{pgfplots}\
 \usepackage{tikz}\
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
 \thispagestyle{empty}\centering\input{swap.tex}\
 \end{document}" > /dev/null
