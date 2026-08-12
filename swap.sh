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
    echo "Usage: $0 [--dry-run] [--test] [--protocols=LIST] [--nodesperdc=N]"
    echo "  --dry-run        Skip the experiment run; only draw plots using existing data."
    echo "  --test           Use a 60s run time and right-size containers to fit this machine."
    echo "  --protocols=LIST Override the list of protocols to run (comma-separated)."
    echo "  --nodesperdc=N   Override number of nodes per DC (default from exp.config)."
}

dry_run=0
test_run=0
protocols_override=""
nodesperdc_override=""
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
        --nodesperdc=*)
            nodesperdc_override="${arg#*=}"
            ;;
        --nodes-per-dc=*)
            nodesperdc_override="${arg#*=}"
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
workload="sw"
protocols="accord cockroachdb"
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi
nodes=7
replication_factor=3
records=$(config records)
client_counts="1 50"
ops_per_thread=0
s_values=$(seq 3 8)

original_machine=$(config machine)
original_maxexecutiontime=$(config maxexecutiontime)
original_nodesperdc=$(config "nodesperdc")

restore_test_settings() {
    sed -i "s/^machine=.*/machine=${original_machine}/" "${CONFIG_FILE}"
    sed -i "s/^maxexecutiontime=.*/maxexecutiontime=${original_maxexecutiontime}/" "${CONFIG_FILE}"
    sed -i "s/^nodesperdc=.*/nodesperdc=${original_nodesperdc}/" "${CONFIG_FILE}"
}
trap restore_test_settings EXIT

if [ -n "$nodesperdc_override" ]; then
    sed -i "s/^nodesperdc=.*/nodesperdc=${nodesperdc_override}/" "${CONFIG_FILE}"
fi

if [ "$test_run" -eq 1 ]; then
    nodes=3
    records=1000
    compute_test_machine "${nodes}"
    sed -i "s/^maxexecutiontime=.*/maxexecutiontime=10/" "${CONFIG_FILE}"
fi
maxexecutiontime=$(config maxexecutiontime)

if [ "$dry_run" -eq 0 ]; then
    pull_images
    echo "protocol,S,clients,dc,fast_commit,slow_commit,commit,ordering,execution" > ${RESULTSDIR}/swap/breakdown.csv

    dcs_list=""
    for i in $(seq 1 ${nodes}); do
        loc=$(get_location $i ${LOCATIONS_FILE})
        dcs_list="${dcs_list} ${loc}"
    done

    for p in ${protocols}
    do
        rm -f ${LOGDIR}/swap/*${p}*
	
	for clients in ${client_counts}
	do
            do_create_and_load=1
            for s in ${s_values}
            do
                ts=$(date +%Y%m%d%H%M%S%N)
                output_file="${LOGDIR}/swap/${p}_${nodes}_${workload}_${ts}.dat"

                tracing_opts=()
                if [[ "$p" == cockroachdb* ]]; then
                    tracing_opts=("-p" "db.tracing=true")
                fi

                run_benchmark ${p} ${clients} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((clients * ops_per_thread)) ${output_file} ${do_create_and_load} 0 "${tracing_opts[@]}" -p swap.s=${s} -p maxexecutiontime=${maxexecutiontime}

                if [[ "$p" == cockroachdb* ]]; then
                    tmp_logdir=$(mktemp -d)
                    for i in $(seq 1 ${nodes}); do
                        loc=$(get_location $i ${LOCATIONS_FILE})
                        src="${output_file%.dat}_${loc}.dat"
                        if [ -f "${src}" ]; then
                            cp "${src}" "${tmp_logdir}/${p}_${nodes}_${workload}_${ts}_${loc}.dat"
                        fi
                    done
                    python3 ${DIR}/cockroachdb/cockroachdb_breakdown.py \
                        ${p} ${tmp_logdir} ${workload} ${nodes} ${dcs_list} | \
                        awk -F',' -v s="${s}" -v c="${clients}" -v proto="${p}" '{print proto "," s "," c "," $0}' >> ${RESULTSDIR}/swap/breakdown.csv
                    rm -rf "${tmp_logdir}"
                elif [ "$p" == "accord" ]; then
                    compute_breakdown ${nodes} accord | \
                        awk -F',' -v s="${s}" -v c="${clients}" '{print "accord," s "," c "," $0}' >> ${RESULTSDIR}/swap/breakdown.csv
                fi

                do_create_and_load=0
            done

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

pdflatex -interaction nonstopmode -jobname=swap -output-directory=${RESULTSDIR} \
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
