#!/usr/bin/env bash

# Swap workload experiment.
# This workload atomically swaps S items per transaction.
# The parameter S varies from 3 to 8, and the experiment measures total throughput.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

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

workload_type="site.ycsb.workloads.SwapWorkload"
workload="sw" # workloads/workloadsw
protocols="accord cockroachdb" # only backends that support transactions
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi
nodes=7
replication_factor=3
records=$(config records)
threads=$(config threads)
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
    do_clean_up=0
    for p in ${protocols}
    do
        # clean prior logs
        rm -f ${LOGDIR}/swap/*${p}*

        do_create_and_load=1
        total=$(echo ${s_values} | wc -w)
        count=0
        for s in ${s_values}
        do
            do_clean_up=$(( count == total-1 ? 1 : 0 ))
            ts=$(date +%Y%m%d%H%M%S%N)
            output_file="${LOGDIR}/swap/${p}_${nodes}_${workload}_${ts}.dat"
            run_benchmark ${p} ${threads} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((threads * ops_per_thread)) ${output_file} ${do_create_and_load} ${do_clean_up} -p swap.s=${s} -p maxexecutiontime=${maxexecutiontime}
            do_create_and_load=0
            count=$((count+1))
        done
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/swap/* > ${RESULTSDIR}/swap.csv

debug "Plotting..."
python3 ${DIR}/swap.py ${RESULTSDIR}/swap.csv ${RESULTSDIR}/swap.tex

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
