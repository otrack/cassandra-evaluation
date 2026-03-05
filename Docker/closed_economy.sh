#!/usr/bin/env bash

# Closed Economy experiment (aka., YCSB-T).
# This workload models a banking scenario similar to TPC-B where transactions transfer money between accounts.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

usage() {
    echo "Usage: $0 [--dry-run]"
    echo "  --dry-run  Skip the experiment run; only draw plots using existing data."
}

dry_run=0
for arg in "$@"; do
    case "$arg" in
        --dry-run)
            dry_run=1
            ;;
        *)
            echo "Unknown parameter: $arg"
            usage
            exit 1
            ;;
    esac
done

mkdir -p ${LOGDIR}/closed_economy

workload_type="site.ycsb.workloads.ClosedEconomyWorkload"
workload="ce"
protocols="accord cockroachdb cassandra-paxos"
node_counts="3 5 7"
replication_factor=3
records=10000
total_threads=1
ops_per_thread=1000

if [ "$dry_run" -eq 0 ]; then
    for p in ${protocols}
    do
        # clean prior logs
        rm -f ${LOGDIR}/closed_economy/*${p}*
        
        for nodes in ${node_counts}
        do
	    t=$((total_threads / nodes))
	    if [ ${t} -lt 1 ]; then
	        t=1
	    fi
	    ts=$(date +%Y%m%d%H%M%S%N)
	    output_file="${LOGDIR}/closed_economy/${p}_${nodes}_${workload}_${ts}.dat"
	    # Each node count requires a fresh cluster, so always create and always clean up
	    run_benchmark ${p} ${t} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((t * ops_per_thread)) ${output_file} 1 1
        done
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/closed_economy/* > ${RESULTSDIR}/closed_economy.csv

debug "Plotting..."
python3 ${DIR}/closed_economy.py ${RESULTSDIR}/closed_economy.csv ${RESULTSDIR}/closed_economy.tex

pdflatex -jobname=closed_economy -output-directory=${RESULTSDIR} \
"\documentclass{article}\
 \usepackage{pgfplots}\
 \usepackage{tikz}\
 \usepackage{amssymb}\
 \usepackage{wasysym}\
 \usetikzlibrary{decorations.pathreplacing,positioning,automata,calc}\
 \usetikzlibrary{shapes,arrows}\
 \usepgflibrary{shapes.symbols}\
 \usetikzlibrary{shapes.symbols}\
 \usetikzlibrary{patterns}\
 \usetikzlibrary{matrix, positioning, pgfplots.groupplots}\
 \pgfplotsset{compat=1.17}\
 \begin{document}\
 \thispagestyle{empty}\centering\input{closed_economy.tex}\
 \end{document}" > /dev/null
