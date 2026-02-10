#!/usr/bin/env bash

# Closed Economy experiment (aka., YCSB-T).
# This workload models a banking scenario similar to TPC-B where transactions transfer money between accounts.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

clean_logdir

workload_type="site.ycsb.workloads.ClosedEconomyWorkload"
workload="ce"
protocols="accord cockroachdb"
node_counts="3 5 7"
records=100000
total_threads=1
ops_per_thread=200

for p in ${protocols}
do
    for nodes in ${node_counts}
    do
	t=$((total_threads / nodes))
	if [ ${t} -lt 1 ]; then
	    t=1
	fi
	ts=$(date +%Y%m%d%H%M%S%N)
	output_file="${LOGDIR}/${p}_${nodes}_${workload}_${ts}.dat"
	# Each node count requires a fresh cluster, so always create and always clean up
	run_benchmark ${p} ${t} ${nodes} ${workload_type} ${workload} ${records} $((t * ops_per_thread)) ${output_file} 1 1
    done
done

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/* > ${RESULTSDIR}/closed_economy.csv

debug "Plotting..."
python ${DIR}/closed_economy.py ${RESULTSDIR}/closed_economy.csv ${RESULTSDIR}/closed_economy.tex

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
