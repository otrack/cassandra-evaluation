#!/usr/bin/env bash

# Closed Economy experiment: runs the closed economy workload in YCSB.
# This workload models a banking scenario where transactions transfer money between accounts.
# Only transaction-supporting protocols are used: accord and cockroachdb.
# The number of nodes varies from 3 to 5.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

clean_logdir

workload_type="site.ycsb.workloads.ClosedEconomyWorkload"
workload="ce"
protocols="accord cockroachdb"
node_counts="3 5 7"
records=1000
threads=$((1000/node_counts))
ops_per_thread=1000

# For each protocol and node count combination, we need to:
# 1. Create a new cluster (do_create_and_load=1)
# 2. Run the workload
# 3. Clean up (do_clean_up=1) before moving to next node count (different topology)

for p in ${protocols}
do
    for nodes in ${node_counts}
    do
	ts=$(date +%Y%m%d%H%M%S%N)
	output_file="${LOGDIR}/${p}_${nodes}_${workload}_${ts}.dat"
	# Each node count requires a fresh cluster, so always create and always clean up
	run_benchmark ${p} ${threads} ${nodes} ${workload_type} ${workload} ${records} $((threads * ops_per_thread)) ${output_file} 1 1
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
