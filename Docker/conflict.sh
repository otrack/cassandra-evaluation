#!/usr/bin/env bash

# In this workload, we change a parameter theta to increase the conflict rate from 0 to 1, by step of 0.1.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

clean_logdir

workload_type="site.ycsb.workloads.ConflictWorkload"
thetas=$(seq -f "%.1f" 0 0.1 1.0)
protocols="quorum accord swiftpaxos-paxos cockroachdb"
nodes=3
records=1000
threads=1
ops_per_thread=1000

do_clean_up=0
for p in ${protocols}
do
    do_create_and_load=1
    total=$(( $(echo ${thetas} | wc -w) * $(echo ${threads} | wc -w) ))
    count=0
    for t in ${thetas}
    do
	for c in ${threads}
	do
	    do_clean_up=$(( count == total-1 ? 1 : 0 ))
	    ts=$(date +%Y%m%d%H%M%S%N)
	    output_file="${LOGDIR}/${p}_${nodes}_a_${ts}.dat"
	    run_benchmark ${p} ${c} ${nodes} ${workload_type} a ${records} $((threads * ops_per_thread)) ${output_file} ${do_create_and_load} ${do_clean_up} -p conflict.theta=${t} -p updateproportion=1.0 -p readproportion=0.0
	    do_create_and_load=0
	    count=$((count+1))
	done
    done
done

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/* > ${RESULTSDIR}/conflict.csv

debug "Plotting..."
python ${DIR}/conflict.py ${RESULTSDIR}/conflict.csv a 3 ${DIR}/latencies.csv ${RESULTSDIR}/conflict.tex

pdflatex -jobname=conflict -output-directory=${RESULTSDIR} \
"\documentclass{article}\
 \usepackage{pgfplots}\
 \usepackage{tikz}\
 \usetikzlibrary{decorations.pathreplacing,positioning,automata,calc}\
 \usetikzlibrary{shapes,arrows}\
 \usepgflibrary{shapes.symbols}\
 \usetikzlibrary{shapes.symbols}\
 \usetikzlibrary{patterns}\
 \usetikzlibrary{matrix, positioning, pgfplots.groupplots}\
 \begin{document}\
 \thispagestyle{empty}\centering\input{conflict.tex}\
 \end{document}" > /dev/null
