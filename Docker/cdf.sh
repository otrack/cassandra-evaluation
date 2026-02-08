#!/usr/bin/env bash

# Compute the latency distribution of each protocol (broken down per operation) over all the YCSB workloads.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

clean_logdir

workload_type="site.ycsb.workloads.CoreWorkload"
workloads="a b c d"
workloads="a"
protocols="quorum accord paxos swiftpaxos-paxos swiftpaxos-epaxos swiftpaxos-swiftpaxos"
protocols="quorum accord swiftpaxos-paxos cockroachdb"
nodes=3
city="Lyon"
records=1000
clients=1
ops_per_client=1000

do_clean_up=0
for p in ${protocols}
do
    do_create_and_load=1
    total=$(( $(echo ${workloads} | wc -w) * $(echo ${clients} | wc -w) ))
    count=0
    for w in ${workloads}
    do
	for c in ${clients}
	do
	    do_clean_up=$(( count == total-1 ? 1 : 0 ))
	    ts=$(date +%Y%m%d%H%M%S%N)
	    output_file="${LOGDIR}/${p}_${nodes}_${w}_${ts}.dat"
	    run_benchmark ${p} ${c} ${nodes} ${workload_type} ${w} ${records} $((clients * ops_per_client)) ${output_file} ${do_create_and_load} ${do_clean_up}
	    do_create_and_load=0
	    count=$((count+1))
	done
    done    
done

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/* > ${RESULTSDIR}/cdf.csv

debug "Plotting..."
python ${DIR}/cdf.py ${RESULTSDIR}/cdf.csv ${workloads} ${nodes} ${city} ${DIR}/latencies.csv ${RESULTSDIR}/cdf.tex

pdflatex -jobname=cdf -output-directory=${RESULTSDIR} \
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
 \thispagestyle{empty}\centering\input{cdf.tex}\
 \end{document}" > /dev/null
