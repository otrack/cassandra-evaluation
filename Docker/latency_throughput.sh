#!/usr/bin/env bash

# Latency vs Throughput experiment: increases the number of clients by a factor of 2
# until a "hockey stick" is observed (both latency and throughput degrade compared to prior measure).
# This produces a classical latency vs throughput graph.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

clean_logdir

workload_type="site.ycsb.workloads.CoreWorkload"
workload="a"
protocols="quorum accord swiftpaxos-paxos cockroachdb"
nodes=3
records=1000
ops_per_thread=1000

# Start with 1 thread and double until we detect a hockey stick
# Maximum threads to prevent infinite loop
max_threads=128

do_clean_up=0
for p in ${protocols}
do
    do_create_and_load=1
    threads=1
    
    while [ ${threads} -le ${max_threads} ]
    do
        ts=$(date +%Y%m%d%H%M%S%N)
        output_file="${LOGDIR}/${p}_${nodes}_${workload}_${ts}.dat"
        
        # Clean up after the last iteration of each protocol's thread sequence
        # This ensures we start fresh for the next protocol
        do_clean_up=0
        next_threads=$((threads * 2))
        if [ ${next_threads} -gt ${max_threads} ]; then
            do_clean_up=1
        fi
        
        run_benchmark ${p} ${threads} ${nodes} ${workload_type} ${workload} ${records} $((threads * ops_per_thread)) ${output_file} ${do_create_and_load} ${do_clean_up}
        do_create_and_load=0
        
        # Double the number of threads for next iteration
        threads=${next_threads}
    done
done

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/* > ${RESULTSDIR}/latency_throughput.csv

debug "Plotting..."
python ${DIR}/latency_throughput.py ${RESULTSDIR}/latency_throughput.csv ${RESULTSDIR}/latency_throughput.tex

pdflatex -jobname=latency_throughput -output-directory=${RESULTSDIR} \
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
 \thispagestyle{empty}\centering\input{latency_throughput.tex}\
 \end{document}" > /dev/null
