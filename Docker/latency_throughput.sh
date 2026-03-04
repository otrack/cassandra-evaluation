#!/usr/bin/env bash

# Latency vs Throughput experiment: increases the number of clients by a factor of 2
# (1, 2, 4, 8, ..., up to max_threads) to produce a classical latency vs throughput graph.
# The graph shows the "hockey stick" effect where latency increases sharply and
# throughput plateaus/degrades as the system becomes saturated.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

mkdir -p ${LOGDIR}/latency_throughput
rm -f ${LOGDIR}/latency_throughput/*

workload_type="site.ycsb.workloads.ConflictWorkload"
theta=0.05
workload="a" # does not matter
protocols="accord cockroachdb swiftpaxos-paxos swiftpaxos-epaxos swiftpaxos-curp"
protocols="cockroachdb swiftpaxos-paxos swiftpaxos-epaxos swiftpaxos-curp"
nodes=5
replication_factor=${nodes}
records=1
ops_per_thread=10

# Start with 1 thread and double until reaching max_threads
# The resulting graph demonstrates the hockey stick effect
# Maximum threads to prevent infinite loop
max_threads=256

do_clean_up=0
for p in ${protocols}
do
    do_create_and_load=1
    threads=1

    while [ ${threads} -le ${max_threads} ]
    do
        ts=$(date +%Y%m%d%H%M%S%N)
        output_file="${LOGDIR}/latency_throughput/${p}_${nodes}_${workload}_${ts}.dat"
        
        # Clean up after the last iteration of each protocol's thread sequence
        # This ensures we start fresh for the next protocol
        do_clean_up=0
        next_threads=$((threads * 2))
        if [ ${next_threads} -gt ${max_threads} ]; then
            do_clean_up=1
        fi
        
        run_benchmark ${p} ${threads} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((threads * ops_per_thread)) ${output_file} ${do_create_and_load} ${do_clean_up} -p conflict.theta=${theta} -p updateproportion=1.0 -p readproportion=0.0
        do_create_and_load=0

        # Check if average latency exceeded 1s (500 ms); if so, stop increasing threads
	city=$(cat latencies.csv | head -n 2 | tail -n 1 | awk -F, '{print $3}')
        max_avg_latency=$(cat "${output_file%.dat}_${city}.dat" | grep -v CLEANUP | awk -F',' '/AverageLatency\(us\)/{lat=$3; gsub(/[[:space:]]/,"",lat); if(lat+0>max) max=lat+0} END{print max/1000}')
        if [ "${max_avg_latency}" -gt 500 ]; then
            log "Average latency ${max_avg_latency}us exceeds 1s for protocol ${p}, stopping thread increase"
	    stop_benchmark ${p} ${nodes}
            break
        fi

        # Double the number of threads for next iteration
        threads=${next_threads}
    done
done

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/latency_throughput/* > ${RESULTSDIR}/latency_throughput.csv

debug "Plotting..."
python3 ${DIR}/latency_throughput.py ${RESULTSDIR}/latency_throughput.csv ${RESULTSDIR}/latency_throughput.tex

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
