#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

# Function to clean up Cassandra cluster
cleanup_cluster() {
    log "Cleaning up Cassandra cluster..."
    python3 cleanup_cassandra_cluster.py
    if [ $? -ne 0 ]; then
        error "Failed to clean up Cassandra cluster."
        exit 1
    fi
}

workloads="a b c d e"
protocols="quorum paxos accord"
records=1000
clients=$(nproc)
ops_per_client=100

for p in ${protocols}
do
    do_create_and_load=1
    for w in ${workloads}
    do
	for c in ${clients}
	do
	    ./run_benchmarks.sh ${p} 12 3 3 ${w} ${records} $((clients*ops_per_client)) ${do_create_and_load}
	    do_create_and_load=0
	done
    done
    cleanup_cluster
done

${DIR}/parse_ycsb_to_csv.sh ${DIR}/logs/* > ${RESULTSDIR}/results.csv

# CDF per proto, op over all workloads
python ${DIR}/cdf.py ${RESULTSDIR}/results.csv ${workloads} 3 ${DIR}/latencies.csv ${RESULTSDIR}/cdf.tex
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
 \end{document}" > /dev/null 2>&1
