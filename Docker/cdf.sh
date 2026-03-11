#!/usr/bin/env bash

# Compute the latency distribution of each protocol (broken down per operation) over all the YCSB workloads.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

usage() {
    echo "Usage: $0 [--dry-run] [--test]"
    echo "  --dry-run  Skip the experiment run; only draw plots using existing data."
    echo "  --test     Use a 120s run time and right-size containers to fit this machine."
}

dry_run=0
test_run=0
for arg in "$@"; do
    case "$arg" in
        --dry-run)
            dry_run=1
            ;;
        --test)
            test_run=1
            ;;
        *)
            echo "Unknown parameter: $arg"
            usage
            exit 1
            ;;
    esac
done

mkdir -p ${LOGDIR}/cdf

workload_type="site.ycsb.workloads.CoreWorkload"
workloads="a"
protocols=$(awk -F',' 'NR>1 && $1!="" {print $1}' protocols.csv | paste -sd' ')
nodes=5
replication_factor=${nodes}
cities="Hanoi Lyon NewYork Rotterdam SaoPaulo" # can be ""
plot_average=true
records=$(config records)
threads=$(config threads)
ops_per_thread=0

maxexecutiontime=600
if [ "$test_run" -eq 1 ]; then
    maxexecutiontime=120
    original_machine=$(config machine)
    restore_machine() { sed -i "s/^machine=.*/machine=${original_machine}/" "${CONFIG_FILE}"; }
    trap restore_machine EXIT
    compute_test_machine "${nodes}"
fi

if [ "$dry_run" -eq 0 ]; then
    do_clean_up=0
    for p in ${protocols}
    do
        # clean prior logs
        rm -f ${LOGDIR}/cdf/*${p}*
        
        do_create_and_load=1
        total=$(( $(echo ${workloads} | wc -w) * $(echo ${threads} | wc -w) ))
        count=0
        tracing="false"
        if printf '%s\n' "$p" | grep -wF -q -- "cockroachdb";
        then
	    tracing="false" # FIXME
        fi
        for w in ${workloads}
        do
	    for c in ${threads}
	    do
	        do_clean_up=$(( count == total-1 ? 1 : 0 ))
	        ts=$(date +%Y%m%d%H%M%S%N)
	        output_file="${LOGDIR}/cdf/${p}_${nodes}_${w}_${ts}.dat"
	        run_benchmark ${p} ${c} ${nodes} ${replication_factor} ${workload_type} ${w} ${records} $((threads * ops_per_thread)) ${output_file} ${do_create_and_load} ${do_clean_up} -p db.tracing=${tracing} -p maxexecutiontime=${maxexecutiontime}
	        do_create_and_load=0
	        count=$((count+1))
	    done
        done
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/cdf/* > ${RESULTSDIR}/cdf.csv

debug "Plotting..."
if [ "$plot_average" = true ]; then
    python3 ${DIR}/cdf.py ${RESULTSDIR}/cdf.csv ${workloads} ${nodes} ${cities} ${DIR}/latencies.csv ${RESULTSDIR}/cdf.tex --average
else
    python3 ${DIR}/cdf.py ${RESULTSDIR}/cdf.csv ${workloads} ${nodes} ${cities} ${DIR}/latencies.csv ${RESULTSDIR}/cdf.tex
fi

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

debug "Plotting breakdown..."
python3 ${DIR}/breakdown.py ${LOGDIR}/cdf ${workloads} ${nodes} ${cities} ${RESULTSDIR}/breakdown
