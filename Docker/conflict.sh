#!/usr/bin/env bash

# In this workload, we change a parameter theta to increase the conflict rate from 0 to 1.0, by step of 0.1.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh
source ${DIR}/cassandra/cassandra_breakdown.sh

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

mkdir -p ${LOGDIR}/conflict
mkdir -p ${RESULTSDIR}/conflict

workload_type="site.ycsb.workloads.ConflictWorkload"
thetas=$(seq -f "%.2f" 0 0.1 1.0)
workload="a" # this does not matter
protocols=$(awk -F',' 'NR>1 && $1!="" {print $1}' protocols.csv | grep -v cockroachdb-opt | grep -v cockroachdb-bad | grep -v accord-cmt | paste -sd' ')
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi
nodes=5
replication_factor=${nodes}
threads=$(config threads)
records=10000 # at least one per thread * DC plus one
ops_per_thread=0

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
    pull_images
    do_clean_up=0
    for p in ${protocols}
    do
        # clean prior logs
        rm -f ${LOGDIR}/conflict/*${p}*

        if [ "$p" == "accord-cmt" ]; then
            # For Accord (Committed), restart the server for each theta value so that JMX metrics
            # (accumulated over the full server lifetime) reflect only that theta's workload.
            rm -f ${RESULTSDIR}/conflict/accord_cmt_*.txt

            for t in ${thetas}
            do
                do_create_and_load=1
                for c in ${threads}
                do
                    ts=$(date +%Y%m%d%H%M%S%N)
                    output_file="${LOGDIR}/conflict/${p}_${nodes}_a_${ts}.dat"
                    # Pass do_clean_up=0 so we can query the cluster for breakdown before stopping
                    run_benchmark ${p} ${c} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((threads * ops_per_thread)) ${output_file} ${do_create_and_load} 0 -p conflict.theta=${t} -p updateproportion=1.0 -p readproportion=0.0 -p maxexecutiontime=${maxexecutiontime}
                    do_create_and_load=0
                done

                # Collect Accord commit latency breakdown while the server is still running
                compute_breakdown ${nodes} accord > ${RESULTSDIR}/conflict/accord_cmt_${t}.txt

                # Stop the cluster before the next theta
                stop_benchmark accord ${nodes}
            done
        else
            do_create_and_load=1
            total=$(( $(echo ${thetas} | wc -w) * $(echo ${threads} | wc -w) ))
            count=0
            for t in ${thetas}
            do
	        for c in ${threads}
	        do
	            do_clean_up=$(( count == total-1 ? 1 : 0 ))
	            ts=$(date +%Y%m%d%H%M%S%N)
	            output_file="${LOGDIR}/conflict/${p}_${nodes}_a_${ts}.dat"
	            run_benchmark ${p} ${c} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((threads * ops_per_thread)) ${output_file} ${do_create_and_load} ${do_clean_up} -p conflict.theta=${t} -p updateproportion=1.0 -p readproportion=0.0 -p maxexecutiontime=${maxexecutiontime}
	            do_create_and_load=0
	            count=$((count+1))
	        done
            done
        fi
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/conflict/* > ${RESULTSDIR}/conflict.csv

# Append accord-cmt commit latency rows (one per node/city, per theta value)
for t in ${thetas}
do
    cmt_file="${RESULTSDIR}/conflict/accord_cmt_${t}.txt"
    if [ -f "${cmt_file}" ]; then
        awk -F',' -v t="${t}" -v n="${nodes}" '
        NF >= 4 {
            city = $1
            commit_us = $4
            commit_ms = commit_us / 1000
            row = "accord-cmt," n ",a," t "," city ",update,NA,NA,NA"
            # Percentile columns p1-p49: not available from JMX breakdown
            for (i = 1; i <= 49; i++) row = row ",unknown"
            # p50: commit latency in ms (the only percentile reported by compute_breakdown)
            row = row "," sprintf("%.3f", commit_ms)
            # Percentile columns p51-p100: not available from JMX breakdown
            for (i = 51; i <= 100; i++) row = row ",unknown"
            row = row ",0,NA,NA,NA,NA"
            print row
        }' "${cmt_file}" >> ${RESULTSDIR}/conflict.csv
    fi
done

debug "Plotting..."
python3 ${DIR}/conflict.py ${RESULTSDIR}/conflict.csv ${workload} ${nodes} ${DIR}/latencies.csv ${RESULTSDIR}/conflict.tex

pdflatex -jobname=conflict -output-directory=${RESULTSDIR} \
"\documentclass{article}\
 \usepackage{pgfplots}\
 \usepackage{tikz}\
 \usepackage{xspace}\
 \newcommand{\Accord}{\textsc{Entente}\xspace}\
 \usetikzlibrary{decorations.pathreplacing,positioning,automata,calc}\
 \usetikzlibrary{shapes,arrows}\
 \usepgflibrary{shapes.symbols}\
 \usetikzlibrary{shapes.symbols}\
 \usetikzlibrary{patterns}\
 \usetikzlibrary{matrix, positioning, pgfplots.groupplots}\
 \begin{document}\
 \newcommand{\commitP}{{\normalfont\textsc{committed}}}\
 \thispagestyle{empty}\centering\input{conflict.tex}\
 \end{document}" > /dev/null
