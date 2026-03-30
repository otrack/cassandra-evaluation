#!/usr/bin/env bash

# Latency vs Throughput experiment: increases the number of clients by a factor of 2
# (1, 2, 4, 8, ..., up to max_threads) to produce a classical latency vs throughput graph.
# The graph shows the "hockey stick" effect where latency increases sharply and
# throughput plateaus/degrades as the system becomes saturated.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

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

mkdir -p ${LOGDIR}/latency_throughput

workload_type="site.ycsb.workloads.ConflictWorkload"
theta=0.01
workload="a" # does not matter
protocols=$(awk -F',' 'NR>1 && $1!="" {print $1}' protocols.csv | grep -v cockroachdb-opt | grep -v cockroachdb-bad | paste -sd' ')
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi
nodes=5
replication_factor=${nodes}
records=$(config records)
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

# Start with 1 thread and double until reaching max_threads
# The resulting graph demonstrates the hockey stick effect
# Maximum threads to prevent infinite loop
max_threads=2048

if [ "$dry_run" -eq 0 ]; then
    pull_images
    do_clean_up=0
    for p in ${protocols}
    do

        # clean prior logs
        rm -f ${LOGDIR}/latency_throughput/*${p}*

        do_create_and_load=1
        threads=16
        prev_latency=-1
        prev_throughput=-1

        while [ ${threads} -le ${max_threads} ]
        do
            ts=$(date +%Y%m%d%H%M%S%N)
            output_file="${LOGDIR}/latency_throughput/${p}_${nodes}_${workload}_${ts}.dat"
            
            # Clean up after the last iteration of each protocol's thread sequence
            # This ensures we start fresh for the next protocol
            do_clean_up=0
            next_threads=$(( (threads * 3 + 1) / 2 ));
	    if [ "$next_threads" -gt "$max_threads" ] || [ "$p" = "cassandra*" ]; then
                do_clean_up=1
            fi
            
            run_benchmark ${p} ${threads} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((threads * ops_per_thread)) ${output_file} ${do_create_and_load} ${do_clean_up} -p conflict.theta=${theta} -p updateproportion=1.0 -p readproportion=0.0 -p maxexecutiontime=${maxexecutiontime}
            do_create_and_load=0

            # Extract global metrics aggregated across all sites:
            # sum throughput and average latency over the first ${nodes} cities
            total_tput=0
            total_latency=0
            city_count=0
            for i in $(seq 1 ${nodes}); do
                city=$(get_location ${i} ${DIR}/latencies.csv)
                city_file="${output_file%.dat}_${city}.dat"
                [ -f "${city_file}" ] || continue
                city_tput=$(awk -F',' '/^\[OVERALL\], Throughput\(ops\/sec\),/{t=$3; gsub(/[[:space:]]/,"",t); print int(t+0.5); exit}' "${city_file}")
                city_tput=${city_tput:-0}
                city_lat=$(grep -v CLEANUP "${city_file}" | grep -v FAILED | awk -F',' '/AverageLatency\(us\)/{lat=$3; gsub(/[[:space:]]/,"",lat); if(lat+0>max) max=lat+0} END{print int(max/1000)}')
                city_lat=${city_lat:-0}
                total_tput=$(( total_tput + city_tput ))
                total_latency=$(( total_latency + city_lat ))
                city_count=$(( city_count + 1 ))
            done
            tput=${total_tput}
            if [ "${city_count}" -gt 0 ]; then
                max_avg_latency=$(( total_latency / city_count ))
            else
                max_avg_latency=0
            fi

            # Stop when both latency and throughput degrade wrt. previous values (Pareto front)
            if [ "${prev_latency}" -ge 0 ] && [ "${prev_throughput}" -ge 0 ] && [ "${max_avg_latency}" -gt "${prev_latency}" ] && [ "${tput}" -lt "${prev_throughput}" ]; then
                log "Pareto front reached for ${p}: latency ${max_avg_latency}ms > ${prev_latency}ms and throughput ${tput} < ${prev_throughput} ops/s, stopping thread increase"
                stop_benchmark ${p} ${nodes}
                break
            fi
            prev_latency=${max_avg_latency}
            prev_throughput=${tput}

            # Double the number of threads for next iteration
            threads=${next_threads}
        done
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/latency_throughput/* > ${RESULTSDIR}/latency_throughput.csv

debug "Plotting..."
python3 ${DIR}/latency_throughput.py ${RESULTSDIR}/latency_throughput.csv ${RESULTSDIR}/latency_throughput.tex

pdflatex -jobname=latency_throughput -output-directory=${RESULTSDIR} \
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
 \pgfplotsset{compat=1.17}\
 \begin{document}\
 \thispagestyle{empty}\centering\input{latency_throughput.tex}\
 \end{document}" > /dev/null
