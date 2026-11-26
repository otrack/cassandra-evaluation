#!/usr/bin/env bash

# Latency vs Throughput experiment: increases the number of clients by a factor of 2
# (1, 2, 4, 8, ..., up to max_threads) to produce a classical latency vs throughput graph.
# The graph shows the "hockey stick" effect where latency increases sharply and
# throughput plateaus/degrades as the system becomes saturated.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

usage() {
    echo "Usage: $0 [--dry-run] [--test] [--resume] [--protocols=LIST] [--dcs=N]"
    echo "  --dry-run        Skip the experiment run; only draw plots using existing data."
    echo "  --test           Use a 60s run time and right-size containers to fit this machine."
    echo "  --resume         Keep existing per-point output and skip whatever it already covers,"
    echo "                   instead of wiping all logs and starting every protocol over. If a"
    echo "                   protocol already has a complete result, it's skipped entirely."
    echo "                   run_resilient.sh passes this on every retry after the first."
    echo "  --protocols=LIST Override the list of protocols to run (comma-separated)."
    echo "  --dcs=N          Override the number of DCs (default 5, 3 in --test mode)."
}

dry_run=0
test_run=0
resume=0
protocols_override=""
dcs_override=""
for arg in "$@"; do
    case "$arg" in
        --dry-run)
            dry_run=1
            ;;
        --test)
            test_run=1
            ;;
        --resume)
            resume=1
            ;;
        --protocols=*)
            protocols_override=$(echo "${arg#*=}" | tr ',' ' ')
            ;;
        --dcs=*)
            dcs_override="${arg#*=}"
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
theta=0.001
workload="a" # does not matter
protocols=$(awk -F',' 'NR>1 && $1!="" {print $1}' protocols.csv | grep -v cockroachdb-bad | grep -v accord-cmt | paste -sd' ')
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi
# 5 DCs by default, 3 in --test mode
default_dcs=5
[ "$test_run" -eq 1 ] && default_dcs=3
nodes=${dcs_override:-${default_dcs}}
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
    WARMUP_EXECUTION_TIME=0
    records=1000
    compute_test_machine "${nodes}"
    sed -i "s/^maxexecutiontime=.*/maxexecutiontime=10/" "${CONFIG_FILE}"
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

        if [ "${resume}" -eq 0 ]; then
            # clean prior logs
            rm -f ${LOGDIR}/latency_throughput/*${p}*
        fi

        cluster_started=0
        threads=16
        prev_latency=-1
        prev_throughput=-1

        while [ ${threads} -le ${max_threads} ]
        do
            # Deterministic per-thread-count token instead of a timestamp, so
            # a --resume re-run computes the exact same filename and
            # benchmark_already_done can recognize it. The thread sequence
            # below is fixed (depends only on threads, not on any measured
            # data), so it is identical across resumes.
            threads_token=$(printf '%05d' "${threads}")
            output_file="${LOGDIR}/latency_throughput/${p}_${nodes}_${workload}_${threads_token}.dat"

            # Clean up after the last iteration of each protocol's thread sequence, or in case the protocol uses Cassandra.
            # This ensures we start fresh for the next protocol
            do_clean_up=0
            next_threads=$(( (threads * 3 + 1) / 2 ));
	    if [ "$next_threads" -gt "$max_threads" ] || [ "$p" = "cassandra-paxos" ] || [ "$p" = "accord" ]; then
                do_clean_up=1
            fi

            if [ "$p" = "cassandra-paxos" ] || [ "$p" = "accord" ]; then
                do_create_and_load=1
            else
                do_create_and_load=$(( cluster_started == 0 ? 1 : 0 ))
            fi

            if [ "${resume}" -eq 1 ] && benchmark_already_done "${output_file%.dat}" "${nodes}"; then
                log "Skipping ${p} threads=${threads}: already done (${output_file})"
            else
                run_benchmark ${p} ${threads} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((threads * ops_per_thread)) ${output_file} ${do_create_and_load} ${do_clean_up} -p conflict.theta=${theta} -p updateproportion=1.0 -p readproportion=0.0 -p maxexecutiontime=${maxexecutiontime}

                if [ "$do_create_and_load" -eq 1 ] && [ "$p" != "cassandra-paxos" ] && [ "$p" != "accord" ]; then
                    cluster_started=1
                fi
            fi

            # Extract global metrics aggregated across all sites:
            # sum throughput and average latency over the first ${nodes} DCs
            total_tput=0
            total_latency=0
            dc_count=0
            for i in $(seq 1 ${nodes}); do
                dc=$(get_location ${i} ${LOCATIONS_FILE})
                dc_file="${output_file%.dat}_${dc}.dat"
                [ -f "${dc_file}" ] || continue
                dc_tput=$(awk -F',' '/^\[OVERALL\], Throughput\(ops\/sec\),/{t=$3; gsub(/[[:space:]]/,"",t); print int(t+0.5); exit}' "${dc_file}")
                dc_tput=${dc_tput:-0}
                dc_lat=$(grep -v CLEANUP "${dc_file}" | grep -v FAILED | awk -F',' '/AverageLatency\(us\)/{lat=$3; gsub(/[[:space:]]/,"",lat); if(lat+0>max) max=lat+0} END{print int(max/1000)}')
                dc_lat=${dc_lat:-0}
                total_tput=$(( total_tput + dc_tput ))
                total_latency=$(( total_latency + dc_lat ))
                dc_count=$(( dc_count + 1 ))
            done
            tput=${total_tput}
            if [ "${dc_count}" -gt 0 ]; then
                max_avg_latency=$(( total_latency / dc_count ))
            else
                max_avg_latency=0
            fi

            # Stop when both latency and throughput degrade wrt. previous values (Pareto front)
            if [ "${prev_latency}" -ge 0 ] && [ "${prev_throughput}" -ge 0 ] && [ "${max_avg_latency}" -gt "${prev_latency}" ] && [ "${tput}" -lt "${prev_throughput}" ]; then
                log "Pareto front reached for ${p}: latency ${max_avg_latency}ms > ${prev_latency}ms and throughput ${tput} < ${prev_throughput} ops/s, stopping thread increase"
                break
            fi
            prev_latency=${max_avg_latency}
            prev_throughput=${tput}

            # Double the number of threads for next iteration
            threads=${next_threads}
        done
        # Always tear down once per protocol, whether the loop broke on the
        # Pareto front or ran out of thread values. With --resume the final
        # (cleanup) iteration may itself have been skipped because it was
        # already done, in which case run_benchmark never ran its own
        # cleanup; stop_benchmark is safe to call even when nothing is left
        # running (e.g. every iteration was skipped).
        stop_benchmark ${p} ${nodes}
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/latency_throughput/* > ${RESULTSDIR}/latency_throughput.csv

debug "Plotting..."
python3 ${DIR}/latency_throughput.py ${RESULTSDIR}/latency_throughput.csv ${RESULTSDIR}/latency_throughput.tex

pdflatex -interaction nonstopmode -jobname=latency_throughput -output-directory=${RESULTSDIR} \
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
