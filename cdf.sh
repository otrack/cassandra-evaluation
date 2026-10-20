#!/usr/bin/env bash

# Compute the latency distribution of each protocol (broken down per operation) over all the YCSB workloads.

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
    echo "  --dcs=N          Override the number of DCs (default 5)."
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

mkdir -p ${LOGDIR}/cdf

workload_type="site.ycsb.workloads.CoreWorkload"
workloads="a"
protocols=$(awk -F',' 'NR>1 && $1!="" {print $1}' protocols.csv | grep -v cockroachdb-bad | grep -v accord-cmt | paste -sd' ')
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi
# 5 DCs by default, 3 in --test mode
default_dcs=5
[ "$test_run" -eq 1 ] && default_dcs=3
num_dcs=${dcs_override:-${default_dcs}}
replication_factor=${num_dcs}
dcs=$(locations_list ${num_dcs}) # the DCs of the active infra; can be ""
plot_average=true
records=$(config records)
threads=$(config threads)
ops_per_thread=0

if [ "$test_run" -eq 1 ]; then
    records=1000
    WARMUP_EXECUTION_TIME=0
    original_machine=$(config machine)
    original_maxexecutiontime=$(config maxexecutiontime)
    restore_test_settings() {
        sed -i "s/^machine=.*/machine=${original_machine}/" "${CONFIG_FILE}"
        sed -i "s/^maxexecutiontime=.*/maxexecutiontime=${original_maxexecutiontime}/" "${CONFIG_FILE}"
    }
    trap restore_test_settings EXIT
    compute_test_machine "${num_dcs}"
    sed -i "s/^maxexecutiontime=.*/maxexecutiontime=10/" "${CONFIG_FILE}"
fi
maxexecutiontime=$(config maxexecutiontime)

if [ "$dry_run" -eq 0 ]; then
    pull_images
    for p in ${protocols}
    do
        if [ "${resume}" -eq 0 ]; then
            # clean prior logs
            rm -f ${LOGDIR}/cdf/*${p}*
        fi

        cluster_started=0
        tracing="false"
        if printf '%s\n' "$p" | grep -wF -q -- "cockroachdb";
        then
	    tracing="false" # FIXME
        fi
        # --test mode disables the warm-up phase (WARMUP_EXECUTION_TIME=0
        # above), so db.init() and the timed operation loop again share one
        # maxexecutiontime budget via YCSB's own TerminatorThread. A
        # swiftpaxos client's connection setup calls findClosest(), which
        # shells out to `ping` once per replica, sequentially -- routinely
        # several seconds -- so the 10s test-mode budget can be consumed
        # almost entirely by init() alone, leaving nothing for the terminator
        # to measure before it force-stops every thread. Give this protocol
        # family a longer budget in test mode; other protocols don't pay that
        # connect cost and keep the fast default.
        protocol_maxexecutiontime=${maxexecutiontime}
        if [ "$test_run" -eq 1 ]; then
            case "$p" in
                swiftpaxos-*) protocol_maxexecutiontime=30 ;;
            esac
        fi
        for w in ${workloads}
        do
	    for c in ${threads}
	    do
	        # Deterministic per-(workload, threads) token instead of a
	        # timestamp, so a --resume re-run computes the exact same
	        # filename and benchmark_already_done can recognize it.
	        c_token=$(printf '%04d' "${c}")
	        output_file="${LOGDIR}/cdf/${p}_${num_dcs}_${w}_${c_token}.dat"
	        if [ "${resume}" -eq 1 ] && benchmark_already_done "${output_file%.dat}" "${num_dcs}"; then
	            log "Skipping ${p} workload=${w} threads=${c}: already done (${output_file})"
	            continue
	        fi
	        do_create_and_load=$(( cluster_started == 0 ? 1 : 0 ))
	        run_benchmark ${p} ${c} ${num_dcs} ${replication_factor} ${workload_type} ${w} ${records} $((threads * ops_per_thread)) ${output_file} ${do_create_and_load} 0 -p db.tracing=${tracing} -p maxexecutiontime=${protocol_maxexecutiontime}
	        cluster_started=1
	    done
        done
        [ "${cluster_started}" -eq 1 ] && stop_benchmark ${p} ${num_dcs}
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/cdf/* > ${RESULTSDIR}/cdf.csv

debug "Plotting..."
if [ "$plot_average" = true ]; then
    python3 ${DIR}/cdf.py ${RESULTSDIR}/cdf.csv ${workloads} ${num_dcs} ${dcs} ${LOCATIONS_FILE} ${RESULTSDIR}/cdf.tex --average
else
    python3 ${DIR}/cdf.py ${RESULTSDIR}/cdf.csv ${workloads} ${num_dcs} ${dcs} ${LOCATIONS_FILE} ${RESULTSDIR}/cdf.tex
fi

pdflatex -interaction nonstopmode -jobname=cdf -output-directory=${RESULTSDIR} \
"\documentclass{article}\
 \usepackage{pgfplots}\
 \usepackage{tikz}\
 \usepackage{ifthen}\
 \usepackage{xspace}\
 \newcommand{\Accord}{\textsc{Entente}\xspace}\
 \usetikzlibrary{decorations.pathreplacing,positioning,automata,calc}\
 \usetikzlibrary{shapes,arrows}\
 \usepgflibrary{shapes.symbols}\
 \usetikzlibrary{shapes.symbols}\
 \usetikzlibrary{patterns}\
 \usetikzlibrary{matrix, positioning, pgfplots.groupplots}\
 \newboolean{details}\setboolean{details}{true}\
 \begin{document}\
 \thispagestyle{empty}\centering\input{cdf.tex}\
 \end{document}"  > /dev/null
