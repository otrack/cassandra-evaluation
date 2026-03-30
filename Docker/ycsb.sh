#!/usr/bin/env bash

# Average latency comparison across YCSB workloads A to D for each protocol.
# For each workload, the average latency (in ms, averaged over all YCSB clients
# and all executed operations) is plotted as a grouped bar chart: one group per
# workload, one bar per protocol.

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

mkdir -p ${LOGDIR}/ycsb

workload_type="site.ycsb.workloads.CoreWorkload"
workloads="a b c d"
protocols=$(awk -F',' 'NR>1 && $1!="" {print $1}' protocols.csv | grep -v cockroachdb-opt | grep -v cockroachdb-bad | grep -v accord-cmt | paste -sd' ')
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi
nodes=5
replication_factor=${nodes}
records=$(config records)
threads=$(config threads)
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
	rm -f ${LOGDIR}/ycsb/*${p}*
	
	do_create_and_load=1
	total=$(( $(echo ${workloads} | wc -w) * $(echo ${threads} | wc -w) ))
	count=0
	for w in ${workloads}
	do
	    for c in ${threads}
	    do
		do_clean_up=$(( count == total-1 ? 1 : 0 ))
		ts=$(date +%Y%m%d%H%M%S%N)
		output_file="${LOGDIR}/ycsb/${p}_${nodes}_${w}_${ts}.dat"
		run_benchmark ${p} ${c} ${nodes} ${replication_factor} ${workload_type} ${w} ${records} $((threads * ops_per_thread)) ${output_file} ${do_create_and_load} ${do_clean_up} -p maxexecutiontime=${maxexecutiontime}
		do_create_and_load=0
		count=$((count+1))
	    done
	done
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/ycsb/* > ${RESULTSDIR}/ycsb.csv

debug "Plotting..."
python3 ${DIR}/ycsb.py ${RESULTSDIR}/ycsb.csv ${workloads} ${nodes} ${RESULTSDIR}/ycsb.tex

pdflatex -jobname=ycsb -output-directory=${RESULTSDIR} \
"\documentclass{article}\
 \usepackage{pgfplots}\
 \usepackage{xspace}\
 \newcommand{\Accord}{\textsc{Entente}\xspace}\
 \usepackage{tikz}\
 \usetikzlibrary{decorations.pathreplacing,positioning,automata,calc}\
 \usetikzlibrary{shapes,arrows}\
 \usepgflibrary{shapes.symbols}\
 \usetikzlibrary{shapes.symbols}\
 \usetikzlibrary{patterns}\
 \usetikzlibrary{matrix, positioning, pgfplots.groupplots}\
 \pgfplotsset{compat=1.17}\
 \begin{document}\
 \thispagestyle{empty}\centering\input{ycsb.tex}\
 \end{document}" > /dev/null
