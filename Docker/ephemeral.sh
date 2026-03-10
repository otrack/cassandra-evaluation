#!/usr/bin/env bash

# Ephemeral reads experiment.
# Illustrates the benefit of activating ephemeral reads in Accord.
# Runs YCSB workloads A to D with Accord only, comparing:
#   - accord.ephemeral_read_enabled=true  (ephemeral reads ON)
#   - accord.ephemeral_read_enabled=false (ephemeral reads OFF)
# Outputs a LaTeX table with the speed-up when ephemeral reads are enabled.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

usage() {
    echo "Usage: $0 [--dry-run]"
    echo "  --dry-run  Skip the experiment run; only draw plots using existing data."
}

dry_run=0
for arg in "$@"; do
    case "$arg" in
        --dry-run)
            dry_run=1
            ;;
        *)
            echo "Unknown parameter: $arg"
            usage
            exit 1
            ;;
    esac
done

mkdir -p ${LOGDIR}/ephemeral

workload_type="site.ycsb.workloads.CoreWorkload"
workloads="a b c d"
protocol="accord"
nodes=5
replication_factor=${nodes}
records=10000
threads=10
ops_per_thread=0

# Helper to update accord.ephemeral_read_enabled in exp.config
set_ephemeral_read() {
    local value=$1
    sed -i "s/^accord\.ephemeral_read_enabled=.*/accord.ephemeral_read_enabled=${value}/" ${CONFIG_FILE}
}

# Save original ephemeral setting and restore it on exit
original_ephemeral=$(config "accord.ephemeral_read_enabled")
restore_config() {
    set_ephemeral_read "${original_ephemeral:-true}"
}
trap restore_config EXIT

if [ "$dry_run" -eq 0 ]; then
    total=$(echo ${workloads} | wc -w)

    # --- Run with ephemeral reads ENABLED ---
    # Output files are named accord_<nodes>_<workload>_<ts>_<city>.dat so that
    # parse_ycsb_to_csv.sh labels them with protocol "accord".
    # Note: run_benchmark appends _<city>.dat per YCSB client, so the variable
    # below is only the base template passed to run_benchmark.
    set_ephemeral_read "true"
    rm -f ${LOGDIR}/ephemeral/accord_*
    do_create_and_load=1
    count=0
    for w in ${workloads}
    do
        do_clean_up=$(( count == total-1 ? 1 : 0 ))
        ts=$(date +%Y%m%d%H%M%S%N)
        output_file="${LOGDIR}/ephemeral/accord_${nodes}_${w}_${ts}.dat"
        run_benchmark ${protocol} ${threads} ${nodes} ${replication_factor} ${workload_type} ${w} ${records} $((threads * ops_per_thread)) ${output_file} ${do_create_and_load} ${do_clean_up} -p maxexecutiontime=600
        do_create_and_load=0
        count=$((count+1))
    done

    # --- Run with ephemeral reads DISABLED ---
    # Output files are named accord-noephem_<nodes>_<workload>_<ts>_<city>.dat so
    # that parse_ycsb_to_csv.sh labels them with protocol "accord-noephem".
    # Note: run_benchmark appends _<city>.dat per YCSB client, so the variable
    # below is only the base template passed to run_benchmark.
    set_ephemeral_read "false"
    rm -f ${LOGDIR}/ephemeral/accord-noephem_*
    do_create_and_load=1
    count=0
    for w in ${workloads}
    do
        do_clean_up=$(( count == total-1 ? 1 : 0 ))
        ts=$(date +%Y%m%d%H%M%S%N)
        output_file="${LOGDIR}/ephemeral/accord-noephem_${nodes}_${w}_${ts}.dat"
        run_benchmark ${protocol} ${threads} ${nodes} ${replication_factor} ${workload_type} ${w} ${records} $((threads * ops_per_thread)) ${output_file} ${do_create_and_load} ${do_clean_up} -p maxexecutiontime=600
        do_create_and_load=0
        count=$((count+1))
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh ${LOGDIR}/ephemeral/*.dat > ${RESULTSDIR}/ephemeral.csv

debug "Generating table..."
python3 ${DIR}/ephemeral.py ${RESULTSDIR}/ephemeral.csv ${workloads} ${nodes} ${RESULTSDIR}/ephemeral.tex

pdflatex -jobname=ephemeral -output-directory=${RESULTSDIR} \
"\documentclass{article}\
 \usepackage{booktabs}\
 \begin{document}\
 \thispagestyle{empty}\centering\input{ephemeral.tex}\
 \end{document}" > /dev/null
