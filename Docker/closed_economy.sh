#!/usr/bin/env bash

# Closed Economy experiment (aka., YCSB-T).
# This workload models a banking scenario similar to TPC-B where transactions transfer money between accounts.

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

mkdir -p ${LOGDIR}/closed_economy
mkdir -p ${RESULTSDIR}/closed_economy

workload_type="site.ycsb.workloads.ClosedEconomyWorkload"
workload="ce"
protocols="accord cockroachdb cockroachdb-opt" # only backends to support this
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi
node_counts="3 5 7"
replication_factor=3
records=$(config records)
threads=$(config threads)
single_client_threads=1  # 1 thread/DC for tracing+breakdown; second set uses threads from config
ops_per_thread=0

original_machine=$(config machine)
original_maxexecutiontime=$(config maxexecutiontime)
original_fix_lh=$(config "cockroachdb.fix_lease_holder")
restore_settings() {
    sed -i "s/^machine=.*/machine=${original_machine}/" "${CONFIG_FILE}"
    sed -i "s/^maxexecutiontime=.*/maxexecutiontime=${original_maxexecutiontime}/" "${CONFIG_FILE}"
    sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=${original_fix_lh}/" "${CONFIG_FILE}"
}
trap restore_settings EXIT

if [ "$test_run" -eq 1 ]; then
    sed -i "s/^maxexecutiontime=.*/maxexecutiontime=60/" "${CONFIG_FILE}"
fi

maxexecutiontime=$(config maxexecutiontime)

if [ "$dry_run" -eq 0 ]; then
    # Write CSV header for breakdown results
    echo "protocol,nodes,city,fast_commit,slow_commit,commit,ordering,execution" > ${RESULTSDIR}/closed_economy/breakdown.csv

    # ---- Phase 1: single-client runs (1 thread/DC) – tracing enabled, breakdown collected ----
    for p in ${protocols}
    do
        # Set fix_lease_holder based on CockroachDB flavor:
        #   cockroachdb-opt → lease holder pinned at geographically optimal location
        #   all others (cockroachdb, accord) → default settings
        if [[ "$p" == "cockroachdb-opt" ]]; then
            sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=true/" "${CONFIG_FILE}"
        else
            sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=false/" "${CONFIG_FILE}"
        fi

        # clean prior logs
        rm -f ${LOGDIR}/closed_economy/*${p}*
        
        for nodes in ${node_counts}
        do
	    if [ "$test_run" -eq 1 ]; then
	        compute_test_machine "${nodes}"
	    fi
	    ts=$(date +%Y%m%d%H%M%S%N)
	    output_file="${LOGDIR}/closed_economy/${p}_${nodes}_${workload}_${ts}.dat"

	    # Enable tracing for CockroachDB (both flavors) so breakdown data can be collected
	    tracing_opts=()
	    if [[ "$p" == cockroachdb* ]]; then
	        tracing_opts=("-p" "db.tracing=true")
	    fi

	    # Run benchmark without cluster cleanup so breakdown scripts can query the cluster
	    run_benchmark ${p} ${single_client_threads} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((single_client_threads * ops_per_thread)) ${output_file} 1 0 "${tracing_opts[@]}" -p maxexecutiontime=${maxexecutiontime}

	    # Collect city names for this node count
	    cities_list=""
	    for i in $(seq 1 ${nodes}); do
	        loc=$(get_location $i ${DIR}/latencies.csv)
	        cities_list="${cities_list} ${loc}"
	    done

	    # Compute performance breakdown and append to CSV
	    if [[ "$p" == cockroachdb* ]]; then
	        python3 ${DIR}/cockroachdb/cockroachdb_breakdown.py \
	            ${p} ${LOGDIR}/closed_economy ${workload} ${nodes} ${cities_list} | \
	            awk -F',' -v n="${nodes}" -v proto="${p}" '{print proto "," n "," $0}' >> ${RESULTSDIR}/closed_economy/breakdown.csv
	    elif [ "$p" == "accord" ]; then
	        compute_breakdown ${nodes} accord | \
	            awk -F',' '{
	                # Field mapping from cassandra_breakdown.sh output:
	                # $1=city, $2=fast_commit, $3=slow_commit, $4=commit (weighted avg), $5=execution
	                # ordering is 0 for accord (commit serves as the ordering phase)
	                print $1","$2","$3","$4",0,"$5
	            }' | \
	            awk -F',' -v n="${nodes}" '{print "accord," n "," $0}' >> ${RESULTSDIR}/closed_economy/breakdown.csv
	    fi

	    # Clean up cluster after breakdown is computed
	    stop_benchmark ${p} ${nodes}
        done
    done

    # ---- Phase 2: multi-client runs (default threads from exp.config) – no tracing, no breakdown ----
    mkdir -p ${LOGDIR}/closed_economy_multi
    for p in ${protocols}
    do
        if [[ "$p" == "cockroachdb-opt" ]]; then
            sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=true/" "${CONFIG_FILE}"
        else
            sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=false/" "${CONFIG_FILE}"
        fi

        # clean prior multi-client logs for this protocol
        rm -f ${LOGDIR}/closed_economy_multi/*${p}*

        for nodes in ${node_counts}
        do
	    if [ "$test_run" -eq 1 ]; then
	        compute_test_machine "${nodes}"
	    fi
	    ts=$(date +%Y%m%d%H%M%S%N)
	    output_file="${LOGDIR}/closed_economy_multi/${p}_${nodes}_${workload}_${ts}.dat"

	    # No tracing; run with default thread count and clean up automatically
	    run_benchmark ${p} ${threads} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((threads * ops_per_thread)) ${output_file} 1 1 -p maxexecutiontime=${maxexecutiontime}
        done
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh \
    $(ls ${LOGDIR}/closed_economy/*.dat 2>/dev/null) \
    $(ls ${LOGDIR}/closed_economy_multi/*.dat 2>/dev/null) \
    > ${RESULTSDIR}/closed_economy.csv

debug "Plotting..."
python3 ${DIR}/closed_economy.py ${RESULTSDIR}/closed_economy.csv ${RESULTSDIR}/closed_economy/breakdown.csv ${RESULTSDIR}/closed_economy.tex ${threads}

pdflatex -jobname=closed_economy -output-directory=${RESULTSDIR} \
"\documentclass{article}\
 \usepackage{pgfplots}\
 \usepackage{tikz}\
 \usepackage{amssymb}\
 \usepackage{wasysym}\
 \usetikzlibrary{decorations.pathreplacing,positioning,automata,calc}\
 \usetikzlibrary{shapes,arrows}\
 \usepgflibrary{shapes.symbols}\
 \usetikzlibrary{shapes.symbols}\
 \usetikzlibrary{patterns}\
 \usetikzlibrary{matrix, positioning, pgfplots.groupplots}\
 \pgfplotsset{compat=1.17}\
 \begin{document}\
 \thispagestyle{empty}\centering\input{closed_economy.tex}\
 \end{document}" > /dev/null
