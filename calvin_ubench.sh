#!/usr/bin/env bash

# Calvin micro-benchmark (Thomson et al., SIGMOD 2012, Section 6.2, Figure 5).
# Each transaction reads 10 records, one of them taken from a small pool of "hot"
# records and the others from the large pool of "cold" ones, checks that the sum
# of their counters is non-negative, and if so increments each counter.  The
# contention index (CI) is the fraction of the hot records a transaction
# accesses, i.e. 1/(number of hot records).  As in Figure 5, the experiment
# measures the throughput under low (CI=0.0001) and high (CI=0.01) contention.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

usage() {
    echo "Usage: $0 [--dry-run] [--test] [--protocols=LIST] [--nodesperdc=N] [--ci=LIST] [--clients=LIST] [--records=N]"
    echo "  --dry-run        Skip the experiment run; only parse existing data."
    echo "  --test           Small and short runs (20000 records, 10s, 10 clients/site),"
    echo "                   with containers right-sized to fit this machine."
    echo "  --protocols=LIST Override the list of protocols to run (comma-separated)."
    echo "  --nodesperdc=N   Override number of nodes per DC (default from exp.config)."
    echo "  --ci=LIST        Contention indexes to run (comma-separated, default: 0.0001,0.01)."
    echo "  --clients=LIST   Clients per site (comma-separated, default: threads in exp.config)."
    echo "  --records=N      Number of records (default: 1000000)."
}

dry_run=0
test_run=0
protocols_override=""
nodesperdc_override=""
ci_override=""
clients_override=""
records_override=""
for arg in "$@"; do
    case "$arg" in
        --dry-run)
            dry_run=1
            ;;
        --test)
            test_run=1
            ;;
        --protocols=*)
            protocols_override=$(echo "${arg#*=}" | tr ',' ' ')
            ;;
        --nodesperdc=*|--nodes-per-dc=*)
            nodesperdc_override="${arg#*=}"
            ;;
        --ci=*)
            ci_override=$(echo "${arg#*=}" | tr ',' ' ')
            ;;
        --clients=*)
            clients_override=$(echo "${arg#*=}" | tr ',' ' ')
            ;;
        --records=*)
            records_override="${arg#*=}"
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown parameter: $arg"
            usage
            exit 1
            ;;
    esac
done

mkdir -p ${LOGDIR}/calvin_ubench
mkdir -p ${RESULTSDIR}/calvin_ubench

workload_type="site.ycsb.workloads.CalvinWorkload"
workload="calvin"
# The transactional systems whose YCSB client implements checkAndIncrement
protocols="accord cockroachdb-opt tiga"
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi

# Parameters of the Calvin paper: 10 records per transaction, one of them hot
# (calvin.txnsize and calvin.hotfraction keep the defaults of the workload), a
# contention index of 0.0001 (low) or 0.01 (high), 1M records.
ci_values="0.0001 0.01"
threads=$(config threads)
client_counts="${threads}"
records=1000000
nodes=3
replication_factor=3
ops_per_thread=0

original_machine=$(config machine)
original_maxexecutiontime=$(config maxexecutiontime)
original_fix_lh=$(config "cockroachdb.fix_lease_holder")
original_nodesperdc=$(config "nodesperdc")

restore_settings() {
    sed -i "s/^machine=.*/machine=${original_machine}/" "${CONFIG_FILE}"
    sed -i "s/^maxexecutiontime=.*/maxexecutiontime=${original_maxexecutiontime}/" "${CONFIG_FILE}"
    sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=${original_fix_lh}/" "${CONFIG_FILE}"
    sed -i "s/^nodesperdc=.*/nodesperdc=${original_nodesperdc}/" "${CONFIG_FILE}"
}
trap restore_settings EXIT

if [ -n "$nodesperdc_override" ]; then
    sed -i "s/^nodesperdc=.*/nodesperdc=${nodesperdc_override}/" "${CONFIG_FILE}"
fi

if [ "$test_run" -eq 1 ]; then
    # The hot pool has 1/CI = 10000 records at CI=0.0001, and the cold pool
    # must still provide 9 records per transaction.
    records=20000
    client_counts="10"
    compute_test_machine "${nodes}"
    sed -i "s/^maxexecutiontime=.*/maxexecutiontime=10/" "${CONFIG_FILE}"
fi

[ -n "$ci_override" ] && ci_values="$ci_override"
[ -n "$clients_override" ] && client_counts="$clients_override"
[ -n "$records_override" ] && records="$records_override"

maxexecutiontime=$(config maxexecutiontime)

if [ "$dry_run" -eq 0 ]; then
    pull_images

    for p in ${protocols}
    do
        if [[ "$p" == "cockroachdb-opt" ]]; then
            sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=true/" "${CONFIG_FILE}"
        elif [[ "$p" == "cockroachdb-bad" ]]; then
            sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=bad/" "${CONFIG_FILE}"
        else
            sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=false/" "${CONFIG_FILE}"
        fi

        rm -f ${LOGDIR}/calvin_ubench/*${p}*

        # The data set does not depend on the contention index nor on the
        # number of clients: deploy and load it once per protocol.
        do_create_and_load=1
        for clients in ${client_counts}
        do
            for ci in ${ci_values}
            do
                ts=$(date +%Y%m%d%H%M%S%N)
                output_file="${LOGDIR}/calvin_ubench/${p}_${nodes}_${workload}_${ts}.dat"

                run_benchmark ${p} ${clients} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} $((clients * ops_per_thread)) ${output_file} ${do_create_and_load} 0 -p calvin.contentionindex=${ci} -p maxexecutiontime=${maxexecutiontime}

                do_create_and_load=0
            done
        done

        stop_benchmark ${p} ${nodes}
    done
fi

debug "Parsing results..."
${DIR}/parse_ycsb_to_csv.sh \
    $(ls ${LOGDIR}/calvin_ubench/*.dat 2>/dev/null) \
    > ${RESULTSDIR}/calvin_ubench.csv

# The plot shows the runs with the default number of clients per site
plot_threads=$(echo ${client_counts} | awk '{print $NF}')

debug "Plotting..."
python3 ${DIR}/calvin_ubench.py ${RESULTSDIR}/calvin_ubench.csv ${RESULTSDIR}/calvin_ubench.tex ${plot_threads}

pdflatex -interaction nonstopmode -jobname=calvin_ubench -output-directory=${RESULTSDIR} \
"\documentclass{article}\
 \usepackage{pgfplots}\
 \usepackage{tikz}\
 \usepackage{amssymb}\
 \usepackage{wasysym}\
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
 \thispagestyle{empty}\centering\input{calvin_ubench.tex}\
 \end{document}" > /dev/null
