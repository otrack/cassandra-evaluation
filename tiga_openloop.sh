#!/usr/bin/env bash

# Tiga open-loop evaluation.
#
# Drives only Tiga with the Swap workload under an open-loop (rate-limited)
# pump built into the ycsb_jni binding, so the observed latency distribution is
# independent of the closed-loop YCSB client.  The cluster is deployed and
# loaded once; each configured rate then runs one single-threaded YCSB client
# per DC whose native pump submits transactions at the given rate, bounded by
# maxOutstanding (= 2 x rate) in-flight requests.
#
# The per-rate client logs (LOGDIR/tiga_openloop/tiga_<nodes>_r<rate>_sw_*.dat)
# carry the pump's raw [OL] latency samples, its [OL-SUMMARY], and fresh
# per-rate [COMMIT-PROBE] counters, all parsed by parse_openloop_tiga.sh.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

usage() {
    echo "Usage: $0 [--dry-run] [--test] [--rates=LIST] [--seconds=N] [--nodesperdc=N] [--fieldlength=N] [--s=N]"
    echo "  --dry-run         Skip the experiment run; only parse existing data and print CSV."
    echo "  --test            Use 10s rates and right-size containers to fit this machine."
    echo "  --rates=LIST      Comma-separated submission rates in txns/sec (default: 120,200,267,400,800)."
    echo "  --seconds=N       Pump duration per rate (default: 60; 10 in --test)."
    echo "  --nodesperdc=N    Nodes per DC (default: from exp.config)."
    echo "  --fieldlength=N   Value size in bytes for load (default: from exp.config)."
    echo "  --s=N             Swap size, items per transaction (default: 3)."
}

dry_run=0
test_run=0
rates_override=""
seconds_override=""
nodesperdc_override=""
fieldlength_override=""
s_override=""
for arg in "$@"; do
    case "$arg" in
        --dry-run)
            dry_run=1
            ;;
        --test)
            test_run=1
            ;;
        --rates=*)
            rates_override=$(echo "${arg#*=}" | tr ',' ' ')
            ;;
        --seconds=*)
            seconds_override="${arg#*=}"
            ;;
        --nodesperdc=*)
            nodesperdc_override="${arg#*=}"
            ;;
        --fieldlength=*)
            fieldlength_override="${arg#*=}"
            ;;
        --s=*)
            s_override="${arg#*=}"
            ;;
        *)
            echo "Unknown parameter: $arg"
            usage
            exit 1
            ;;
    esac
done

if [ "$test_run" -eq 1 ]; then
    seconds=${seconds_override:-10}
    rates=${rates_override:-"200 300"}
else
    seconds=${seconds_override:-60}
    rates=${rates_override:-"1000 8000"}
fi
s=${s_override:-3}

mkdir -p ${LOGDIR}/tiga_openloop
mkdir -p ${RESULTSDIR}

workload_type="site.ycsb.workloads.SwapWorkload"
workload="sw"
protocol="tiga"
nodes=3
replication_factor=3
records=$(config records)
ops_per_thread=0
maxexecutiontime=$((${seconds} + 8))

original_machine=$(config machine)
original_maxexecutiontime=$(config maxexecutiontime)
original_nodesperdc=$(config "nodesperdc")
original_fieldlength=$(config fieldlength)

restore_test_settings() {
    sed -i "s/^machine=.*/machine=${original_machine}/" "${CONFIG_FILE}"
    sed -i "s/^maxexecutiontime=.*/maxexecutiontime=${original_maxexecutiontime}/" "${CONFIG_FILE}"
    sed -i "s/^nodesperdc=.*/nodesperdc=${original_nodesperdc}/" "${CONFIG_FILE}"
    sed -i "s/^fieldlength=.*/fieldlength=${original_fieldlength}/" "${CONFIG_FILE}"
}
trap restore_test_settings EXIT

if [ -n "$nodesperdc_override" ]; then
    sed -i "s/^nodesperdc=.*/nodesperdc=${nodesperdc_override}/" "${CONFIG_FILE}"
fi
if [ -n "$fieldlength_override" ]; then
    sed -i "s/^fieldlength=.*/fieldlength=${fieldlength_override}/" "${CONFIG_FILE}"
fi

if [ "$test_run" -eq 1 ]; then
    compute_test_machine "${nodes}"
fi

if [ "$dry_run" -eq 0 ]; then
    pull_images

    do_create_and_load=1
    for rate in ${rates}; do
        log "Open-loop run: rate=${rate} txn/s for ${seconds}s (s=${s}, nodesperdc=$(config nodesperdc), fieldlength=$(config fieldlength))"

        ts=$(date +%Y%m%d%H%M%S%N)
        output_file="${LOGDIR}/tiga_openloop/tiga_${nodes}_r${rate}_sw_${ts}.dat"

        # nthreads=1: a single YCSB worker per DC.  Its init() (the first to
        # observe tiga.openloop.rate) starts the native pump; swap() then
        # no-ops so the pump's own [OL] sampling is the only load source.
        run_benchmark ${protocol} 1 ${nodes} ${replication_factor} \
            ${workload_type} ${workload} ${records} $((1 * ops_per_thread)) \
            ${output_file} ${do_create_and_load} 0 \
            -p maxexecutiontime=${maxexecutiontime} \
            -p tiga.openloop.rate=${rate} \
            -p tiga.openloop.sec=${seconds} \
            -p tiga.openloop.recordCount=${records} \
            -p swap.s=${s}

        do_create_and_load=0
    done

    stop_benchmark ${protocol} ${nodes}
fi

debug "Parsing results..."
${DIR}/parse_openloop_tiga.sh \
    $(ls ${LOGDIR}/tiga_openloop/*.dat 2>/dev/null) \
    > ${RESULTSDIR}/tiga_openloop.csv
log "Wrote ${RESULTSDIR}/tiga_openloop.csv"
