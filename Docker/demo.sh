#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

usage() {
    echo "Usage: $0 [--test] [--fast] [--protocols=LIST]"
    echo "  --test           Use a 60s run time and right-size containers to fit this machine."
    echo "  --fast           Disable slow-motion mode (enabled by default)."
    echo "  --protocols=LIST Override the list of protocols to run (space-separated)."
}

test_run=0
slow_motion=1
protocols_override=""
for arg in "$@"; do
    case "$arg" in
        --test)
            test_run=1
            ;;
        --slow)
            slow_motion=1
            ;;
        --fast)
            slow_motion=0
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

mkdir -p ${LOGDIR}/demo
mkdir -p ${RESULTSDIR}/demo

workload_type="site.ycsb.workloads.ClosedEconomyWorkload"
workload="ce"
protocols="accord"
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi
node_counts=5
replication_factor=5
records=3
threads=1

original_illustration=$(config "illustration")
restore_settings() {
    sed -i "s/^illustration=.*/illustration=${original_illustration}/" "${CONFIG_FILE}"
}
trap restore_settings EXIT

sed -i "s/^illustration=.*/illustration=true/" "${CONFIG_FILE}"

maxexecutiontime=60

# pull_images

for p in ${protocols}
do

    rm -f ${LOGDIR}/demo/*${p}*

    echo "Building and starting live visualization container..."
    docker build -t accord-live-viz ${DIR}/live-viz
    docker stop accord-viz >/dev/null 2>&1 || true
    docker rm accord-viz >/dev/null 2>&1 || true
    
    slow_env=""
    if [ "$slow_motion" -eq 1 ]; then
        slow_env="-e SLOW_MODE=true"
    fi

    docker run --rm -d --name accord-viz -p 3000:3000 $slow_env -v ${DIR}/logs/demo:/app/logs/demo -v ${DIR}/latencies.csv:/app/latencies.csv:ro --network $(config network_name) accord-live-viz
    echo "========================================================"
    echo "Live visualization running at http://localhost:3000"
    echo "========================================================"

    if command -v xdg-open > /dev/null; then
        xdg-open http://localhost:3000 > /dev/null 2>&1 &
    elif command -v open > /dev/null; then
        open http://localhost:3000 > /dev/null 2>&1 &
    fi

    for nodes in ${node_counts}
    do
	if [ "$test_run" -eq 1 ]; then
	    compute_test_machine "${nodes}"
	fi
	ts=$(date +%Y%m%d%H%M%S%N)
	output_file="${LOGDIR}/demo/${p}_${nodes}_${workload}_${ts}.dat"
	run_benchmark ${p} ${threads} ${nodes} ${replication_factor} ${workload_type} ${workload} ${records} 0 ${output_file} 1 1 -p maxexecutiontime=${maxexecutiontime} -p target=1 -p db.tracing=true
    done

    echo "Waiting for visualization to finish replaying all transactions..."
    docker wait accord-viz
done
