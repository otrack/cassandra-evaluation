#!/usr/bin/env bash

# Fault-tolerance experiment (mimics Figure 6 in CockroachDB SIGMOD'20 paper).
# The experiment runs a YCSB workload for X minutes.
# At X/4 : adds 400ms latency on database-node1 (slowdown event).
# At 3X/4: kills database-node1 (crash event).
# Plots aggregated YCSB throughput over time.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

clean_logdir

# Configuration
duration_minutes=${DURATION_MINUTES:-12}    # X: total duration in minutes (configurable)
protocol="cockroachdb"
nodes=3
workload_type="site.ycsb.workloads.CoreWorkload"
workload="a"
records=1000
threads=4
status_interval=5   # YCSB -s reporting interval in seconds

duration_s=$((duration_minutes * 60))
slowdown_s=$((duration_s / 4))
crash_s=$((3 * duration_s / 4))

ts=$(date +%Y%m%d%H%M%S%N)
output_file="${LOGDIR}/${protocol}_${nodes}_${workload}_${ts}.dat"

# Determine cluster prefix
pref=cassandra
if printf '%s\n' "$protocol" | grep -wF -q -- "cockroachdb"; then
    pref=cockroachdb
elif printf '%s\n' "$protocol" | grep -wF -q -- "swiftpaxos"; then
    pref=swiftpaxos
fi

log "Fault-tolerance experiment: ${duration_minutes}min total"
log "  Slowdown (+400ms latency on node1) at ${slowdown_s}s"
log "  Crash (docker kill node1) at ${crash_s}s"

# Start cluster and load data
init_logdir
start_network
${pref}_start_cluster "${nodes}" "${protocol}"

node_count=$(${pref}_get_node_count)
hosts=$(${pref}_get_hosts "${node_count}")
port=$(${pref}_get_port)

if [ -z "$hosts" ]; then
    error "Failed to retrieve IP addresses."
    exit 1
fi

# Load YCSB data
nearby_database=$(config "node_name")1
run_ycsb "load" "${workload_type}" "${workload}" "${hosts}" "${port}" \
    "${records}" "${records}" "${protocol}" \
    "${output_file%.dat}.load" "1" "ycsb" "${nearby_database}"
wait_container "ycsb"

# Emulate WAN latency
log "Emulating latency for ${node_count} node(s)..."
emulate_latency "${node_count}"

# Start YCSB run clients from each node (time-bounded via maxexecutiontime)
for i in $(seq 1 ${node_count}); do
    nearby_database=$(config "node_name")${i}
    location=$(get_location $i ${DIR}/latencies.csv)
    run_ycsb "run" "${workload_type}" "${workload}" "${hosts}" "${port}" \
        "${records}" 999999999 "${protocol}" \
        "${output_file%.dat}_${location}.dat" "${threads}" "ycsb-${i}" "${nearby_database}" \
        -p maxexecutiontime=${duration_s} \
        -p status.interval=${status_interval}
done

# Event 1: at X/4, add 400ms latency to database-node1 outbound traffic
(
    sleep ${slowdown_s}
    node1=$(config "node_name")1
    log "Event 1 @ ${slowdown_s}s: Adding 400ms latency to ${node1}"
    docker exec "${node1}" tc qdisc del dev eth0 root 2>/dev/null || true
    docker exec "${node1}" tc qdisc add dev eth0 root netem delay 400ms
) &
event1_pid=$!

# Event 2: at 3X/4, kill database-node1
(
    sleep ${crash_s}
    node1=$(config "node_name")1
    log "Event 2 @ ${crash_s}s: Killing ${node1}"
    docker kill "${node1}"
) &
event2_pid=$!

# Wait for all YCSB clients to complete
for i in $(seq 1 ${node_count}); do
    wait_container "ycsb-${i}"
done

wait ${event1_pid} ${event2_pid} 2>/dev/null || true

# Cleanup - node1 may already be gone (docker kill + --rm)
for i in $(seq 1 ${node_count}); do
    docker stop "$(config 'node_name')${i}" 2>/dev/null || true
done
stop_network

# Plot results
debug "Plotting..."
python3 ${DIR}/fault_tolerance.py \
    "${LOGDIR}" \
    "${protocol}" \
    "${duration_s}" \
    "${slowdown_s}" \
    "${crash_s}" \
    "${RESULTSDIR}/fault_tolerance.tex"

pdflatex -jobname=fault_tolerance -output-directory=${RESULTSDIR} \
"\documentclass{article}\
 \usepackage{pgfplots}\
 \usepackage{tikz}\
 \usetikzlibrary{decorations.pathreplacing,positioning,automata,calc}\
 \usetikzlibrary{shapes,arrows}\
 \usepgflibrary{shapes.symbols}\
 \usetikzlibrary{shapes.symbols}\
 \usetikzlibrary{patterns}\
 \usetikzlibrary{matrix, positioning, pgfplots.groupplots}\
 \begin{document}\
 \thispagestyle{empty}\centering\input{fault_tolerance.tex}\
 \end{document}" > /dev/null
