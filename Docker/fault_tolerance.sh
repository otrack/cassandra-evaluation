#!/usr/bin/env bash

# Fault-tolerance experiment (mimics Figure 6 in CockroachDB SIGMOD'20 paper).
# The experiment runs a YCSB workload for X minutes.
# At X/4     : adds 400ms latency on database-node1 (slowdown event).
# At X/4+X/8 : removes the slowdown (restores normal operation).
# At 3X/4    : kills database-node1 (crash event).
# Plots aggregated YCSB throughput over time for each protocol.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh
source ${DIR}/run_benchmarks.sh

usage() {
    echo "Usage: $0 [--dry-run] [--test] [--protocols=LIST]"
    echo "  --dry-run        Skip the experiment run; only draw plots using existing data."
    echo "  --test           Use a 120s run time and right-size containers to fit this machine."
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

mkdir -p ${LOGDIR}/fault_tolerance

# Configuration
duration_minutes=${DURATION_MINUTES:-12}    # X: total duration in minutes (configurable)
protocols="accord cockroachdb-opt"
if [ -n "$protocols_override" ]; then
    protocols="$protocols_override"
fi
nodes=5
replication_factor=$nodes
workload_type="site.ycsb.workloads.ClosedEconomyWorkload"
workload="ce"
records=$(config records)
threads=100
status_interval=1   # YCSB -s reporting interval in seconds

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
    duration_minutes=3
    original_machine=$(config machine)
    compute_test_machine "${nodes}"
fi

# set a single lease at the optimal location
sed -i "s/^cockroachdb\.fix_lease_holder=.*/cockroachdb.fix_lease_holder=true/" "${CONFIG_FILE}"

duration_s=$((duration_minutes * 60))
slowdown_s=$((duration_s / 4))
slowdown_end_s=$((duration_s / 8))
crash_s=$((3 * duration_s / 8))

log "Fault-tolerance experiment: ${duration_minutes}min total"
log "  Slowdown (+400ms latency on the leader) from ${slowdown_s}s to ${slowdown_end_s}s"
log "  Crash (SIGWAIT the leader) at ${crash_s}s"

if [ "$dry_run" -eq 0 ]; then
    pull_images
    for protocol in ${protocols}; do
        
        # clean prior logs
        rm -f ${LOGDIR}/fault_tolerance/*${protocol}*
        
        ts=$(date +%Y%m%d%H%M%S%N)
        output_file="${LOGDIR}/fault_tolerance/${protocol}_${nodes}_${workload}_${ts}.dat"

        # Determine cluster prefix
        pref=cassandra
        if printf '%s\n' "$protocol" | grep -wF -q -- "cockroachdb"; then
            pref=cockroachdb
        elif printf '%s\n' "$protocol" | grep -wF -q -- "swiftpaxos"; then
            pref=swiftpaxos
        fi

        log "Running fault-tolerance experiment for protocol: ${protocol}"

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
            "${records}" "${records}" "${protocol}" "${replication_factor}" \
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
                "${records}" 0 "${protocol}" "${replication_factor}" \
                "${output_file%.dat}_${location}.dat" "${threads}" "ycsb-${i}" "${nearby_database}" \
                -p maxexecutiontime=${duration_s} \
                -p status.interval=${status_interval} \
		-p warmupexecutiontime=10 &
        done

	# 2. Fetch the leader in the background (as it can be long...)
	tmp_file=$(mktemp)
	{
	    sleep 30 # wait that the system stabilizes
	    leader=$(${pref}_get_leaders "${protocol}" | head -n 1)
	    log "Chosen leader is ${leader}"
	    echo "$leader" > "$tmp_file"
	} &
	leader_pid=$!

        # Event 1: at X/4, add 400ms latency to (some) leader outbound traffic
        sleep ${slowdown_s}
	wait $leader_pid; leader=$(cat "$tmp_file"); rm "$tmp_file"
        # Save the leader's current tc policies before injecting the slowdown so
        # they can be restored when the slowdown disappears.
        docker exec "${leader}" bash -c \
            "tc qdisc show dev eth0 > /tmp/tc_qdisc_save.txt && \
             tc filter show dev eth0 2>/dev/null > /tmp/tc_filter_save.txt" \
            || log "Warning: failed to save tc policies for ${leader}; restore after slowdown may be incomplete"
        log "Event 1 @ ${slowdown_s}s: Adding 400ms latency to ${leader}"
        docker exec "${leader}" tc qdisc del dev eth0 root 2>/dev/null || true
        docker exec "${leader}" tc qdisc add dev eth0 root netem delay 400ms

        # Event 1b: at X/4+X/8, remove the slowdown from leader and restore the
        # tc policies that were in effect before the slowdown was injected.
        sleep ${slowdown_end_s}
        log "Event 1b @t1 = t0 + ${slowdown_end_s}s: Removing slowdown from ${leader} and restoring tc policies"
        docker exec "${leader}" tc qdisc del dev eth0 root 2>/dev/null || true
        python3 ${DIR}/restore_tc.py "${leader}"

        # Event 2: at 3X/4, suspend leader (to mimick an actual crash)
        sleep ${crash_s}
        log "Event 2 @t2 = t1 + ${crash_s}s: Killing ${leader}"
        docker kill --signal=19 ${leader}

        # Wait for all YCSB clients to complete
        for i in $(seq 1 ${node_count}); do
            wait_container "ycsb-${i}"
        done

        # Cleanup
        for i in $(seq 1 ${node_count}); do
            docker stop "$(config 'node_name')${i}" 2>/dev/null || true
        done
        docker stop "swiftpaxos-master" 2>/dev/null || true
        stop_network

    done
fi

# Plot results for all protocols
debug "Plotting... (duration=${duration_s}, slowdown=${slowdown_s}, slowdown_end=${slowdown_end_s}, crash=${crash_s})"
python3 ${DIR}/fault_tolerance.py \
    "${LOGDIR}/fault_tolerance" \
    ${protocols} \
    "${duration_s}" \
    "$((slowdown_s - 10))" \
    "$((slowdown_s + slowdown_end_s - 10))" \
    "$((slowdown_s + slowdown_end_s + crash_s - 10))" \
    "${RESULTSDIR}/fault_tolerance.tex"

pdflatex -jobname=fault_tolerance -output-directory=${RESULTSDIR} \
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
 \begin{document}\
 \thispagestyle{empty}\centering\input{fault_tolerance.tex}\
 \end{document}" > /dev/null
