#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

workloads="a b c d e f"
protocols="quorum paxos accord"
clients="1 20 40"
records=1000000
operations=1000

# workloads="a b c d e f"
# protocols="quorum"
# clients="1 10"
# records=1
# operations=1

# Function to clean up Cassandra cluster
cleanup_cluster() {
    log "Cleaning up Cassandra cluster..."
    python3 cleanup_cassandra_cluster.py
    if [ $? -ne 0 ]; then
        error "Failed to clean up Cassandra cluster."
        exit 1
    fi
}

for p in ${protocols}
do
    for w in ${workloads}
    do
	do_load=1
	for c in ${clients}
	do
	    ./run_benchmarks.sh ${p} 12 3 3 ${w} ${records} ${operations} ${do_load}
	    do_load=0
	done
    done
    cleanup_cluster
done

