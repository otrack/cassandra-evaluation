#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

workloads="a b c d e f"
protocols="quorum paxos accord"

for p in ${protocols}
do
    for w in ${workloads}
    do
	./run_benchmarks.sh ${p} 1 3 3 ${w} 1 100
    done
done

