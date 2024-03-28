#!/bin/bash

EXP_DIR=$(dirname "${BASH_SOURCE[0]}")
source ${EXP_DIR}/plot-function.sh

sclusters=3
init_clusters ${sclusters} false
init_log_dir "ycsb/${sclusters}"

ycsb_lat 3 cassandra 0 ONE 10 a READ
ycsb_lat 3 cassandra 0 QUORUM 10 a READ

consistency="false"
threads=10
batch_wait=0
workloads=("a" "b" "c" "d" "e")
protocols=("cassandra" "accord")
operations=("READ" "INSERT" "UPDATE" "SCAN")

output=${EXP_DIR}/ycsb.dat

rm ${output}
for protocol in ${protocols[@]}; do
    for workload in ${workloads[@]}; do
	for op in ${operations[@]}; do    
            perf=$(ycsb_lat ${protocol} ${batch_wait} ${consistency} ${workload} ${threads})
            line=${protocol}" "${total}
	done	
    done	
    echo ${line} >>${output}
done

cat ${output}
