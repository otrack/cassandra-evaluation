#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/protocol-function.sh
source ${DIR}/ycsb-function.sh

batch_wait=0
opt_delivery=true

threads_loading=10
threads=10
operations_per_thread=1000
operationcount=$(echo ${threads} ${operations_per_thread} | awk '{ print $1 * $2 }')
operationcount=1000
recordcount=1000

protocols=("accord")
# protocols=("cassandra")
max_faults=(0) 
workloads=("a" "b" "c" "d" "e")
# workloads=("c")
# consistencies=(ONE QUORUM SERIAL)
consistencies=(SERIAL)

sclusters=3
init_clusters ${sclusters} false
init_log_dir "ycsb/${sclusters}"

cleanup() {
    ${DIR}/pods-stop.sh client
    rm -f ${vcd_template_dir}/tmp.*
    rm -f ${epaxos_template_dir}/tmp.*
    rm -f ${ycsb_template_dir}/tmp.*
}

trap "cleanup; exit 255" SIGINT SIGTERM

do_run() {
    if [ $# -ne 3 ]; then
        echo "usage: do_run protocol max_faults consistency"
        exit -1
    fi

    local protocol=$1
    local max_faults=$2
    local consistency=$3
    
    local protocol_name=${protocol}
    if [[ "${protocol}" == "accord" ]]; then
        protocol_name="accord"
	protocol="cassandra"
    fi

    # since we don't know the actual conflict rate,
    # let's use the percentage of writes
    local conflicts=0
    case ${workload} in
    c)
        conflicts=0
        ;;
    a)
        conflicts=50
        ;;
    a2)
        conflicts=20
        ;;
    a3)
        conflicts=80
        ;;
    esac

    protocol_start ${protocol} ${max_faults} ${threads} ${conflicts} ${batch_wait} ${opt_delivery} ${consistency}        
    ycsb_start ${protocol} "load" "a" ${threads_loading} ${operationcount} ${recordcount} "ALL"

    for workload in ${workloads[@]};
    do
	ycsb_start ${protocol} "run" ${workload} ${threads} ${operationcount} ${recordcount} ${consistency}
	ycsb_wait ${protocol_name} "run" ${workload} ${threads} ${batch_wait} ${consistency}
	sleep 5
    done
    
    protocol_stop ${protocol}
}

do_run_all_consistencies() {
    if [ $# -ne 2 ]; then
        echo "usage: do_run_all_consistencies protocol max_faults"
        exit -1
    fi

    local protocol=$1
    local max_faults=$2

    for consistency in ${consistencies[@]}; do
        do_run ${protocol} ${max_faults} ${consistency}
    done
}

for protocol in ${protocols[@]}; do
    case ${protocol} in
	accord)
        do_run_all_consistencies ${protocol} 0
        ;;	    	    
    cassandra)
        do_run_all_consistencies ${protocol} 0
        ;;	    
    paxos)
        do_run_all_workloads ${protocol} 0 false
        ;;
    epaxos)
        do_run_all_consistencies ${protocol} 0
        ;;
    vcd)
        for f in ${max_faults[@]}; do
            do_run_all_consistencies ${protocol} ${f}
        done
        ;;
    esac
done

log "will sleep forever"
while true; do sleep 10000; done
