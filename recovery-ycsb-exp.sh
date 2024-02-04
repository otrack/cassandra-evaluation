#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/protocol-function.sh
source ${DIR}/ycsb-function.sh

max_faults=1
lread=false
verbose=false
batch_wait=0
opt_delivery=true

threads=128
protocols=("vcd" "epaxos" "paxos")
workload="a"
operationcount=100000
recordcount=1000000
extra_args="-p target 300"

# ignore the previous computation
wait_before_crash=20

# id of the site to crash
site_id=1

cleanup() {
    ${DIR}/pods-stop.sh
    rm -f ${vcd_template_dir}/tmp.*
    rm -f ${epaxos_template_dir}/tmp.*
    rm -f ${ycsb_template_dir}/tmp.*
}

trap "cleanup; exit 255" SIGINT SIGTERM

wait_started() {
    while true; do
        for id in $(ids); do
            local cluster=${CLUSTERS[id]}
            local pod_name=$(k8s_pod_name ${ycsb_template}-${id})
            local started=$(kubectl --context=${context} logs ${pod_name} 2>/dev/null |
                grep -E "operations; [0-9\.]+ current" |
                wc -l |
                xargs echo)
            if [[ ${started} -gt 0 ]]; then
                return
            fi
        done
    done
}

for protocol in ${protocols[@]}; do
    protocol_start ${protocol} ${lread} ${verbose} ${max_faults} 1 100 ${batch_wait} ${opt_delivery} "true"
    ycsb_start ${protocol} "run" ${workload} ${threads} ${lread} ${operationcount} ${recordcount} "${extra_args}"

    wait_started
    log "ycsb clients running..."
    sleep ${wait_before_crash}
    node_crash ${protocol} ${site_id}

    ycsb_wait ${protocol} "run" ${workload} ${threads} ${lread} ${batch_wait}
    protocol_stop ${protocol}
done
