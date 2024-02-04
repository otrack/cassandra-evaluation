#!/bin/bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/protocol-function.sh

# fixed
batch_wait=0
opt_ds=(true false)
pure_optimization=false
write=100

# config
ncmd=800
max_faults=1
nclient=128

# ignore the previous computation
wait_before_crash=30

# id of the site to crash
site_id=2

conflicts=(142)
protocols=("paxos" "vcdf1")

# DONT FORGET
# setting us-east1-b europe-north1-b asia-east1-b
# set paxos leader to: asia-east1-b

init_log_dir "recovery/$(config clusters)"

cleanup() {
    ${DIR}/pods-stop.sh
    rm -f ${vcd_template_dir}/tmp.*
    rm -f ${epaxos_template_dir}/tmp.*
}

trap "cleanup; exit 255" SIGINT SIGTERM

just_do_it() {
    local protocol=$1
    local conflict=$2
    local opt_delivery=$3

    protocol_start ${protocol} ${max_faults} ${nclient} ${conflict} ${batch_wait} ${opt_delivery} ${pure_optimization}
    clients_start ${protocol} ${max_faults} ${nclient} ${conflict} ${batch_wait} ${opt_delivery} ${write} ${ncmd}
    sleep ${wait_before_crash}
    node_crash ${protocol} ${site_id}
    clients_wait ${protocol} ${max_faults} ${nclient} ${conflict} ${batch_wait} ${write}
    protocol_stop ${protocol}
}

for protocol in ${protocols[@]}; do
    case ${protocol} in
    paxos)
        just_do_it ${protocol} 100 true
        ;;
    epaxos)
        for conflict in ${conflicts[@]}; do
            just_do_it ${protocol} ${conflict} true
        done
        ;;
    vcd)
        for conflict in ${conflicts[@]}; do
            for opt_delivery in ${opt_ds[@]}; do
                just_do_it ${protocol} ${conflict} ${opt_delivery}
            done
        done
        ;;
    esac
done

log "will sleep forever"
while true; do sleep 10000; done
