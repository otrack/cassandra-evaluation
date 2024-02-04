#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/protocol-function.sh

batch_wait=0
opt_delivery=true
pure_optimization=false
write=100
ncmd=500

tclients=1000
sclusters=(13 11 9 7 5 3)
cclusters=13

# calculate number of clients per site
nclient=$((tclients / cclusters))

protocols=("vcd" "paxos" "epaxos" "mencius")
max_faults=(1 2)
conflicts=(2)

protocols=("mencius")

cleanup() {
    ${DIR}/pods-stop.sh
    rm -f ${vcd_template_dir}/tmp.*
    rm -f ${epaxos_template_dir}/tmp.*
}

do_run() {
    local scluster=$1
    local protocol=$2
    local max_faults=$3
    local conflict=$4

    # init log dir
    init_log_dir "large/${scluster}_${cclusters}"

    # start servers
    init_clusters ${scluster} false
    protocol_start ${protocol} ${max_faults} ${nclient} ${conflict} ${batch_wait} ${opt_delivery} ${pure_optimization}

    # start and stop clients
    init_clusters ${cclusters} false
    clients_start ${protocol} ${max_faults} ${nclient} ${conflict} ${batch_wait} ${opt_delivery} ${write} ${ncmd} ${scluster}
    clients_wait ${protocol} ${max_faults} ${nclient} ${conflict} ${batch_wait} ${write}

    # stop servers
    init_clusters ${scluster} false
    protocol_stop ${protocol}
}

do_run_all_conflicts() {
    local scluster=$1
    local protocol=$2
    local max_faults=$3

    for conflict in ${conflicts[@]}; do
        do_run ${scluster} ${protocol} ${max_faults} ${conflict}
    done
}

trap "cleanup; exit 255" SIGINT SIGTERM

for protocol in ${protocols[@]}; do
    for scluster in ${sclusters[@]}; do
        case ${protocol} in
        mencius)
            do_run ${scluster} ${protocol} undef 100
            ;;
        epaxos)
            do_run_all_conflicts ${scluster} ${protocol} 0
            ;;
        paxos)
            for f in ${max_faults[@]}; do
                if [[ ${f} == 1 && ${scluster} == 3 ]]; then
                    continue
                else
                    do_run ${scluster} ${protocol} ${f} 100
                fi
            done
            ;;
        vcd)
            for f in ${max_faults[@]}; do
                if [[ ${f} == 1 && ${scluster} == 3 ]]; then
                    continue
                else
                    do_run_all_conflicts ${scluster} ${protocol} ${f}
                fi
            done
            ;;
        esac
    done
done

# paplay /usr/share/sounds/freedesktop/stereo/complete.oga
