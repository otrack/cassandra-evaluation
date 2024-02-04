#!/bin/bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/protocol-function.sh

batch_wait=0
opt_delivery=true
pure_optimization=false
write=100
ncmd=1000
sclusters=${#CLUSTERS[@]}

# FAST PATH PLOT:
# protocols=("vcd" "epaxos")
# max_faults=(2) # for n = 5
# max_faults=(3) # for n = 7
# nclients=(1)
# conflicts=(0 2 5 10 20 40 60 80 100)
# ncmd=500
# init_log_dir "fast_path/$(config clusters)"

# LATENCY PENALTY PLOT:
# CHANGE PAYLOAD_SIZE!!!
# protocols=("vcd" "paxos" "epaxos" "mencius")
# max_faults=(1 2)
# nclients=(128)
# conflicts=(1)
# ncmd=1000
# init_log_dir "latency-penalty-3000/$(config clusters)"

# TPUT LAT PLOT
# CHANGE PAYLOAD_SIZE!!!
# protocols=("vcd" "epaxos" "paxos")
# max_faults=(1 2)
# nclients=(8 16 32 64 128 256 512)
# # nclients=(512 256 128 64 32 16 8)
# conflicts=(10 100)
# ncmd=500
# init_log_dir "tput-latency-3000/$(config clusters)"

# vcdf2 was run with 3K
# while the other were run with 4K

# CHAINS
# protocols=("vcd")
# nclients=(100)
# conflicts=(10)
# max_faults=(1)
# ncmd=500
# init_log_dir "chains/$(config clusters)"
# init_log_dir "chains-union/$(config clusters)"

# EXECUTION PLOT:
# ncmd=1000
# opt_ds=(false true)
# nclients=(128)
# conflicts=(10)
# protocols=("vcd" "epaxos")
# max_faults=(1)

# report
# protocols=("vcd")
# max_faults=(1 2)
# conflicts=(5 20)
# nclients=(64)
# ncmd=300
# init_log_dir "queue-report/$(config clusters)"

cleanup() {
    ${DIR}/pods-stop.sh
    rm -f ${vcd_template_dir}/tmp.*
    rm -f ${epaxos_template_dir}/tmp.*
}

do_run() {
    local protocol=$1
    local nclient=$2
    local max_faults=$3
    local conflict=$4

    protocol_start ${protocol} ${max_faults} ${nclient} ${conflict} ${batch_wait} ${opt_delivery} ${pure_optimization}
    clients_start ${protocol} ${max_faults} ${nclient} ${conflict} ${batch_wait} ${opt_delivery} ${write} ${ncmd}
    clients_wait ${protocol} ${max_faults} ${nclient} ${conflict} ${batch_wait} ${write}
    protocol_stop ${protocol}
}

do_run_all_conflicts() {
    local protocol=$1
    local nclient=$2
    local max_faults=$3

    for conflict in ${conflicts[@]}; do
        do_run ${protocol} ${nclient} ${max_faults} ${conflict}
    done
}

trap "cleanup; exit 255" SIGINT SIGTERM

for nclient in ${nclients[@]}; do
    for protocol in ${protocols[@]}; do
        case ${protocol} in
        mencius)
            do_run ${protocol} ${nclient} undef 100
            ;;
        epaxos)
            do_run_all_conflicts ${protocol} ${nclient} 0
            ;;
        paxos)
            for f in ${max_faults[@]}; do
                if [[ ${sclusters} == 3 && ${f} == 2 ]]; then
                    continue
                else
                    do_run ${protocol} ${nclient} ${f} 100
                fi
            done
            ;;
        vcd)
            for f in ${max_faults[@]}; do
                if [[ ${sclusters} == 3 && ${f} == 2 ]]; then
                    continue
                else
                    do_run_all_conflicts ${protocol} ${nclient} ${f}
                fi
            done
            ;;
        esac
    done
done

# paplay /usr/share/sounds/freedesktop/stereo/complete.oga
