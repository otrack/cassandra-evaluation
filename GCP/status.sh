#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh

function image() {
    if [ $# -ne 2 ]; then
        echo "usage: protocol cluster pod"
        exit -1
    fi

    local cluster=$1
    local pod=$2
    kubectl --context=${cluster} describe pod ${pod} 2>/dev/null |
        grep "Image:" |
        cut -d: -f2 |
        cut -d/ -f2
}

function just_do_it() {
    if [ $# -ne 1 ]; then
        echo "usage: just_do_it id"
        exit -1
    fi

    local id=$1
    local cluster=${CLUSTERS[id]}
    local pod=client-${id}
    local image=$(image ${cluster} ${pod})

    case ${image} in
    epaxos)
        op_number=$(kubectl --context=${cluster} exec ${pod} cat logs/c_1.txt 2>/dev/null |
            grep latency |
            wc -l |
            xargs)
        ;;
    vcd-java-client)
        op_number=$(kubectl --context=${cluster} logs ${pod} 2>/dev/null |
            grep -Eo "[0-9]+ of [0-9]+" |
            tail -n 1 |
            awk ' { print $1 }')
        ;;
    ycsb)
        op_number=$(kubectl --context=${cluster} logs ${pod} 2>/dev/null |
            grep -Eo "sec: [0-9]+ operations;" |
            grep -Eo "[0-9]+" |
            tail -n 1)
        ;;
    *)
        exit -1
        ;;
    esac

    echo "${cluster} -> ${op_number}"
}

for id in $(ids); do
    just_do_it ${id} &
done
wait_jobs
