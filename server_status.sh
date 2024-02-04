#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh

function protocol() {
    if [ $# -ne 2 ]; then
        echo "usage: protocol cluster pod"
        exit -1
    fi

    local cluster=$1
    local pod=$2
    kubectl --context=${cluster} describe pod ${pod} 2>/dev/null |
        grep PROTOCOL |
        cut -d: -f2 |
        xargs
}

function just_do_it() {
    if [ $# -ne 1 ]; then
        echo "usage: just_do_it id"
        exit -1
    fi

    local id=$1
    local cluster=${CLUSTERS[id]}
    local pod=server-${id}
    local protocol=$(protocol ${cluster} ${pod})

    case ${protocol} in
    vcd | epaxos)
        lines=$(kubectl --context=${cluster} logs ${pod} |
            grep -E "(TCP|recovery)" |
            grep -v "initialized" |
            grep -v "queue len" |
            grep -v "TCP client socket")
        ;;
    *)
        exit -1
        ;;
    esac

    echo "${cluster} -> ${lines}"
}

for id in $(ids); do
    just_do_it ${id} &
done
wait_jobs
