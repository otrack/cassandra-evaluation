#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh

schedule() {
    local cluster=$1
    let min=3

    let count=$(kubectl --context ${cluster} describe pods  | grep "Node:" | awk '{ print $2 }' | cut -d/ -f1 | sort -u | wc -l | xargs)
    
    if [[ ${count} -gt 0 && ${count} -lt ${min} ]]; then
        echo "${cluster}: only found ${count} of ${min}"
    fi
}

for cluster in $(unique_clusters); do
    schedule ${cluster} &
done
wait
