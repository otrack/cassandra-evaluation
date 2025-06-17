#!/bin/bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh

for cluster in ${CLUSTERS[@]}; do
    pod_name=$(kubectl --context="${cluster}" get pods |
        grep client |
        awk '{ print $1 }')

    echo "${cluster} -> ${pod_name}"
    kubectl --context=${cluster} exec ${pod_name} cat logs/c_1.txt | tail
done
