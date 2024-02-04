#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh
lines_to_show=5

for id in $(ids); do
    cluster=${CLUSTERS[id]}
    pod=client-${id}

    batch_size=$(kubectl --context=${cluster} logs ${pod} 2>/dev/null |
        grep -A 3 batchSize |
        grep mean |
        tail -n 1 |
        awk ' { print $3 }')

    echo "${pod} -> ${batch_size}"
    echo
done
