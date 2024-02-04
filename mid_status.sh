#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh

for id in $(ids); do
    cluster=${CLUSTERS[id]}
    pod=client-${id}

    mid=$(kubectl --context=${cluster} logs ${pod} 2>/dev/null |
        grep -A2 midExecution |
        grep mean |
        tail -n 1 |
        cut -d= -f2 |
        xargs echo)

    echo "${pod} -> ${mid}"
    echo
done
