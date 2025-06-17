#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh

for id in $(ids); do
    cluster=${CLUSTERS[id]}
    pod=server-${id}

    kubectl --context=${cluster} logs ${pod} 2>/dev/null |
        grep "Fast quorum" |
        tail -n 1 |
        grep -Eo "of [0-9]+ is \[[0-9,]+\]" |
        awk ' { print $2" -> " $4 }'
    echo
done
