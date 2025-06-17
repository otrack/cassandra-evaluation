#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh

for id in $(ids); do
    cluster=${CLUSTERS[id]}
    pod=client-${id}
    kubectl --context=${cluster} top pod ${pod} | grep -v NAME &
done
wait_jobs
