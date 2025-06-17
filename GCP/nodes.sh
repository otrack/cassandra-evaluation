#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh

for id in $(ids); do
    cluster=${CLUSTERS[id]}
    kubectl --context=${cluster} get nodes --no-headers &
done
wait_jobs
