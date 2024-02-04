#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh

cluster_list() {
    local cluster=$1
    local list_log_file=.${cluster}-list.log

    echo "    >>>> ${cluster}" >${list_log_file}
    kubectl --context="${cluster}" get all 2>&1 | grep -v service/kubernetes | grep -v "NAME" >>${list_log_file}
    cat ${list_log_file} | sed '$d' # prune the last line
    rm ${list_log_file}
}

for cluster in $(unique_clusters); do
    cluster_list ${cluster} &
done
wait
