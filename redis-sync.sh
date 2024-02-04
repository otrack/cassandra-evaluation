#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh

cli=$(which redis-cli)
host=localhost
port=6379
PARALLEL=16

cmd() {
    ${cli} -h ${host} -p ${port} --raw "$@"
}

pull_log() {
    if [ $# -ne 1 ]; then
        echo "usage: pull_log key"
        exit -1
    fi
    local key=$1
    local output=${LOGDIR}/${key}
    rm -f ${output}

    info "fetching key ${key}"

    if [[ ${key} == *"path"* ]]; then
        local hkeys=$(cmd "hkeys" ${key})

        for hkey in ${hkeys}; do
            local hval=$(cmd "hget" ${key} ${hkey})
            echo "${hkey}-${hval}," >>${output}
        done
    elif [[ ${key} == *"trace"* ]]; then
        cmd "lrange" ${key} "0" "-1" >${output}
    else
        cmd "smembers" ${key} >${output}
    fi
}

pull_logs_parallel() {
    if [ $# -ne 2 ]; then
        echo "usage: pull_logs_parallel p_id keys"
        exit -1
    fi
    local p_id=$1
    local keys=$2

    local my_keys=$(echo "${keys}" |
        awk -v p_id=${p_id} -v par=${PARALLEL} '{if (NR % par == p_id) { print $1 } }'
    )
    for key in ${my_keys}; do
        pull_log ${key}
    done
}

pull_logs() {
    if [ $# -ne 1 ]; then
        echo "usage: pull_logs tunnel_pid"
        exit -1
    fi
    local tunnel_pid=$1

    # get all the redis keys
    # local keys=$(cmd "keys" "*" | grep -v Execution | grep -v path | grep -v Add | grep -v chains)
    local keys=$(cmd "keys" "*")

    local nkeys=$(echo ${keys} | awk ' { print NF }')
    info "found ${nkeys} keys in redis"

    for p in $(seq 0 $((${PARALLEL} - 1))); do
        pull_logs_parallel ${p} "${keys[@]}" &
    done

    # wait for all jobs, except the kubectl tunnel
    for job in $(jobs -p | grep -v ${tunnel_pid}); do
        wait ${job}
    done
}

tunnel_pull_logs() {
    local redis_region=$(cat ${REDIS_REGION_LOG})
    kubectl --request-timeout=0 --context="${redis_region}" port-forward master ${port}:${port} >/dev/null &
    tunnel_pid=$!
    log "port-forwarding starting at ${redis_region}..."

    while [ "$(lsof -i:${port})" == "" ]; do
        sleep 1
    done

    pull_logs ${tunnel_pid}
    log "all files downloaded!"

    kill ${tunnel_pid}
}
