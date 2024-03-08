#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

TMPLDIR="${DIR}/templates"
YCSBDIR="${DIR}/bench/ycsb/"
WKLDDIR="${DIR}/bench/ycsb/workloads"
LATDIR="${DIR}/latency"
BINDIR="${DIR}"
CASSDIR="/home/otrack/Implementation/cassandra/"

CONFIG_FILE="${DIR}/exp.config"

REDIS_REGION_LOG=${DIR}/.redis_region_log

source ${DIR}/utils.sh
source ${DIR}/clusters.sh

# master() {
#     local REGEXP=$(echo ${CLUSTERS[@]} | sed 's/ /|/g' | awk '{print "("$1")"}')
#
#     local nclusters=${#CLUSTERS[@]}
#
#     local M=$(
#         for C in ${CLUSTERS[@]}; do
#             cat ${LATDIR}/${C}.dat | grep -E ${REGEXP} | grep -v ${C} | cut -f2 -d/ | awk '{s+=$1} END {print s " '$C'"}'
#             # this would be the correct computation !?
#             # cat ${LATDIR}/${C}.dat | grep -E ${REGEXP} | cut -f2 -d/ | head -n $((nclusters / 2 + 1)) | tail -n 1 | awk '{print $1 " '$C'"}'
#         done | sort -n | cut -d' ' -f2 | head -n $((nclusters / 2 + 1)) | tail -n 1
#     )
#     echo ${CLUSTERS[@]} | tr ' ' '\n' | grep ${M}
# }

init_clusters() {
    if [ $# -ne 2 ] || [ "$1" -eq "0" ]; then
        log "usage: init_clusters number_clusters (>0) is_local (boolean)"
        exit -1
    fi

    local nclusters=$1
    local is_local=$2

    if [ "${is_local}" = "true" ]; then
        CLUSTERS=($(for i in $(seq 1 ${nclusters}); do echo "minikube"; done))
        MASTER_CLUSTER="minikube"
    elif [ "${is_local}" = "false" ]; then
        CLUSTERS=("${ALL_CLUSTERS[@]:0:${nclusters}}")
    fi
}

init_log_dir() {
    if [ $# -ne 1 ] ; then
        log "usage: init_log_dir suffix"
        exit -1
    fi

    local suffix=$1
    LOGDIR="${DIR}/logs/${suffix}"
    mkdir -p ${LOGDIR}
}


# init given configuration in configuration file
init_clusters $(config clusters) $(config local)

list_all_clusters() {
    for CLUSTER in ${CLUSTERS[@]}; do
        echo ${CLUSTER}
    done
}

unique_clusters() {
    echo ${CLUSTERS[@]} | tr ' ' '\n' | uniq
}

ip() {
    if [ $# -ne 2 ]; then
        log "usage: ip cluster pod_selector"
        exit -1
    fi
    local cluster=$1
    local pod_selector=$2

    kubectl --context="${cluster}" get pod \
        -l"${pod_selector}" \
        -o jsonpath="{.items[*].status.podIP}" |
        tr ' ' '\n' |
        grep -E "([0-9]{1,3}\.){3}[0-9]{1,3}"
}

find_master() {
    if [ $# -ne 1 ]; then
        log "usage: find_master master_cluster"
        exit -1
    fi

    local master_cluster=$1
    local master=""

    while [ -z "${master}" ]; do
        sleep 1
        master=$(ip "${master_cluster}" "app=master")
    done

    echo ${master}
}

# FED

k8s_fed_create() {
    if [ $# -ne 1 ]; then
        log "usage: k8s_fed_create template.yaml"
        exit -1
    fi
    local template=$1

    info "> k8s create started!"
    for id in $(ids); do
        local cluster=${CLUSTERS[id]}
        k8s_create ${template} ${cluster} ${id} &
    done

    # wait for all created
    wait_jobs
    info "> k8s create done!"
}

k8s_fed_wait_completion() {
    if [ $# -ne 1 ]; then
        log "usage: k8s_fed_wait_completion template.yaml"
        exit -1
    fi
    local template=$1

    info "> k8s wait completion started!"
    for id in $(ids); do
        local cluster=${CLUSTERS[id]}
        k8s_wait_completion ${template} ${cluster} ${id} &
    done

    # wait for all completed
    wait_jobs
    info "> k8s wait completion done!"
}

k8s_fed_delete() {
    if [ $# -ne 1 ]; then
        log "usage: k8s_fed_delete template.yaml"
        exit -1
    fi
    local template=$1

    info "> k8s delete started!"
    for id in $(ids); do
        local cluster=${CLUSTERS[id]}
        k8s_delete ${template} ${cluster} ${id} &
    done

    # wait for all deleted
    wait_jobs
    info "> k8s delete done!"
}

# CLUSTER

k8s_create() {
    if [ $# -ne 2 ] && [ $# -ne 3 ]; then
        log "usage: k8s_create template.yaml cluster [id]"
        exit -1
    fi
    local template=$1
    local cluster=$2
    local id=${3:-0} # default id is 0
    local file=${template}-${id}
    local pull=$(config pull-images)

    # create final template
    cat ${template} |
        sed s/%ID%/${id}/g |
        sed s/%CLUSTER%/${cluster}/g |
        sed s/%PULL_IMAGES%/${pull}/g \
            >${file}

    # if cluster is minikube, "ignore" cpu request
    if [ "${cluster}" = "minikube" ]; then
        sed -i 's/cpu:.*$/cpu:\ 0.1/g' ${file}
    fi

    # create pod
    log "k8s_create ${cluster} (${file})"
    kubectl --context="${cluster}" create -f ${file}  >&/dev/null

    local pod_name=$(k8s_pod_name ${file})
    local pod_status="NotFound"

    # loop until pod is running
    while [ "${pod_status}" != "Running" ]; do
        sleep 1
        pod_status=$(k8s_pod_status ${cluster} ${pod_name})
    done
    info "pod ${pod_name} running at ${cluster}"
}

k8s_wait_completion() {
    if [ $# -ne 2 ] && [ $# -ne 3 ]; then
        log "usage: k8s_completion template.yaml cluster [id]"
        exit -1
    fi
    local template=$1
    local cluster=$2
    local id=${3:-0} # default id is 0
    local file=${template}-${id}
    local pod_name=$(k8s_pod_name ${file})

    log "waiting for ${pod_name} at ${cluster}..."
    local pod_status="Running"

    while [ "${pod_status}" != "Completed" ]; do
        sleep 5
        pod_status=$(k8s_pod_status ${cluster} ${pod_name})
    done
    info "${pod_name} at ${cluster} done!"
}

k8s_delete() {
    if [ $# -ne 2 ] && [ $# -ne 3 ]; then
        log "usage: k8s_delete template.yaml cluster [id]"
        exit -1
    fi
    local template=$1
    local cluster=$2
    local id=${3:-0} # default id is 0
    local file=${template}-${id}
    local pod_name=$(k8s_pod_name ${file})
    local pod_status="Running"

    # loop until pod is down
    while [ "${pod_status}" != "NotFound" ]; do
        kubectl --context="${cluster}" delete pod ${pod_name} \
            --grace-period=0 --force \
            >&/dev/null
        sleep 1
        pod_status=$(k8s_pod_status ${cluster} ${pod_name})
    done
    info "pod ${pod_name} deleted at ${cluster}"
}

k8s_pod_name() {
    if [ $# -ne 1 ]; then
        log "usage: k8s_pod_name file"
        exit -1
    fi
    local file=$1
    grep -E "^  name: " ${file} | head -n 1 | awk '{ print $2 }'
}

k8s_pod_status() {
    if [ $# -ne 2 ]; then
        log "usage: k8s_pod_status cluster pod_name"
        exit -1
    fi
    local cluster=$1
    local pod_name=$2
    kubectl --context="${cluster}" get pod ${pod_name} 2>&1 |
        grep -oE "(Running|Completed|Terminating|NotFound)"
}

k8s_get_pod_ip(){
    if [ $# -ne 2 ]; then
        log "usage: k8s_get_pod_ip cluster name"
        exit -1
    fi
    local cluster=$1
    local name=$2
    local proxy=$(kubectl --context="${cluster}" get pod ${name} -o yaml | grep "\- ip:" | awk '{print $3}')
    echo ${proxy}
}

k8s_get_service_ip(){
    if [ $# -ne 2 ]; then
        log "usage: k8s_get_service_ip cluster name"
        exit -1
    fi
    local cluster=$1
    local name=$2
    
    info "waiting that ${name} is up at ${cluster}..."
    local proxy=""
    while [ "${proxy}" == "" ]; do
	local proxy=$(kubectl --context="${cluster}" get service ${name} -o yaml | grep "\- ip:" | awk '{print $3}')
        sleep 5
    done
    info "done (${proxy})"
    
    echo ${proxy}
}

ids() {
    local nclusters=${#CLUSTERS[@]}
    seq 0 $((nclusters - 1))
}

wait_jobs() {
    for job in $(jobs -p); do
        wait ${job}
    done
}
