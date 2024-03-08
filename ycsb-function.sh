#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/protocol-function.sh

ycsb_template_dir="${TMPLDIR}/ycsb"
ycsb_template="${ycsb_template_dir}/tmp.ycsb.yaml"

compute_ratio() {
    if [ $# -ne 2 ]; then
        echo "usage: compute_ratio workload op"
        exit -1
    fi
    local workload=$1
    local op=$2

    grep proportion ${WKLDDIR}/workload${workload} |
        awk '{print toupper($0)}' |
        grep ${op} |
        awk -F'=' '{ print $2 }' |
        sed 's/[[:space:]]//g'
}

compute_latency() {
    if [ $# -ne 2 ]; then
        echo "usage: compute_latency file op"
        exit -1
    fi
    local file=$1
    local op=$2

    cat ${file} |
        grep "AverageLatency" |
        grep ${op} |
        grep -v "FAILED" |
        awk -F ', ' '{ print $3 }'
}

compute_tput() {
    if [ $# -ne 1 ]; then
        echo "usage: compute_tput file"
        exit -1
    fi
    local file=$1

    cat ${file} |
        grep "Throughput" |
        grep "OVERALL" |
        awk -F', ' '{ print $3 }'
}

ycsb_start() {
    if [ $# -ne 7 ]; then
        echo "usage: ycsb_start protocol type workload threads operationcount recordcount consistency"
        exit -1
    fi
    local protocol=$1
    local typ=$2
    local workload=$3
    local thread=$4
    local operationcount=$5
    local recordcount=$6
    local consistency=$7
    local extra_args=$8

    local sessions=$((thread * 1000))

    local docker_image=$(config ycsb)
    local ycsbverbose="false"
    local host=""
    local port=""
    local leaderless="false"

    # local extra="-s -p sessions=${sessions} -p status.interval=1 -jvm-args=-Xmx16g -p fieldcount=1 -p fieldlength=100"
    local extra="-s -p status.interval=1 -p cassandra.readtimeoutmillis=5000 -p cassandra.writeconsistencylevel=${consistency} -p cassandra.readconsistencylevel=${consistency}" # -jvm-args=-Xmx16g"
    # -p sessions=${sessions}
    # -p fieldcount=1 -p fieldlength=1
    # -p updateproportion=1  -p readproportion=0
    # -p verbose=true -p dataintegrity=true
    local database=""

    if [ "${consistency}" == "ONE" ]; then
	operationcount=$(echo ${threads} ${operationcount} | awk '{ print $1 * $2 }')
    fi

    log "ycsb_start: protocol=${protocol}, type=${typ}, workload=${workload}, thread=${thread}, operationcount=${operationcount}, recordcount=${recordcount}, extra=${extra}"

    case $protocol in
    mencius)
        host="${MASTER_CLUSTER_IP}"
        port="7087"
        database="epaxos"
        leaderless="true"
        ;;
    paxos)
        host="${MASTER_CLUSTER_IP}"
        port="7087"
        database="epaxos"
        ;;
    vcd | epaxos)
        # before, it was using zk to find the closest smap
        # now let's do that statically
        # host="${MASTER_CLUSTER_IP}"
        # port="2181"
        host="STATIC_SMAP_IP"
        port="0"
        database="mgbsmap"
        extra=${extra}" -p static=true"
        ;;
    cassandra)
        host="server"
        port="9042"
        database="cassandra-cql"
        extra=${extra}" -p hosts=${host}"
        ;;
    *)
        exit -1
        ;;
    esac

    cat ${ycsb_template_dir}/ycsb.yaml.tmpl |
        sed s/%IMAGE%/"${docker_image}"/g |
        sed s/%TYPE%/"${typ}"/g |
        sed s/%DATABASE%/"${database}"/g |
        sed s/%WORKLOAD%/"workload${workload}"/g |
        sed s/%RECORDCOUNT%/"${recordcount}"/g |
        sed s/%OPERATIONCOUNT%/"${operationcount}"/g |
        sed s/%HOST%/"${host}"/g |
        sed s/%PORT%/"${port}"/g |
        sed s/%VERBOSE%/"${ycsbverbose}"/g |
        sed s/%THREADS%/"${thread}"/g |
        sed s/%LEADERLESS%/"${leaderless}"/g |
        sed s/%EXTRA%/"${extra}"/g \
            >${ycsb_template}

    if [ ${typ} == "load" ]; then
	if [[ "${protocol}" == "cassandra" ]]; then
	    cassandra_execute_cql ${YCSBDIR}/cassandra.cql
	    sleep 3 # FIXME need to wait that the last epoch stabilizes everywhere
            k8s_create ${ycsb_template} ${CLUSTERS[0]} 42
	    k8s_wait_completion ${ycsb_template} ${CLUSTERS[0]} 42
	    k8s_delete ${ycsb_template} ${CLUSTERS[0]} 42
	else
	    log "NYI"
	fi
    else
        if [[ "${protocol}" == *"vcd"* || "${protocol}" == "epaxos" ]]; then
            for id in $(ids); do
                k8s_create $(create_smap_template ${id}) ${CLUSTERS[id]} ${id} &
            done
            wait_jobs
        else
            k8s_fed_create ${ycsb_template}
        fi
    fi
}

ycsb_wait() {
    if [ $# -le 4 ]; then
        echo "usage: ycsb_wait protocol type workload threads [batch_wait consistency]"
        exit -1
    fi
    local protocol=$1
    local typ=$2
    local workload=$3
    local thread=$4
    local batch_wait=$5
    local consistency=$6

    log "ycsb_wait: protocol=${protocol}, type=${typ}, workload=${workload}, thread=${thread}, batch_wait=${batch_wait}, consistency=${consistency}"

    if [ ${typ} == "load" ]; then
	if [[ "${protocol}" == *"vcd"* || "${protocol}" == "epaxos" ]];
	then
            k8s_wait_completion ${ycsb_template} ${CLUSTERS[0]} 42
	fi

        pod_name=$(k8s_pod_name "${ycsb_template}-42")
        file="${LOGDIR}/.ycsb-load-${protocol}-${CLUSTERS[0]}-${workload}.log"
        kubectl --context=${CLUSTERS[0]} logs ${pod_name} >${file}

        op="INSERT"
        ratio=$(compute_ratio ${workload} ${op})
        latency=$(compute_latency ${file} ${op})
        output="${LOGDIR}/ycsb-load-${protocol}-${CLUSTERS[0]}-${workload}-${op}.txt"
        echo ${thread} ${ratio} ${latency} >${output}

        k8s_delete ${ycsb_template} ${CLUSTERS[0]} 42
    else
        if [[ "${protocol}" == *"vcd"* || "${protocol}" == "epaxos" ]]; then
            for id in $(ids); do
                k8s_wait_completion $(smap_template ${id}) ${CLUSTERS[id]} ${id} &
            done
            wait_jobs
        else
            k8s_fed_wait_completion ${ycsb_template}
        fi

        for id in $(ids); do
            fetch_ycsb_log ${id} ${protocol} ${workload} ${thread} ${batch_wait} ${consistency} &
        done
        wait_jobs

        if [[ "${protocol}" == *"vcd"* || "${protocol}" == "epaxos" ]]; then
            for id in $(ids); do
                k8s_delete $(smap_template ${id}) ${CLUSTERS[id]} ${id} &
            done
            wait_jobs
        else
            k8s_fed_delete ${ycsb_template}
        fi
    fi
}

fetch_ycsb_log() {
    if [ $# -ne 6 ]; then
        echo "usage: fetch_ycsb_log id protocol workload threads batch_wait consistency"
        exit -1
    fi
    local id=$1
    local protocol=$2
    local workload=$3
    local thread=$4
    local batch_wait=$5
    local consistency=$6

    # find template used in creation
    local template=${ycsb_template}
    if [[ "${protocol}" == *"vcd"* || "${protocol}" == "epaxos" ]]; then
        template=$(smap_template ${id})
    fi

    # append id (appended by k8s_create)
    template="${template}-${id}"

    # find pod name
    local pod_name=$(k8s_pod_name ${template})

    local cluster=${CLUSTERS[id]}
    local file="${LOGDIR}/ycsb_log-${batch_wait}-${consistency}-${protocol}-${cluster}-${thread}-${workload}"
    kubectl --context=${cluster} logs ${pod_name} >${file}

    for op in "READ" "INSERT" "UPDATE" "SCAN"; do
        local ratio=$(compute_ratio ${workload} ${op})

        if [ "${ratio}" != "0" ]; then
            local latency=$(compute_latency ${file} ${op})
            local throughput=$(compute_tput ${file})
            local output="${LOGDIR}/ycsb-${batch_wait}-${consistency}-${protocol}-${cluster}-${thread}-${workload}-${op}"
            echo ${ratio} ${latency} ${throughput} >${output}
        fi
    done
    info "ycsb logs from ${pod_name} at ${cluster} fetched!"
}

create_smap_template() {
    if [[ $# -eq 0 || $# -gt 2 ]]; then
        echo "usage: create_smap_template id"
        exit -1
    fi
    local id=$1

    local create="true"
    if [ $# -eq 2 ]; then
        create=$2
    fi

    # get cluster
    local cluster=${CLUSTERS[id]}

    # find smap ip
    local smap_ip=""
    while [[ "${smap_ip}" == "" ]]; do
        smap_ip=$(ip ${cluster} "app=smap")
    done

    # template per ycsb client pod
    local template="${ycsb_template}_${smap_ip}"

    if [[ "${create}" == "true" ]]; then
        # create it
        cat ${ycsb_template} |
            sed s/STATIC_SMAP_IP/"${smap_ip}"/g \
                >${template}
    fi

    # return it
    echo ${template}
}

smap_template() {
    if [ $# -ne 1 ]; then
        echo "usage: smap_template id"
        exit -1
    fi
    local id=$1
    local create="false"
    create_smap_template ${id} ${create}
}
