#!usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh
source ${DIR}/redis-sync.sh
source ${DIR}/latency/latency.sh

cassandra_template_dir="${TMPLDIR}/cassandra"
cassandra_server_template="${cassandra_template_dir}/tmp.server.yaml"

epaxos_template_dir="${TMPLDIR}/epaxos"
epaxos_master_template="${epaxos_template_dir}/tmp.master.yaml"
epaxos_server_template="${epaxos_template_dir}/tmp.server.yaml"
epaxos_client_template="${epaxos_template_dir}/tmp.client.yaml"

vcd_template_dir="${TMPLDIR}/vcd"
vcd_master_template="${vcd_template_dir}/tmp.master.yaml"
vcd_server_template="${vcd_template_dir}/tmp.server.yaml"
vcd_smap_template="${vcd_template_dir}/tmp.smap.yaml"
vcd_client_template="${vcd_template_dir}/tmp.client.yaml"

paxos_master() {
    # this is the fairest master for all configurations in 13-clusters.txt
    echo "southamerica-east1"
}

vcd_master() {
    # just because it might provide better latency for us (when pulling redis logs)
    echo "europe-west4" # for 13-clusters.txt
    # echo "europe-north1" # for new-clusters.txt
}

protocol_start() {
    if [ $# -ne 7 ]; then
        echo "usage: protocol_start protocol max_faults nclient conflict batch_wait opt_delivery pure_optimization"
        exit -1
    fi
    local protocol=$1
    local max_faults=$2
    local nclient=$3
    local conflict=$4
    local batch_wait=$5
    local opt_delivery=$6

    case ${protocol} in
    paxos | mencius)
        epaxos_create_servers ${protocol} ${max_faults} ${batch_wait}
        ;;
    vcd | epaxos)
        vcd_create_servers ${protocol} ${max_faults} ${nclient} ${conflict} ${batch_wait} ${opt_delivery} ${pure_optimization}
        ;;
    cassandra)
	cassandra_create_servers
	;;
    *)
        exit -1
        ;;
    esac
}

clients_start() {
    if [[ $# -lt 8 || $# -gt 9 ]]; then
        # the last argument is optional
        # if it's not set, the size of $CLUSTERS will be used
        echo "usage: clients_start protocol max_faults nclient conflict batch_wait opt_delivery write ncmd (ssites)"
        exit -1
    fi
    local protocol=$1
    local max_faults=$2
    local nclient=$3
    local conflict=$4
    local batch_wait=$5
    local opt_delivery=$6
    local write=$7
    local ncmd=$8
    local ssites=${#CLUSTERS[@]}
    local csites=${#CLUSTERS[@]}

    if [ $# == 9 ]; then
        # if we have 9 arguments, then the 9th is the actual number of servers sites
        # (that differs from the number of client sites)
        ssites=$9
    fi

    log "clients: protocol=${protocol}, ssites=${ssites}, csites=${csites}, max_faults=${max_faults}, nclient=${nclient}, conflict=${conflict}, batch_wait=${batch_wait}, opt_delivery=${opt_delivery}, write=${write}, ncmd=${ncmd}"

    case ${protocol} in
    paxos | mencius)
        epaxos_create_clients ${protocol} ${nclient} ${conflict} ${write} ${ncmd}
        ;;
    vcd | epaxos)
        vcd_create_clients ${protocol} ${max_faults} ${nclient} ${conflict} ${batch_wait} ${opt_delivery} ${ncmd} ${ssites}
        ;;
    *)
        exit -1
        ;;
    esac
}

clients_wait() {
    if [ $# -ne 6 ]; then
        echo "usage: clients_wait protocol max_faults nclient conflict batch_wait write"
        exit -1
    fi
    local protocol=$1
    local max_faults=$2
    local nclient=$3
    local conflict=$4
    local batch_wait=$5
    local write=$6

    case ${protocol} in
    paxos | mencius)
        epaxos_wait_clients ${protocol} ${max_faults} ${nclient} ${conflict} ${batch_wait} ${write}
        ;;
    vcd | epaxos)
        vcd_wait_clients
        ;;
    *)
        exit -1
        ;;
    esac
}

protocol_stop() {
    if [ $# -ne 1 ]; then
        echo "usage: protocol_stop protocol"
        exit -1
    fi
    local protocol=$1
    case ${protocol} in
    cassandra)
        cassandra_delete_servers
	;;
    paxos | mencius)
        epaxos_delete_servers
        ;;
    vcd | epaxos)
        vcd_delete_servers
        ;;
    *)
        exit -1
        ;;
    esac
}

cassandra_create_servers() {
    if [ $# -ne 0 ]; then
        echo "usage: cassandra_create_servers"
        exit -1
    fi
    local docker_image=$(config cassandra)
    local ssites=${#CLUSTERS[@]}

    log "servers: ssites=${ssites}"

    # seed
    local seed=${CLUSTERS[0]}
    cat ${cassandra_template_dir}/server.yaml.tmpl |	
        sed s/%IMAGE%/"${docker_image}"/g |	
	sed s/%CASSANDRA_DC%/"${seed}"/g |
	sed s/%CASSANDRA_AUTO_BOOTSTRAP%/"true"/g |
	sed s/%CASSANDRA_ENDPOINT_SNITCH%/"GoogleCloudSnitch"/g | 
	sed s/%CASSANDRA_SEEDS%/""/g \
            >${cassandra_server_template}
    k8s_create ${cassandra_server_template} ${seed} 0

    # rest
    local seeds=$(k8s_get_pod_ip ${seed} server-0)
    local nclusters=${#CLUSTERS[@]}
    for id in $(seq 1 $((nclusters - 1))); do	
        local cluster=${CLUSTERS[id]}
	cat ${cassandra_template_dir}/server.yaml.tmpl |	
        sed s/%IMAGE%/"${docker_image}"/g |
	sed s/%CASSANDRA_DC%/"${cluster}"/g |
	sed s/%CASSANDRA_AUTO_BOOTSTRAP%/"false"/g |
	sed s/%CASSANDRA_ENDPOINT_SNITCH%/"GoogleCloudSnitch"/g | 
	sed s/%CASSANDRA_SEEDS%/"${seeds}"/g \
            >${cassandra_server_template}
        k8s_create ${cassandra_server_template} ${cluster} ${id} &
    done

    # wait for all created
    wait_jobs
    info "> k8s create done!"

    # block until servers can receive client connections
    for id in $(ids); do
        cassandra_wait_server "${CLUSTERS[id]}" ${id} &
    done
    wait_jobs
    log "servers running..."
}

cassandra_wait_server() {
    if [ $# -ne 2 ]; then
        echo "usage: cassandra_wait_server cluster id"
        exit -1
    fi
    local cluster=$1
    local id=$2
    local up=0

    info "checking if server ${id} at ${cluster} is ready..."
    while [ ${up} != 1 ]; do
        sleep 1
        up=$(kubectl --context=${cluster} -lapp=server,id=${id} logs --tail 10000 2>&1 |
            grep "Starting listening for CQL clients" | 
            wc -l)
    done
    info "server ${id} at ${cluster} is ready for client connections!"
}

cassandra_delete_servers() {
    if [ $# -ne 0 ]; then
        echo "usage: cassandra_delete_servers"
        exit -1
    fi
    log "deleting pods..."
    k8s_fed_delete ${cassandra_server_template} &
    wait_jobs
}

cassandra_execute_cql() {
    if [ $# -ne 1 ]; then
        echo "usage: cassandra_cql file"
        exit -1
    fi
    file=${1}
    log "executing cql (${file})"
    kubectl --context="${CLUSTERS[0]}" create -f ${TMPLDIR}/cassandra/service.yml >&/dev/null
    ip=$(k8s_get_service_ip ${CLUSTERS[0]} "cassandra-ext")
    cqlsh -f ${file} ${ip} >&/dev/null
    log "done"
}


## EPaxos et al.
epaxos_create_servers() {
    if [ $# -ne 3 ]; then
        echo "usage: epaxos_create_servers protocol max_faults batch_wait"
        exit -1
    fi
    local protocol=$1
    local max_faults=$2
    local batch_wait=$3
    local docker_image=$(config epaxos)
    local ssites=${#CLUSTERS[@]}

    log "servers: protocol=${protocol}, ssites=${ssites}, max_faults=${max_faults}, batch_wait=${batch_wait}"

    # create the master
    cat ${epaxos_template_dir}/master.yaml.tmpl |
        sed s/%IMAGE%/"${docker_image}"/g |
        sed s/%NSITE%/${ssites}/g \
            >${epaxos_master_template}

    MASTER_CLUSTER=$(paxos_master)
    k8s_create ${epaxos_master_template} ${MASTER_CLUSTER}
    MASTER_CLUSTER_IP=$(find_master ${MASTER_CLUSTER})
    log "master with ip ${MASTER_CLUSTER_IP} running at ${MASTER_CLUSTER}..."

    # create the servers
    # local server_cmd=" -opt_delivery"
    local server_cmd="-batchwait ${batch_wait}"
    if [ "${protocol}" == "paxos" ]; then
        server_cmd=${server_cmd}" -thrifty"
    elif [ "${protocol}" == "epaxos" ]; then
        server_cmd=${server_cmd}" -thrifty -e"
    elif [ "${protocol}" == "mencius" ]; then
        server_cmd=${server_cmd}" -m"
    fi

    # if paxos, set max faults
    if [ "${protocol}" == "paxos" ]; then
        server_cmd=${server_cmd}" -maxfailures ${max_faults}"
    fi

    cat ${epaxos_template_dir}/server.yaml.tmpl |
        sed s/%IMAGE%/"${docker_image}"/g |
        sed s/%MASTER%/"${MASTER_CLUSTER_IP}"/g |
        sed s/%SERVER_EXTRA_ARGS%/"${server_cmd}"/g \
            >${epaxos_server_template}
    k8s_fed_create ${epaxos_server_template}

    # block until servers can receive client connections
    for id in $(ids); do
        epaxos_wait_server "${CLUSTERS[id]}" ${id} &
    done
    wait_jobs
    log "servers running..."
}

epaxos_wait_server() {
    if [ $# -ne 2 ]; then
        echo "usage: epaxos_wait_server cluster id"
        exit -1
    fi
    local cluster=$1
    local id=$2
    local up=0

    info "checking if server ${id} at ${cluster} is ready..."
    while [ ${up} != 1 ]; do
        sleep 1
        up=$(kubectl --context=${cluster} -lapp=server,id=${id} logs 2>&1 |
            grep "Waiting for client connections" |
            wc -l)
    done
    info "server ${id} at ${cluster} is ready for client connections!"
}

epaxos_create_clients() {
    if [ $# -ne 5 ]; then
        echo "usage: epaxos_create_clients protocol nclient conflict write ncmd"
        exit -1
    fi
    local protocol=$1
    local nclient=$2
    local conflict=$3
    local write=$4
    local ncmd=$5
    local docker_image=$(config epaxos)

    local client_cmd="-q ${ncmd} -c ${conflict} -w ${write} -psize 100"
    case ${protocol} in
    mencius)
        client_cmd=${client_cmd}" -e"
        ;;
    paxos) ;;
    *)
        exit -1
        ;;
    esac

    cat ${epaxos_template_dir}/client.yaml.tmpl |
        sed s/%IMAGE%/"${docker_image}"/g |
        sed s/%MASTER%/"${MASTER_CLUSTER_IP}"/g |
        sed s/%CLIENT_EXTRA_ARGS%/"\"${client_cmd}\""/g |
        sed s/%NCLIENT%/"${nclient}"/g \
            >${epaxos_client_template}
    k8s_fed_create ${epaxos_client_template}

    # block until clients are connected
    for id in $(ids); do
        epaxos_wait_client_running "${CLUSTERS[id]}" ${id} &
    done
    wait_jobs
    log "clients running..."
}

epaxos_wait_client_running() {
    if [ $# -ne 2 ]; then
        echo "usage: epaxos_wait_client_running cluster id"
        exit -1
    fi
    local cluster=$1
    local id=$2
    local up=0

    info "checking if client ${id} at ${cluster} is connected..."
    while [ ${up} != 1 ]; do
        up=$(kubectl --context=${cluster} -lapp=client,id=${id} logs 2>&1 |
            grep "Connect OK" |
            wc -l)
    done
    info "client ${id} at ${cluster} is connected!"
}

epaxos_wait_clients() {
    if [ $# -ne 6 ]; then
        echo "usage: epaxos_wait_clients protocol max_faults nclient conflict batch_wait write"
        exit -1
    fi
    local protocol=$1
    local mxa_faults=$2
    local nclient=$3
    local conflict=$4
    local batch_wait=$5
    local write=$6

    for id in $(ids); do
        epaxos_wait_client ${protocol} ${max_faults} ${nclient} ${conflict} ${batch_wait} ${write} ${id} &
    done
    wait_jobs

    k8s_fed_delete ${epaxos_client_template}
}

epaxos_wait_client() {
    if [ $# -ne 7 ]; then
        echo "usage: epaxos_wait_client protocol max_faults nclient conflict batch_wait write id"
        exit -1
    fi
    local protocol=$1
    local max_faults=$2
    local nclient=$3
    local conflict=$4
    local batch_wait=$5
    local write=$6
    local id=$7
    local cluster=${CLUSTERS[id]}
    local pod_name=$(k8s_pod_name ${epaxos_client_template}-${id})

    log "waiting for ${pod_name} at ${cluster}..."
    local protocol_suffix=""
    if [[ ${batch_wait} -gt 0 ]]; then
        protocol_suffix="Batching"
    fi

    # if paxos, append the number of max faults to protocol name
    if [ "${protocol}" == "paxos" ]; then
        protocol="${protocol}f${max_faults}"
    fi

    local suffix="${protocol}${protocol_suffix}-${cluster}-${nclient}-${conflict}-${write}-PUT"
    local output="${LOGDIR}/log-${suffix}"
    local path_output="${LOGDIR}/path-${suffix}"
    local ts_output="${LOGDIR}/timeseries-${suffix}"
    local sleeping=0

    while [ ${sleeping} != 1 ]; do
        sleep 5
        sleeping=$(kubectl --context="${cluster}" logs ${pod_name} |
            grep "Will sleep forever" |
            wc -l)
    done
    info "${pod_name} at ${cluster} done!"

    local lines=0
    while [ ${lines} == 0 ]; do
        kubectl --context="${cluster}" exec ${pod_name} cat all_logs |
            grep -Eo "latency [0-9]+" |
            grep -Eo "[0-9]+" \
                >"${output}"
        kubectl --context="${cluster}" exec ${pod_name} cat all_logs |
            grep "stats" \
                >"${path_output}"
        kubectl --context="${cluster}" exec ${pod_name} cat all_logs |
            grep -Eo "chain [0-9]+-1" |
            grep -Eo "[0-9]+-1" \
                >"${ts_output}"

        lines=$(cat ${output} | wc -l | xargs echo)
        info "lines fetched: ${lines}!"
    done
}

epaxos_delete_servers() {
    if [ $# -ne 0 ]; then
        echo "usage: epaxos_delete_servers"
        exit -1
    fi
    log "deleting pods..."
    k8s_delete ${epaxos_master_template} ${MASTER_CLUSTER} &
    k8s_fed_delete ${epaxos_server_template} &
    wait_jobs
}

## VCD
vcd_create_servers() {
    if [ $# -ne 7 ]; then
        echo "usage: vcd_create_servers protocol max_faults nclient conflict batch_wait opt_delivery pure_optimization"
        exit -1
    fi
    local protocol=$1
    local max_faults=$2
    local nclient=$3
    local conflict=$4
    local batch_wait=$5
    local opt_delivery=$6
    local pure_optimization=$7
    local vcd_docker_image=$(config vcd)
    local ssites=${#CLUSTERS[@]}

    log "servers: protocol=${protocol}, ssites=${ssites}, max_faults=${max_faults}, nclient=${nclient}, conflict=${conflict}, batch_wait=${batch_wait}, opt_delivery=${opt_delivery}, pure_optimization=${pure_optimization}"

    # create the master
    cat ${vcd_template_dir}/master.yaml.tmpl \
        >${vcd_master_template}

    MASTER_CLUSTER=$(vcd_master)
    k8s_create ${vcd_master_template} ${MASTER_CLUSTER}
    MASTER_CLUSTER_IP=$(find_master ${MASTER_CLUSTER})
    log "master with ip ${MASTER_CLUSTER_IP} running at ${MASTER_CLUSTER}..."
    echo ${MASTER_CLUSTER} > ${REDIS_REGION_LOG}

    # get template to use
    local server_template=${vcd_template_dir}/server.yaml.tmpl

    # create the servers
    cat ${server_template} 2>/dev/null |
        sed s/%VCD_IMAGE%/${vcd_docker_image}/g |
        sed s/%PROTOCOL%/${protocol}/g |
        sed s/%NSITE%/${ssites}/g |
        sed s/%MAX_FAULTS%/${max_faults}/g |
        sed s/%MASTER%/"${MASTER_CLUSTER_IP}"/g |
        sed s/%NCLIENT%/"${nclient}"/g |
        sed s/%CONFLICT%/"${conflict}"/g |
        sed s/%BATCH_WAIT%/"${batch_wait}"/g |
        sed s/%OPT_DELIVERY%/"${opt_delivery}"/g |
        sed s/%PURE_OPTIMIZATION%/"${pure_optimization}"/g \
            >${vcd_server_template}
    k8s_fed_create ${vcd_server_template}

    # block until servers can receive client connections
    for id in $(ids); do
        vcd_wait_server "${CLUSTERS[id]}" ${id} &
    done
    wait_jobs
    log "servers running..."
}

vcd_wait_server() {
    if [ $# -ne 2 ]; then
        echo "usage: vcd_wait_server cluster id"
        exit -1
    fi
    local cluster=$1
    local id=$2
    local pod_name=$(kubectl --context=${cluster} get pods -lapp=server,id=${id} --no-headers |
        awk '{ print $1 }')
    local up=0

    # check if the server is up
    info "checking if server ${id} at ${cluster} is ready..."
    while [ ${up} != 1 ]; do
        sleep 1
        up=$(kubectl --context=${cluster} logs ${pod_name} |
            grep "Clustering OK!" |
            wc -l)
    done

    info "server ${id} at ${cluster} is ready for client connections!"
}

vcd_create_smaps() {
    if [ $# -ne 1 ]; then
        echo "usage: vcd_create_smaps batch_wait"
        exit -1
    fi
    local batch_wait=$1
    local smap_docker_image=$(config smap)

    log "smaps: batch_wait=${batch_wait}"

    MASTER_CLUSTER=$(vcd_master)
    MASTER_CLUSTER_IP=$(find_master ${MASTER_CLUSTER})

    # get template to use
    local smap_template=${vcd_template_dir}/smap.yaml.tmpl

    # create the smaps
    cat ${smap_template} 2>/dev/null |
        sed s/%SMAP_IMAGE%/${smap_docker_image}/g |
        sed s/%MASTER%/"${MASTER_CLUSTER_IP}"/g |
        sed s/%BATCH_WAIT%/"${batch_wait}"/g \
            >${vcd_smap_template}
    k8s_fed_create ${vcd_smap_template}

    # block until smaps can receive client connections
    for id in $(ids); do
        vcd_wait_smap "${CLUSTERS[id]}" ${id} &
    done
    wait_jobs
    log "smaps running..."
}

vcd_wait_smap() {
    if [ $# -ne 2 ]; then
        echo "usage: vcd_wait_smap cluster id"
        exit -1
    fi
    local cluster=$1
    local id=$2
    local pod_name=$(kubectl --context=${cluster} get pods -lapp=smap,id=${id} --no-headers |
        awk '{ print $1 }')
    local up=0

    # check if the smap is up
    info "checking if smap ${id} at ${cluster} is ready..."
    while [ ${up} != 1 ]; do
        sleep 1
        up=$(kubectl --context=${cluster} logs ${pod_name} |
            grep "MGB-SMap Server started" |
            wc -l)
    done

    info "smap ${id} at ${cluster} is ready for client connections!"
}

vcd_create_clients() {
    if [ $# -ne 8 ]; then
        echo "usage: vcd_create_clients protocol max_faults nclient conflict batch_wait opt_delivery ncmd ssites"
        exit -1
    fi
    local protocol=$1
    local max_faults=$2
    local nclient=$3
    local conflict=$4
    local batch_wait=$5
    local opt_delivery=$6
    local ncmd=$7
    local ssites=$8
    local docker_image=$(config vcd-client)

    cat ${vcd_template_dir}/client.yaml.tmpl |
        sed s/%IMAGE%/${docker_image}/g |
        sed s/%NSITE%/${ssites}/g |
        sed s/%MAX_FAULTS%/${max_faults}/g |
        sed s/%MASTER%/"${MASTER_CLUSTER_IP}"/g |
        sed s/%PROTOCOL%/${protocol}/g |
        sed s/%NCLIENT%/"${nclient}"/g |
        sed s/%CONFLICT%/"${conflict}"/g |
        sed s/%BATCH_WAIT%/"${batch_wait}"/g |
        sed s/%OPT_DELIVERY%/"${opt_delivery}"/g |
        sed s/%QUEUE_TYPE%/"${queue_type}"/g |
        sed s/%NCMD%/"${ncmd}"/g \
            >${vcd_client_template}
    k8s_fed_create ${vcd_client_template}

    # block until clients are connected
    for id in $(ids); do
        vcd_wait_client_running "${CLUSTERS[id]}" ${id} &
    done
    wait_jobs
    log "clients running..."
}

vcd_wait_client_running() {
    if [ $# -ne 2 ]; then
        echo "usage: vcd_wait_client_running cluster id"
        exit -1
    fi
    local cluster=$1
    local id=$2
    local up=0

    info "checking if client ${id} at ${cluster} is connected..."
    while [ ${up} != 1 ]; do
        up=$(kubectl --context=${cluster} -l app=client,id=${id} logs 2>&1 |
            grep "Connect OK" |
            wc -l)
    done
    info "client ${id} at ${cluster} is connected!"
}

vcd_wait_clients() {
    if [ $# -ne 0 ]; then
        echo "usage: vcd_wait_clients"
        exit -1
    fi

    # wait clients are done
    k8s_fed_wait_completion ${vcd_client_template}

    # maybe fetch vcd client logs
    local save_logs=$(config save-vcd-client-logs)

    if [ "${save_logs}" == "true" ]; then
        local timestamp=$(date +%s)
        for id in $(ids); do
            vcd_pull_client_log ${timestamp} ${id} &
        done
        wait_jobs
    fi

    # pull logs
    tunnel_pull_logs

    k8s_fed_delete ${vcd_client_template}
}

vcd_pull_client_log() {
    if [ $# -ne 2 ]; then
        echo "usage: vcd-pull_client_log timestamp id"
        exit -1
    fi

    local cluster=${CLUSTERS[id]}
    local output=${timestamp}-${cluster}
    local pod_name=$(kubectl --context=${cluster} get pods -lapp=client,id=${id} --no-headers |
        awk '{ print $1 }')

    kubectl --context=${cluster} logs ${pod_name} >${output}-client.log
}

vcd_delete_servers() {
    if [ $# -ne 0 ]; then
        echo "usage: vcd_delete_servers"
        exit -1
    fi

    # maybe fetch vcd logs
    local save_logs=$(config save-vcd-logs)

    if [ "${save_logs}" == "true" ]; then
        local timestamp=$(date +%s)
        for id in $(ids); do
            local cluster=${CLUSTERS[id]}
            local pod_name=$(kubectl --context=${cluster} get pods -lapp=server,id=${id} --no-headers |
                awk '{ print $1 }')
            local output=${timestamp}-${cluster}
            kubectl --context=${cluster} logs ${pod_name} >${output}-server.log
        done
    fi

    log "deleting pods..."
    k8s_delete ${vcd_master_template} ${MASTER_CLUSTER} &
    k8s_fed_delete ${vcd_server_template} &
    wait_jobs
}

vcd_delete_smaps() {
    if [ $# -ne 0 ]; then
        echo "usage: vcd_delete_smaps"
        exit -1
    fi

    # maybe fetch vcd logs
    local save_logs=$(config save-vcd-smap-logs)

    if [ "${save_logs}" == "true" ]; then
        local timestamp=$(date +%s)
        for id in $(ids); do
            local cluster=${CLUSTERS[id]}
            local pod_name=$(kubectl --context=${cluster} get pods -lapp=smap,id=${id} --no-headers |
                awk '{ print $1 }')
            local output=${timestamp}-${cluster}
            kubectl --context=${cluster} logs ${pod_name} >${output}-smap.log
        done
    fi

    log "deleting smap pods..."
    k8s_fed_delete ${vcd_smap_template}
}

node_crash() {
    if [ $# -ne 2 ]; then
        echo "usage: node_crash protocol site_id"
        exit -1
    fi
    local protocol=$1
    local site_id=$2
    local cluster=${CLUSTERS[site_id]}
    local template=""

    log "stopping ${protocol} node with id ${site_id} at ${cluster}..."

    case ${protocol} in
    paxos | mencius)
        template=${epaxos_server_template}
        ;;
    vcd)
        template=${vcd_server_template}
        ;;
    *)
        exit -1
        ;;
    esac

    k8s_delete ${template} ${cluster} ${site_id}
}
