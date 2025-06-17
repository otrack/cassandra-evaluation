#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh

# VERSION=1.9.7-gke.6
MACHINE_TYPE=n2-standard-2
NODE_NUMBER=1
GCP_PROJECT=$(gcloud config list --format='value(core.project)')
NETWORK="projects/${GCP_PROJECT}/global/networks/default"

zone() {
    if [ $# -ne 1 ]; then
        echo "usage: zone region"
    fi

    local region=$1

    # compute name of the zone, given cluster name (that is the name of the region)
    # given a region R, a zone is typically R-a, or R-b, ...
    # return the first region
    grep ${region} ${DIR}/clusters.txt | head -n 1
}

create_cluster() {
    if [ $# -ne 2 ]; then
        echo "usage: create_cluster region sleep_time"
    fi

    local region=$1
    local zone=$(zone ${region})
    local seconds=$2

    sleep ${seconds}

    cmd="gcloud container clusters create ${region} \
        --zone ${zone} \
        --num-nodes ${NODE_NUMBER} \
        --machine-type ${MACHINE_TYPE} \
        --network ${NETWORK} \
        --preemptible \
        --no-enable-autoupgrade"

    log ${cmd}

    eval ${cmd}
}

fetch_credentials() {
    if [ $# -ne 1 ]; then
        echo "usage: fetch_credentials region"
    fi

    local region=$1
    local zone=$(zone ${region})

    local count=0
    local seconds=0

    while [ ${count} != 1 ]; do
        # get credential
        gcloud container clusters get-credentials ${region} \
            --zone ${zone}

        # create alias
        kubectl config set-context ${region} \
            --cluster=gke_${GCP_PROJECT}_${zone}_${region} \
            --user=gke_${GCP_PROJECT}_${zone}_${region}

        # check connection now
        count=$(kubectl --context=${region} get pods 2>&1 |
            grep "No resources found." |
            wc -l |
            xargs
        )

        # sleep backoff mechanism
        sleep ${seconds}
        seconds=$((seconds + 1))
    done
}

create_federation() {    
    # 1. Create clusters
    for i in "${!CLUSTERS[@]}"; do
	region=${CLUSTERS[${i}]}
	sleep_time=$((i * 2))
	# the sleep tries to avoid gcloud database locked errors
	create_cluster ${region} ${sleep_time} &
    done
    wait

    # 2. Save the cluster credentials and create context aliases:
    for region in "${CLUSTERS[@]}"; do
	fetch_credentials ${region}
    done
}

# # 3. Install cadvisor daemonset
# for cluster in "${CLUSTERS[@]}"; do
#     cat ${DIR}/templates/cadvisor.yml | kubectl --context="${cluster}" apply -f -
# done

# create_cluster europe-west4 0
# fetch_credentials europe-west4

create_federation
