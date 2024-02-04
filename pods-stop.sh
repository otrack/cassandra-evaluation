#!/bin/bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh

arg=$1

if [[ "${arg}" == "" ]];
then
    arg="--all"
fi

for cluster in $(unique_clusters); do
    kubectl --context=${cluster} delete pods ${arg} \
        --grace-period=0 --force \
        2>/dev/null &
done
wait_jobs

if [[ "${arg}" == "--all" ]];
then
    # wait for all pods to terminate
    for cluster in $(unique_clusters); do
	echo ${cluster}
	while [ "${empty}" != "1" ]; do
            empty=$(kubectl --context=${cluster} get pods 2>&1 |
			grep "No resources found" |
			wc -l |
			xargs echo
		 )
	done
    done
fi
