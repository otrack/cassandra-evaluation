#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source "${DIR}"/context.sh

# Remove GKE Clusters
for region in ${CLUSTERS[@]}; do
    # compute name of the zone, given the name of the region
    # - given a region R, a zone is typically R-a, or R-b, ...
    zone=$(grep ${region} ${DIR}/clusters.txt)
    gcloud container clusters delete ${region} -q --zone=${zone} &
done

# Wait for stop.
FAIL=0
for job in $(jobs -p); do
    wait ${job} || let "FAIL+=1"
done
if [ "${FAIL}" == "0" ]; then
    echo "All clusters stopped!"
else
    echo "FAIL! (${FAIL})"
fi
