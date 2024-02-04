#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
clusters=(13 11 9 7 5 3)

trap "pkill -KILL -P $$; exit 255" SIGINT SIGTERM

for cluster in ${clusters[@]}; do
    # update exp.config
    sed -i s/clusters=[0-9]*/clusters=${cluster}/ ${DIR}/exp.config

    # create the necessary clusters
    # ${DIR}/fed-bootstrap.sh

    # run the base experiment
    ${DIR}/base-exp.sh
done

# stop all clusters
# ${DIR}/fed-stop.sh

# sleep forever
echo "will sleep forever"
while true; do sleep 10000; done
