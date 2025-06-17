#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/context.sh

mailboxes() {
    local id=$1
    cluster=${CLUSTERS[id]}
    pod=server-${id}

    kubectl --context=${cluster} logs -f ${pod} |
        grep "Mailbox of" |
        awk -v cluster=${cluster} '{ print $cluster }'
}

for id in $(ids); do
    mailboxes ${id} &
done
wait
