#!/usr/bin/env bash

# Report the state of the emulated WAN links.
#
# emulate_latency.py installs one netem qdisc per (source, destination) pair.
# netem has a fixed queue -- 1000 packets unless `limit` says otherwise -- and
# silently DROPS whatever does not fit.  Those drops are indistinguishable from
# a slow protocol at the client, so this prints the counters that tell them
# apart: `dropped` is the emulator discarding traffic, `backlog` is how close
# the queue is to its limit right now.
#
# Usage: ./netem_stats.sh [container ...]      (default: every DC container)

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/utils.sh

containers=("$@")
if [ ${#containers[@]} -eq 0 ]; then
    nodes_per_dc=$(config nodesperdc); nodes_per_dc=${nodes_per_dc:-1}
    i=1
    while loc=$(get_location $i "${LOCATIONS_FILE}" 2>/dev/null) && [ -n "$loc" ]; do
        for k in $(seq 1 "${nodes_per_dc}"); do
            container_exists "${loc}${k}" && containers+=("${loc}${k}")
        done
        i=$((i + 1))
    done
fi

if [ ${#containers[@]} -eq 0 ]; then
    error "no running DC containers found"
    exit 1
fi

printf '%-12s %-10s %-12s %10s %12s %10s\n' CONTAINER QDISC DELAY LIMIT DROPPED BACKLOG
for c in "${containers[@]}"; do
    dev=$(infra_net_device "$(node_index_of "$c")")
    dexec "$c" tc -s qdisc show dev "${dev}" 2>/dev/null | awk -v c="$c" '
        /qdisc netem/ {
            handle = $3; delay = "-"; limit = "default(1000p)"
            for (i = 1; i <= NF; i++) {
                if ($i == "delay") delay = $(i+1)
                if ($i == "limit") limit = $(i+1) "p"
            }
            next_is_stats = 1
        }
        next_is_stats && /Sent/ {
            dropped = "?"; backlog = "-"
            for (i = 1; i <= NF; i++) if ($i ~ /^\(?dropped$/) { d = $(i+1); sub(/,/, "", d); dropped = d }
            getline
            for (i = 1; i <= NF; i++) if ($i ~ /backlog/) backlog = $(i+1)
            printf "%-12s %-10s %-12s %10s %12s %10s\n", c, handle, delay, limit, dropped, backlog
            next_is_stats = 0
        }'
done
