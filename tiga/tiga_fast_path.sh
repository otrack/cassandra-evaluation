#!/bin/bash
# Read fast/slow commit counts logged by Tiga client to dat files.
# Called by run_benchmarks.sh as: tiga_fast_path.sh <container_name_or_id>

set -e

TIGA_FAST_PATH_DIR=$(dirname "${BASH_SOURCE[0]}")
source ${TIGA_FAST_PATH_DIR}/../utils.sh

CONTAINER_ID="${1:?Usage: $0 <container_id_or_name>}"

# The container name carries its own location (Lyon1 -> Lyon).  Deriving it
# from the trailing digit instead would read the *intra-DC* index, which is 1
# for every DC at the default nodesperdc.
LOCATION="${CONTAINER_ID%%[0-9]*}"
if [ -z "${LOCATION}" ] || [ "${LOCATION}" = "$(config node_name)" ]; then
    # Legacy database-nodeN naming: fall back to the provider's location map.
    INDEX=$(echo "$CONTAINER_ID" | grep -oE '[0-9]+$')
    LOCATION=$(get_location "${INDEX:-1}")
fi

# Find the most recent YCSB log file for this location in logs/ycsb/
LAST_LOG=$(ls -t "${LOGDIR}/ycsb"/*_${LOCATION}.dat 2>/dev/null | head -n 1)

LAST_LINE=""
if [ -n "$LAST_LOG" ] && [ -f "$LAST_LOG" ]; then
    LAST_LINE=$(grep -F "[FastPathRatio]" "$LAST_LOG" | tail -n 1 || true)
fi

if [ -z "$LAST_LINE" ]; then
    echo "Fast ratio: 0.0000"
    echo "Medium ratio: 0.0000"
    echo "Slow ratio: 1.0000"
    echo "Ephemeral ratio: 0.0000"
    exit 0
fi

FAST=$(echo "$LAST_LINE" | grep -oP 'fast=\K[0-9]+')
SLOW=$(echo "$LAST_LINE" | grep -oP 'slow=\K[0-9]+')

FAST=${FAST:-0}
SLOW=${SLOW:-0}

TOTAL=$((FAST + SLOW))

if [ "$TOTAL" -gt 0 ]; then
    echo "Fast ratio: $(awk "BEGIN {printf \"%.4f\", $FAST/$TOTAL}")"
    echo "Medium ratio: 0.0000"
    echo "Slow ratio: $(awk "BEGIN {printf \"%.4f\", $SLOW/$TOTAL}")"
    echo "Ephemeral ratio: 0.0000"
else
    echo "Fast ratio: 0.0000"
    echo "Medium ratio: 0.0000"
    echo "Slow ratio: 1.0000"
    echo "Ephemeral ratio: 0.0000"
fi
