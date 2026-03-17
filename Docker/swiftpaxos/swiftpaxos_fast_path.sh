#!/bin/bash
set -e

CONTAINER_ID="${1:?Usage: $0 <container_id_or_name>}"

# Get the last log line matching the swiftpaxos path stats format
LAST_LINE=$(docker logs "$CONTAINER_ID" 2>&1 | grep -E "weird [0-9]+; conflicted [0-9]+; slow [0-9]+; fast [0-9]+" | tail -n 1)

if [ -z "$LAST_LINE" ]; then
    echo "Fast ratio: 0.0"
    echo "Medium ratio: 0.0"
    echo "Slow ratio: 1.0"
    echo "Ephemeral ratio: 0.0"
    exit 0
fi

FAST=$(echo "$LAST_LINE" | grep -oP 'fast \K[0-9]+')
SLOW=$(echo "$LAST_LINE" | grep -oP 'slow \K[0-9]+')

FAST=${FAST:-0}
SLOW=${SLOW:-0}

TOTAL=$((FAST + SLOW))

if [ "$TOTAL" -gt 0 ]; then
    echo "Fast ratio: $(awk "BEGIN {printf \"%.4f\", $FAST/$TOTAL}")"
    echo "Medium ratio: 0.0"
    echo "Slow ratio: $(awk "BEGIN {printf \"%.4f\", $SLOW/$TOTAL}")"
    echo "Ephemeral ratio: 0.0"
else
    echo "Fast ratio: 0.0"
    echo "Medium ratio: 0.0"
    echo "Slow ratio: 1.0"
    echo "Ephemeral ratio: 0.0"
fi
