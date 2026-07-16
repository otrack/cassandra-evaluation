#!/bin/bash
# Read fast/slow commit counts logged by Tiga client to dat files.
# Called by run_benchmarks.sh as: tiga_fast_path.sh <container_name_or_id>

set -e

CONTAINER_ID="${1:?Usage: $0 <container_id_or_name>}"

# Extract node index from CONTAINER_ID (e.g., database-node1 -> 1)
INDEX=$(echo "$CONTAINER_ID" | grep -oE '[0-9]+$')

# Map node index to location (1=Hanoi, 2=Lyon, 3=NewYork, etc.)
case "$INDEX" in
    1) LOCATION="Hanoi" ;;
    2) LOCATION="Lyon" ;;
    3) LOCATION="NewYork" ;;
    4) LOCATION="SaoPaulo" ;;
    5) LOCATION="Tokyo" ;;
    *) LOCATION="unknown" ;;
esac

# Find the most recent YCSB log file for this location in logs/ycsb/
DIR=$(dirname "${BASH_SOURCE[0]}")
LAST_LOG=$(ls -t "${DIR}/../logs/ycsb"/*_${LOCATION}.dat 2>/dev/null | head -n 1)

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
