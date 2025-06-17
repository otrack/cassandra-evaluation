#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
TOPK=5

${DIR}/nodes.sh | awk '{ print $4 }' | sort -n | uniq -c

echo ""

${DIR}/nodes.sh | awk '{ print $4" "$1 }' | sort -n | head -n ${TOPK} | awk '{ print $2 }' | cut -d- -f2,3 | sort -u
