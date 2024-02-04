#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
${DIR}/list.sh | grep -Eio "(running|error)" | sort | uniq -c
