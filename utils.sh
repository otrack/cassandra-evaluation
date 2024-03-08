#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
CONFIG_FILE="${DIR}/exp.config"

config() {
    if [ $# -ne 1 ]; then
        echo "usage: config key"
        exit -1
    fi
    local key=$1
    cat ${CONFIG_FILE} | grep -E "^${key}=" | cut -d= -f2
}

log() {
    local message=$@
    >&2 echo "["$(date +%H:%M:%S)"] ${message}"
}

# only displayed if verbose in config file
info() {
    if [ $# -ne 1 ]; then
        echo "usage: info message"
        exit -1
    fi
    local message=$1
    local verbose=$(config verbose)

    if [ "${verbose}" == "true" ]; then
        log "${message}"
    fi
}
