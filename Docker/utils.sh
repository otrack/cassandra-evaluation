#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

BINDIR="${DIR}"
LOGDIR="${DIR}/logs"
RESULTSDIR="${DIR}/results/"
CONFIG_FILE="${DIR}/exp.config"

config() {
    if [ $# -ne 1 ]; then
        echo "usage: config key"
        exit -1
    fi
    local key=$1
    cat ${CONFIG_FILE} | grep -E "^${key}=" | cut -d= -f2
}

debug() {
    if [[ DEBUG -eq 1 ]]
    then
	local message=$1
	echo -e >&1 "["$(date +%s:%N)"] \033[32m${message}\033[0m"
    fi
}

log() {
    local message=$1
    echo -e >&1 "["$(date +%s:%N)"] \033[33m${message}\033[0m"
}

error() {
    local message=$1
    echo -e >&2 "["$(date +%s:%N)"] \033[31m${message}\033[0m"
}

clean_logdir() {
    rm -Rf ${LOGDIR}/*
}

DEBUG=$(config debug)

# Function to get the IP address of a container
get_container_ip() {
    container_name=$1
    ip_address=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$container_name" 2>/dev/null)
    echo "$ip_address"
}

# Function to stop a container after a delay
stop_container_after_delay() {
    container_name=$1
    delay=$2
    (
        sleep "$delay"
        docker stop "$container_name"
        log "Stopped container '$container_name' after $delay seconds."
    ) &
}

