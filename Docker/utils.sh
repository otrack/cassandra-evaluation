#!/usr/bin/env bash

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
	local message="$@"
	echo -e >&1 "["$(date +%s:%N)"] \033[32m${message}\033[0m"
    fi
}

log() {
    local message="$@"
    echo -e >&1 "["$(date +%s:%N)"] \033[33m${message}\033[0m"
}

error() {
    local message="$@"
    echo -e >&2 "["$(date +%s:%N)"] \033[31m${message}\033[0m"
}

clean_logdir() {
    rm -Rf ${LOGDIR}/*
}

DEBUG=$(config debug)

start_container() {
    if [ $# -lt 3 ]; then
        error "usage: start_container <image> <name> <message> [args...]"
        return 2
    fi

    local image="$1"
    local cname="$2"
    local wait_msg="$3"
    shift 3
    local docker_args=("$@")
    
    echo 

    log "Starting container from image '${image}' as '${cname}' with args '${docker_args[@]}'"
    local cid
    cid=$(docker run ${docker_args[@]} -d --name "$cname" "$image"  2>&1) || {
        error "docker run failed: ${cid}"
        return 3
    }
    log "Started container '${cname}' (id: ${cid})"

    local start_time
    start_time=$(date +%s)
    local timeout
    timeout=${START_CONTAINER_TIMEOUT:-60}   # seconds, override by exporting START_CONTAINER_TIMEOUT

    while true; do
        # Check for the readiness message in logs
        if docker logs "$cname" 2>&1 | grep -F -- "$wait_msg" >/dev/null; then
            log "Container '${cname}' is ready (found: '${wait_msg}')"
            return 0
        fi

        # Check whether container is still running
        local running
        running=$(docker inspect -f '{{.State.Running}}' "$cname" 2>/dev/null) || {
            error "Failed to inspect container '${cname}'"
            return 4
        }
        if [ "$running" != "true" ]; then
            local exit_code
            exit_code=$(docker inspect -f '{{.State.ExitCode}}' "$cname" 2>/dev/null || echo "unknown")
            error "Container '${cname}' exited with code ${exit_code} before readiness message appeared. Logs:"
            docker logs "$cname" 2>&1 | sed 's/^/  /'
            return 5
        fi

        # Timeout check
        if (( $(date +%s) - start_time >= timeout )); then
            error "Timeout (${timeout}s) waiting for '${wait_msg}' in container '${cname}' logs. Logs:"
            docker logs "$cname" 2>&1 | sed 's/^/  /'
            return 6
        fi

        sleep 0.5
    done
}

stop_container() {
    if [ $# -lt 1 ]; then
        error "usage: stop_container <name> [timeout_seconds]"
        return 2
    fi

    local cname="$1"
    local timeout="${2:-${STOP_CONTAINER_TIMEOUT:-30}}"  # seconds, can be overridden by arg or STOP_CONTAINER_TIMEOUT env

    log "Stopping container '${cname}' (timeout: ${timeout}s)"

    # Check container existence / get running state
    local running
    running=$(docker inspect -f '{{.State.Running}}' "$cname" 2>/dev/null) || {
        error "Container '${cname}' does not exist or cannot be inspected"
        return 3
    }

    if [ "$running" != "true" ]; then
        log "Container '${cname}' is already stopped"
        return 0
    fi

    if ! docker stop "$cname" >/dev/null 2>&1; then
        error "docker stop failed for container '${cname}'"
        return 4
    fi

    local start_time
    start_time=$(date +%s)

    # Wait until container is no longer running (or inspect disappears)
    while true; do
        running=$(docker inspect -f '{{.State.Running}}' "$cname" 2>/dev/null) || {
            # Inspect failing -> container removed or no longer present; treat as stopped
            log "Container '${cname}' no longer present; considered stopped"
            return 0
        }
        if [ "$running" != "true" ]; then
            log "Container '${cname}' stopped"
            return 0
        fi

        if (( $(date +%s) - start_time >= timeout )); then
            error "Timeout (${timeout}s) waiting for container '${cname}' to stop"
            return 5
        fi

        sleep 0.5
    done
}

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

