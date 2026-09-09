#!/usr/bin/env bash

# Simulation provider: every container runs on the local Docker daemon and
# geo-distribution is emulated with tc/netem.  This is the historical behavior
# of the benchmark suite, and the reference implementation of the provider
# contract documented in infra/README.md.
#
# Every function here is deliberately trivial: an empty Docker context means
# "the local daemon", so the d* wrappers in utils.sh degenerate to plain
# `docker` calls and the simulated mode behaves exactly as it did before the
# infra layer existed.

infra_is_real() {
    return 1
}

# The set of cities this provider pretends to deploy to.  Authored by hand and
# version controlled: it *is* the simulated geography, and the distances
# between these coordinates are what tc reproduces.
infra_locations_file() {
    echo "${INFRA_DIR}/locations.csv"
}

infra_provision() {
    debug "simulation: no machine to provision"
    return 0
}

infra_teardown() {
    debug "simulation: no machine to tear down"
    return 0
}

infra_sync() {
    debug "simulation: no remote state to re-establish"
    return 0
}

infra_ssh() {
    error "simulation: containers run locally; there is no machine to connect to"
    return 1
}

infra_reset_network() {
    # tc rules live in each container's own network namespace and are destroyed
    # along with it, so there is never stale shaping to clear.
    return 0
}

infra_host_ip() {
    # Addresses come from `docker inspect` in simulated mode; see
    # get_container_ip in utils.sh.
    echo ""
}

infra_context() {
    # Empty context == the local Docker daemon.
    echo ""
}

infra_net_device() {
    # Containers on a Docker bridge always see their interface as eth0.
    echo "eth0"
}

infra_open_ports() {
    # Containers share one bridge network; nothing to open.
    return 0
}

infra_stage_file() {
    if [ $# -lt 2 ]; then
        error "usage: infra_stage_file <node_idx> <local_path> [remote_path]"
        return 2
    fi
    # The daemon shares this filesystem, so the local path is already usable
    # in a -v bind-mount.
    echo "$2"
}

infra_resource_limits() {
    # Several containers share one machine here, so each is capped to the
    # CPU/memory of the `machine` entry in exp.config, looked up in gcp.csv.
    local machine
    machine=$(config machine)

    if [ -z "$machine" ]; then
        echo ""
        return 0
    fi

    local gcp_csv="${DIR}/gcp.csv"
    if [ ! -f "$gcp_csv" ]; then
        error "gcp.csv not found: ${gcp_csv}"
        echo ""
        return 1
    fi

    local row
    row=$(awk -F',' -v name="$machine" 'NR>1 && $1==name {print $0; exit}' "$gcp_csv")
    if [ -z "$row" ]; then
        error "Machine type '${machine}' not found in gcp.csv"
        echo ""
        return 1
    fi

    local vcpus memory_gb memory_mb
    vcpus=$(echo "$row" | cut -d',' -f2)
    memory_gb=$(echo "$row" | cut -d',' -f3)
    memory_mb=$(awk "BEGIN { printf \"%d\", ${memory_gb} * 1024 }")

    echo "--cpus ${vcpus} --memory ${memory_mb}m"
}

infra_machine_shape() {
    config machine
}

infra_rewrite_docker_args() {
    # Never called in simulated mode (start_container guards on infra_is_real),
    # but implemented for completeness: the arguments are used as-is.
    shift
    echo "$@"
}
