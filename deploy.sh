#!/usr/bin/env bash

# Manage the machines the benchmarks run on.
#
# The provider is selected with `infra=` in exp.config; with the default
# `simulation` provider every command below is a no-op, because the containers
# run on the local Docker daemon.  See infra/README.md.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

usage() {
    cat <<EOF
Usage: $0 <command> [args]

  bootstrap <n>   Provision <n> machines and open the protocol ports.  The one
                  command needed before a first run; the experiment scripts pull
                  the images themselves.
  provision <n>   Provision <n> machines only.
  sync            Re-create the Docker contexts and SSH aliases of an existing
                  deployment (after a machine restart, or on another laptop).
  status          Show what the locations map, the provider and Docker agree on.
  ssh <idx>       Open a shell on node <idx>.
  teardown        Destroy every machine, firewall rule and context.
  push-image <local_image> [as <tag>]
                  Distribute a locally built image to every node's Docker
                  daemon directly (docker save/load), no registry needed.
                  Defaults to distributing it under its own tag; "as <tag>"
                  re-tags it first, e.g. what an exp.config *_image key
                  should be pointed at.
  use-image <name> <tag>
                  Point exp.config's <name>_..._image key (e.g. "accord" ->
                  accord_cassandra_image) at <tag>, to switch a protocol
                  between its official and a pushed custom image.

Current provider: $(config infra)
EOF
}

cmd_status() {
    local nodes_per_dc dc loc host users dev idx
    nodes_per_dc=$(config nodesperdc); nodes_per_dc=${nodes_per_dc:-1}

    echo "provider   : $(config infra)"
    echo "machine    : $(config machine)"
    echo "nodesperdc : ${nodes_per_dc}"
    if ! infra_is_real; then
        echo
        echo "Simulated deployment: all containers run on the local Docker daemon."
        return 0
    fi

    echo
    printf '%-5s %-12s %-22s %-16s %-10s %s\n' "NODE" "DC" "REGION" "PEER IP" "DEVICE" "CONTEXT"
    for idx in $(infra_all_indices); do
        dc=$(( (idx - 1) / nodes_per_dc + 1 ))
        loc=$(get_location "${dc}" "${LOCATIONS_FILE}" 2>/dev/null)
        printf '%-5s %-12s %-22s %-16s %-10s %s\n' \
            "${idx}" "${loc}" "$(state_get "${idx}" zone)" \
            "$(infra_host_ip "${idx}")" "$(infra_net_device "${idx}")" \
            "$(infra_context "${idx}")"
    done

    echo
    echo "Docker daemons:"
    for idx in $(infra_all_indices); do
        if docker --context "$(infra_context "${idx}")" info >/dev/null 2>&1; then
            printf '  node %-3s reachable\n' "${idx}"
        else
            printf '  node %-3s UNREACHABLE\n' "${idx}"
        fi
    done
}

cmd_use_image() {
    local name="$1" tag="$2"
    local -a matches=()
    local key
    while IFS='=' read -r key _; do
        [[ "$key" =~ ^[[:space:]]*# ]] && continue
        [[ "$key" =~ _image$ ]] || continue
        [[ "$key" == "${name}_"* || "$key" == "${name}_image" ]] && matches+=("$key")
    done < "${CONFIG_FILE}"

    if [ ${#matches[@]} -eq 0 ]; then
        error "use-image: no *_image key in exp.config starts with '${name}_'"
        return 1
    fi
    if [ ${#matches[@]} -gt 1 ]; then
        error "use-image: '${name}' matches more than one key: ${matches[*]} -- be more specific"
        return 1
    fi

    sed -i "s|^${matches[0]}=.*|${matches[0]}=${tag}|" "${CONFIG_FILE}"
    log "use-image: ${matches[0]}=${tag}"
}

case "${1:-}" in
    bootstrap)
        [ -n "${2:-}" ] || { usage; exit 1; }
        infra_bootstrap "$2"
        ;;
    provision)
        [ -n "${2:-}" ] || { usage; exit 1; }
        infra_provision "$2"
        ;;
    sync)
        infra_sync
        ;;
    status)
        cmd_status
        ;;
    ssh)
        [ -n "${2:-}" ] || { usage; exit 1; }
        infra_ssh "${@:2}"
        ;;
    teardown)
        infra_teardown
        ;;
    push-image)
        [ -n "${2:-}" ] || { usage; exit 1; }
        push_local_image "${@:2}"
        ;;
    use-image)
        [ -n "${3:-}" ] || { usage; exit 1; }
        cmd_use_image "$2" "$3"
        ;;
    -h|--help|help|"")
        usage
        ;;
    *)
        echo "Unknown command: $1"
        usage
        exit 1
        ;;
esac
