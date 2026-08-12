#!/usr/bin/env bash

# Google Compute Engine provider: one VM per node, reached through SSH-backed
# Docker contexts.  See infra/README.md for the contract implemented here, and
# docs/geo-distributed-deployment.md for the design.
#
# Locations are *derived*, not authored: locations.csv names the zones this
# deployment uses, in order, and regions.csv says where Google's regions are on
# Earth.  Joining them yields the lat,lon,loc map the rest of the suite reads,
# so the coordinates used for the theoretical bounds are the real positions of
# the machines rather than a city the deployment is imitating.
#
# Addressing works on two levels:
#   * peers talk to each other over the VPC-internal IP, which is what
#     infra_host_ip returns and what lands in the containers' /etc/hosts;
#   * the orchestrator reaches each VM over SSH using its external IP.
# Both are recorded per node in the gitignored state file, and nothing is ever
# written to ~/.ssh/config.

GCP_INSTANCE_PREFIX="bench-node"
GCP_CONTEXT_PREFIX="bench-ctx-"
GCP_NETWORK_TAG="bench-node"
GCP_FIREWALL="bench-internal"
GCP_IMAGE_FAMILY="ubuntu-2204-lts"
GCP_IMAGE_PROJECT="ubuntu-os-cloud"
GCP_BOOT_DISK_SIZE="50GB"
GCP_BOOT_DISK_TYPE="pd-balanced"
# The operator's own key.  Its public half is registered in each instance's
# metadata at creation, so that ssh, scp and Docker's ssh transport all reach
# the machines with no client-side configuration.  Nothing is ever written to
# ~/.ssh/config: to open a shell by hand, use `gcloud compute ssh` (or
# ./deploy.sh ssh, which wraps it).
GCP_SSH_DEFAULT_KEYS=("${HOME}/.ssh/id_ed25519" "${HOME}/.ssh/id_rsa" "${HOME}/.ssh/id_ecdsa")

###############################################################################
# Locations
###############################################################################

_gcp_zones_file()   { echo "${INFRA_DIR}/locations.csv"; }
_gcp_regions_file() { echo "${INFRA_DIR}/regions.csv"; }

# lat,lon,loc for the zones of locations.csv, joined against regions.csv.
# Materialised next to the deployment state because it is derived, not authored.
infra_locations_file() {
    local out="${DIR}/.deployment/${INFRA}.locations.csv"
    local zones regions
    zones=$(_gcp_zones_file)
    regions=$(_gcp_regions_file)

    mkdir -p "$(dirname "${out}")"
    if [ -f "${out}" ] && [ "${out}" -nt "${zones}" ] && [ "${out}" -nt "${regions}" ]; then
        echo "${out}"
        return 0
    fi

    awk -F',' -v OFS=',' '
        NR == FNR {
            if (FNR == 1) next
            gsub(/\r|^[ \t]+|[ \t]+$/, "")
            if ($1 == "") next
            loc[$1] = $2; lat[$1] = $3; lon[$1] = $4
            next
        }
        FNR == 1 { print "lat", "lon", "loc"; next }
        {
            gsub(/\r|^[ \t]+|[ \t]+$/, "")
            if ($1 == "") next
            zone = $1
            region = zone; sub(/-[a-z]$/, "", region)
            if (!(region in loc)) {
                printf "gcp: zone %s: region %s is not in regions.csv\n", zone, region > "/dev/stderr"
                bad = 1; next
            }
            if (loc[region] in seen) {
                printf "gcp: %s and %s both map to \"%s\"; container names would collide\n", \
                       seen[loc[region]], zone, loc[region] > "/dev/stderr"
                bad = 1; next
            }
            seen[loc[region]] = zone
            print lat[region], lon[region], loc[region]
        }
        END { if (bad) exit 3 }
    ' "${regions}" "${zones}" > "${out}.tmp" || {
        rm -f "${out}.tmp"
        return 1
    }
    mv "${out}.tmp" "${out}"
    echo "${out}"
}

###############################################################################
# Node <-> zone arithmetic
###############################################################################

_gcp_nodes_per_dc() {
    local n
    n=$(config nodesperdc)
    echo "${n:-1}"
}

_gcp_dc_of()    { echo $(( ($1 - 1) / $(_gcp_nodes_per_dc) + 1 )); }
_gcp_instance() { echo "${GCP_INSTANCE_PREFIX}$1"; }

# Private half of the key whose public half is pushed to instance metadata.
_gcp_ssh_key() {
    local k
    k=$(config ssh_key)
    if [ -n "${k}" ]; then
        echo "${k/#\~/${HOME}}"
        return 0
    fi
    for k in "${GCP_SSH_DEFAULT_KEYS[@]}"; do
        [ -f "${k}" ] && { echo "${k}"; return 0; }
    done
    echo ""
}

# user@external-ip for node <idx>.  The user recorded at provisioning wins, so
# a deployment stays reachable even if exp.config changes afterwards.
_gcp_ssh_target() {
    local host user
    host=$(state_get "$1" ssh_host)
    [ -z "${host}" ] && return 1
    user=$(state_get "$1" ssh_user)
    echo "${user:-$(_gcp_ssh_user)}@${host}"
}

# ssh to a node with everything stated explicitly, so that no entry in
# ~/.ssh/config is needed for the suite to work.
_gcp_ssh() {
    local idx=$1; shift
    local target
    target=$(_gcp_ssh_target "${idx}") || {
        error "gcp: node ${idx} has no recorded address; run './deploy.sh sync'"
        return 1
    }
    ssh -i "$(_gcp_ssh_key)" \
        -o IdentitiesOnly=yes \
        -o BatchMode=yes \
        -o StrictHostKeyChecking=accept-new \
        -o ConnectTimeout=10 \
        "${target}" "$@"
}

# The zone locations.csv asks for.
_gcp_planned_zone() {
    local dc zone
    dc=$(_gcp_dc_of "$1")
    zone=$(awk -F',' -v n=$((dc + 1)) 'NR == n { gsub(/\r|[ \t]/, "", $1); print $1 }' "$(_gcp_zones_file)")
    if [ -z "${zone}" ]; then
        error "gcp: node $1 needs DC ${dc}, but $(_gcp_zones_file) lists fewer zones"
        return 1
    fi
    echo "${zone}"
}

# The zone a node actually landed in, which may differ from the planned one
# when capacity forced a fallback.  Recorded at provisioning time.
_gcp_zone_of() {
    local zone
    zone=$(state_get "$1" zone)
    if [ -n "${zone}" ]; then
        echo "${zone}"
        return 0
    fi
    _gcp_planned_zone "$1"
}

# Sibling zones of a zone's region, the planned one first.
_gcp_zone_candidates() {
    local planned=$1 region
    region="${planned%-*}"
    echo "${planned}"
    gcloud compute zones list --filter="region:(${region})" --format='value(name)' --quiet 2>/dev/null \
        | grep -vx "${planned}"
}

# Create one instance, falling back to another zone of the same region when the
# requested shape is temporarily unavailable.  Sibling zones share the region,
# so `loc` and the coordinates derived from it are unaffected.
_gcp_create_instance() {
    local idx=$1 shape=$2 planned zone out
    planned=$(_gcp_planned_zone "${idx}") || return 1
    local instance
    instance=$(_gcp_instance "${idx}")

    for zone in $(_gcp_zone_candidates "${planned}"); do
        if gcloud compute instances describe "${instance}" --zone "${zone}" >/dev/null 2>&1; then
            log "gcp: ${instance} already exists in ${zone}"
            state_set "${idx}" zone "${zone}"
            return 0
        fi

        [ "${zone}" = "${planned}" ] || log "gcp: retrying ${instance} in ${zone}"
        local -a cmd=(compute instances create "${instance}"
                      --zone "${zone}"
                      --machine-type "${shape}"
                      --image-family "${GCP_IMAGE_FAMILY}"
                      --image-project "${GCP_IMAGE_PROJECT}"
                      --boot-disk-size "${GCP_BOOT_DISK_SIZE}"
                      --boot-disk-type "${GCP_BOOT_DISK_TYPE}"
                      --tags "${GCP_NETWORK_TAG}"
                      --metadata-from-file "startup-script=${INFRA_DIR}/install-docker.sh,ssh-keys=$(_gcp_ssh_keys_file)")
        log "+ gcloud ${cmd[*]}" >&2
        out=$(gcloud "${cmd[@]}" 2>&1)
        if [ $? -eq 0 ]; then
            log "gcp: created ${instance} in ${zone}"
            state_set "${idx}" zone "${zone}"
            return 0
        fi

        # Only capacity shortages are worth retrying elsewhere; anything else
        # (quota, permissions, a bad shape) would fail identically next door.
        if ! printf '%s' "${out}" | grep -q "ZONE_RESOURCE_POOL_EXHAUSTED\|does not have enough resources"; then
            error "gcp: failed to create ${instance} in ${zone}"
            printf '%s\n' "${out}" >&2
            return 1
        fi
        log "gcp: ${shape} unavailable in ${zone}"
    done

    error "gcp: no zone of ${planned%-*} can provide a ${shape} right now."
    error "     Try a different shape via 'gcp.machine' in exp.config, another"
    error "     region in ${INFRA_DIR}/locations.csv, or the same request later."
    return 1
}

# "<user>:<public key>" in a temp file, the form GCE expects for ssh-keys
# metadata.  Registering it at creation is what lets ssh, scp and Docker's ssh
# transport reach the machine without any ~/.ssh/config entry.
_gcp_ssh_keys_file() {
    local key pub
    key=$(_gcp_ssh_key)
    pub="${key}.pub"
    if [ -z "${key}" ] || [ ! -f "${pub}" ]; then
        error "gcp: no SSH public key found; generate one with 'ssh-keygen'"
        error "     or point 'ssh_key' in exp.config at an existing private key"
        return 1
    fi
    local f="${DIR}/.deployment/${INFRA}.ssh-keys"
    mkdir -p "$(dirname "${f}")"
    printf '%s:%s\n' "$(_gcp_ssh_user)" "$(cat "${pub}")" > "${f}"
    echo "${f}"
}

# Echo a gcloud invocation before running it, so that an operator can see
# exactly what the script is doing.  Debug aid; safe to drop later.
_gcp_gcloud() {
    # On stderr, so that callers redirecting gcloud's own output to /dev/null
    # do not silence the trace as well.
    log "+ gcloud $*" >&2
    gcloud "$@"
}

_gcp_ssh_user() {
    local u
    u=$(config ssh_user)
    echo "${u:-${USER}}"
}

###############################################################################
# Contract
###############################################################################

infra_is_real() {
    return 0
}

infra_host_ip() {
    local idx=${1:-0}
    [ "${idx}" -eq 0 ] 2>/dev/null && { echo ""; return 0; }
    state_get "${idx}" host
}

# Pure by design: this runs on every Docker operation, so it must not shell out.
# Contexts are created by infra_provision and re-synced by _gcp_sync_contexts.
infra_context() {
    local idx=${1:-0}
    [ "${idx}" -eq 0 ] 2>/dev/null && { echo ""; return 0; }
    echo "${GCP_CONTEXT_PREFIX}${idx}"
}

infra_net_device() {
    local dev
    dev=$(state_get "${1:-1}" net_device)
    echo "${dev:-ens4}"
}

infra_resource_limits() {
    # One node per VM: the machine shape is the limit.
    echo ""
}

infra_machine_shape() {
    # `machine` sizes the containers the simulation packs onto one host, which
    # is a different question from which VM shape to rent.  Keep them separate
    # so that switching provider cannot change simulated results.
    local m
    m=$(config "gcp.machine")
    echo "${m:-$(config machine)}"
}

infra_stage_file() {
    if [ $# -lt 3 ]; then
        error "usage: infra_stage_file <node_idx> <local_path> <remote_path>"
        return 2
    fi
    local idx=$1 local_path=$2 remote=$3 target
    target=$(_gcp_ssh_target "${idx}") || return 1
    if ! scp -q -i "$(_gcp_ssh_key)" -o IdentitiesOnly=yes -o BatchMode=yes \
             -o StrictHostKeyChecking=accept-new \
             "${local_path}" "${target}:${remote}"; then
        error "gcp: failed to stage ${local_path} onto node ${idx}"
        return 1
    fi
    echo "${remote}"
}

# Swap the shared bridge for host networking and hand the container an
# /etc/hosts covering the whole deployment, so that every name-based reference
# in the protocol scripts (seeds, --join, MADDR, the Tiga YAML) keeps working.
infra_rewrite_docker_args() {
    local cname="$1"; shift
    local args=("$@") out=() i=0 arg val host_net=0

    while [ ${i} -lt ${#args[@]} ]; do
        arg="${args[$i]}"
        case "${arg}" in
            --network)
                val="${args[$((i + 1))]}"
                if [[ "${val}" == container:* ]]; then
                    out+=("--network" "${val}")
                else
                    out+=("--network" "host"); host_net=1
                fi
                i=$((i + 2)); continue ;;
            --network=*)
                val="${arg#--network=}"
                if [[ "${val}" == container:* ]]; then
                    out+=("${arg}")
                else
                    out+=("--network" "host"); host_net=1
                fi
                i=$((i + 1)); continue ;;
            -p|--publish)
                # Host networking exposes every port already.
                i=$((i + 2)); continue ;;
            -p*|--publish=*)
                i=$((i + 1)); continue ;;
            *)
                out+=("${arg}"); i=$((i + 1)) ;;
        esac
    done

    # --add-host conflicts with `--network container:`, and a container sharing
    # another's namespace already inherits its hosts file.
    if [ ${host_net} -eq 1 ]; then
        out+=($(host_aliases))
    fi

    echo "${out[@]}"
}

infra_open_ports() {
    local allow="icmp" p
    for p in "$@"; do
        allow="${allow},tcp:${p}"
    done

    if gcloud compute firewall-rules describe "${GCP_FIREWALL}" >/dev/null 2>&1; then
        _gcp_gcloud compute firewall-rules update "${GCP_FIREWALL}" --allow "${allow}" >/dev/null || return 1
    else
        _gcp_gcloud compute firewall-rules create "${GCP_FIREWALL}" \
            --allow "${allow}" \
            --source-tags "${GCP_NETWORK_TAG}" \
            --target-tags "${GCP_NETWORK_TAG}" \
            --description "cassandra-evaluation inter-node traffic" >/dev/null || return 1
    fi
}

infra_provision() {
    local num_nodes=${1:-0}
    if [ "${num_nodes}" -le 0 ] 2>/dev/null; then
        error "usage: infra_provision <num_nodes>"
        return 2
    fi

    _gcp_preflight "${num_nodes}" || return 1

    local shape ssh_user idx zone instance
    shape=$(infra_machine_shape)
    ssh_user=$(_gcp_ssh_user)

    log "gcp: provisioning ${num_nodes} x ${shape} in project $(gcloud config get-value project 2>/dev/null)"

    # Creation is one independent API call per node, each taking tens of
    # seconds; run them together rather than end to end.
    local -a pids=() nodes=()
    for idx in $(seq 1 "${num_nodes}"); do
        _gcp_create_instance "${idx}" "${shape}" &
        pids+=($!)
        nodes+=("${idx}")
    done
    _gcp_wait_all "creation" pids nodes || return 1

    _gcp_record_addresses "${num_nodes}" "${ssh_user}" || return 1
    _gcp_wait_ready "${num_nodes}" || return 1
    _gcp_sync_contexts "${num_nodes}" || return 1

    log "gcp: ${num_nodes} node(s) ready"
}

infra_sync() {
    local num_nodes
    num_nodes=$(infra_all_indices | wc -l)
    if [ "${num_nodes}" -eq 0 ]; then
        error "gcp: no deployment recorded; run './deploy.sh provision <n>' first"
        return 1
    fi
    # External addresses change when an instance is restarted, so they are
    # re-read before the contexts that embed them are rebuilt.
    _gcp_record_addresses "${num_nodes}" "$(_gcp_ssh_user)" || return 1
    _gcp_sync_contexts "${num_nodes}" --force
}

# Interactive logins go through gcloud, which owns key distribution, IAP and
# OS Login.  The suite's own remote calls use _gcp_ssh instead.
infra_ssh() {
    local idx=${1:?usage: infra_ssh <node_idx> [command...]}
    shift
    local zone
    zone=$(_gcp_zone_of "${idx}") || return 1
    if [ $# -gt 0 ]; then
        _gcp_gcloud compute ssh "$(_gcp_instance "${idx}")" --zone "${zone}" --command "$*"
    else
        _gcp_gcloud compute ssh "$(_gcp_instance "${idx}")" --zone "${zone}"
    fi
}

# Drop any traffic shaping left on the machines.
#
# Under host networking a `tc` rule added from inside a container is applied to
# the VM's own interface and outlives that container, so a fault-tolerance run
# interrupted between injecting its 400ms delay and removing it leaves the node
# shaped -- and the next experiment would inherit it silently.  Simulated mode
# needs no equivalent: there the rule lives in the container's own network
# namespace and dies with it.
infra_reset_network() {
    local idx dev
    local -a pids=() nodes=()

    for idx in $(infra_all_indices); do
        dev=$(infra_net_device "${idx}")
        # A node with no custom qdisc makes this fail; that is the normal case.
        _gcp_ssh "${idx}" "sudo tc qdisc del dev ${dev} root" >/dev/null 2>&1 &
        pids+=($!)
        nodes+=("${idx}")
    done

    local i
    for i in "${!pids[@]}"; do
        if wait "${pids[$i]}"; then
            log "gcp: cleared a stale tc qdisc on node ${nodes[$i]}"
        fi
    done
}

infra_teardown() {
    local name zone idx

    log "gcp: tearing down"

    # Deletion blocks until the instance is really gone, so the zones are
    # torn down together rather than one after another.
    local -a pids=() nodes=()
    while read -r name zone; do
        [ -z "${name}" ] && continue
        _gcp_gcloud compute instances delete "${name}" --zone "${zone}" --quiet >/dev/null &
        pids+=($!)
        nodes+=("${name}")
    done < <(gcloud compute instances list \
                --filter="name~^${GCP_INSTANCE_PREFIX}[0-9]+$" \
                --format="value(name,zone)" 2>/dev/null)
    _gcp_wait_all "deletion" pids nodes || true

    _gcp_gcloud compute firewall-rules delete "${GCP_FIREWALL}" --quiet >/dev/null || true

    for idx in $(docker context ls --format '{{.Name}}' 2>/dev/null | grep "^${GCP_CONTEXT_PREFIX}"); do
        docker context rm -f "${idx}" >/dev/null 2>&1 || true
    done

    state_clear

    log "gcp: teardown complete"
}

###############################################################################
# Internals
###############################################################################

_gcp_preflight() {
    local num_nodes=$1 idx zone zones

    command -v gcloud >/dev/null 2>&1 || {
        error "gcp: gcloud not found in PATH"
        return 1
    }
    command -v scp >/dev/null 2>&1 || {
        error "gcp: scp not found in PATH"
        return 1
    }

    local project
    project=$(gcloud config get-value project 2>/dev/null)
    if [ -z "${project}" ] || [ "${project}" = "(unset)" ]; then
        error "gcp: no active project; run 'gcloud config set project <id>'"
        return 1
    fi

    local shape
    shape=$(infra_machine_shape)
    if [ -z "${shape}" ]; then
        error "gcp: 'machine' is unset in exp.config"
        return 1
    fi

    # Fail before spending money if the deployment map is short or unjoinable.
    infra_locations_file >/dev/null || return 1
    for idx in $(seq 1 "${num_nodes}"); do
        _gcp_planned_zone "${idx}" >/dev/null || return 1
    done

    # A disabled Compute API makes every later call fail with an error that
    # says nothing about what to do, so check it once, up front.
    zones=$(gcloud compute zones list --format='value(name)' --quiet 2>/dev/null)
    if [ -z "${zones}" ]; then
        error "gcp: cannot list zones in project ${project}."
        error "     If the Compute Engine API is not enabled yet, run:"
        error "       gcloud services enable compute.googleapis.com --project ${project}"
        return 1
    fi

    # Catch a typo in locations.csv before it becomes a half-built cluster.
    for idx in $(seq 1 "${num_nodes}"); do
        zone=$(_gcp_planned_zone "${idx}")
        if ! printf '%s\n' "${zones}" | grep -qx "${zone}"; then
            error "gcp: '${zone}' (node ${idx}) is not a zone of this project"
            return 1
        fi
    done
}

_gcp_record_addresses() {
    local num_nodes=$1 ssh_user=$2 idx zone instance internal external

    for idx in $(seq 1 "${num_nodes}"); do
        zone=$(_gcp_zone_of "${idx}") || return 1
        instance=$(_gcp_instance "${idx}")
        internal=$(gcloud compute instances describe "${instance}" --zone "${zone}" \
                    --format='get(networkInterfaces[0].networkIP)' 2>/dev/null)
        if [ -z "${internal}" ]; then
            error "gcp: could not read the internal IP of ${instance}"
            return 1
        fi
        external=$(gcloud compute instances describe "${instance}" --zone "${zone}" \
                    --format='get(networkInterfaces[0].accessConfigs[0].natIP)' 2>/dev/null)
        state_set "${idx}" host "${internal}"
        state_set "${idx}" ssh_host "${external}"
        state_set "${idx}" ssh_user "${ssh_user}"
    done
}

_gcp_wait_all() {
    local what=$1
    local -n _pids=$2
    local -n _nodes=$3
    local i rc=0
    for i in "${!_pids[@]}"; do
        if ! wait "${_pids[$i]}"; then
            error "gcp: ${what} failed for node ${_nodes[$i]}"
            rc=1
        fi
    done
    return ${rc}
}

# Everything a single node needs before it can run containers.  Written as one
# function so that the nodes can be brought up concurrently: each machine
# installs Docker on its own schedule and there is nothing to serialise.
_gcp_prepare_node() {
    local idx=$1 instance deadline dev
    local timeout=${GCP_READY_TIMEOUT:-600}

    instance=$(_gcp_instance "${idx}")
    deadline=$(( $(date +%s) + timeout ))
    log "gcp: waiting for ${instance} to finish installing Docker"

    while true; do
        if _gcp_ssh "${idx}" "test -f /var/lib/bench-node-ready" >/dev/null 2>&1; then
            break
        fi
        if [ "$(date +%s)" -ge "${deadline}" ]; then
            error "gcp: ${instance} did not become ready within ${timeout}s"
            return 1
        fi
        sleep 5
    done

    # Group membership only takes effect on the next login, which is why this
    # runs before the first Docker call rather than in the startup script
    # (where the user may not exist yet).
    _gcp_ssh "${idx}" \
        "getent group docker >/dev/null && id -nG | grep -qw docker || sudo usermod -aG docker \$(id -un)" \
        >/dev/null 2>&1 || true

    if ! _gcp_ssh "${idx}" "docker info" >/dev/null 2>&1; then
        error "gcp: docker is not usable as $(_gcp_ssh_user) on ${instance}"
        return 1
    fi

    # Read the real interface name rather than assuming ens4.
    dev=$(_gcp_ssh "${idx}" "ip -o route show default | awk '{print \$5; exit}'" 2>/dev/null | tr -d '\r')
    [ -n "${dev}" ] && state_set "${idx}" net_device "${dev}"
    log "gcp: ${instance} ready"
}

_gcp_wait_ready() {
    local num_nodes=$1 idx
    local -a pids=() nodes=()
    for idx in $(seq 1 "${num_nodes}"); do
        _gcp_prepare_node "${idx}" &
        pids+=($!)
        nodes+=("${idx}")
    done
    _gcp_wait_all "setup" pids nodes
}

# Contexts embed the machine's address, so --force recreates them after the
# addresses have been re-read.
_gcp_sync_contexts() {
    local num_nodes=${1:-$(infra_all_indices | wc -l)}
    local force=${2:-}
    local idx name existing target

    existing=$(docker context ls --format '{{.Name}}' 2>/dev/null)

    for idx in $(seq 1 "${num_nodes}"); do
        name="${GCP_CONTEXT_PREFIX}${idx}"
        if printf '%s\n' "${existing}" | grep -qx "${name}"; then
            [ "${force}" = "--force" ] || continue
            docker context rm -f "${name}" >/dev/null 2>&1 || true
        fi
        target=$(_gcp_ssh_target "${idx}") || {
            error "gcp: node ${idx} has no recorded address"
            return 1
        }
        docker context create "${name}" --docker "host=ssh://${target}" >/dev/null || {
            error "gcp: failed to create Docker context ${name}"
            return 1
        }
    done
}

# Self-heal on source: a deployment recorded in the state file is useless if its
# Docker contexts have gone missing.  One `docker context ls` per script run.
_gcp_autosync() {
    local expected have
    expected=$(infra_all_indices | wc -l)
    [ "${expected}" -eq 0 ] && return 0
    have=$(docker context ls --format '{{.Name}}' 2>/dev/null | grep -c "^${GCP_CONTEXT_PREFIX}")
    [ "${have}" -ge "${expected}" ] && return 0
    _gcp_sync_contexts "${expected}"
}

_gcp_autosync
