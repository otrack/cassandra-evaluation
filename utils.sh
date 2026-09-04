#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")
LOGDIR="${DIR}/logs"
RESULTSDIR="${DIR}/results/"
CONFIG_FILE="${DIR}/exp.config"

config() {
    if [ $# -ne 1 ]; then
        echo "usage: config key"
        exit -1
    fi
    local key=$1
    local val
    val=$(cat ${CONFIG_FILE} | grep -E "^${key}=" | cut -d= -f2)
    if [ -z "$val" ] && [ "$key" == "nodesperdc" ]; then
        val=1
    fi
    echo "$val"
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

init_logdir() {
    mkdir -p ${LOGDIR};
}

clean_logdir() {
    rm -f ${LOGDIR}/*    
}

DEBUG=$(config debug)

###############################################################################
# Locations and deployment state
#
# Each provider owns the map of the locations it deploys to: the simulation
# mimics a set of cities, while a cloud provider derives its locations from the
# regions it runs in.  ${LOCATIONS_FILE} always exposes that map in the same
# shape -- lat,lon,loc, one row per DC, in order -- and is set below, once the
# provider has been sourced.
#
# Anything discovered at provisioning time (addresses, ssh users, interface
# names) is deployment state, not configuration: it lives per node in
# ${STATE_FILE}, under a gitignored directory, and never touches a tracked
# file.  See infra/README.md.
###############################################################################

STATE_FILE="${DIR}/.deployment/$(config infra).csv"
STATE_COLUMNS="node,zone,host,ssh_host,ssh_user,net_device"

state_get() {
    if [ $# -ne 2 ]; then
        error "usage: state_get <node_idx> <column>"
        return 2
    fi
    [ -f "${STATE_FILE}" ] || return 0
    awk -F',' -v n="$1" -v c="$2" '
        NR==1 { for (i = 1; i <= NF; i++) { h = $i; gsub(/\r|^[ \t]+|[ \t]+$/, "", h)
                                            if (h == c) k = i; if (h == "node") nk = i } next }
        nk && k && $nk == n { gsub(/\r|^[ \t]+|[ \t]+$/, "", $k); print $k; exit }
    ' "${STATE_FILE}"
}

state_set() {
    if [ $# -ne 3 ]; then
        error "usage: state_set <node_idx> <column> <value>"
        return 2
    fi
    local node=$1 col=$2 val=$3 tmp
    mkdir -p "$(dirname "${STATE_FILE}")"
    # Provisioning updates several nodes at once, and this is a
    # read-modify-write of one shared file, so writers are serialised.
    (
        flock 9
        [ -f "${STATE_FILE}" ] || echo "${STATE_COLUMNS}" > "${STATE_FILE}"
        grep -q "^${node}," "${STATE_FILE}" || \
            echo "${node}$(printf ',%.0s' $(seq 2 $(echo "${STATE_COLUMNS}" | tr ',' '\n' | wc -l)))" >> "${STATE_FILE}"
        tmp=$(mktemp)
        awk -F',' -v OFS=',' -v n="${node}" -v c="${col}" -v v="${val}" '
            NR==1 { for (i = 1; i <= NF; i++) { h = $i; gsub(/\r|^[ \t]+|[ \t]+$/, "", h)
                                                if (h == c) k = i; if (h == "node") nk = i }
                    sub(/\r$/, ""); print; next }
            { sub(/\r$/, ""); if (nk && k && $nk == n) $k = v; print }
        ' "${STATE_FILE}" > "${tmp}" && mv "${tmp}" "${STATE_FILE}"
    ) 9>"${STATE_FILE}.lock"
}

# Every node recorded by a provisioning run, in order.
state_nodes() {
    [ -f "${STATE_FILE}" ] || return 0
    awk -F',' 'NR > 1 && $1 ~ /^[0-9]+$/ { print $1 }' "${STATE_FILE}" | sort -n
}

state_clear() {
    rm -f "${STATE_FILE}"
}

# node_index_of <container_name> -> 1..N, or 0 for the orchestrator.
#
# Database containers are named "${city}${k}", so the DC is recovered by a
# reverse lookup on the city name: the numeric suffix is the index of the node
# *within* its DC, not the node index.
node_index_of() {
    local name="$1"
    local nodes_per_dc
    nodes_per_dc=$(config nodesperdc)
    nodes_per_dc=${nodes_per_dc:-1}

    case "${name}" in
        ""|accord-viz)
            # Runs alongside the benchmark scripts, not on a node.
            echo 0; return 0 ;;
        swiftpaxos-master|ycsb)
            # No DC of their own; pinned to the first node.
            echo 1; return 0 ;;
        ycsb-*|database-node*)
            local dc="${name#ycsb-}"
            dc="${dc#database-node}"
            if [[ "$dc" =~ ^[0-9]+$ ]]; then
                echo $(( (dc - 1) * nodes_per_dc + 1 )); return 0
            fi
            echo 0; return 0 ;;
    esac

    local city="${name%%[0-9]*}"
    local k="${name##*[!0-9]}"
    [[ "$k" =~ ^[0-9]+$ ]] || k=1
    if [ -n "${city}" ]; then
        local i=1 loc
        while loc=$(get_location $i "${LOCATIONS_FILE}" 2>/dev/null) && [ -n "$loc" ]; do
            if [ "$loc" = "$city" ]; then
                echo $(( (i - 1) * nodes_per_dc + k ))
                return 0
            fi
            i=$((i + 1))
        done
    fi

    error "node_index_of: cannot resolve container name '${name}'"
    echo 0
    return 1
}

# Every node a provisioning run has recorded.
infra_all_indices() {
    state_nodes
}

# The first <count> location names of the active provider (all of them when
# <count> is omitted), space separated.
locations_list() {
    local count=${1:-0} i=1 loc out=""
    while :; do
        [ "${count}" -gt 0 ] && [ "${i}" -gt "${count}" ] && break
        loc=$(get_location ${i} "${LOCATIONS_FILE}" 2>/dev/null) || break
        [ -z "${loc}" ] && break
        out="${out}${out:+ }${loc}"
        i=$((i + 1))
    done
    echo "${out}"
}

# --add-host entries for every container of the deployment, for providers that
# give up Docker's embedded DNS.
host_aliases() {
    local nodes_per_dc i k loc
    nodes_per_dc=$(config nodesperdc)
    nodes_per_dc=${nodes_per_dc:-1}
    i=1
    while loc=$(get_location $i "${LOCATIONS_FILE}" 2>/dev/null) && [ -n "$loc" ]; do
        for k in $(seq 1 "${nodes_per_dc}"); do
            local ip
            ip=$(infra_host_ip $(( (i - 1) * nodes_per_dc + k )))
            [ -n "$ip" ] && printf -- '--add-host %s%s:%s ' "$loc" "$k" "$ip"
        done
        i=$((i + 1))
    done
    local first
    first=$(infra_host_ip 1)
    [ -n "$first" ] && printf -- '--add-host swiftpaxos-master:%s ' "$first"
}

# Images are deliberately not pulled here: every experiment script pulls them
# itself, so doing it now would only fetch versions that may be superseded
# before the first run.
infra_bootstrap() {
    local num_nodes=$1
    infra_provision "${num_nodes}" || return 1
    infra_open_ports 7000 7087 8080 9042 10000 26257
}

###############################################################################
# Docker dispatch
#
# Every Docker operation is routed to the daemon owning the container through
# these wrappers.  The first argument is always the container name, used only
# to pick the daemon; in simulated mode infra_is_real fails and the wrappers
# degenerate to plain `docker` calls.
###############################################################################

d() {
    if [ $# -lt 2 ]; then
        error "usage: d <container_name> <docker args...>"
        return 2
    fi
    local name="$1"; shift
    local ctx=""
    if infra_is_real; then
        ctx=$(infra_context "$(node_index_of "${name}")")
    fi
    if [ -n "${ctx}" ]; then
        docker --context "${ctx}" "$@"
    else
        docker "$@"
    fi
}

# dexec <container> [-i|-t|-u user|-e VAR=val ...] <command...>
dexec() {
    local name="$1"; shift
    local flags=()
    while [ $# -gt 0 ]; do
        case "$1" in
            -i|-t|-it|-ti|--interactive|--tty|--privileged|-d|--detach)
                flags+=("$1"); shift ;;
            -u|--user|-w|--workdir|-e|--env)
                flags+=("$1" "$2"); shift 2 ;;
            *) break ;;
        esac
    done
    d "${name}" exec "${flags[@]}" "${name}" "$@"
}

# The remaining verbs all take the container name last on the docker command
# line, so any flags the caller passes are simply forwarded ahead of it.
dlogs()    { local name="$1"; shift; d "${name}" logs    "$@" "${name}"; }
dinspect() { local name="$1"; shift; d "${name}" inspect "$@" "${name}"; }
dstop()    { local name="$1"; shift; d "${name}" stop    "$@" "${name}"; }
dkill()    { local name="$1"; shift; d "${name}" kill    "$@" "${name}"; }
drm()      { local name="$1"; shift; d "${name}" rm -f   "$@" "${name}"; }

# dpull <node_idx> <image>
dpull() {
    local idx=$1 image=$2 ctx=""
    if infra_is_real; then
        ctx=$(infra_context "${idx}")
    fi
    if [ -n "${ctx}" ]; then
        docker --context "${ctx}" pull "${image}"
    else
        docker pull "${image}"
    fi
}

start_container() {
    if [ $# -lt 4 ]; then
        error "usage: start_container <image> <name> <message> <logfile> [docker args...] [-- container_cmd...]"
        return 2
    fi

    local image="$1"
    local cname="$2"
    local wait_msg="$3"
    local log_file="$4"
    shift 4
    
    # Split arguments on "--" separator
    # Everything before "--" is docker_args, everything after is container_cmd
    local docker_args=()
    local container_cmd=()
    local found_separator=0
    
    while [ $# -gt 0 ]; do
        if [ "$1" = "--" ]; then
            found_separator=1
            shift
        elif [ $found_separator -eq 0 ]; then
            docker_args+=("$1")
            shift
        else
            container_cmd+=("$1")
            shift
        fi
    done
    
    # Let the provider adjust placement-related arguments (network mode, host
    # aliases, ...).  Skipped entirely in simulated mode so that the command
    # line stays exactly what the caller asked for.
    if infra_is_real; then
        docker_args=($(infra_rewrite_docker_args "${cname}" "${docker_args[@]}"))
    fi

    log "Starting container from image '${image}' as '${cname}' using ${log_file} to log and args '${docker_args[*]}'"
    if [ ${#container_cmd[@]} -gt 0 ]; then
        log "Container command: ${container_cmd[*]}"
    fi

    local cid
    echo "docker run ${docker_args[@]} --name $cname $image ${container_cmd[@]}"
    cid=$(d $cname run ${docker_args[@]} --log-opt max-size=10m  --log-opt max-file=3 --name $cname $image ${container_cmd[@]} 2>&1) || {
         error "docker run failed: ${cid}"
         return 3
    }
    log "Started container '${cname}' (id: ${cid})"

    dlogs $cname -f > ${log_file} 2>&1 &

    local start_time
    start_time=$(date +%s)
    local timeout
    timeout=${START_CONTAINER_TIMEOUT:-90}   # seconds, override by exporting START_CONTAINER_TIMEOUT

    while true; do
        # Check for the readiness message in logs
        if dlogs "$cname" 2>&1 | grep -F -- "$wait_msg" >/dev/null; then
            log "Container '${cname}' is ready (found: '${wait_msg}')"
            return 0
        fi

        # Check whether container is still running
        local running
        running=$(dinspect "$cname" -f '{{.State.Running}}' 2>/dev/null) || {
            error "Failed to inspect container '${cname}'"
            return 4
        }
        if [ "$running" != "true" ]; then
            local exit_code
            exit_code=$(dinspect "$cname" -f '{{.State.ExitCode}}' 2>/dev/null || echo "unknown")
            error "Container '${cname}' exited with code ${exit_code} before readiness message appeared. Logs:"
            dlogs "$cname" 2>&1 | sed 's/^/  /'
            return 5
        fi

        # Timeout check
        if (( $(date +%s) - start_time >= timeout )); then
            error "Timeout (${timeout}s) waiting for '${wait_msg}' in container '${cname}' logs. Logs:"
            dlogs "$cname" 2>&1 | sed 's/^/  /'
            return 6
        fi

        sleep 0.5
    done
}

wait_container() {
    if [ $# -lt 1 ]; then
        error "usage: wait_container <name>"
        return 2
    fi

    local cname="$1"

    # Wait until container is no longer running (or inspect disappears)
    log "Waiting container '${cname}' to terminate"
    while true; do
        running=$(dinspect "$cname" -f '{{.State.Running}}' 2>/dev/null) || {
            # Inspect failing -> container removed or no longer present; treat as stopped
            log "Container '${cname}' no longer present; considered stopped"
            return 0
        }

        if [ "$running" != "true" ]; then
            log "Container '${cname}' stopped"
            return 0
        fi

        sleep 0.5
    done    
}

fetch_logs_container() {
    if [ $# -lt 1 ]; then
        error "usage: fetch_logs_container <name> [timeout_seconds]"
        return 2
    fi

    local cname="$1"

    dlogs "$cname" 2>&1 | sed 's/^/  /'
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
    running=$(dinspect "$cname" -f '{{.State.Running}}' 2>/dev/null) || {
        log "Container '${cname}' does not exist or is already removed"
        return 0
    }

    if [ "$running" != "true" ]; then
        # Not running, but still present: a normal --rm exit would already
        # have removed it, so this is a container stuck in a state --rm
        # never fires for (e.g. "Created" -- creation succeeded but it never
        # started, which happens when a run command's client disconnects
        # mid-flight over a laggy SSH-tunneled Docker API). Left alone, it
        # blocks the next `docker run --name` for this container with a
        # "Conflict... already in use" error.
        log "Container '${cname}' is present but not running; removing it"
        drm "$cname" >/dev/null 2>&1 || true
        return 0
    fi

    if ! dstop "$cname" >/dev/null 2>&1; then
        error "docker stop failed for container '${cname}'"
        return 4
    fi

    local start_time
    start_time=$(date +%s)

    # Wait until container is no longer running (or inspect disappears)
    while true; do
        running=$(dinspect "$cname" -f '{{.State.Running}}' 2>/dev/null) || {
            # Inspect failing -> container removed or no longer present; treat as stopped
            log "Container '${cname}' no longer present; considered stopped"
            return 0
        }
        if [ "$running" != "true" ]; then
            log "Container '${cname}' stopped"
            # --rm removes it on its own exit event, but that can race this
            # check (or not apply at all for a container started without
            # --rm); force removal so a lingering, name-conflicting container
            # never survives a stop_container call.
            drm "$cname" >/dev/null 2>&1 || true
            return 0
        fi

        if (( $(date +%s) - start_time >= timeout )); then
            error "Timeout (${timeout}s) waiting for container '${cname}' to stop"
            return 5
        fi

        sleep 0.5
    done
}

# Function to get the IP address of a container.
#
# Simulated mode reads the bridge IP off the daemon.  With a real provider the
# container shares its machine's network, so the address is the machine's --
# note that this answers even when the container is down, which is why callers
# that need liveness must use container_exists() instead.
get_container_ip() {
    container_name=$1
    if infra_is_real; then
        infra_host_ip "$(node_index_of "$container_name")"
        return
    fi
    ip_address=$(dinspect "$container_name" -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null)
    echo "$ip_address"
}

# True when the container exists on the daemon that owns it.
container_exists() {
    dinspect "$1" >/dev/null 2>&1
}

# True when the node backing *container* is part of the running deployment.
#
# Simulated mode infers this from the presence of a bridge IP, which is how the
# ${pref}_get_node_count functions have always counted nodes.  That inference
# does not survive a real provider, where get_container_ip answers from the
# registry whether or not the container is up, so liveness is probed directly.
node_is_up() {
    if infra_is_real; then
        container_exists "$1"
    else
        [ -n "$(get_container_ip "$1")" ]
    fi
}

# Function to stop a container after a delay
stop_container_after_delay() {
    container_name=$1
    delay=$2
    (
        sleep "$delay"
        dstop "$container_name"
        log "Stopped container '$container_name' after $delay seconds."
    ) &
}

# Container CPU/memory caps.  The policy belongs to the provider: simulated
# mode squeezes several nodes onto one machine and caps each of them, whereas a
# real provider gives every node a machine of its own and returns nothing.
get_resource_limits() {
    infra_resource_limits "${1:-1}"
}

compute_test_machine() {
    local num_dcs=$1
    local nodes_per_dc=${2:-$(config nodesperdc)}

    # Right-sizing containers to the local box is meaningless once every node
    # owns a machine, and rewriting machine= would desynchronise exp.config
    # from the shapes already provisioned.
    if infra_is_real; then
        log "Test mode: keeping machine spec '$(config machine)' (provisioned by $(config infra))"
        return 0
    fi
    if [ -z "$num_dcs" ] || ! [[ "$num_dcs" =~ ^[0-9]+$ ]] || [ "$num_dcs" -le 0 ]; then
        error "compute_test_machine: num_dcs must be a positive integer"
        return 1
    fi
    if [ -z "$nodes_per_dc" ] || ! [[ "$nodes_per_dc" =~ ^[0-9]+$ ]] || [ "$nodes_per_dc" -le 0 ]; then
        nodes_per_dc=1
    fi
    local total_nodes=$((num_dcs * nodes_per_dc))

    # Get actual machine CPUs
    local actual_cpus
    actual_cpus=$(nproc)

    # Get actual machine memory in GB (1 GB = 1048576 kB)
    local actual_mem_kb
    actual_mem_kb=$(awk '/^MemTotal:/{print $2}' /proc/meminfo)
    local actual_mem_gb
    actual_mem_gb=$(awk "BEGIN { printf \"%.6f\", ${actual_mem_kb} / 1048576 }")

    local gcp_csv="${DIR}/gcp.csv"
    if [ ! -f "$gcp_csv" ]; then
        error "gcp.csv not found: ${gcp_csv}"
        return 1
    fi

    # Find the best spec s where actual_mem_gb <= s.g * total_nodes
    # gcp.csv columns: $1=name, $2=vcpus, $3=memory(GB)
    local machine
    machine=$(awk -F',' -v c="$actual_cpus" -v g="$actual_mem_gb" -v k="$total_nodes" '
        NR>1 && ($3+0)*k <= g+0 {
            if (best == "" || $2+0 >= best_c+0) {
                best = $1
                best_c = $2
                best_g = $3
            }
        }
        END { print best }
    ' "$gcp_csv")

    if [ -z "$machine" ]; then
        error "No suitable machine spec found in gcp.csv for ${actual_cpus} CPUs and ${actual_mem_gb}GB memory with ${total_nodes} nodes (${num_dcs} DCs x ${nodes_per_dc} nodes/DC)"
        return 1
    fi

    log "Test mode: machine spec '${machine}' for ${actual_cpus} CPUs, ${actual_mem_gb}GB memory, ${total_nodes} total nodes (${num_dcs} DCs x ${nodes_per_dc} nodes/DC)"
    sed -i "s/^machine=.*/machine=${machine}/" "${CONFIG_FILE}"
    return 0
}

pull_images() {
    log "Pulling all Docker images from ${CONFIG_FILE}..."

    local -a images=()
    local key value
    while IFS='=' read -r key value; do
        [[ -z "$key" || "$key" =~ ^[[:space:]]*# ]] && continue
        [[ "$key" =~ _image$ ]] || continue
        images+=("$(echo "$value" | xargs)")
    done < "${CONFIG_FILE}"

    # One local daemon: pull in sequence, as this has always done.
    if ! infra_is_real; then
        local image
        for image in "${images[@]}"; do
            log "Pulling image: ${image}"
            dpull 1 "${image}"
            if [ $? -ne 0 ]; then
                log "ERROR: Failed to pull image '${image}'. Aborting."
                exit 1
            fi
        done
        log "All Docker images pulled successfully."
        return 0
    fi

    # Every machine needs every image, and each pull crosses a WAN, so the
    # machines are done concurrently.  Images stay sequential *within* a
    # machine, to avoid one daemon fighting itself over shared layers.
    local -a pids=() nodes=()
    local idx image
    for idx in $(infra_all_indices); do
        log "Pulling ${#images[@]} image(s) onto node ${idx}..."
        (
            for image in "${images[@]}"; do
                dpull "${idx}" "${image}" >/dev/null || exit 1
            done
        ) &
        pids+=($!)
        nodes+=("${idx}")
    done

    local i rc=0
    for i in "${!pids[@]}"; do
        if wait "${pids[$i]}"; then
            log "Images ready on node ${nodes[$i]}"
        else
            error "Failed to pull images on node ${nodes[$i]}"
            rc=1
        fi
    done
    if [ ${rc} -ne 0 ]; then
        log "ERROR: image pull failed. Aborting."
        exit 1
    fi
    log "All Docker images pulled successfully."
}

get_location() {
  local k="$1"
  local file="${2:-${LOCATIONS_FILE}}"

  # Validate arguments
  if [ -z "$k" ]; then
    echo "Usage: get_location <k> [file]" >&2
    return 2
  fi
  if ! [[ "$k" =~ ^[0-9]+$ ]] || [ "$k" -le 0 ]; then
    echo "Error: k must be a positive integer" >&2
    return 2
  fi
  if [ ! -r "$file" ]; then
    echo "Error: file '$file' not found or not readable" >&2
    return 3
  fi

  # Compute the target file line number (header is line 1)
  local target_line=$((k + 1))

  # Extract the 3rd CSV field from the target line.
  # NOTE: some awk implementations (e.g., busybox awk) do not accept "--" as an option terminator.
  # Do NOT pass "--" to awk; pass the filename as the last argument instead.
  local loc
  loc=$(awk -F',' -v n="$target_line" 'NR==n{
      field=$3
      gsub(/\r/,"",field)               # drop CR if file has CRLF
      sub(/^[ \t]+/,"",field)           # trim leading space
      sub(/[ \t]+$/,"",field)           # trim trailing space
      print field
    }' "$file") || { echo "Error: failed to read '$file' with awk" >&2; return 3; }

  if [ -z "$loc" ]; then
    echo "Error: no such data line (k=$k) in '$file'" >&2
    return 4
  fi

  # Strip surrounding double quotes if present
  loc="${loc%\"}"
  loc="${loc#\"}"

  # Extract leading alphabetic sequence for the location
  if [[ "$loc" =~ ^([A-Za-z]+) ]]; then
    printf '%s\n' "$(printf '%s' "${BASH_REMATCH[1]}")"
    return 0
  fi

  echo "Error: location value does not contain alphabetic name: '$loc'" >&2
  return 5
}

###############################################################################
# Infrastructure provider
#
# Sourced last so that a provider may use every helper above.  See
# infra/README.md for the contract each provider implements.
###############################################################################

INFRA=$(config infra)
INFRA=${INFRA:-simulation}
if [ ! -r "${DIR}/infra/${INFRA}/provider.sh" ]; then
    error "Unknown infra provider '${INFRA}' (no ${DIR}/infra/${INFRA}/provider.sh)"
    exit 1
fi
INFRA_DIR="${DIR}/infra/${INFRA}"
source "${INFRA_DIR}/provider.sh"

# The map of locations this provider deploys to: lat,lon,loc, one row per DC.
LOCATIONS_FILE=$(infra_locations_file)
if [ ! -r "${LOCATIONS_FILE}" ]; then
    error "Provider '${INFRA}' produced no locations file (${LOCATIONS_FILE})"
    exit 1
fi
