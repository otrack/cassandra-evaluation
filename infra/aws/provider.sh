#!/usr/bin/env bash

# Amazon EC2 provider: one instance per node, reached through SSH-backed
# Docker contexts.  See infra/README.md for the contract implemented here, and
# docs/aws-deployment.md for the design.
#
# Locations are *derived*, not authored: locations.csv names the availability
# zones (AZs) this deployment uses, in order, and regions.csv says where AWS's
# regions are on Earth.  Joining them yields the lat,lon,loc map the rest of
# the suite reads, so the coordinates used for the theoretical bounds are the
# real positions of the machines rather than a city the deployment is
# imitating.
#
# Every AWS CLI call is region-scoped (there is no project-global session the
# way there is with gcloud), so a node's region is derived from its AZ by
# stripping the trailing letter, and every per-region resource (subnet,
# security group, AMI id) is resolved once per distinct region in use.
#
# There is no metadata-driven key injection the way GCE has ssh-keys metadata:
# the operator's public key is appended to ~/.ssh/authorized_keys by the
# user-data script (install-docker.sh) that also installs Docker, which is the
# only hook that runs before an operator can reach the instance at all.
# Nothing is ever written to ~/.ssh/config.

AWS_INSTANCE_PREFIX="bench-node"
AWS_CONTEXT_PREFIX="bench-ctx-aws-"
AWS_SECURITY_GROUP="bench-internal"
AWS_AMI_SSM_PARAM="/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
AWS_BOOT_VOLUME_SIZE="50"
AWS_BOOT_VOLUME_TYPE="gp3"
# The operator's own key.  Its public half is injected via user-data at
# instance creation, so that ssh, scp and Docker's ssh transport all reach the
# machines with no client-side configuration.
AWS_SSH_DEFAULT_KEYS=("${HOME}/.ssh/id_ed25519" "${HOME}/.ssh/id_rsa" "${HOME}/.ssh/id_ecdsa")

###############################################################################
# Locations
###############################################################################

_aws_az_file()      { echo "${INFRA_DIR}/locations.csv"; }
_aws_regions_file() { echo "${INFRA_DIR}/regions.csv"; }

# lat,lon,loc for the AZs of locations.csv, joined against regions.csv.
# Materialised next to the deployment state because it is derived, not
# authored.
infra_locations_file() {
    local out="${DIR}/.deployment/${INFRA}.locations.csv"
    local azs regions
    azs=$(_aws_az_file)
    regions=$(_aws_regions_file)

    mkdir -p "$(dirname "${out}")"
    if [ -f "${out}" ] && [ "${out}" -nt "${azs}" ] && [ "${out}" -nt "${regions}" ]; then
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
            az = $1
            region = az; sub(/[a-z]$/, "", region)
            if (!(region in loc)) {
                printf "aws: AZ %s: region %s is not in regions.csv\n", az, region > "/dev/stderr"
                bad = 1; next
            }
            if (loc[region] in seen) {
                printf "aws: %s and %s both map to \"%s\"; container names would collide\n", \
                       seen[loc[region]], az, loc[region] > "/dev/stderr"
                bad = 1; next
            }
            seen[loc[region]] = az
            print lat[region], lon[region], loc[region]
        }
        END { if (bad) exit 3 }
    ' "${regions}" "${azs}" > "${out}.tmp" || {
        rm -f "${out}.tmp"
        return 1
    }
    mv "${out}.tmp" "${out}"
    echo "${out}"
}

###############################################################################
# Node <-> AZ arithmetic
###############################################################################

_aws_nodes_per_dc() {
    local n
    n=$(config nodesperdc)
    echo "${n:-1}"
}

_aws_dc_of()      { echo $(( ($1 - 1) / $(_aws_nodes_per_dc) + 1 )); }
_aws_instance()   { echo "${AWS_INSTANCE_PREFIX}$1"; }
_aws_region_of()  { echo "${1%[a-z]}"; }

# Taken from the environment rather than exp.config: which key an operator
# uses is a property of their machine, not of the experiment, and exp.config
# is version controlled.
_aws_ssh_key() {
    local k
    if [ -n "${BENCH_SSH_KEY}" ]; then
        echo "${BENCH_SSH_KEY/#\~/${HOME}}"
        return 0
    fi
    for k in "${AWS_SSH_DEFAULT_KEYS[@]}"; do
        [ -f "${k}" ] && { echo "${k}"; return 0; }
    done
    echo ""
}

# The Ubuntu AMI has one fixed login account; there is no per-operator OS
# identity the way GCE's OS Login can provide.
_aws_ssh_user() {
    echo "${BENCH_SSH_USER:-ubuntu}"
}

# user@private-or-public-ip for node <idx>.  The user recorded at provisioning
# wins, so a deployment stays reachable even if exp.config changes afterwards.
_aws_ssh_target() {
    local host user
    host=$(state_get "$1" ssh_host)
    [ -z "${host}" ] && return 1
    user=$(state_get "$1" ssh_user)
    echo "${user:-$(_aws_ssh_user)}@${host}"
}

# ssh to a node with everything stated explicitly, so that no entry in
# ~/.ssh/config is needed for the suite to work.
_aws_ssh() {
    local idx=$1; shift
    local target
    target=$(_aws_ssh_target "${idx}") || {
        error "aws: node ${idx} has no recorded address; run './deploy.sh sync'"
        return 1
    }
    ssh -i "$(_aws_ssh_key)" \
        -o IdentitiesOnly=yes \
        -o BatchMode=yes \
        -o StrictHostKeyChecking=accept-new \
        -o ConnectTimeout=10 \
        "${target}" "$@"
}

# The AZ locations.csv asks for.
_aws_planned_az() {
    local dc az
    dc=$(_aws_dc_of "$1")
    az=$(awk -F',' -v n=$((dc + 1)) 'NR == n { gsub(/\r|[ \t]/, "", $1); print $1 }' "$(_aws_az_file)")
    if [ -z "${az}" ]; then
        error "aws: node $1 needs DC ${dc}, but $(_aws_az_file) lists fewer AZs"
        return 1
    fi
    echo "${az}"
}

# The AZ a node actually landed in, which may differ from the planned one when
# capacity forced a fallback.  Recorded at provisioning time.
_aws_az_of() {
    local az
    az=$(state_get "$1" zone)
    if [ -n "${az}" ]; then
        echo "${az}"
        return 0
    fi
    _aws_planned_az "$1"
}

# Sibling AZs of an AZ's region, the planned one first.
_aws_az_candidates() {
    local planned=$1 region
    region=$(_aws_region_of "${planned}")
    echo "${planned}"
    aws ec2 describe-availability-zones --region "${region}" \
        --filters Name=state,Values=available \
        --query 'AvailabilityZones[].ZoneName' --output text 2>/dev/null \
        | tr '\t' '\n' | grep -vx "${planned}"
}

# Resolved at most once per region per process; every node of a region shares
# the same VPC, subnet-per-AZ, security group and AMI.
declare -gA _AWS_AMI_CACHE=()
declare -gA _AWS_ROOT_DEVICE_CACHE=()

_aws_vpc_id() {
    local region=$1
    aws ec2 describe-vpcs --region "${region}" --filters Name=isDefault,Values=true \
        --query 'Vpcs[0].VpcId' --output text 2>/dev/null
}

_aws_default_subnet_id() {
    local region=$1 az=$2
    aws ec2 describe-subnets --region "${region}" \
        --filters "Name=availability-zone,Values=${az}" "Name=default-for-az,Values=true" \
        --query 'Subnets[0].SubnetId' --output text 2>/dev/null
}

# Lookup only -- never creates.  Used by teardown and infra_open_ports, which
# must not conjure a security group into existence out of a stale state file.
_aws_find_security_group_id() {
    local region=$1 vpc
    vpc=$(_aws_vpc_id "${region}")
    [ -z "${vpc}" ] || [ "${vpc}" = "None" ] && return 0
    aws ec2 describe-security-groups --region "${region}" \
        --filters "Name=group-name,Values=${AWS_SECURITY_GROUP}" "Name=vpc-id,Values=${vpc}" \
        --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null
}

# One security group per region, created once and reused after.  Opens SSH to
# the world at creation time, before infra_open_ports (called later, from
# infra_bootstrap) ever runs -- the default VPC's own security group allows no
# inbound traffic from outside itself, so this is the first chance the
# provisioning node has to reach anything at all.  See docs/aws-deployment.md
# §2.2 for why SSH-open-by-design is the accepted tradeoff here.
_aws_security_group_id() {
    local region=$1 vpc sg out

    vpc=$(_aws_vpc_id "${region}")
    if [ -z "${vpc}" ] || [ "${vpc}" = "None" ]; then
        error "aws: no default VPC in ${region}; see docs/aws-deployment.md §7"
        return 1
    fi

    sg=$(_aws_find_security_group_id "${region}")
    if [ -n "${sg}" ] && [ "${sg}" != "None" ]; then
        echo "${sg}"
        return 0
    fi

    out=$(aws ec2 create-security-group --region "${region}" --group-name "${AWS_SECURITY_GROUP}" \
        --description "cassandra-evaluation inter-node traffic" --vpc-id "${vpc}" \
        --query 'GroupId' --output text 2>&1)
    if [ $? -ne 0 ]; then
        # Lost a race with a sibling node's instance creation in the same
        # region -- another _aws_create_instance created it a moment ago.
        sg=$(_aws_find_security_group_id "${region}")
        if [ -n "${sg}" ] && [ "${sg}" != "None" ]; then
            echo "${sg}"
            return 0
        fi
        error "aws: failed to create security group in ${region}"
        printf '%s\n' "${out}" >&2
        return 1
    fi
    sg="${out}"

    aws ec2 authorize-security-group-ingress --region "${region}" --group-id "${sg}" \
        --protocol tcp --port 22 --cidr 0.0.0.0/0 >/dev/null 2>&1
    aws ec2 authorize-security-group-ingress --region "${region}" --group-id "${sg}" \
        --protocol all --source-group "${sg}" >/dev/null 2>&1
    echo "${sg}"
}

# The same Ubuntu release has a different AMI id in every region; Canonical
# publishes the current one as a public SSM parameter.
_aws_ami_for_region() {
    local region=$1 ami
    [ -n "${_AWS_AMI_CACHE[${region}]:-}" ] && { echo "${_AWS_AMI_CACHE[${region}]}"; return 0; }

    ami=$(aws ssm get-parameters --region "${region}" \
        --names "${AWS_AMI_SSM_PARAM}" \
        --query 'Parameters[0].Value' --output text 2>/dev/null)
    if [ -z "${ami}" ] || [ "${ami}" = "None" ]; then
        error "aws: could not resolve the Ubuntu AMI for ${region}"
        return 1
    fi
    _AWS_AMI_CACHE[${region}]="${ami}"
    echo "${ami}"
}

# The AMI's root device name (typically /dev/sda1), needed to override its
# default 8GB volume -- too small for the Docker images this suite pulls.
_aws_root_device_for_ami() {
    local region=$1 ami=$2 dev
    [ -n "${_AWS_ROOT_DEVICE_CACHE[${ami}]:-}" ] && { echo "${_AWS_ROOT_DEVICE_CACHE[${ami}]}"; return 0; }

    dev=$(aws ec2 describe-images --region "${region}" --image-ids "${ami}" \
        --query 'Images[0].RootDeviceName' --output text 2>/dev/null)
    if [ -z "${dev}" ] || [ "${dev}" = "None" ]; then
        dev="/dev/sda1"
    fi
    _AWS_ROOT_DEVICE_CACHE[${ami}]="${dev}"
    echo "${dev}"
}

# install-docker.sh with the operator's public key spliced in, base64-encoded
# for run-instances --user-data.  There is no metadata-driven key injection in
# EC2, so this is the only hook that runs before an operator can reach the
# instance at all.
_aws_render_user_data() {
    local pub script
    pub="$(_aws_ssh_key).pub"
    if [ ! -f "${pub}" ]; then
        error "aws: no SSH public key found at ${pub}; generate one with 'ssh-keygen'"
        return 1
    fi
    script=$(cat "${INFRA_DIR}/install-docker.sh")
    script="${script//__SSH_PUBLIC_KEY__/$(cat "${pub}")}"
    printf '%s' "${script}" | base64 -w0
}

# Whether to request spot capacity instead of on-demand.  A benchmark run is
# short and disposable, so the risk -- AWS can reclaim the instance mid-run
# with ~2 minutes' notice -- is usually worth 60-90% off; on-demand stays the
# default so this has to be opted into per exp.config.
_aws_spot_enabled() {
    [ "$(config "aws.spot")" = "1" ]
}

# Create one instance, falling back to another AZ of the same region when the
# requested shape is temporarily unavailable.  Sibling AZs share the region,
# so `loc` and the coordinates derived from it are unaffected.
_aws_create_instance() {
    local idx=$1 shape=$2 planned name user_data az region subnet sg ami root_device out existing
    local -a market_opts=()

    planned=$(_aws_planned_az "${idx}") || return 1
    name=$(_aws_instance "${idx}")
    user_data=$(_aws_render_user_data) || return 1
    _aws_spot_enabled && market_opts=(--instance-market-options 'MarketType=spot')

    for az in $(_aws_az_candidates "${planned}"); do
        region=$(_aws_region_of "${az}")

        existing=$(aws ec2 describe-instances --region "${region}" \
            --filters "Name=tag:Name,Values=${name}" "Name=instance-state-name,Values=pending,running" \
            --query 'Reservations[0].Instances[0].InstanceId' --output text 2>/dev/null)
        if [ -n "${existing}" ] && [ "${existing}" != "None" ]; then
            log "aws: ${name} already exists in ${az}"
            state_set "${idx}" zone "${az}"
            return 0
        fi

        subnet=$(_aws_default_subnet_id "${region}" "${az}")
        if [ -z "${subnet}" ] || [ "${subnet}" = "None" ]; then
            error "aws: no default subnet for ${az}"
            return 1
        fi
        sg=$(_aws_security_group_id "${region}") || return 1
        ami=$(_aws_ami_for_region "${region}") || return 1
        root_device=$(_aws_root_device_for_ami "${region}" "${ami}")

        [ "${az}" = "${planned}" ] || log "aws: retrying ${name} in ${az}"
        out=$(aws ec2 run-instances --region "${region}" \
            --image-id "${ami}" --instance-type "${shape}" \
            --subnet-id "${subnet}" --security-group-ids "${sg}" \
            --associate-public-ip-address \
            --block-device-mappings "[{\"DeviceName\":\"${root_device}\",\"Ebs\":{\"VolumeSize\":${AWS_BOOT_VOLUME_SIZE},\"VolumeType\":\"${AWS_BOOT_VOLUME_TYPE}\"}}]" \
            --user-data "${user_data}" \
            "${market_opts[@]}" \
            --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${name}}]" 2>&1)
        if [ $? -eq 0 ]; then
            log "aws: created ${name} in ${az}"
            state_set "${idx}" zone "${az}"
            return 0
        fi

        # Only capacity shortages are worth retrying elsewhere; anything else
        # (quota, permissions, a bad instance type) would fail identically
        # next door.
        if ! printf '%s' "${out}" | grep -q "InsufficientInstanceCapacity"; then
            error "aws: failed to create ${name} in ${az}"
            printf '%s\n' "${out}" >&2
            return 1
        fi
        log "aws: ${shape} unavailable in ${az}"
    done

    error "aws: no AZ of $(_aws_region_of "${planned}") can provide a ${shape} right now."
    if _aws_spot_enabled; then
        error "     aws.spot=1 -- this is a spot capacity shortage, not a hard limit; the"
        error "     same request often succeeds a few minutes later, or set aws.spot=0 in"
        error "     exp.config to fall back to on-demand."
    fi
    error "     Try a different shape via 'aws.machine' in exp.config, another"
    error "     region in ${INFRA_DIR}/locations.csv, or the same request later."
    return 1
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

# Pure by design: this runs on every Docker operation, so it must not shell
# out.  Contexts are created by infra_provision and re-synced by
# _aws_sync_contexts.
infra_context() {
    local idx=${1:-0}
    [ "${idx}" -eq 0 ] 2>/dev/null && { echo ""; return 0; }
    echo "${AWS_CONTEXT_PREFIX}${idx}"
}

infra_net_device() {
    local dev
    dev=$(state_get "${1:-1}" net_device)
    echo "${dev:-ens5}"
}

infra_resource_limits() {
    # One node per instance: the instance shape is the limit.
    echo ""
}

infra_machine_shape() {
    # `machine` sizes the containers the simulation packs onto one host, which
    # is a different question from which instance type to rent.  Keep them
    # separate so that switching provider cannot change simulated results.
    local m
    m=$(config "aws.machine")
    echo "${m:-$(config machine)}"
}

infra_stage_file() {
    if [ $# -lt 3 ]; then
        error "usage: infra_stage_file <node_idx> <local_path> <remote_path>"
        return 2
    fi
    local idx=$1 local_path=$2 remote=$3 target
    target=$(_aws_ssh_target "${idx}") || return 1
    if ! scp -q -i "$(_aws_ssh_key)" -o IdentitiesOnly=yes -o BatchMode=yes \
             -o StrictHostKeyChecking=accept-new \
             "${local_path}" "${target}:${remote}"; then
        error "aws: failed to stage ${local_path} onto node ${idx}"
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
    local region regions p sg

    regions=$(state_nodes | while read -r n; do _aws_region_of "$(state_get "${n}" zone)"; done | sort -u)
    for region in ${regions}; do
        sg=$(_aws_find_security_group_id "${region}")
        if [ -z "${sg}" ] || [ "${sg}" = "None" ]; then
            error "aws: no security group in ${region}; run './deploy.sh provision' first"
            return 1
        fi
        for p in "$@"; do
            aws ec2 authorize-security-group-ingress --region "${region}" --group-id "${sg}" \
                --protocol tcp --port "${p}" --source-group "${sg}" >/dev/null 2>&1
        done
    done
}

infra_provision() {
    local num_nodes=${1:-0}
    if [ "${num_nodes}" -le 0 ] 2>/dev/null; then
        error "usage: infra_provision <num_nodes>"
        return 2
    fi

    _aws_preflight "${num_nodes}" || return 1

    local shape idx
    shape=$(infra_machine_shape)

    log "aws: provisioning ${num_nodes} x ${shape}"

    # Creation is one independent API call per node, each taking tens of
    # seconds; run them together rather than end to end.
    local -a pids=() nodes=()
    for idx in $(seq 1 "${num_nodes}"); do
        _aws_create_instance "${idx}" "${shape}" &
        pids+=($!)
        nodes+=("${idx}")
    done
    _aws_wait_all "creation" pids nodes || return 1

    _aws_record_addresses "${num_nodes}" || return 1
    _aws_wait_ready "${num_nodes}" || return 1
    _aws_sync_contexts "${num_nodes}" || return 1

    log "aws: ${num_nodes} node(s) ready"
}

infra_sync() {
    local num_nodes
    num_nodes=$(infra_all_indices | wc -l)
    if [ "${num_nodes}" -eq 0 ]; then
        error "aws: no deployment recorded; run './deploy.sh provision <n>' first"
        return 1
    fi
    # Addresses can change across a stop/start; re-read them before the
    # contexts that embed them are rebuilt.
    _aws_record_addresses "${num_nodes}" || return 1
    _aws_sync_contexts "${num_nodes}" --force
}

# Plain ssh needs no --command-style split between the two cases: with no
# arguments, "ssh ... target" already opens an interactive shell.
infra_ssh() {
    local idx=${1:?usage: infra_ssh <node_idx> [command...]}
    shift
    local rc
    _aws_ssh "${idx}" "$@"; rc=$?
    [ ${rc} -eq 0 ] || _aws_diagnose_unreachable "${idx}"
    return ${rc}
}

# Drop any traffic shaping left on the machines.
#
# Under host networking a `tc` rule added from inside a container is applied
# to the instance's own interface and outlives that container, so a
# fault-tolerance run interrupted between injecting its delay and removing it
# leaves the node shaped -- and the next experiment would inherit it silently.
infra_reset_network() {
    local idx dev
    local -a pids=() nodes=()

    for idx in $(infra_all_indices); do
        dev=$(infra_net_device "${idx}")
        # A node with no custom qdisc makes this fail; that is the normal case.
        _aws_ssh "${idx}" "sudo tc qdisc del dev ${dev} root" >/dev/null 2>&1 &
        pids+=($!)
        nodes+=("${idx}")
    done

    local i
    for i in "${!pids[@]}"; do
        if wait "${pids[$i]}"; then
            log "aws: cleared a stale tc qdisc on node ${nodes[$i]}"
        fi
    done
}

infra_teardown() {
    local region regions

    log "aws: tearing down"

    regions=$(_aws_configured_regions)

    local -a pids=() names=()
    for region in ${regions}; do
        (
            local ids
            local -a id_list
            ids=$(aws ec2 describe-instances --region "${region}" \
                --filters "Name=tag:Name,Values=${AWS_INSTANCE_PREFIX}*" \
                          "Name=instance-state-name,Values=pending,running,stopping,stopped" \
                --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)
            if [ -n "${ids}" ] && [ "${ids}" != "None" ]; then
                read -ra id_list <<< "${ids}"
                aws ec2 terminate-instances --region "${region}" --instance-ids "${id_list[@]}" >/dev/null
                aws ec2 wait instance-terminated --region "${region}" --instance-ids "${id_list[@]}"
            fi
            _aws_delete_security_group "${region}"
        ) &
        pids+=($!)
        names+=("${region}")
    done
    _aws_wait_all "teardown" pids names || true

    docker context ls --format '{{.Name}}' 2>/dev/null | grep "^${AWS_CONTEXT_PREFIX}" \
        | while read -r ctx; do docker context rm -f "${ctx}" >/dev/null 2>&1 || true; done

    state_clear

    log "aws: teardown complete"
}

###############################################################################
# Internals
###############################################################################

_aws_preflight() {
    local num_nodes=$1 idx region regions vpc

    command -v aws >/dev/null 2>&1 || {
        error "aws: aws CLI not found in PATH"
        return 1
    }
    command -v scp >/dev/null 2>&1 || {
        error "aws: scp not found in PATH"
        return 1
    }

    if ! aws sts get-caller-identity >/dev/null 2>&1; then
        error "aws: no working credentials; run 'aws configure'"
        return 1
    fi

    local shape
    shape=$(infra_machine_shape)
    if [ -z "${shape}" ]; then
        error "aws: 'machine' is unset in exp.config"
        return 1
    fi

    # Fail before spending money if the deployment map is short or unjoinable.
    infra_locations_file >/dev/null || return 1
    for idx in $(seq 1 "${num_nodes}"); do
        _aws_planned_az "${idx}" >/dev/null || return 1
    done

    # Catch a missing default VPC (an operator may have deleted it) before it
    # becomes a half-built cluster.
    regions=$(for idx in $(seq 1 "${num_nodes}"); do
        _aws_region_of "$(_aws_planned_az "${idx}")"
    done | sort -u)
    for region in ${regions}; do
        vpc=$(_aws_vpc_id "${region}")
        if [ -z "${vpc}" ] || [ "${vpc}" = "None" ]; then
            error "aws: no default VPC in ${region}; see docs/aws-deployment.md §7"
            return 1
        fi
    done
}

# Public IP assignment lags instance creation by a few seconds, so both
# addresses are polled together rather than assumed present on the first read.
_aws_record_addresses() {
    local num_nodes=$1 idx az region name ips priv pub deadline

    for idx in $(seq 1 "${num_nodes}"); do
        az=$(_aws_az_of "${idx}") || return 1
        region=$(_aws_region_of "${az}")
        name=$(_aws_instance "${idx}")

        deadline=$(( $(date +%s) + 60 ))
        while true; do
            ips=$(aws ec2 describe-instances --region "${region}" \
                --filters "Name=tag:Name,Values=${name}" "Name=instance-state-name,Values=pending,running" \
                --query 'Reservations[0].Instances[0].[PrivateIpAddress,PublicIpAddress]' \
                --output text 2>/dev/null)
            priv=$(awk '{print $1}' <<< "${ips}")
            pub=$(awk '{print $2}' <<< "${ips}")
            if [ -n "${priv}" ] && [ "${priv}" != "None" ] && [ -n "${pub}" ] && [ "${pub}" != "None" ]; then
                break
            fi
            if [ "$(date +%s)" -ge "${deadline}" ]; then
                error "aws: ${name} never got a private and public IP"
                return 1
            fi
            sleep 3
        done

        state_set "${idx}" host "${priv}"
        state_set "${idx}" ssh_host "${pub}"
        state_set "${idx}" ssh_user "$(_aws_ssh_user)"
    done
}

_aws_wait_all() {
    local what=$1
    local -n _pids=$2
    local -n _nodes=$3
    local i rc=0
    for i in "${!_pids[@]}"; do
        if ! wait "${_pids[$i]}"; then
            error "aws: ${what} failed for node ${_nodes[$i]}"
            rc=1
        fi
    done
    return ${rc}
}

# Explains an unreachable node when the cause is visible on the instance
# itself, rather than leaving the operator with only an SSH timeout.  A spot
# reclamation looks identical to a hung boot or a broken security group until
# this checks StateTransitionReason -- worth calling out by name since
# aws.spot=1 makes it an expected failure mode, not a bug.  Uses only
# ec2:DescribeInstances, already required for everything else this provider
# does, so no extra IAM permission is needed.
_aws_diagnose_unreachable() {
    local idx=$1 az region name info state reason
    az=$(_aws_az_of "${idx}") || return 1
    region=$(_aws_region_of "${az}")
    name=$(_aws_instance "${idx}")
    # No instance-state-name filter -- a dead instance is exactly what this is
    # looking for -- so sort_by(...LaunchTime)[-1] picks the most recent match
    # rather than an arbitrary one, in case a prior deployment's terminated
    # instance still shares this node's Name tag (AWS keeps it visible for a
    # while after termination).
    info=$(aws ec2 describe-instances --region "${region}" \
        --filters "Name=tag:Name,Values=${name}" \
        --query 'sort_by(Reservations[].Instances[], &LaunchTime)[-1].[State.Name,StateTransitionReason]' \
        --output text 2>/dev/null)
    if [ -z "${info}" ] || [ "${info}" = "None" ]; then
        return 1
    fi
    state=$(printf '%s' "${info}" | cut -f1)
    reason=$(printf '%s' "${info}" | cut -f2-)

    case "${reason}" in
        *[Ss]pot*)
            error "aws: ${name} is unreachable because AWS reclaimed it (spot interruption)."
            error "     Instance state: ${state} -- ${reason}"
            error "     Expected with aws.spot=1 when demand/price rose; re-provision to"
            error "     replace it, or set aws.spot=0 in exp.config for guaranteed capacity."
            ;;
        *)
            [ "${state}" = "running" ] && return 1
            error "aws: ${name} is unreachable; instance state is now '${state}' (${reason})."
            ;;
    esac
}

# Everything a single node needs before it can run containers.  Written as one
# function so that the nodes can be brought up concurrently: each machine
# installs Docker on its own schedule and there is nothing to serialise.
_aws_prepare_node() {
    local idx=$1 instance deadline dev
    local timeout=${AWS_READY_TIMEOUT:-600}

    instance=$(_aws_instance "${idx}")
    deadline=$(( $(date +%s) + timeout ))
    log "aws: waiting for ${instance} to finish installing Docker"

    while true; do
        if _aws_ssh "${idx}" "test -f /var/lib/bench-node-ready" >/dev/null 2>&1; then
            break
        fi
        if [ "$(date +%s)" -ge "${deadline}" ]; then
            error "aws: ${instance} did not become ready within ${timeout}s"
            _aws_diagnose_unreachable "${idx}"
            return 1
        fi
        sleep 5
    done

    # Group membership only takes effect on the next login, which is why this
    # runs before the first Docker call rather than in the user-data script
    # (where the user may not exist yet).
    _aws_ssh "${idx}" \
        "getent group docker >/dev/null && id -nG | grep -qw docker || sudo usermod -aG docker \$(id -un)" \
        >/dev/null 2>&1 || true

    if ! _aws_ssh "${idx}" "docker info" >/dev/null 2>&1; then
        error "aws: docker is not usable as $(_aws_ssh_user) on ${instance}"
        _aws_diagnose_unreachable "${idx}"
        return 1
    fi

    # Read the real interface name rather than assuming ens5.
    dev=$(_aws_ssh "${idx}" "ip -o route show default | awk '{print \$5; exit}'" 2>/dev/null | tr -d '\r')
    [ -n "${dev}" ] && state_set "${idx}" net_device "${dev}"
    log "aws: ${instance} ready"
}

_aws_wait_ready() {
    local num_nodes=$1 idx
    local -a pids=() nodes=()
    for idx in $(seq 1 "${num_nodes}"); do
        _aws_prepare_node "${idx}" &
        pids+=($!)
        nodes+=("${idx}")
    done
    _aws_wait_all "setup" pids nodes
}

# Contexts embed the machine's address, so --force recreates them after the
# addresses have been re-read.
_aws_sync_contexts() {
    local num_nodes=${1:-$(infra_all_indices | wc -l)}
    local force=${2:-}
    local idx name existing target

    existing=$(docker context ls --format '{{.Name}}' 2>/dev/null)

    for idx in $(seq 1 "${num_nodes}"); do
        name="${AWS_CONTEXT_PREFIX}${idx}"
        if printf '%s\n' "${existing}" | grep -qx "${name}"; then
            [ "${force}" = "--force" ] || continue
            docker context rm -f "${name}" >/dev/null 2>&1 || true
        fi
        target=$(_aws_ssh_target "${idx}") || {
            error "aws: node ${idx} has no recorded address"
            return 1
        }
        docker context create "${name}" --docker "host=ssh://${target}" >/dev/null || {
            error "aws: failed to create Docker context ${name}"
            return 1
        }
    done
}

# A security group can't be deleted while a member instance's ENI is still
# releasing, which lags a few seconds behind terminate-instances returning --
# so deletion is retried briefly rather than treated as a hard failure.
_aws_delete_security_group() {
    local region=$1 sg attempt
    sg=$(_aws_find_security_group_id "${region}")
    [ -z "${sg}" ] || [ "${sg}" = "None" ] && return 0

    for attempt in 1 2 3 4 5; do
        if aws ec2 delete-security-group --region "${region}" --group-id "${sg}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 3
    done
    error "aws: could not delete security group ${sg} in ${region} (an ENI may still be releasing)"
    return 1
}

# The union of state-derived regions and locations.csv-derived regions.
# Every AWS call is region-scoped, so unlike GCE's project-global instance
# list, there is no way to ask "what does this account have running
# anywhere" in one call -- teardown has to be told which regions to check.
# Relying on the state file alone means a lost or stale state file (deleted
# to "start fresh", teardown run from another machine before a sync, ...)
# would make teardown check zero regions and silently leave real instances
# running and billing.  locations.csv is the fallback: even with no state at
# all, it names the regions the deployment would have used.
_aws_configured_regions() {
    {
        state_nodes | while read -r n; do _aws_region_of "$(state_get "${n}" zone)"; done
        if [ -f "$(_aws_az_file)" ]; then
            awk -F',' 'NR > 1 { gsub(/\r|[ \t]/, ""); if ($1 != "") print $1 }' "$(_aws_az_file)" \
                | while read -r az; do _aws_region_of "${az}"; done
        fi
    } | sort -u
}
