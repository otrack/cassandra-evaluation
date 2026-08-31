# AWS EC2 infra provider

The infra abstraction that lets this suite run on real machines is already
built and already has one live implementation: `infra/README.md` defines the
contract, `utils.sh` sources `infra/${infra}/provider.sh` and routes every
Docker/SSH operation through it, and `infra/gcp/provider.sh` satisfies that
contract against Google Compute Engine. This note designs the second
implementation, `infra/aws/provider.sh`, against Amazon EC2.

**Verdict: a direct implementation of the existing contract, no new
orchestration.** The `aws` CLI covers every operation the contract needs —
instance lifecycle, security-group rules, SSH-key distribution via
`run-instances --user-data` — so this is the same fifteen functions
(`infra/README.md:149-169`) satisfied with `aws ec2`/`aws ssm` calls instead
of a different tool. The work is entirely inside `infra/aws/`; nothing in
`utils.sh` or any experiment script changes.

## 1. Findings

**Every AWS CLI call is region-scoped.** Unlike a project-global `gcloud`
session, `aws ec2`/`aws ssm` calls take an explicit `--region`. A node's
region is derived from its availability zone (AZ) by stripping the trailing
letter (`us-east-1a` → `us-east-1`) — AWS AZ names have no separating dash
before that letter. Because a deployment spans several regions at once (one
per DC), every per-region resource (VPC lookup, security group, AMI id) is
resolved once per distinct region actually in use, not once globally.

**The default VPC allows no inbound traffic from outside itself.** A fresh
AWS account's default security group only permits traffic among its own
members — there is no equivalent of an "allow SSH from anywhere" rule shipped
by default. `infra_provision`'s own readiness check has to SSH into a new
instance before `infra_open_ports` (called later, from `infra_bootstrap`)
ever runs, so the security group has to open `tcp:22` at *creation* time, not
as an afterthought.

**AMI ids are per region, not portable.** The same Ubuntu release has a
different AMI id in every region. Canonical publishes the current one per
region as a public SSM parameter, so the AMI is resolved with one
`aws ssm get-parameters` call per region rather than hardcoded.

**There is no metadata-driven key-injection mechanism.** EC2 has no guest
agent that turns instance metadata into `authorized_keys` the way some other
clouds' agents do. The only hook available before an operator can reach the
box at all is `run-instances --user-data`, so the operator's public key is
appended to `~/.ssh/authorized_keys` from inside the same user-data script
that installs Docker.

**The Ubuntu AMI has one fixed login account.** There's no per-operator
identity tied to the AWS account the way there can be with an IAM-integrated
OS login; the AMI's account is simply `ubuntu`. The SSH user therefore
defaults to `ubuntu`, overridable with `$BENCH_SSH_USER` like any provider.

**New accounts carry low, per-region EC2 quotas.** The default "Running
On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances" quota is commonly
5–32 vCPUs *per region*. A deployment spanning several regions provisions
against each region's quota independently, so a large `aws.machine` × node
count can fail one region's `run-instances` with `VcpuLimitExceeded` — a hard
failure that has nothing to do with capacity and shouldn't be retried.

**Capacity shortfalls are a distinct, retryable error.** `run-instances`
returns `InsufficientInstanceCapacity` when a specific AZ is temporarily out
of a given instance type. That's worth one retry against a sibling AZ of the
same region — anything else (quota, a bad instance type, auth) is not.

**A security group can't be deleted while a member is still shutting down.**
`terminate-instances` returns immediately; the ENI a terminating instance
holds in the security group takes a few seconds longer to release. Teardown
has to wait for termination before deleting the group.

## 2. Design

### 2.1 Scope

**Same-cloud addressing.** `infra_host_ip` returns each node's AWS private
VPC IP, the address peers use to reach each other. Mixing nodes from
different clouds in one deployment is out of scope for this design —
`infra_host_ip` never has to decide between a private and a public address.

**Default VPC, no VPC lifecycle.** Provisioning launches into each region's
existing default VPC and default subnets, and manages only a security group.
This is deliberate: a VPC/subnet/route-table/internet-gateway lifecycle is a
lot of surface area for a benchmark harness to own correctly, and every AWS
account already has a default VPC in every region unless someone has
explicitly deleted it (§7, step 7 covers that edge case for the operator).

### 2.2 Security posture

The security group this provider manages opens `tcp:22` from `0.0.0.0/0` at
creation (Finding above), plus a self-referencing rule letting members talk
to each other on any port. `infra_open_ports` later adds the benchmark
protocol ports (Cassandra 9042 + 7000, CockroachDB 26257 + 8080, SwiftPaxos
7087 and its per-server range, Tiga 10000 — the exact list `infra_bootstrap`
passes, `utils.sh:206`), scoped the same way. SSH open to the world is a
conscious tradeoff, not an oversight: restricting it to the operator's
current IP would break the moment that IP changes (a laptop moving networks,
a team member on a different connection), and this is a benchmark harness
running short-lived, disposable machines, not a production fleet.

### 2.3 Spot instances (optional)

`aws.spot` in `exp.config` (default `0`) opts a deployment into spot
capacity: `_aws_create_instance` adds `--instance-market-options
MarketType=spot` to `run-instances` when it's set to `1`, nothing otherwise.
It is a plain toggle, not a mode this provider otherwise treats specially —
the existing per-AZ retry loop already handles `InsufficientInstanceCapacity`
the same way for both, since a synchronous one-time spot request through
`run-instances` fails with that same error code when its pool has no
capacity at the current price.

What spot changes is that AWS can reclaim a *running* instance with about two
minutes' notice, which on-demand never does. This provider does not try to
auto-recover from that — a run just fails wherever that node was in use, the
same as any other node loss — but it does make the cause visible rather than
leaving an operator staring at a bare SSH timeout. `_aws_diagnose_unreachable`
(§3, Step 8) reads the unreachable instance's `StateTransitionReason` via
`describe-instances` and names a spot interruption explicitly when that's
what happened, distinguishing it from a hung boot, a stale security group, or
a manually stopped instance. It is wired into the two places an operator
actually meets an unreachable node: `infra_ssh`, and the readiness poll
inside `infra_provision`.

### 2.4 Locations and state

`infra/aws/locations.csv` lists the AZs this deployment uses, one per DC row,
in order — the same shape the contract already expects
(`infra/README.md:43-59`). `infra/aws/regions.csv` is a `region,loc,lat,lon`
lookup table of real AWS region coordinates. `infra_locations_file` joins the
two (region = AZ minus its trailing letter) into a `lat,lon,loc` map,
materialized at `.deployment/aws.locations.csv` and rebuilt only when a
source file changes — the same caching shape the contract's locations-file
requirement implies.

Deployment state reuses the schema every provider already shares:
`STATE_COLUMNS="node,zone,host,ssh_host,ssh_user,net_device"`
(`utils.sh:66`), read and written exclusively through `state_get`/`state_set`
(`utils.sh:68-103`). `zone` holds the AZ (not just the region), so a node's
exact placement survives an `infra_sync`.

## 3. Implementation skeleton

### Step 1 — the locations join

```bash
# infra/aws/provider.sh
infra_locations_file() {
    local out="${DIR}/.deployment/${INFRA}.locations.csv"
    local zones="${INFRA_DIR}/locations.csv" regions="${INFRA_DIR}/regions.csv"
    mkdir -p "$(dirname "${out}")"
    if [ -f "${out}" ] && [ "${out}" -nt "${zones}" ] && [ "${out}" -nt "${regions}" ]; then
        echo "${out}"; return 0
    fi
    awk -F',' -v OFS=',' '
        NR == FNR { if (FNR > 1) { loc[$1]=$2; lat[$1]=$3; lon[$1]=$4 }; next }
        FNR == 1 { print "lat","lon","loc"; next }
        {
            az = $1; region = az; sub(/[a-z]$/, "", region)
            if (!(region in loc)) { print "aws: no region data for " az > "/dev/stderr"; bad=1; next }
            if (loc[region] in seen) { print "aws: " seen[loc[region]] " and " az " both map to \"" loc[region] "\"" > "/dev/stderr"; bad=1; next }
            seen[loc[region]] = az
            print lat[region], lon[region], loc[region]
        }
        END { if (bad) exit 3 }
    ' "${regions}" "${zones}" > "${out}.tmp" || { rm -f "${out}.tmp"; return 1; }
    mv "${out}.tmp" "${out}"
    echo "${out}"
}
```

### Step 2 — region, AZ and SSH plumbing

```bash
_aws_region_of()   { local az=$1; echo "${az%[a-z]}"; }
_aws_dc_of()        { echo $(( ($1 - 1) / $(config nodesperdc) + 1 )); }
_aws_instance()     { echo "bench-node$1"; }
_aws_ssh_user()     { echo "${BENCH_SSH_USER:-ubuntu}"; }
_aws_ssh_key()      { echo "${BENCH_SSH_KEY:-$(ls "${HOME}"/.ssh/id_{ed25519,rsa,ecdsa} 2>/dev/null | head -1)}"; }
_aws_ssh_target()   { echo "$(_aws_ssh_user)@$(state_get "$1" ssh_host)"; }
_aws_ssh() {
    local idx=$1; shift
    ssh -i "$(_aws_ssh_key)" -o IdentitiesOnly=yes -o BatchMode=yes \
        -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 \
        "$(_aws_ssh_target "${idx}")" "$@"
}
_aws_planned_az() {
    local dc; dc=$(_aws_dc_of "$1")
    awk -F',' -v n=$((dc + 1)) 'NR==n { gsub(/\r/,"",$1); print $1 }' "${INFRA_DIR}/locations.csv"
}
_aws_az_of() { state_get "$1" zone || _aws_planned_az "$1"; }
_aws_az_candidates() {
    local planned=$1 region; region=$(_aws_region_of "${planned}")
    echo "${planned}"
    aws ec2 describe-availability-zones --region "${region}" \
        --filters Name=state,Values=available --query 'AvailabilityZones[].ZoneName' \
        --output text | tr '\t' '\n' | grep -vx "${planned}"
}
```

### Step 3 — network and image resolution, per region

```bash
_aws_default_subnet_id() {
    local region=$1 az=$2
    aws ec2 describe-subnets --region "${region}" \
        --filters "Name=availability-zone,Values=${az}" "Name=default-for-az,Values=true" \
        --query 'Subnets[0].SubnetId' --output text
}

# Lookup only -- never creates.  Used by teardown and infra_open_ports, which
# must not conjure a security group into existence out of a stale state file.
_aws_find_security_group_id() {
    local region=$1 vpc
    vpc=$(aws ec2 describe-vpcs --region "${region}" --filters Name=isDefault,Values=true \
        --query 'Vpcs[0].VpcId' --output text)
    [ -z "${vpc}" ] || [ "${vpc}" = "None" ] && return 0
    aws ec2 describe-security-groups --region "${region}" \
        --filters "Name=group-name,Values=bench-internal" "Name=vpc-id,Values=${vpc}" \
        --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null
}

_aws_security_group_id() {   # one per region, created once, reused after
    local region=$1 vpc sg
    vpc=$(aws ec2 describe-vpcs --region "${region}" --filters Name=isDefault,Values=true \
        --query 'Vpcs[0].VpcId' --output text)
    sg=$(_aws_find_security_group_id "${region}")
    [ -n "${sg}" ] && [ "${sg}" != "None" ] && { echo "${sg}"; return 0; }

    sg=$(aws ec2 create-security-group --region "${region}" --group-name bench-internal \
        --description "cassandra-evaluation inter-node traffic" --vpc-id "${vpc}" \
        --query 'GroupId' --output text 2>&1) || {
        # Lost a race with a sibling node's instance creation in the same
        # region -- reuse the group the winner just created.
        sg=$(_aws_find_security_group_id "${region}")
        [ -n "${sg}" ] && [ "${sg}" != "None" ] && { echo "${sg}"; return 0; }
        return 1
    }
    aws ec2 authorize-security-group-ingress --region "${region}" --group-id "${sg}" \
        --protocol tcp --port 22 --cidr 0.0.0.0/0 >/dev/null
    aws ec2 authorize-security-group-ingress --region "${region}" --group-id "${sg}" \
        --protocol all --source-group "${sg}" >/dev/null
    echo "${sg}"
}

_aws_ami_for_region() {   # cached per region for the life of the process
    local region=$1
    aws ssm get-parameters --region "${region}" \
        --names /aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
        --query 'Parameters[0].Value' --output text
}

# The AMI's root device name (typically /dev/sda1), needed to override its
# default 8GB volume -- too small for the Docker images this suite pulls.
_aws_root_device_for_ami() {   # cached per AMI for the life of the process
    local region=$1 ami=$2
    aws ec2 describe-images --region "${region}" --image-ids "${ami}" \
        --query 'Images[0].RootDeviceName' --output text
}
```

### Step 4 — `infra_provision`

```bash
infra_provision() {
    local n=${1:?usage: infra_provision <n>}
    _aws_preflight "${n}" || return 1
    local shape; shape=$(infra_machine_shape)
    local idx
    for idx in $(seq 1 "${n}"); do
        ( _aws_create_instance "${idx}" "${shape}" ) &
    done
    wait
    _aws_record_addresses "${n}" || return 1
    _aws_wait_ready "${n}" || return 1
    _aws_sync_contexts "${n}"
}

# Whether to request spot capacity instead of on-demand -- see §2.3.
_aws_spot_enabled() {
    [ "$(config "aws.spot")" = "1" ]
}

_aws_create_instance() {
    local idx=$1 shape=$2 planned az region subnet sg ami root_device name existing user_data
    local -a market_opts=()
    planned=$(_aws_planned_az "${idx}") || return 1
    name=$(_aws_instance "${idx}")
    user_data=$(_aws_render_user_data)
    _aws_spot_enabled && market_opts=(--instance-market-options 'MarketType=spot')

    for az in $(_aws_az_candidates "${planned}"); do
        region=$(_aws_region_of "${az}")
        existing=$(aws ec2 describe-instances --region "${region}" \
            --filters "Name=tag:Name,Values=${name}" "Name=instance-state-name,Values=pending,running" \
            --query 'Reservations[0].Instances[0].InstanceId' --output text)
        if [ -n "${existing}" ] && [ "${existing}" != "None" ]; then
            state_set "${idx}" zone "${az}"; return 0
        fi

        subnet=$(_aws_default_subnet_id "${region}" "${az}")
        sg=$(_aws_security_group_id "${region}") || return 1
        ami=$(_aws_ami_for_region "${region}")
        root_device=$(_aws_root_device_for_ami "${region}" "${ami}")
        out=$(aws ec2 run-instances --region "${region}" \
            --image-id "${ami}" --instance-type "${shape}" \
            --subnet-id "${subnet}" --security-group-ids "${sg}" \
            --associate-public-ip-address \
            --block-device-mappings "[{\"DeviceName\":\"${root_device}\",\"Ebs\":{\"VolumeSize\":50,\"VolumeType\":\"gp3\"}}]" \
            --user-data "${user_data}" \
            "${market_opts[@]}" \
            --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${name}}]" 2>&1)
        if [ $? -eq 0 ]; then
            state_set "${idx}" zone "${az}"; return 0
        fi
        printf '%s\n' "${out}" | grep -q "InsufficientInstanceCapacity" || {
            error "aws: failed to create ${name} in ${az}"; printf '%s\n' "${out}" >&2; return 1
        }
        log "aws: ${shape} unavailable in ${az}, retrying sibling AZ"
    done
    error "aws: no AZ of $(_aws_region_of "${planned}") can provide a ${shape} right now"
    _aws_spot_enabled && error "     aws.spot=1 -- a spot capacity shortage, not a hard limit; retry later"
    return 1
}
```

`--associate-public-ip-address` is what gives the orchestrator an address to SSH
to at all (§2.4's `ssh_host`); the default subnet's own setting is not relied
on. The block-device override exists because the Ubuntu AMI's default root
volume (8GB) is too small for the Docker images this suite pulls. `market_opts`
is empty unless `aws.spot=1` (§2.3), so on-demand's `run-instances` call is
byte-for-byte what it always was — the array just expands to nothing.

`_aws_preflight` checks `aws`/`scp` on `PATH`, `aws sts get-caller-identity`,
`infra_machine_shape` non-empty, that `infra_locations_file` builds, and that
every distinct region the run will touch still has a default VPC — an
operator who deleted theirs (§7, step 7) hears about it before any billing
starts, not partway through. `_aws_record_addresses` polls `describe-instances`
per node (`--query 'Reservations[0].Instances[0].[PrivateIpAddress,PublicIpAddress]'`,
up to 60s) rather than reading once, since the public IP can lag instance
creation by a few seconds, and calls `state_set … host/ssh_host/ssh_user`.
`_aws_wait_ready` fans
`_aws_prepare_node` out across nodes: poll SSH for `/var/lib/bench-node-ready`
(written by `install-docker.sh`, Step 9), add the SSH user to the `docker`
group, and record the real primary interface
(`ip -o route show default | awk '{print $5; exit}'`) rather than assuming
one.

### Step 5 — the small contract functions

```bash
infra_is_real() { return 0; }

infra_host_ip() {
    local idx=${1:-0}
    [ "${idx}" -eq 0 ] 2>/dev/null && { echo ""; return 0; }
    state_get "${idx}" host
}

infra_context() {
    local idx=${1:-0}
    [ "${idx}" -eq 0 ] 2>/dev/null && { echo ""; return 0; }
    echo "bench-ctx-aws-${idx}"
}

infra_net_device() {
    local dev; dev=$(state_get "${1:-1}" net_device)
    echo "${dev:-ens5}"
}

infra_resource_limits() { echo ""; }   # the instance is the limit

infra_machine_shape() {
    local m; m=$(config "aws.machine")
    echo "${m:-$(config machine)}"
}
```

### Step 6 — `infra_open_ports`

```bash
infra_open_ports() {
    local region p sg
    for region in $(state_nodes | while read -r n; do _aws_region_of "$(state_get "${n}" zone)"; done | sort -u); do
        sg=$(_aws_find_security_group_id "${region}")
        [ -n "${sg}" ] && [ "${sg}" != "None" ] || { error "aws: no security group in ${region}"; return 1; }
        for p in "$@"; do
            aws ec2 authorize-security-group-ingress --region "${region}" --group-id "${sg}" \
                --protocol tcp --port "${p}" --source-group "${sg}" >/dev/null 2>&1
        done
    done
}
```

`infra_open_ports` uses the lookup-only helper, not the create-capable one: it
only ever runs right after a successful `infra_provision` (from
`infra_bootstrap`), so a missing group means something already went wrong, not
something to paper over by creating one.

### Step 7 — file staging and Docker args

```bash
infra_stage_file() {
    local idx=$1 local_path=$2 remote=$3
    scp -q -i "$(_aws_ssh_key)" -o IdentitiesOnly=yes -o BatchMode=yes \
        -o StrictHostKeyChecking=accept-new \
        "${local_path}" "$(_aws_ssh_target "${idx}"):${remote}" || {
        error "aws: failed to stage ${local_path} onto node ${idx}"; return 1
    }
    echo "${remote}"
}

infra_rewrite_docker_args() {
    local cname="$1"; shift
    local args=("$@") out=() i=0 arg val host_net=0
    while [ ${i} -lt ${#args[@]} ]; do
        arg="${args[$i]}"
        case "${arg}" in
            --network)
                val="${args[$((i+1))]}"
                if [[ "${val}" == container:* ]]; then out+=("--network" "${val}")
                else out+=("--network" "host"); host_net=1; fi
                i=$((i+2)); continue ;;
            --network=*)
                val="${arg#--network=}"
                if [[ "${val}" == container:* ]]; then out+=("${arg}")
                else out+=("--network" "host"); host_net=1; fi
                i=$((i+1)); continue ;;
            -p|--publish) i=$((i+2)); continue ;;
            -p*|--publish=*) i=$((i+1)); continue ;;
            *) out+=("${arg}"); i=$((i+1)) ;;
        esac
    done
    [ ${host_net} -eq 1 ] && out+=($(host_aliases))
    echo "${out[@]}"
}
```

`host_aliases` is shared, provider-agnostic code already in `utils.sh:182-198`
— it walks `LOCATIONS_FILE` and calls `infra_host_ip` per node. The four
`--network`/`-p` cases (bare flag, `=`-form, short flag, and the passthrough
default) mirror what GCP's own `infra_rewrite_docker_args` already handles —
genuinely provider-agnostic argument parsing, not something to reinvent
less completely just because this is a different provider.

### Step 8 — sync, ssh, network reset, teardown

```bash
infra_sync() {
    local n; n=$(infra_all_indices | wc -l)
    [ "${n}" -eq 0 ] && { error "aws: no deployment recorded"; return 1; }
    _aws_record_addresses "${n}" && _aws_sync_contexts "${n}" --force
}

infra_ssh() {
    local idx=${1:?usage: infra_ssh <node_idx> [command...]}; shift
    local rc
    _aws_ssh "${idx}" "$@"; rc=$?
    [ ${rc} -eq 0 ] || _aws_diagnose_unreachable "${idx}"
    return ${rc}
}
```

Plain `ssh` needs no command/no-command split the way `gcloud compute ssh`
does (GCP's version needs `--command` for one case and not the other):
`ssh ... target` with no trailing arguments already opens an interactive
shell, so `infra_ssh` is `_aws_ssh` with the leading index consumed, a
diagnosis (below) attached on failure, and the exact underlying exit code
preserved — GCP's `infra_ssh` propagates its command's exit status too, and
`./deploy.sh ssh <idx> <cmd>` relies on that to surface the remote command's
own result, not just "it failed."

```bash
# Explains an unreachable node when the cause is visible on the instance
# itself, rather than leaving the operator with only an SSH timeout -- a
# spot reclamation (§2.3) looks identical to a hung boot or a broken security
# group until this checks StateTransitionReason.  Only needs
# ec2:DescribeInstances, already required for everything else this provider
# does.
_aws_diagnose_unreachable() {
    local idx=$1 az region name info state reason
    az=$(_aws_az_of "${idx}") || return 1
    region=$(_aws_region_of "${az}")
    name=$(_aws_instance "${idx}")
    # No instance-state-name filter -- a dead instance is exactly what this is
    # looking for -- so sort_by(...LaunchTime)[-1] picks the most recent match
    # rather than an arbitrary one, in case a prior deployment's terminated
    # instance still shares this node's Name tag.
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
            error "aws: ${name} was reclaimed by AWS (spot interruption) -- ${state}: ${reason}"
            ;;
        *)
            [ "${state}" = "running" ] && return 1
            error "aws: ${name} is unreachable; instance state is now '${state}' (${reason})"
            ;;
    esac
}
```

This is also wired into `_aws_prepare_node` (Step 4) at both of its failure
exits — the readiness-poll timeout and the "docker not usable" check —
since a spot reclamation during provisioning shows up there first, before an
operator ever runs `infra_ssh` by hand.

```bash
infra_reset_network() {
    local idx dev
    for idx in $(infra_all_indices); do
        dev=$(infra_net_device "${idx}")
        _aws_ssh "${idx}" "sudo tc qdisc del dev ${dev} root" >/dev/null 2>&1 &
    done
    wait
}

# The union of state-derived regions and locations.csv-derived regions.
# Every AWS call is region-scoped, so unlike GCE's project-global instance
# list, there is no "what does this account have running anywhere" query --
# teardown has to be told which regions to check.  Relying on the state file
# alone means a lost or stale state file (deleted to "start fresh", teardown
# run from another machine before a sync, ...) would make teardown check zero
# regions and silently leave real instances running and billing.
# locations.csv is the fallback: even with no state at all, it names the
# regions the deployment would have used.
_aws_configured_regions() {
    {
        state_nodes | while read -r n; do _aws_region_of "$(state_get "${n}" zone)"; done
        awk -F',' 'NR > 1 { gsub(/\r|[ \t]/, ""); if ($1 != "") print $1 }' "${INFRA_DIR}/locations.csv" \
            | while read -r az; do _aws_region_of "${az}"; done
    } | sort -u
}

# A security group can't be deleted while a member instance's ENI is still
# releasing, which lags a few seconds behind terminate-instances returning
# (Finding above) -- so deletion is retried briefly rather than treated as a
# hard failure.
_aws_delete_security_group() {
    local region=$1 sg attempt
    sg=$(_aws_find_security_group_id "${region}")
    [ -z "${sg}" ] || [ "${sg}" = "None" ] && return 0
    for attempt in 1 2 3 4 5; do
        aws ec2 delete-security-group --region "${region}" --group-id "${sg}" >/dev/null 2>&1 && return 0
        sleep 3
    done
    error "aws: could not delete security group ${sg} in ${region} (an ENI may still be releasing)"
    return 1
}

infra_teardown() {
    local region
    for region in $(_aws_configured_regions); do
        (
            local ids; local -a id_list
            ids=$(aws ec2 describe-instances --region "${region}" \
                --filters "Name=tag:Name,Values=bench-node*" \
                          "Name=instance-state-name,Values=pending,running,stopping,stopped" \
                --query 'Reservations[].Instances[].InstanceId' --output text)
            if [ -n "${ids}" ] && [ "${ids}" != "None" ]; then
                read -ra id_list <<< "${ids}"
                aws ec2 terminate-instances --region "${region}" --instance-ids "${id_list[@]}" >/dev/null
                aws ec2 wait instance-terminated --region "${region}" --instance-ids "${id_list[@]}"
            fi
            _aws_delete_security_group "${region}"
        ) &
    done
    wait
    docker context ls --format '{{.Name}}' 2>/dev/null | grep '^bench-ctx-aws-' \
        | xargs -r -n1 docker context rm -f >/dev/null 2>&1
    state_clear
}
```

### Step 9 — `install-docker.sh` (EC2 user-data)

```bash
#!/bin/bash
set -e
MARKER=/var/lib/bench-node-ready
[ -f "${MARKER}" ] && exit 0

mkdir -p /home/ubuntu/.ssh
echo "__SSH_PUBLIC_KEY__" >> /home/ubuntu/.ssh/authorized_keys
chown -R ubuntu:ubuntu /home/ubuntu/.ssh
chmod 700 /home/ubuntu/.ssh && chmod 600 /home/ubuntu/.ssh/authorized_keys

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y ca-certificates curl iproute2
command -v docker >/dev/null 2>&1 || curl -fsSL https://get.docker.com | sh
groupadd -f docker
systemctl enable --now docker

cat > /etc/security/limits.d/99-bench.conf <<'EOF'
*  soft  nofile  1048576
*  hard  nofile  1048576
*  soft  memlock unlimited
*  hard  memlock unlimited
EOF
sysctl -w vm.max_map_count=1048575 >/dev/null
echo "vm.max_map_count=1048575" > /etc/sysctl.d/99-bench.conf

touch "${MARKER}"
```

The SSH key is injected *before* the Docker install rather than after: if
`apt-get`/Docker install fails partway (a transient mirror hiccup, e.g.), the
operator can still SSH in to see why, rather than the instance being
unreachable and indistinguishable from one still installing.

`_aws_render_user_data` substitutes `__SSH_PUBLIC_KEY__` with the contents of
`$(_aws_ssh_key).pub` before base64-encoding the script for `run-instances
--user-data`, using bash's `//` (all-occurrences) substitution rather than
`/` (first-occurrence) — the real `install-docker.sh` also names the
placeholder in its header comment, and a first-occurrence replace would
splice the key into the comment instead of the `authorized_keys` line.

## 4. Files touched

| File | Change |
| --- | --- |
| `infra/aws/provider.sh`, `infra/aws/locations.csv`, `infra/aws/regions.csv`, `infra/aws/install-docker.sh` | **New.** The contract, satisfied for EC2. |
| `exp.config` | Add `aws.machine=<instance type>`, alongside the existing `machine=`/`gcp.machine=`. |
| `infra/README.md` | Add an `### aws` section (default-VPC assumption, `ubuntu` default user, SSH open by design). |

Nothing in `utils.sh` or any experiment script changes — that's the point of
the contract already being in place.

## 5. Phasing and verification

1. **Two hosts, one region.** `infra/aws/locations.csv` with two AZs of the
   same region; `./deploy.sh provision 2`; `./deploy.sh status` shows both
   nodes with a private IP and a working Docker context.
2. **Simplest protocol.** `./cdf.sh --test --protocols=cassandra-paxos` —
   exercises seeds, `get_container_ip`, and the YCSB co-location in one pass.
3. **Full protocol sweep, multi-region.** `accord`, `cockroachdb`,
   `swiftpaxos-paxos`, `tiga`, across three or more regions — each exercises
   a different addressing path, and this is the first test of the per-region
   VPC/security-group/AMI resolution actually working across regions rather
   than within one.
4. **Fault injection.** `fault_tolerance.sh --test` — confirm the slowdown
   lands on the correct remote leader and device, `restore_tc.py` reverses
   it, and `infra_teardown`/`infra_reset_network` leave no shaped interface
   behind.
5. **Sanity check.** Compare observed latency against `distance.py`'s
   haversine bound for the regions used.
6. **Teardown correctness.** Confirm no EC2 instances, security groups or
   Docker contexts survive a `./deploy.sh teardown` (`aws ec2
   describe-instances`/`describe-security-groups`, `docker context ls`).

## 6. Open questions

- **`nodesperdc > 1`.** One VM per node means `nodesperdc=2` over 3 DCs is 6
  VMs, and intra-DC latency becomes real (same-region, sub-millisecond)
  rather than the current exact zero.
- **Cost control across regions.** A run bills per second, per region, for as
  long as `infra_teardown` hasn't run. `run-all.sh`-style multi-experiment
  runs should provision once, run every experiment, and tear down in a
  `trap`, not provision per experiment.
- **`--test` in real mode.** `compute_test_machine` rewrites `machine=` to
  fit the local box, which is meaningless against real instances; either
  `--test` means "short duration, small record count" only, and
  `aws.machine=` alone governs provisioning, or `--test` is refused outright
  when `infra != simulation`.
- **Quota headroom as a first-class check.** `_aws_preflight` currently
  checks credentials and shape validity but not whether each region's vCPU
  quota can actually fit `n × aws.machine`'s vCPU count — worth adding once
  it's clear how often this actually bites in practice (§1's Finding on
  per-region quotas).

## 7. Operator prerequisites

Everything below is one-time AWS-account setup, outside the repo, done once
before `./deploy.sh` is ever invoked.

1. **AWS account.** https://aws.amazon.com/free/
2. **IAM user for CLI access, not root.**
   https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html —
   programmatic access (access key/secret), with either the managed
   `AmazonEC2FullAccess` policy or this custom one
   (https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_create.html):
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Action": [
         "ec2:RunInstances", "ec2:TerminateInstances", "ec2:DescribeInstances",
         "ec2:DescribeAvailabilityZones", "ec2:DescribeVpcs", "ec2:DescribeSubnets",
         "ec2:CreateSecurityGroup", "ec2:DeleteSecurityGroup", "ec2:DescribeSecurityGroups",
         "ec2:AuthorizeSecurityGroupIngress", "ec2:CreateTags", "ec2:DescribeImages",
         "ssm:GetParameters", "sts:GetCallerIdentity"
       ],
       "Resource": "*"
     }]
   }
   ```
   Add this second statement only if `aws.spot=1` (§2.3) will ever be used —
   an account's *first* spot request creates the
   `AWSServiceRoleForEC2Spot` service-linked role automatically, but the IAM
   principal making that first request needs permission to create it:
   ```json
   {
     "Effect": "Allow",
     "Action": "iam:CreateServiceLinkedRole",
     "Resource": "arn:aws:iam::*:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot*",
     "Condition": {"StringEquals": {"iam:AWSServiceName": "spot.amazonaws.com"}}
   }
   ```
   Without it, the first `run-instances --instance-market-options
   MarketType=spot` fails with an `AccessDenied` on `CreateServiceLinkedRole`
   — surfaced verbatim by `_aws_create_instance`'s existing catch-all error
   printing, no special-casing needed. Every request after the role exists
   works with only the first statement.
3. **AWS CLI v2.**
   https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
4. **`aws configure`**, then verify with `aws sts get-caller-identity`.
   https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html
5. **An SSH key** (`ssh-keygen -t ed25519` if none exists) — no EC2 key-pair
   resource needed, the public half goes through user-data (§3, Step 9).
6. **Per-region EC2 vCPU quota**, checked/raised before a run that needs it
   (§1's Finding): https://docs.aws.amazon.com/servicequotas/latest/userguide/request-quota-increase.html
7. **A default VPC in each region used** (present by default; restore if
   deleted): https://docs.aws.amazon.com/vpc/latest/userguide/work-with-default-vpc.html#create-default-vpc
8. **A budget alert** (recommended, multi-region runs bill continuously
   until teardown): https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-create.html

Per-deployment, after the above: pick AZs into `infra/aws/locations.csv`, set
`infra=aws`/`latency_simulation=0`/`aws.machine=` in `exp.config` (optionally
`aws.spot=1`, §2.3), `./deploy.sh bootstrap <n>`, run experiments,
`./deploy.sh teardown`.
