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

### 2.3 Locations and state

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

_aws_security_group_id() {   # one per region, created once, reused after
    local region=$1 vpc sg
    vpc=$(aws ec2 describe-vpcs --region "${region}" --filters Name=isDefault,Values=true \
        --query 'Vpcs[0].VpcId' --output text)
    sg=$(aws ec2 describe-security-groups --region "${region}" \
        --filters "Name=group-name,Values=bench-internal" "Name=vpc-id,Values=${vpc}" \
        --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
    [ -n "${sg}" ] && [ "${sg}" != "None" ] && { echo "${sg}"; return 0; }

    sg=$(aws ec2 create-security-group --region "${region}" --group-name bench-internal \
        --description "cassandra-evaluation inter-node traffic" --vpc-id "${vpc}" \
        --query 'GroupId' --output text) || return 1
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

_aws_create_instance() {
    local idx=$1 shape=$2 planned az region subnet sg ami name existing user_data
    planned=$(_aws_planned_az "${idx}") || return 1
    name=$(_aws_instance "${idx}")
    user_data=$(_aws_render_user_data)

    for az in $(_aws_az_candidates "${planned}"); do
        region=$(_aws_region_of "${az}")
        existing=$(aws ec2 describe-instances --region "${region}" \
            --filters "Name=tag:Name,Values=${name}" "Name=instance-state-name,Values=pending,running" \
            --query 'Reservations[].Instances[0].InstanceId' --output text)
        if [ -n "${existing}" ] && [ "${existing}" != "None" ]; then
            state_set "${idx}" zone "${az}"; return 0
        fi

        subnet=$(_aws_default_subnet_id "${region}" "${az}")
        sg=$(_aws_security_group_id "${region}") || return 1
        ami=$(_aws_ami_for_region "${region}")
        out=$(aws ec2 run-instances --region "${region}" \
            --image-id "${ami}" --instance-type "${shape}" \
            --subnet-id "${subnet}" --security-group-ids "${sg}" \
            --user-data "${user_data}" \
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
    return 1
}
```

`_aws_preflight` checks `aws`/`scp` on `PATH`, `aws sts get-caller-identity`,
`infra_machine_shape` non-empty, and that `infra_locations_file` builds.
`_aws_record_addresses` loops `describe-instances` per node
(`--query 'Reservations[].Instances[].[PrivateIpAddress,PublicIpAddress]'`)
and calls `state_set … host/ssh_host/ssh_user`. `_aws_wait_ready` fans
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
        sg=$(_aws_security_group_id "${region}") || return 1
        for p in "$@"; do
            aws ec2 authorize-security-group-ingress --region "${region}" --group-id "${sg}" \
                --protocol tcp --port "${p}" --source-group "${sg}" >/dev/null 2>&1
        done
    done
}
```

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
    local args=("$@") out=() host_net=0 i=0
    while [ ${i} -lt ${#args[@]} ]; do
        case "${args[$i]}" in
            --network)
                if [[ "${args[$((i+1))]}" == container:* ]]; then
                    out+=(--network "${args[$((i+1))]}")
                else out+=(--network host); host_net=1; fi
                i=$((i+2)); continue ;;
            -p|--publish) i=$((i+2)); continue ;;
            *) out+=("${args[$i]}"); i=$((i+1)) ;;
        esac
    done
    [ ${host_net} -eq 1 ] && out+=($(host_aliases))
    echo "${out[@]}"
}
```

`host_aliases` is shared, provider-agnostic code already in `utils.sh:182-198`
— it walks `LOCATIONS_FILE` and calls `infra_host_ip` per node, so nothing
AWS-specific is needed beyond the two calls above.

### Step 8 — sync, ssh, network reset, teardown

```bash
infra_sync() {
    local n; n=$(infra_all_indices | wc -l)
    [ "${n}" -eq 0 ] && { error "aws: no deployment recorded"; return 1; }
    _aws_record_addresses "${n}" && _aws_sync_contexts "${n}" --force
}

infra_ssh() {
    local idx=${1:?usage: infra_ssh <node_idx> [command...]}; shift
    if [ $# -gt 0 ]; then
        ssh -i "$(_aws_ssh_key)" "$(_aws_ssh_target "${idx}")" "$@"
    else
        ssh -i "$(_aws_ssh_key)" "$(_aws_ssh_target "${idx}")"
    fi
}

infra_reset_network() {
    local idx dev
    for idx in $(infra_all_indices); do
        dev=$(infra_net_device "${idx}")
        _aws_ssh "${idx}" "sudo tc qdisc del dev ${dev} root" >/dev/null 2>&1 &
    done
    wait
}

infra_teardown() {
    local region
    for region in $(state_nodes | while read -r n; do _aws_region_of "$(state_get "${n}" zone)"; done | sort -u); do
        (
            local ids; ids=$(aws ec2 describe-instances --region "${region}" \
                --filters "Name=tag:Name,Values=bench-node*" "Name=instance-state-name,Values=pending,running" \
                --query 'Reservations[].Instances[].InstanceId' --output text)
            if [ -n "${ids}" ] && [ "${ids}" != "None" ]; then
                aws ec2 terminate-instances --region "${region}" --instance-ids ${ids} >/dev/null
                aws ec2 wait instance-terminated --region "${region}" --instance-ids ${ids}
            fi
            local sg; sg=$(_aws_security_group_id "${region}" 2>/dev/null)
            [ -n "${sg}" ] && aws ec2 delete-security-group --region "${region}" --group-id "${sg}" >/dev/null 2>&1
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

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y ca-certificates curl iproute2
command -v docker >/dev/null 2>&1 || curl -fsSL https://get.docker.com | sh
groupadd -f docker
systemctl enable --now docker

mkdir -p /home/ubuntu/.ssh
echo "__SSH_PUBLIC_KEY__" >> /home/ubuntu/.ssh/authorized_keys
chown -R ubuntu:ubuntu /home/ubuntu/.ssh
chmod 700 /home/ubuntu/.ssh && chmod 600 /home/ubuntu/.ssh/authorized_keys

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

`_aws_render_user_data` substitutes `__SSH_PUBLIC_KEY__` with the contents of
`$(_aws_ssh_key).pub` before base64-encoding the script for `run-instances
--user-data`.

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
`infra=aws`/`latency_simulation=0`/`aws.machine=` in `exp.config`,
`./deploy.sh bootstrap <n>`, run experiments, `./deploy.sh teardown`.
