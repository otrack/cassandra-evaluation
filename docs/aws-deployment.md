# AWS EC2 infra provider

## Overview

The benchmark suite picks where containers run through a provider abstraction
(`infra/README.md`): `utils.sh` sources `infra/${infra}/provider.sh`, and
every script drives Docker/SSH exclusively through the contract functions it
defines — nothing outside `infra/` may call a provider tool or `docker`
directly. This document is the design for `infra/aws/provider.sh`: one EC2
instance per node, reached through SSH-backed Docker contexts, so that
`infra=aws` in `exp.config` works like any other provider.

Two scope decisions fix the shape of the design below:
- **Same-cloud addressing.** `infra_host_ip` returns each node's AWS private
  VPC IP — the address peers use to reach each other. Mixing nodes from
  different clouds in one deployment is out of scope.
- **Default VPC.** Provisioning launches into each region's existing default
  VPC and default subnets, managing only a security group. No VPC, subnet,
  route-table or internet-gateway lifecycle to build or tear down.

Everything `utils.sh` already gives every provider is reused as-is:
`state_get`/`state_set`/`state_nodes`/`state_clear` (backed by
`.deployment/aws.csv`, same `node,zone,host,ssh_host,ssh_user,net_device`
schema every provider uses), `config`, `node_index_of`, `get_location`,
`host_aliases`, the `d*`/`dpull` Docker dispatch wrappers, and
`infra_all_indices`/`infra_bootstrap`. `infra/aws/provider.sh` only needs to
supply the AWS-specific half: the contract functions themselves, plus
whatever private helpers they need underneath.

## File layout

```
infra/aws/
├── provider.sh        # the contract + private helpers
├── locations.csv       # one column "az": the AWS availability zones this deployment uses, in DC order
├── regions.csv          # region,loc,lat,lon — real-world coordinates of each AWS region
└── install-docker.sh    # EC2 user-data: installs Docker, injects the operator's SSH key
```

## Contract functions

| Function | AWS behavior |
| --- | --- |
| `infra_is_real` | `return 0`. |
| `infra_locations_file` | Joins `locations.csv` × `regions.csv` into the `lat,lon,loc` map. See **Locations** below. |
| `infra_provision <n>` | Creates `n` EC2 instances and waits until they're usable. See **Provisioning** below — this is the one genuinely complex function. |
| `infra_teardown` | Per region: terminate every tagged instance, wait for termination, then delete that region's security group (deletion fails while a member instance/ENI is still shutting down). Then remove this provider's Docker contexts and `state_clear`. |
| `infra_sync` | Re-`describe-instances` to refresh `host`/`ssh_host` (a stopped/restarted instance gets a new public IP), then recreate Docker contexts. |
| `infra_ssh <idx> [cmd…]` | Plain `ssh` to the node's recorded public IP as the recorded SSH user. EC2 has no interactive-login layer to wrap (unlike a console-integrated `compute ssh`-style command), so this is the same connection logic used internally, just without a non-interactive flag. |
| `infra_reset_network` | SSH into every node in `infra_all_indices` and `tc qdisc del dev <net_device> root`, ignoring failure (no qdisc set is the normal case). |
| `infra_host_ip <idx>` | `state_get <idx> host` (private IP); empty for index 0. |
| `infra_context <idx>` | A deterministic name from this provider's context prefix; empty for index 0. |
| `infra_net_device <idx>` | The interface recorded during provisioning; falls back to `ens5` (typical primary interface on current Nitro-based instance types) if nothing's recorded yet. |
| `infra_open_ports <port>…` | Idempotently adds ingress rules for the given TCP ports to the security group(s) in use — describes first, only authorizes what's missing. Called by `infra_bootstrap` with `7000 7087 8080 9042 10000 26257`. |
| `infra_stage_file <idx> <local> <remote>` | `scp` to the node's recorded SSH target; echoes `<remote>` back for use in a `-v` mount. |
| `infra_resource_limits [idx]` | Empty string — the instance itself is the resource limit. |
| `infra_machine_shape` | `config "aws.machine"`, falling back to `config machine`. The EC2 instance type string used at launch. |
| `infra_rewrite_docker_args <container> <args…>` | Swaps a bridge network for `--network host` (leaving `--network container:…` attachments — the YCSB co-location case — untouched), drops `-p`/`--publish` (host networking exposes every port already), and appends `host_aliases()`'s `--add-host` entries so name-based addressing (seeds, `--join`, `MADDR`, the Tiga YAML `host:` block) keeps working without Docker's embedded DNS. `host_aliases` is shared, provider-agnostic code — nothing AWS-specific needed beyond calling it. |

## Provisioning walkthrough (`infra_provision <n>`)

**Preflight**, before anything billable happens:
- `aws` and `scp` are on `PATH`.
- `aws sts get-caller-identity` succeeds — credentials are configured.
- `infra_machine_shape` is non-empty.
- `infra_locations_file` builds successfully, and every AZ the run needs
  actually exists (`aws ec2 describe-availability-zones`).

**Per node**, fanned out as parallel background jobs:

1. Resolve the node's DC → planned AZ (row `dc+1` of `locations.csv`) →
   region (the AZ string with its trailing letter stripped, e.g.
   `us-east-1a` → `us-east-1`).
2. Resolve that region's default VPC and the default subnet for the planned
   AZ (`describe-vpcs --filters Name=isDefault,Values=true`;
   `describe-subnets --filters Name=availability-zone,Values=<az>
   Name=default-for-az,Values=true`).
3. Resolve a security group in that VPC — created once per region, reused
   after. At creation it gets two rules: `tcp:22` from `0.0.0.0/0`, and a
   self-referencing rule allowing all traffic between group members. SSH must
   be open from the moment the instance exists, since step 6 SSHes into it
   before `infra_open_ports` ever runs.
4. Resolve the current Ubuntu 22.04 AMI id for that region via the public
   Canonical SSM parameter — AMI ids are per-region, not portable.
5. Check for an existing instance tagged `Name=bench-node<idx>` in
   `pending`/`running` state; if found, record its AZ and skip creation
   (idempotent). Otherwise `run-instances` with the resolved subnet, security
   group, AMI, instance type, and `install-docker.sh` as user-data (the
   operator's SSH public key interpolated in — see below). No EC2 "key pair"
   resource is created or referenced.
   On `InsufficientInstanceCapacity`, retry against a sibling AZ of the same
   region, so the DC's `loc`/coordinates stay correct. Any other failure
   (quota, `VcpuLimitExceeded`, a bad instance type, auth) aborts immediately
   rather than retrying blindly.
6. Poll SSH for the `/var/lib/bench-node-ready` marker file `install-docker.sh`
   writes once Docker is installed, add the SSH user to the `docker` group,
   and detect the instance's actual primary network interface
   (`ip -o route show default`) rather than assuming one.

**Once every node is ready**, record `host` (private IP), `ssh_host` (public
IP), `ssh_user`, `net_device` and `zone` (the AZ) per node via `state_set`,
and create one Docker context per node
(`docker context create … "host=ssh://…"`).

## `install-docker.sh` (EC2 user-data)

Idempotent — guarded by the same ready-marker check at the top, so a reboot
doesn't re-run it. Installs Docker, creates the `docker` group ahead of any
user needing it, raises `nofile`/`memlock` limits and `vm.max_map_count` for
the long-lived JVMs the benchmarks start, appends the operator's SSH public
key to `/home/ubuntu/.ssh/authorized_keys`, then touches the marker file.

The key-injection step lives here, not in a separate metadata field, because
EC2 has no guest-agent mechanism that turns instance metadata into
`authorized_keys` on its own — user-data is the only hook available, so this
one script does both jobs.

## Locations data

`infra/aws/locations.csv` is authored by hand: one AZ per DC row, in the
order the deployment should use them, e.g.:
```
az
us-east-1a
eu-west-1a
ap-southeast-1a
```

`infra/aws/regions.csv` is a lookup table, one row per AWS region this suite
might deploy to, giving its real-world coordinates:
```
region,loc,lat,lon
us-east-1,Ashburn,39.0438,-77.4874
eu-west-1,Dublin,53.3498,-6.2603
ap-southeast-1,Singapore,1.3521,103.8198
```
`loc` must be alphabetic and globally unique within this file (same rule
`infra/README.md` states for any provider) — it becomes the Cassandra DC
name, the container-name prefix, and the plot legend label. `infra_locations_file`
joins the two files (region = AZ minus its trailing letter), rejects two AZs
whose region maps to the same `loc`, and materializes the result at
`.deployment/aws.locations.csv`, regenerated only when either source file
changes.

## Private helpers

A concrete function inventory for `provider.sh`, beyond the contract itself:

| Helper | Purpose |
| --- | --- |
| `_aws_region_of <az>` | Strip the trailing letter to get the region. |
| `_aws_dc_of <idx>` | Node index → DC number, using `config nodesperdc`. |
| `_aws_instance <idx>` | The tag/instance name, `bench-node<idx>`. |
| `_aws_ssh_key`, `_aws_ssh_user` | Resolve `$BENCH_SSH_KEY` (or the default `~/.ssh/id_{ed25519,rsa,ecdsa}` search) and `$BENCH_SSH_USER` (default `ubuntu`). |
| `_aws_ssh_target <idx>`, `_aws_ssh <idx> [cmd…]` | Build `user@host` from state and run `ssh` with it; the shared connection logic `infra_ssh` and every internal caller use. |
| `_aws_planned_az <idx>`, `_aws_az_of <idx>` | The AZ a node should use, and the AZ it actually landed in (state overrides planned). |
| `_aws_az_candidates <az>` | Sibling AZs of the same region, planned one first — the capacity-fallback list. |
| `_aws_ami_for_region <region>` | The SSM AMI lookup, cached per region for the run. |
| `_aws_default_vpc_id <region>`, `_aws_default_subnet_id <region> <az>` | Default-VPC/subnet lookups. |
| `_aws_security_group_id <region>` | Idempotent create-or-reuse, including the initial SSH + self-reference rules. |
| `_aws_create_instance <idx>` | Describe-then-create with the capacity-fallback retry. |
| `_aws_preflight <n>` | The checks listed under Provisioning. |
| `_aws_record_addresses <n>` | `describe-instances` → `state_set` per node. |
| `_aws_prepare_node <idx>`, `_aws_wait_ready <n>` | SSH-poll for the ready marker, add to `docker` group, detect `net_device`; fanned out across nodes. |
| `_aws_sync_contexts [n] [--force]` | (Re)create Docker contexts from current state. |
| `_aws_autosync` | Runs once when `provider.sh` is sourced: recreates any missing Docker context for an already-provisioned deployment, so a stale local Docker install self-heals without an explicit `infra_sync`. |

## Config changes

`exp.config` gains one key, alongside the existing `machine=`:
```
aws.machine=c5.2xlarge
```

`infra/README.md` gains an `### aws` section (mirroring the existing `###
gcp` one in structure, not content) covering: the default-VPC assumption,
that peer addressing is private-IP/same-cloud only, that the default SSH
login user is `ubuntu` (not the operator's local username), that SSH is
reachable from anywhere by design (`tcp:22` opens from `0.0.0.0/0` at
security-group creation, since nothing later in provisioning could open it in
time), and that `BENCH_SSH_KEY`/`BENCH_SSH_USER` behave the same as for any
other provider.

## Operator setup: from a blank AWS account to a running deployment

Everything below happens **outside** the repo, before `./deploy.sh` is ever
invoked — none of it is automated by `infra_provision`. Steps 1–4 are
one-time account bootstrap; 5–8 are per-machine/per-operator; 9 onward is the
regular per-experiment workflow.

1. **Get an AWS account**, if you don't have one:
   https://aws.amazon.com/free/ — a personal or team account is fine, no
   special tier is required for EC2.

2. **Create an IAM user for CLI access — never the root account.** In the
   IAM console, create a user with *programmatic access* (an access
   key/secret pair, not a console password):
   https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html

   Attach a policy covering what this provider calls. Quickest: the
   AWS-managed `AmazonEC2FullAccess` policy. For least privilege, a custom
   policy instead
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
   `ssm:GetParameters` is for the per-region AMI lookup; `sts:GetCallerIdentity`
   is what preflight uses to confirm credentials work before spending anything.

3. **Install the AWS CLI v2** on the machine that will run `./deploy.sh`:
   https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
   (Already present on this machine, at `/usr/local/bin/aws`.)

4. **Configure credentials**: `aws configure`, using the access key/secret
   from step 2 (the default region it asks for is just a CLI fallback — every
   call the provider makes passes `--region` explicitly):
   https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html
   Verify: `aws sts get-caller-identity` should print an account id, not an
   error.

5. **Generate an SSH key, if needed**: `ssh-keygen -t ed25519`. No AWS "key
   pair" resource is created or imported — the provider pushes the public
   half through EC2 user-data — so this is purely a local file, read via
   `$BENCH_SSH_KEY` or the default `~/.ssh/id_{ed25519,rsa,ecdsa}` search.

6. **Check EC2 vCPU quotas in every region you plan to use.** New accounts
   get a modest default quota for "Running On-Demand Standard
   (A, C, D, H, I, M, R, T, Z) instances" **per region**, often as low as
   5–32 vCPUs. A geo-distributed run spans several regions at once, each
   provisioning against its own quota independently; exceeding one fails that
   region's `run-instances` with `VcpuLimitExceeded` — a hard failure, not
   retried. Check/raise it beforehand, per region:
   https://docs.aws.amazon.com/servicequotas/latest/userguide/request-quota-increase.html
   (Service: EC2, quota above; increases can take minutes to a day.)

7. **Confirm each region has a default VPC.** Nearly all accounts do,
   everywhere, by default — but this design deliberately never creates one,
   so a deleted default VPC blocks provisioning in that region. Check and
   restore if needed:
   https://docs.aws.amazon.com/vpc/latest/userguide/work-with-default-vpc.html#create-default-vpc
   (`aws ec2 describe-vpcs --region <region> --filters Name=isDefault,Values=true`
   to check; `aws ec2 create-default-vpc --region <region>` to restore.)

8. **(Recommended) Set a budget alert.** A geo-distributed run bills by the
   second, across several regions, for as long as `infra_teardown` hasn't
   run. A Budget with an email/SNS alert at a low threshold catches a
   forgotten teardown quickly:
   https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-create.html

Steps 1–8 are one-time (re-check step 6 only when adding a region or a bigger
`aws.machine`). Then, per deployment:

9. **Pick regions and edit `infra/aws/locations.csv`** — one AZ per DC row,
   in order.
10. **Set `exp.config`**: `infra=aws`, `latency_simulation=0`,
    `aws.machine=<instance type>` (e.g. `c5.2xlarge`).
11. **Bootstrap**: `./deploy.sh bootstrap <n>` — provisions `n` instances and
    opens the protocol ports; `./deploy.sh status` shows what came up.
12. **Run experiments** exactly as with any other provider — `./cdf.sh
    --protocols=...`, `./fault_tolerance.sh`, etc. — no script changes needed.
13. **Tear down**: `./deploy.sh teardown`, every time — this is what actually
    stops billing (terminate, wait, delete the security group, drop Docker
    contexts, clear `.deployment/aws.csv`).

## Verification

1. `aws sts get-caller-identity` succeeds locally — confirms credentials are
   in place before relying on preflight to catch a missing one.
2. `exp.config`: `infra=aws`, `latency_simulation=0`, `aws.machine=<a cheap
   type, e.g. t3.small>`, 2 AZs in `infra/aws/locations.csv`.
3. `./deploy.sh provision 2`, then `./deploy.sh status` — both nodes show a
   private IP, a Docker context, and `docker --context … info` succeeds.
4. `./cdf.sh --test --protocols=cassandra-paxos` against those 2 nodes — the
   simplest protocol, exercising seeds, `get_container_ip`, and YCSB
   co-location in one pass.
5. `./deploy.sh sync` after manually stopping/starting one EC2 instance —
   confirm its context rebuilds against the new public IP.
6. `fault_tolerance.sh --test` once; confirm `infra_reset_network` and
   `infra_teardown` leave no shaped interface, security group, or Docker
   context behind (`aws ec2 describe-security-groups`, `docker context ls`).
7. `./deploy.sh teardown` — no EC2 instances remain (`aws ec2
   describe-instances --filters Name=tag:Name,Values=bench-node*
   Name=instance-state-name,Values=running`), and `.deployment/aws.csv` is
   gone.
