# Infrastructure providers

An *infra provider* tells the benchmark suite **where containers run**. The
default provider, `simulation`, runs every container on the local Docker
daemon — the historical behavior of this repository. Other providers place one
container per remote machine, provisioned from an IaaS, and reach them through
SSH-backed Docker contexts.

The provider is selected in `exp.config`:

```
infra=simulation      # or gcp, aws, …
```

`utils.sh` sources `infra/${infra}/provider.sh` and exposes the wrappers described below.
**No code outside `infra/` may call a provider tool** (`gcloud`, `aws`, `ssh`,
`scp`) or `docker` directly.

## Node indices

Nodes are numbered `1..N`, where

```
index = (dc - 1) * nodesperdc + k
```

`dc` is the 1-based row of the provider's locations map and `k` is the 1-based
node within that DC. Index **0** is reserved for the
*orchestrator* — the machine running the benchmark scripts. Containers that
belong to the orchestrator rather than to any node (the demo visualization) map
to 0, and `infra_context 0` must always return the local daemon.

`node_index_of <container-name>` (in `utils.sh`, mirrored in `infra.py`) maps a
container name to its index. Database containers are named `${loc}${k}`
(`Hanoi1`, `Paris2`), so the mapping is a reverse lookup on the location name —
the numeric suffix is the intra-DC index, *not* the node index.

## Locations and deployment state

A provider owns **two** things, kept in separate files because they have
different lifetimes.

### The locations map — `infra_locations_file`

Which places this infra deploys to, as `lat,lon,loc`, one row per DC, in order.
Row *i* is DC *i*.

This is deliberately **per provider**, not global. The simulation's map is a
list of cities it pretends to run in, and the distances between them are what
`tc` reproduces. A cloud provider's map is *derived*: it names the zones it
runs in, and looks their coordinates up in a table of where that cloud's
regions physically are. Coordinates therefore describe where the machines
actually are, which is what makes the theoretical-optimum overlays in `cdf.py`,
`conflict.py`, `distance.py` and `closed_economy.py` meaningful on a real
deployment.

`loc` is load-bearing beyond the plots: it is the container-name prefix
(`Paris1`), the Cassandra DC name, and the legend label. It must be
**alphabetic and unique** across the deployment, or container names collide.

### The deployment state — `.deployment/<provider>.csv`

What provisioning discovered, one row per node:

```csv
node,zone,host,ssh_host,ssh_user,net_device
1,asia-southeast1-b,10.0.0.1,34.10.20.30,bench,ens4
```

This is generated, not configured, so it lives under a gitignored directory and
never touches a tracked file. `host` is the address **peers** use; `ssh_host` is
the one the orchestrator connects to. Read and
write it with `state_get <node> <col>`, `state_set <node> <col> <value>`,
`state_nodes` and `state_clear` from `utils.sh`; `infra_all_indices` is just
the set of nodes it records.

## Providers

### `simulation` (default)

Everything on the local Docker daemon, WAN emulated with `tc`. Historical
behavior; every contract function is a no-op or returns the local daemon.

### `gcp`

One Compute Engine VM per node, in the zones listed by `infra/gcp/locations.csv`,
reached through SSH-backed Docker contexts.

```bash
gcloud config set project <id>          # once
# exp.config: infra=gcp, latency_simulation=0
./deploy.sh bootstrap 3                 # create 3 VMs and open the protocol ports
./deploy.sh status
./cdf.sh --protocols=cassandra-paxos
./deploy.sh teardown                    # VMs bill by the second — do not skip
```

Three things specific to this provider are worth knowing.

**Two addresses per machine.** Peers talk over the VPC-internal IP — that is
what `infra_host_ip` returns and what lands in the containers' `/etc/hosts`. The
orchestrator reaches each VM over its *external* IP. Both are recorded per node
in the state file, as `host` and `ssh_host`.

**Nothing is written to `~/.ssh/config`.** The public half of the operator's key
(`$BENCH_SSH_KEY`, defaulting to the first of `~/.ssh/id_ed25519`, `id_rsa`,
`id_ecdsa`) is registered in each instance's `ssh-keys` metadata at
creation. The suite's own remote calls state the key and address explicitly
(`ssh -i … user@ip`), and Docker contexts point at `ssh://user@<external-ip>`.
For an interactive shell, use `gcloud compute ssh bench-nodeN --zone <zone>` —
`./deploy.sh ssh <idx>` is a thin wrapper around exactly that.

Two consequences. The key must be one `ssh` offers by default, since Docker's
SSH transport accepts no `-i` flag; a key under a non-default name works for
everything except Docker contexts unless it is loaded into an `ssh-agent`. And
because contexts embed the address, a restarted instance gets a new ephemeral
IP — `./deploy.sh sync` re-reads the addresses and rebuilds the contexts.

Projects that enforce **OS Login** ignore instance-level `ssh-keys` metadata and
derive their own usernames; there, set `$BENCH_SSH_USER` to the derived name and
rely on `gcloud compute ssh` having published your key.

`BENCH_SSH_KEY` and `BENCH_SSH_USER` are read from the environment rather than
`exp.config` on purpose: they describe the operator's machine, not the
experiment, and `exp.config` is version controlled. See the main
[README](../README.md#running-on-real-machines).

**Locations are derived from the zones.** `locations.csv` lists the zones this
deployment uses, in order; `regions.csv` says where each of Google's regions
physically is. Joining them produces the `lat,lon,loc` map, materialised at
`.deployment/gcp.locations.csv` and refreshed whenever either input changes.
So a run in `europe-west9-a` reports itself as `Paris`, at Paris's coordinates
— nothing is imitating a city it is not in, and the theoretical bounds are
computed from where the machines really are.

The coordinates in `regions.csv` are **metro-level**: Google publishes the city
of each region, not the position of the building. Two zones of the same region
map to the same `loc` and are rejected, since their container names would
collide.

**Host networking.** `infra_rewrite_docker_args` swaps the bridge for
`--network host`, drops `-p` publishing, and injects `--add-host` for every
container of the deployment. A container joining `--network container:<name>`
(the YCSB clients) is left alone: it shares its replica's namespace and hosts
file. Because containers then share the VM's network namespace, `tc` rules from
the fault-tolerance experiment shape the VM's own interface and outlive the
container — `restore_tc.py` stops being cosmetic.

### `aws`

One EC2 instance per node, in the availability zones (AZs) listed by
`infra/aws/locations.csv`, reached through SSH-backed Docker contexts.
Provisioning uses each region's existing **default VPC** and subnets; this
provider owns a security group, not a VPC lifecycle.

```bash
aws configure                           # once; see docs/aws-deployment.md §7
                                         # for full one-time account setup
# exp.config: infra=aws, latency_simulation=0
./deploy.sh bootstrap 3                 # create 3 instances and open the protocol ports
./deploy.sh status
./cdf.sh --protocols=cassandra-paxos
./deploy.sh teardown                    # instances bill by the second — do not skip
```

Five things specific to this provider are worth knowing; the full design,
findings and a step-by-step operator setup guide (IAM policy, CLI install,
SSH key, per-region quotas, default VPC, budget alert) live in
[`docs/aws-deployment.md`](../docs/aws-deployment.md).

**Two addresses per machine.** Peers talk over the private VPC IP — that is
what `infra_host_ip` returns and what lands in the containers' `/etc/hosts`.
The orchestrator reaches each instance over its *public* IP. Both are
recorded per node in the state file, as `host` and `ssh_host`.

**SSH is open by design.** A fresh account's default VPC allows no inbound
traffic from outside itself, so the security group this provider manages
opens `tcp:22` from `0.0.0.0/0` at creation time, before `infra_open_ports`
(called later, from `infra_bootstrap`) ever runs. This is a conscious
tradeoff for a benchmark harness running short-lived, disposable machines,
not an oversight — see `docs/aws-deployment.md` §2.2.

**Nothing is written to `~/.ssh/config`.** EC2 has no metadata-driven key
injection the way some other clouds' guest agents provide, so the operator's
public key (`$BENCH_SSH_KEY`, defaulting to the first of `~/.ssh/id_ed25519`,
`id_rsa`, `id_ecdsa`) is spliced into `install-docker.sh` and delivered
through `run-instances --user-data` at creation — the only hook that runs
before an operator can reach an instance at all. The suite's own remote calls
state the key and address explicitly (`ssh -i … user@ip`), and Docker
contexts point at `ssh://user@<public-ip>`. The Ubuntu AMI has one fixed
login account, `ubuntu`; override it with `$BENCH_SSH_USER` if a custom AMI
needs a different one.

The same two consequences as any provider that embeds an address in a Docker
context apply here: the key must be one `ssh` offers by default, since
Docker's SSH transport accepts no `-i` flag, and a restarted instance gets a
new address — `./deploy.sh sync` re-reads it and rebuilds the contexts.

**Locations are derived from the AZs; one region per DC.** `locations.csv`
lists the AZs this deployment uses, in order; `regions.csv` says where each
of AWS's regions physically is (metro-level, like GCP's table). Joining them
produces the `lat,lon,loc` map, materialised at
`.deployment/aws.locations.csv`. Every AWS CLI call is region-scoped, so a
node's region is derived from its AZ by stripping the trailing letter
(`us-east-1a` → `us-east-1`), and provisioning resolves the VPC, subnet,
security group and AMI once per distinct region in use. A shape temporarily
unavailable in one AZ is retried against a sibling AZ of the same region
before failing.

**Host networking.** `infra_rewrite_docker_args` swaps the bridge for
`--network host`, drops `-p` publishing, and injects `--add-host` for every
container of the deployment, exactly as any real provider must. Because
containers then share the instance's network namespace, `tc` rules from the
fault-tolerance experiment shape the instance's own interface and outlive the
container — `restore_tc.py` stops being cosmetic.

## The contract

Every provider must define all of the following.

| Function | Contract |
| --- | --- |
| `infra_locations_file` | Path to this provider's `lat,lon,loc` map, materialising it first if it is derived. Called once, right after the provider is sourced. |
| `infra_is_real` | Exit 0 if containers run on remote machines, 1 for the local daemon. This is the predicate every mode-dependent branch tests; it must be cheap, as it is called on each Docker operation. |
| `infra_provision <n>` | Create `n` machines through the IaaS — one per node, in the zone of that node's DC — wait for SSH, install Docker, and record `host`, `ssh_user` and `net_device` with `state_set`. Must be idempotent. |
| `infra_teardown` | Destroy everything `infra_provision` created: machines, firewall rules, Docker contexts, and any `tc` state left on a host. Must call `state_clear`. |
| `infra_sync` | Re-establish the *local* state of a deployment that already exists — Docker contexts, SSH aliases — without touching the machines. Needed after a reboot, or when driving an existing deployment from another laptop. |
| `infra_ssh <idx> [cmd…]` | Open a shell on node `idx`, or run `cmd` there. Used by `./deploy.sh ssh`. |
| `infra_reset_network` | Clear traffic shaping left on the machines by an interrupted run. Called from `start_network`, i.e. once per cluster creation. A no-op wherever `tc` state cannot outlive a container. |
| `infra_host_ip <idx>` | The address peers use to reach node `idx` — the private IP within a single cloud, the public IP across providers. Empty for index 0. |
| `infra_context <idx>` | Name of the Docker context for node `idx`, created lazily. **Empty string means the local daemon**, which is what index 0 and the simulation provider always return. |
| `infra_net_device <idx>` | Interface name used by `tc` commands on node `idx`. |
| `infra_open_ports <port>…` | Allow the given TCP ports between the benchmark machines. |
| `infra_stage_file <idx> <local> <remote>` | Make `<local>` available at `<remote>` on node `idx`'s filesystem and echo the path to use in a `-v` bind-mount. Simulation echoes `<local>` unchanged. |
| `infra_resource_limits [idx]` | The `--cpus`/`--memory` flags for a container on node `idx`. Simulation derives them from `machine=` and `gcp.csv`; a real provider returns nothing, because the machine *is* the limit. |
| `infra_machine_shape` | The provider's instance type for `machine=`. This is where `gcp.csv` shape names get translated for other clouds. |
| `infra_rewrite_docker_args <container> <args…>` | Last chance to alter the `docker run` arguments — where a real provider swaps the bridge network for `--network host` and injects `--add-host` entries. Echoes the arguments back, space-separated. Only called when `infra_is_real` succeeds, so simulation never pays for it. |

`utils.sh` builds three helpers on top of the contract, which providers inherit
and must not redefine:

- `infra_all_indices` — every provisioned node index, i.e. the nodes recorded
  in the deployment state file.
- `infra_bootstrap <n>` — `infra_provision`, then `infra_open_ports` for the
  union of all protocol ports, then `pull_images` on each machine.
- `host_aliases` — `--add-host name:ip` for every container name in the
  deployment, for providers that drop Docker's embedded DNS.

## Adding a provider

1. Copy `infra/simulation/` to `infra/<name>/` and implement the contract in
   `provider.sh`.
2. Give it a locations map: a static `locations.csv`, or a derivation like
   GCP's zone list joined against a region table.
3. Set `infra=<name>` and `latency_simulation=0` in `exp.config`.
4. Run `./cdf.sh --test --protocols=cassandra-paxos` against two machines
   first — it is the simplest protocol and exercises seeds, `get_container_ip`
   and the YCSB co-location in one pass.
