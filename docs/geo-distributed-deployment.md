# Porting Local Benchmarks to a Real Geo-Distributed Deployment

The benchmark suite currently simulates geo-distribution on a **single Docker
host**: all replica and client containers share one Docker bridge network,
addresses are resolved via `docker inspect` bridge IPs or Docker's embedded
container-name DNS, and cross-DC "WAN" latency is synthetic — injected with
`tc`/netem between containers on that one bridge (`emulate_latency.py`).

This note assesses whether porting the suite to a **real geo-distributed
deployment** — one real machine per simulated node, provisioned from an IaaS —
is tractable, and lays out a concrete implementation plan.

**Verdict: tractable, via SSH-backed Docker contexts and a new `infra/`
abstraction.** Docker exposes remote daemons natively (`docker context create
--docker "host=ssh://user@ip"`, `docker.DockerClient(base_url="ssh://…")`), so
the port does not require re-implementing any orchestration: it requires
routing existing `docker` calls to the right daemon. Since nearly every Docker
interaction in this repo already funnels through a small number of functions,
the change is a **thin proxy layer** plus a **pluggable infra provider**,
not a scattered rewrite.

A Kubernetes-based deployment (multi-region clusters, pods instead of
containers) is the other classical option. It is deliberately *not* pursued
here: it would replace the entire container-lifecycle layer that this suite
already has working for seven protocols, in exchange for scheduling and
self-healing features that a fixed-size benchmark cluster does not need. It
remains a sensible direction if the project later needs dozens of DCs or
long-lived shared clusters; nothing in the design below forecloses it, because
the infra contract (§3.2) could be implemented a third time by a `k8s.sh`.

## 1. Findings

**Docker coupling is deep but narrow.** Every container operation funnels
through a handful of choke points:

- Bash: `start_container`, `stop_container`, `wait_container`,
  `get_container_ip`, `fetch_logs_container`, `stop_container_after_delay`,
  `pull_images` in `utils.sh`; `start_network`/`stop_network` in
  `run_benchmarks.sh:14-50`.
- Python: `docker.from_env()` in exactly six files — `emulate_latency.py:38`,
  `restore_tc.py:31`, `start_cassandra_cluster.py:56`,
  `cassandra/start_cassandra_data_centers.py:49`,
  `cassandra/cleanup_cassandra_cluster.py:22`, `cassandra/create_new_node.py:54`.
- Residual raw `docker` calls outside those choke points, which must be ported
  individually: `cassandra/cluster.sh:25,87`, `cockroachdb/cluster.sh:33,180`,
  `cassandra/ycsb.sh:25,33,64`, `cockroachdb/ycsb.sh:35,41,60`,
  `swiftpaxos/cluster.sh:50,53,56,60,103`, `tiga/cluster.sh:139`,
  `fault_tolerance.sh:172-190`, `cassandra/cassandra_fast_path.sh:10,15`,
  `swiftpaxos/swiftpaxos_fast_path.sh:7`,
  `cassandra/cassandra_breakdown.sh:25,31,41,46`.

Roughly 45 call sites in total, almost all mechanical one-line rewrites.

**Addressing is the real problem, not process control.** Two mechanisms are
host-local and break the moment containers land on different machines:

1. `get_container_ip` (`utils.sh:225`) returns a Docker bridge IP
   (`172.x.y.z`), which is meaningless off-host. It feeds `${pref}_get_hosts`,
   `${pref}_get_node_count` and the YCSB connection strings.
2. Bare container names are used as hostnames and rely on Docker's embedded
   DNS: `--join=${first_node}` and `--host=${first_node}`
   (`cockroachdb/cluster.sh:20-33`), `MADDR=` (`swiftpaxos/cluster.sh:25`),
   the Cassandra seed wiring, and `tiga/config-ycsb.yml`'s `host:` block, which
   maps `janus-lan-server-0000: Hanoi1`.

**Container naming is city-based, not index-based.** All four systems name
their database containers `${city}${k}` — `Hanoi1`, `Lyon1`, `NewYork1` — where
the city is line *i+1* of `latencies.csv` and `k` is the index within the DC.
The numeric suffix is therefore the *intra-DC* index, not the DC index, so
mapping a container name back to a machine requires a **reverse lookup on the
city name**, not suffix parsing. Four names are special: `swiftpaxos-master`
(no DC of its own), `ycsb` (the load client), `ycsb-${i}` (the run client for DC
*i*), and `accord-viz` (the demo visualization, orchestrator-local).

**The YCSB client is explicitly co-located with its replica** via
`--network container:${nearby_database}` (`run_benchmarks.sh:132`). This is a
*feature* to preserve — client and replica for DC *i* should still share a
machine and a network namespace — but it means the YCSB container has no
machine of its own: its placement is derived from the replica it attaches to.

**The synthetic-latency mechanism *is* the geo-distribution mechanism today.**
`emulate_latency.py` is already gated by an existing `latency_simulation` flag
in `exp.config` (`emulate_latency.py:35`), so real mode disables it and gets
genuine WAN latency instead. Conversely, `distance.py`,
`theoretical_latency_approximation.py` and the `compute_optimum_per_replica`
overlays in `cdf.py`/`conflict.py` are geography-based and become *more*
meaningful with real deployments — they turn into a genuine
observed-vs-theoretical comparison. **No change needed there.**

**Two bind-mounts, and no env-file problem.** Only `run_benchmarks.sh:134` and
`tiga/cluster.sh:118` bind-mount a local file (`config-ycsb.yml`) into a
container, which assumes the file is on the daemon's filesystem. The YCSB
`--env-file` at `run_benchmarks.sh:132` is *not* affected: `--env-file` is read
by the Docker **client** and shipped as environment variables over the API, so
it works unchanged against a remote daemon.

**Machine sizing inverts.** `get_resource_limits` and `compute_test_machine`
translate a `gcp.csv` row into `--cpus`/`--memory` flags so that several
containers on one laptop approximate distinct VMs. In real mode the VM *is* the
machine: the shape becomes a provisioning input rather than a container flag,
and `--test` (which rewrites `machine=` in `exp.config`) is a
simulation-only concept.

**Network device names differ.** All `tc` commands hardcode `eth0`
(`emulate_latency.py:71-103`, `fault_tolerance.sh:172-184`). That is correct for
bridge-networked containers, but a real GCE VM's primary interface is `ens4` and
an EC2 VM's is typically `ens5`. The device must become infra-provided.

## 2. Design

### 2.1 Two modes, one code path

| | simulated (today) | real |
| --- | --- | --- |
| Machines | 1 local host | `num_dcs × nodes_per_dc` VMs |
| Docker daemon | local | one per VM, over `ssh://` |
| Container network | user-defined bridge | `--network host` |
| Inter-node addressing | bridge IP + Docker DNS | VM IP + injected `/etc/hosts` |
| Latency | `tc`/netem | real WAN |
| Container CPU/mem | `--cpus`/`--memory` from `gcp.csv` | the VM shape |
| `tc` device | `eth0` | `ens4` / `ens5` / … |

**Why `--network host` in real mode.** With one node per VM, host networking
removes the entire address-translation problem: a container's address is its
VM's address, every port is reachable without `-p` mapping, and each system's
own broadcast/advertise defaults (Cassandra's `broadcast_address`,
CockroachDB's `--advertise-addr`) become correct automatically. The alternative
— keeping the bridge and publishing ports — requires per-protocol advertise-
address overrides so that a node does not gossip its unreachable `172.x` address
to its peers, which is exactly the per-protocol work this design is trying to
avoid.

The cost of host networking is that container-name DNS disappears. That is
recovered cheaply by injecting `--add-host <name>:<ip>` for every node into
every container, which makes all the existing name-based references
(`--join=Hanoi1`, `MADDR=`, seeds, `tiga/config-ycsb.yml`) resolve to real IPs
with **no change to the strings themselves**. `--add-host` works with
`--network host`, and `--network container:<name>` shares both the namespace
and the hosts file, so the YCSB co-location keeps working unmodified.

### 2.2 The `infra/` directory

Infra-specific knowledge is isolated behind one interface, selected by a single
config key:

```
infra/
├── README.md          # the contract, and how to add a provider
├── simulation.sh      # default; today's behavior, one local Docker daemon
├── gcp.sh             # Google Compute Engine, via gcloud
└── aws.sh             # EC2, via the aws CLI
```

```bash
# exp.config
infra=simulation        # or gcp, aws
infra_region_map=       # optional provider-specific placement override
ssh_user=bench
machine=e2-highcpu-8    # in real mode: the VM shape to provision
```

A dispatcher sourced once by `utils.sh`:

```bash
# utils.sh
INFRA=$(config infra); INFRA=${INFRA:-simulation}
source "${DIR}/infra/${INFRA}.sh" || { error "unknown infra '${INFRA}'"; exit 1; }
```

### 2.3 The infra contract

Every `infra/*.sh` must define exactly these functions. Nothing outside
`infra/` may call a provider tool (`gcloud`, `aws`, `ssh`, `scp`) directly.

| Function | Contract |
| --- | --- |
| `infra_is_real` | Exit 0 if containers run on remote machines, 1 for simulation. The single predicate every mode-dependent branch tests. |
| `infra_provision <n>` | **Create `n` machines** through the IaaS: one per node, named `bench-node1..bench-noden`, in the regions matching the first `n` rows of `latencies.csv`; wait for SSH; install Docker; add `ssh_user` to the `docker` group. Idempotent — re-running with existing machines is a no-op. Writes the discovered addresses back into the registry (§2.4). |
| `infra_teardown` | Delete every machine created by `infra_provision`, plus firewall rules and Docker contexts. |
| `infra_host_ip <idx>` | Routable IP of node `idx` (the address peers use: private IP within one cloud, public IP across providers). |
| `infra_context <idx>` | Name of the Docker context for node `idx`, creating it lazily via `docker context create … "host=ssh://${ssh_user}@$(infra_host_ip idx)"`. Empty string in simulation. |
| `infra_net_device <idx>` | Primary interface name for `tc` (`eth0` simulated, `ens4` GCE, `ens5` EC2). |
| `infra_open_ports <port>…` | Allow the given TCP ports between the benchmark machines (firewall rules / security-group ingress). No-op in simulation. |
| `infra_stage_file <idx> <local> <remote>` | Make a local file available at `<remote>` on node `idx`'s filesystem (`scp`). In simulation, echo `<local>` unchanged. Echoes the path to use in `-v`. |
| `infra_resource_limits <idx>` | The `--cpus`/`--memory` flags for a container on node `idx`. Today's `gcp.csv` lookup in simulation; empty in real mode. |
| `infra_machine_shape` | The provider's instance type for `$(config machine)` — used by `infra_provision`, and the place where `gcp.csv` names are translated (e.g. `e2-highcpu-8` → EC2 `c5.2xlarge`). |

Two orchestration helpers built on top, in `utils.sh` rather than per provider:

- `infra_all_contexts` — iterate node indices 1..N, for scripted per-host loops.
- `infra_bootstrap <n>` — `infra_provision`, then `infra_open_ports` for the
  union of every protocol's ports, then `pull_images` on each host.

### 2.4 The registry

`latencies.csv` already maps node index → city → lat/lon and is read everywhere
through `get_location()`. Extend it rather than inventing a second source of
truth:

```csv
lat,lon,loc,region,host,ssh_user,net_device
21.027763,105.834160,Hanoi,asia-southeast1-a,,,
45.764042,4.835659,Lyon,europe-west9-a,,,
40.712776,-74.005974,NewYork,us-east4-a,,,
```

`region` is authored by hand (the provider zone closest to the city); `host`,
`ssh_user` and `net_device` are left empty and **filled in by
`infra_provision`**. Empty `host` means simulation, which keeps the file valid
for today's workflow and makes the mode self-describing.

Add one accessor next to `get_location`:

```bash
# utils.sh — field 5 = host, 6 = ssh_user, 7 = net_device
registry_field() {  # registry_field <node_idx> <column_name>
    local idx=$1 col=$2
    awk -F',' -v n=$((idx+1)) -v c="$col" '
        NR==1 { for (i=1;i<=NF;i++) if ($i==c) k=i; next }
        NR==n { gsub(/\r|^[ \t]+|[ \t]+$/,"",$k); print $k }' "${DIR}/latencies.csv"
}
```

## 3. Implementation skeleton

### Step 1 — `node_index_of`: name → node

Everything downstream depends on resolving a container name to a node index.
This is the one genuinely new piece of logic, and it must handle the city-based
naming described in §1.

```bash
# utils.sh
node_index_of() {   # node_index_of <container_name> -> 1..N
    local name="$1" nodes_per_dc=$(config nodesperdc); nodes_per_dc=${nodes_per_dc:-1}

    case "$name" in
        swiftpaxos-master|ycsb|accord-viz) echo 1; return ;;   # pinned to node 1
        ycsb-*)  # run client for DC i -> first node of DC i
            local dc=${name#ycsb-}
            echo $(( (dc - 1) * nodes_per_dc + 1 )); return ;;
        database-node*)                                        # legacy fallback
            local dc=${name#database-node}
            echo $(( (dc - 1) * nodes_per_dc + 1 )); return ;;
    esac

    # ${city}${k}: split trailing digits, reverse-lookup the city
    local city="${name%%[0-9]*}" k="${name##*[!0-9]}"
    local i=1 loc
    while loc=$(get_location $i "${DIR}/latencies.csv" 2>/dev/null) && [ -n "$loc" ]; do
        if [ "$loc" == "$city" ]; then
            echo $(( (i - 1) * nodes_per_dc + k )); return
        fi
        i=$((i + 1))
    done
    error "node_index_of: cannot resolve '${name}'"; return 1
}
```

Mirror it once in Python (`infra.py`) so the six SDK files can share it.

### Step 2 — the `d*` verb wrappers

```bash
# utils.sh
d() {   # d <container_name> <docker args...>
    local ctx; ctx=$(infra_context "$(node_index_of "$1")"); shift
    if [ -n "$ctx" ]; then docker --context "$ctx" "$@"; else docker "$@"; fi
}
drun()     { d "$2" run "$@"; }        # $2 is the --name value by convention
dexec()    { d "$1" exec "$@"; }
dinspect() { d "$1" inspect "$@"; }
dlogs()    { d "$1" logs "$@"; }
dstop()    { d "$1" stop "$@"; }
dkill()    { d "$1" kill "$@"; }
```

In simulation `infra_context` returns empty, so every wrapper degenerates to
today's exact `docker` invocation — which is what makes the simulated-mode
regression test in §5 meaningful.

Then rewrite the ~45 call sites listed in §1 mechanically. The choke points in
`utils.sh` (`start_container`, `stop_container`, `wait_container`,
`fetch_logs_container`, `stop_container_after_delay`) cover most of them; the
rest are one-line substitutions in the per-protocol scripts, the fast-path
scripts and `fault_tolerance.sh`.

### Step 3 — `start_container`: placement, hosts file, network mode

`start_container` (`utils.sh:50`) grows three real-mode behaviors, and is the
only function that needs to know about any of them:

```bash
start_container() {
    # … existing arg parsing …
    local idx; idx=$(node_index_of "$cname")

    if infra_is_real; then
        docker_args+=(--network host $(host_aliases))
        docker_args=("${docker_args[@]/--network container:*/--network container:${nearby}}")
    fi
    # resource limits now come from the infra, not directly from gcp.csv
    docker_args+=($(infra_resource_limits "$idx"))

    d "$cname" run "${docker_args[@]}" --log-opt max-size=10m --log-opt max-file=3 \
        --name "$cname" "$image" "${container_cmd[@]}"
    d "$cname" logs -f "$cname" > "${log_file}" 2>&1 &
    # … existing readiness-polling loop, with dlogs/dinspect …
}

host_aliases() {   # --add-host for every node in the deployment
    local i=1 loc
    while loc=$(get_location $i "${DIR}/latencies.csv" 2>/dev/null) && [ -n "$loc" ]; do
        for k in $(seq 1 "$(config nodesperdc)"); do
            printf -- '--add-host %s%s:%s ' "$loc" "$k" "$(infra_host_ip "$(node_index_of "${loc}${k}")")"
        done
        i=$((i + 1))
    done
    printf -- '--add-host swiftpaxos-master:%s ' "$(infra_host_ip 1)"
}
```

`--network container:${nearby_database}` (`run_benchmarks.sh:132`) is left
verbatim — with the YCSB container's context resolved from
`${nearby_database}`'s name via `node_index_of`, it lands on the same VM as its
replica and shares its namespace, exactly as today.

### Step 4 — `get_container_ip`: the one mode-dependent function

```bash
get_container_ip() {
    local name="$1"
    if infra_is_real; then
        infra_host_ip "$(node_index_of "$name")"
    else
        dinspect "$name" -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2>/dev/null
    fi
}
```

Note the consequence for `${pref}_get_node_count`
(`cassandra/cluster.sh:67`, and its peers), which today counts *how many
containers answer* by probing `get_container_ip "${city}1"`. In real mode that
returns a registry value regardless of whether the container is up, so those
functions must probe liveness explicitly — `dinspect "${city}1" >/dev/null 2>&1`
— instead of inferring it from a non-empty IP. This is the one place where a
naive port would silently report a healthy cluster that is not running.

### Step 5 — networks and images, per host

`start_network`/`stop_network` (`run_benchmarks.sh:14-50`) and `pull_images`
(`utils.sh:324`) become per-host loops:

```bash
start_network() {
    infra_is_real || { docker network inspect … || docker network create …; return; }
    return 0   # host networking: nothing to create
}

pull_images() {
    local -a targets=("")
    infra_is_real && targets=($(infra_all_contexts))
    for ctx in "${targets[@]}"; do
        while IFS='=' read -r key value; do
            [[ "$key" =~ _image$ ]] || continue
            ${ctx:+docker --context "$ctx"} ${ctx:-docker} pull "$(echo "$value" | xargs)"
        done < "${CONFIG_FILE}"
    done
}
```

Pulling four images onto N VMs is the slowest part of a real run; do it once in
`infra_bootstrap`, not per experiment.

### Step 6 — the Python side

Add `infra.py` next to `emulate_latency.py`:

```python
import csv, docker, os

def _registry(path="latencies.csv"):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))

def node_index_of(name, nodes_per_dc=1):
    """Mirror of utils.sh:node_index_of."""
    ...

def client_for(name):
    row = _registry()[_dc_of(name)]
    host, user = row.get("host", ""), row.get("ssh_user", "")
    if not host:                      # simulated mode
        return docker.from_env()
    return docker.DockerClient(base_url=f"ssh://{user}@{host}")

def net_device(name):
    return _registry()[_dc_of(name)].get("net_device") or "eth0"
```

Then in each of the six files, replace `docker.from_env()` with
`client_for(<container name>)`. `emulate_latency.py` and
`start_cassandra_cluster.py` touch several containers per call, so they take a
client per container rather than one client per script — a small loop change,
no logic change. `emulate_latency.py` additionally returns early in real mode
via the **existing** `latency_simulation` guard (`emulate_latency.py:35`), so
its remote path only matters if someone deliberately shapes traffic on real
hardware.

### Step 7 — `tc` and fault injection

`fault_tolerance.sh:172-190` and `restore_tc.py` keep their semantics; only the
dispatch and the device name change:

```bash
dev=$(infra_net_device "$(node_index_of "${leader}")")
dexec "${leader}" bash -c "tc qdisc show dev ${dev} > /tmp/tc_qdisc_save.txt && …"
dexec "${leader}" tc qdisc add dev "${dev}" root netem delay 400ms
dkill "${leader}" --signal=19
```

With `--network host`, `tc` inside the container shapes the VM's own interface.
That is the desired effect for the fault-tolerance experiment (the whole node
slows down), but it is a genuine behavioral difference worth stating: the rule
outlives the container, so `restore_tc.py` becomes mandatory rather than
cosmetic, and a crashed run leaves a shaped VM behind. `infra_teardown` should
therefore also clear `tc qdisc del dev <dev> root` on every host.

### Step 8 — a provider, end to end

```bash
# infra/gcp.sh
infra_is_real() { return 0; }

infra_machine_shape() { config machine; }        # gcp.csv names are already GCE shapes

infra_provision() {
    local n=$1 shape; shape=$(infra_machine_shape)
    for i in $(seq 1 "$n"); do
        local name="bench-node${i}" zone; zone=$(registry_field "$i" region)
        gcloud compute instances describe "$name" --zone "$zone" >/dev/null 2>&1 || \
        gcloud compute instances create "$name" \
            --zone "$zone" --machine-type "$shape" \
            --image-family ubuntu-2204-lts --image-project ubuntu-os-cloud \
            --metadata-from-file startup-script="${DIR}/infra/install-docker.sh" \
            --tags bench-node
        local ip; ip=$(gcloud compute instances describe "$name" --zone "$zone" \
            --format='get(networkInterfaces[0].networkIP)')
        registry_set "$i" host "$ip"
        registry_set "$i" ssh_user "$(config ssh_user)"
        registry_set "$i" net_device ens4
    done
    infra_wait_ssh "$n"
}

infra_context() {
    local i=$1 name="bench-ctx-${i}"
    docker context inspect "$name" >/dev/null 2>&1 || \
        docker context create "$name" \
            --docker "host=ssh://$(registry_field "$i" ssh_user)@$(infra_host_ip "$i")" >/dev/null
    echo "$name"
}

infra_host_ip()     { registry_field "$1" host; }
infra_net_device()  { registry_field "$1" net_device; }
infra_resource_limits() { echo ""; }             # the VM is the limit

infra_open_ports() {
    gcloud compute firewall-rules describe bench-internal >/dev/null 2>&1 || \
    gcloud compute firewall-rules create bench-internal \
        --allow "tcp:$(IFS=,; echo "$*")" --source-tags bench-node --target-tags bench-node
}

infra_stage_file() {
    local i=$1 local_path=$2 remote=$3
    scp "$local_path" "$(registry_field "$i" ssh_user)@$(infra_host_ip "$i"):${remote}" >/dev/null
    echo "$remote"
}

infra_teardown() {
    gcloud compute instances delete $(seq -f 'bench-node%g' 1 "$(config nodes)") --quiet
    gcloud compute firewall-rules delete bench-internal --quiet
    # plus: docker context rm bench-ctx-*
}
```

`infra/aws.sh` is the same shape with `aws ec2 run-instances` /
`describe-instances`, a security group instead of a firewall rule, `ens5` as
the device, and a `gcp.csv`-shape → EC2-instance-type translation in
`infra_machine_shape`. `infra/simulation.sh` implements the same ten functions
trivially: `infra_is_real` returns 1, `infra_context`/`infra_host_ip` echo
nothing, `infra_provision`/`infra_open_ports`/`infra_teardown` are no-ops,
`infra_stage_file` echoes its local path, `infra_net_device` echoes `eth0`, and
`infra_resource_limits` keeps today's `gcp.csv` lookup — i.e. the current
`get_resource_limits` body moves there verbatim.

The ports to open are each protocol's `${pref}_get_port` plus its internal
ones: Cassandra 9042 + 7000 (gossip), CockroachDB 26257 + 8080, SwiftPaxos 7087
and its per-server ports, Tiga 10000.

### Step 9 — bind-mounts

Two call sites, both taking the file that must exist on the daemon's host:

```bash
# run_benchmarks.sh:134
docker_args+=" -v $(infra_stage_file "$(node_index_of "$nearby_database")" \
    "${DIR}/tiga/config-ycsb.yml" /tmp/config-ycsb.yml):/ycsb/config-ycsb.yml"
```

and the equivalent at `tiga/cluster.sh:118`. In simulation `infra_stage_file`
echoes the original path, so the line is unchanged in effect.

Note that `tiga/config-ycsb.yml` is *generated* per run by the inline Python in
`tiga/cluster.sh:20-100` and contains a `host:` block mapping server names to
container names (`janus-lan-server-0000: Hanoi1`). Those names keep working
through the injected `/etc/hosts` entries, so the generator needs no change.

## 4. Files touched

| File | Change |
| --- | --- |
| `infra/{simulation,gcp,aws}.sh`, `infra/install-docker.sh`, `infra/README.md` | **New.** The ten contract functions per provider. |
| `latencies.csv` | Add `region,host,ssh_user,net_device` columns. |
| `exp.config` | Add `infra=`, `ssh_user=`; set `latency_simulation=0` in real mode (flag already exists). |
| `utils.sh` | Add `node_index_of`, `d`/`drun`/`dexec`/`dinspect`/`dlogs`/`dstop`/`dkill`, `registry_field`, `registry_set`, `host_aliases`, `infra_bootstrap`, `infra_all_contexts`; make `get_container_ip` and `start_container` mode-aware; move `get_resource_limits` body into `infra/simulation.sh`; `pull_images` loops over hosts. |
| `run_benchmarks.sh` | `start_network`/`stop_network` become mode-aware; YCSB context resolved from `${nearby_database}`; `infra_stage_file` before the `-v` at line 134. |
| `cassandra/cluster.sh`, `cockroachdb/cluster.sh`, `swiftpaxos/cluster.sh`, `tiga/cluster.sh` | Raw `docker` → `d*` wrappers; `${pref}_get_node_count` probes liveness rather than IP presence. |
| `cassandra/ycsb.sh`, `cockroachdb/ycsb.sh` | `docker exec` → `dexec` (6 sites). |
| `cassandra/cassandra_fast_path.sh`, `cassandra/cassandra_breakdown.sh`, `swiftpaxos/swiftpaxos_fast_path.sh` | `docker exec`/`docker logs` → `dexec`/`dlogs` (7 sites). |
| `fault_tolerance.sh`, `restore_tc.py` | `d*`/`client_for`; `eth0` → `infra_net_device`. |
| `infra.py` | **New.** `client_for`, `node_index_of`, `net_device`. |
| `emulate_latency.py`, `restore_tc.py`, `start_cassandra_cluster.py`, `cassandra/start_cassandra_data_centers.py`, `cassandra/cleanup_cassandra_cluster.py`, `cassandra/create_new_node.py` | `docker.from_env()` → `client_for(name)`. |

**Not touched:** per-protocol keyspace/table creation logic, YCSB invocation and
options, `parse_ycsb_to_csv.sh`, `cockroachdb/cockroachdb_breakdown.py`,
`tiga/tiga_fast_path.sh`, `cockroachdb/cockroachdb_fast_path.sh` (neither shells
out to Docker), and every plotting script — all consume `logs/`/`results/`
exactly as today.

## 5. Phasing and verification

1. **Proxy layer, simulation only.** Land `infra/simulation.sh`, the registry
   accessors, `node_index_of` and all ~45 call-site rewrites with no provider.
   *Verify:* `./cdf.sh --test` and `./fault_tolerance.sh --test` produce output
   identical to `main` — every wrapper resolves to an empty context, so this is
   a pure refactor. This is the largest diff and carries almost all the risk;
   keeping it provider-free makes it reviewable against the current behavior.
2. **One provider, two hosts.** Add `infra/gcp.sh`; provision 2 VMs in distant
   regions; `latency_simulation=0`.
   *Verify:* `./cdf.sh --test --protocols=cassandra-paxos` (the simplest
   protocol) — containers land on the right VMs (`docker --context bench-ctx-N
   ps`), replicas gossip across regions, the YCSB client shares its replica's
   VM, and `results/cdf.pdf` is produced from real latency.
3. **Full protocol sweep.** Repeat for `accord`, `cockroachdb`,
   `swiftpaxos-paxos`, `tiga` — each exercises a different addressing path
   (seeds, `--join`, `MADDR`, the YAML `host:` block).
4. **Fault injection.** `./fault_tolerance.sh --test` on the real setup:
   confirm the slowdown lands on the correct remote leader with the right
   device name, that `restore_tc.py` reverses it, and that `infra_teardown`
   leaves no shaped interface behind.
5. **Sanity check.** Compare observed latency against `distance.py`'s
   haversine bound for the same two real locations — the payoff of the whole
   exercise, and a good regression signal for later runs.
6. **Second provider.** `infra/aws.sh`, to prove the contract is not
   GCP-shaped. A cross-provider deployment additionally forces `infra_host_ip`
   to return public addresses and is the strongest test of the abstraction.

## 6. Open questions

- **Cross-provider addressing.** Within one cloud, private IPs suffice. Across
  providers, every node needs a public IP and the firewall rules multiply; a
  WireGuard or Tailscale mesh laid down by `infra_provision` would let
  `infra_host_ip` keep returning a single stable private address in all cases.
  Worth deciding before `aws.sh` lands, since it changes what `infra_host_ip`
  means.
- **`nodesperdc > 1`.** The design provisions one VM per *node*, so
  `nodesperdc=2` over 3 DCs is 6 VMs and intra-DC latency is real (same region,
  ~sub-millisecond) rather than the current exact zero. Cheaper alternative:
  co-locate a DC's nodes on one VM, which reintroduces port conflicts under
  host networking and would need the published-port variant instead.
- **Cost control.** Real runs bill by the minute across regions. `run-all.sh`
  in real mode should `infra_provision` once, run all eight experiments, and
  `infra_teardown` in a `trap` — not provision per experiment.
- **`--test` in real mode.** `compute_test_machine` rewrites `machine=` to fit
  the local box, which is meaningless against VMs. Either make `--test` mean
  "short duration, small record count" only, and let `machine=` govern
  provisioning, or refuse `--test` when `infra != simulation`.
