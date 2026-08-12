# Porting Local Benchmarks to a Real Geo-Distributed Deployment

The benchmark suite currently simulates geo-distribution on a **single Docker host**:
all replica/client containers share one Docker bridge network, addresses are
resolved via `docker inspect` bridge IPs or bare container-name DNS, and
cross-DC "WAN" latency is synthetic — injected with `tc`/netem between
containers on that one bridge (`emulate_latency.py`,
`cassandra/create_new_node.py`, `start_cassandra_cluster.py`).
This note assesses whether porting this to a **real geo-distributed
deployment** (one real machine per simulated DC, via some IaaS) is
tractable, and lays out how to do it, comparing an SSH/Docker-remote-context
approach against adopting the legacy `GCP/` Kubernetes harness.

## Findings

- **Docker coupling is deep but narrow.** Every container operation funnels
  through a handful of choke points: `utils.sh`'s `start_container`,
  `stop_container`, `wait_container`, `get_container_ip`,
  `fetch_logs_container` (Bash), and `docker.from_env()` /
  `container.exec_run(...)` in exactly 6 Python files. Addressing is via
  Docker-bridge IP (`get_container_ip`) or bare container-name DNS
  (`database-node1`, `cassandra-node1`, …) — meaningless off-host. The YCSB
  client is explicitly co-located with its "nearby" replica via
  `--network container:${nearby_database}` (`run_benchmarks.sh:142`), which
  only works within one Docker host.
- **The synthetic-latency mechanism *is* the geo-distribution mechanism today**
  (`emulate_latency.py`, gated by an *already-existing* `latency_simulation`
  config flag in `exp.config`) — porting to real hosts doesn't just relax a
  constraint, it removes this component in favor of genuine WAN latency.
  `distance.py` / `theoretical_latency_approximation.py` and the
  `compute_optimum_per_replica` overlay in `cdf.py`/`conflict.py` are
  geography-based and **remain valid and more meaningful** with real
  deployments — no change needed there.
- **`fault_tolerance.sh` / `restore_tc.py`** inject slowdown/crash purely via
  `docker exec ... tc ...` and `docker kill --signal=19` against a container
  by name/Docker-SDK object — no host/SSH concept anywhere, but nothing in the
  actual `tc` semantics needs to change, only *how* the docker command is
  dispatched.
- **The legacy `GCP/` k8s harness supports none of the current protocols**
  (accord/cockroachdb/swiftpaxos/tiga/calvin/detock/janus) — it only knows
  `vcd`/`epaxos`/`paxos`/`mencius`/`cassandra`, and its "Accord" support is
  just a Cassandra image swap, not real integration. Its ~764-line
  `protocol-function.sh` (readiness-log greps, leader discovery, seed wiring)
  would need to be re-derived from scratch for every current protocol, plus
  cross-region pod networking solved. Reusable *pattern* pieces: `context.sh`
  (multi-cluster kubectl wrapper), `clusters.sh` (region table),
  `fed-start.sh`/`fed-stop.sh` (real multi-region GKE cluster provisioning,
  no `tc` needed since latency is genuinely geographic).

**Conclusion: tractable, and the SSH/Docker-remote-context path is the
right near-term move** — it reuses effectively all existing protocol-driver
logic (per-protocol `cluster.sh`, `run_benchmarks.sh`, breakdown/fast-path
scripts, plotting) unchanged, versus a k8s rewrite that re-derives that same
logic from scratch for 7 protocols the legacy harness has never seen. The k8s
path is documented at the end as a later option if the project outgrows
single-VM-per-DC (e.g., wants scheduling/self-healing/many more DCs).

The core insight that makes this clean: nearly every Docker interaction
already funnels through a small number of functions. The port therefore
becomes a **thin proxy layer** — a handful of new wrapper functions that are
the *only* code aware of "simulated vs. real" — rather than a scattered
rewrite.

## Proxy layer design

### 1. Registry — extend an existing file, don't invent a new one

`latencies.csv` already maps node index → location name → lat/lon and
is read everywhere via `get_location()` in `utils.sh`. Add columns:
`host,ssh_user` (empty in simulated mode). This single file becomes the
node-index → real-host mapping, with no new config surface.

### 2. Bash: `node_context(name)` + thin verb wrappers

- `node_context(name)`: parses the numeric suffix out of a container name
  (`database-node2` → index 2; `swiftpaxos-master`/`ycsb-N` map to node 1's
  or the colocated replica's host — see point 6), looks up `host` in the
  registry, returns a Docker context name (empty/`default` in simulated mode).
- `drun`, `dexec`, `dinspect`, `dlogs`, `dstop`, `dkill`: each resolves
  context via `node_context` on the container-name argument, then delegates
  to `docker --context "<ctx>" <verb> "$@"`. These replace the ~15 raw
  `docker <verb>` call sites currently in `utils.sh`, `cassandra/cluster.sh`
  (L21, L79-80), `cockroachdb/cluster.sh` (L33, L103, L199),
  `swiftpaxos/cluster.sh` (L88), and `fault_tolerance.sh` (tc save/restore,
  `docker kill --signal=19`, `docker stop`) — mechanical, one line each, no
  behavior change in simulated mode (empty context = today's default).
- One-time setup per real host: `docker context create <name> --docker
  "host=ssh://user@ip"` (Docker's native remote-daemon mechanism — no
  hand-rolled SSH wrapping needed).

### 3. Python: `client_for(name)`

One helper, same registry, same index-from-name parsing:
```python
def client_for(name):
    host = registry.host_for(node_index_of(name))
    if not host:  # simulated mode
        return docker.from_env()
    return docker.DockerClient(base_url=f"ssh://{host.user}@{host.ip}")
```
Swap into the 6 Python files' `docker.from_env()` calls:
`emulate_latency.py`, `restore_tc.py`, `create_new_node.py`,
`start_cassandra_cluster.py`, `start_cassandra_data_centers.py`,
`cleanup_cassandra_cluster.py`. Nothing else in these files changes — the
`tc`/exec_run logic is identical, just dispatched to the right daemon.

### 4. `get_container_ip` — the one function with real mode-dependent behavior

- Simulated: unchanged (`docker inspect` bridge IP).
- Real: registry's routable host IP + a published port. Requires adding
  `-p <port>:<port>` to each `${pref}_start_cluster`'s `docker run` args when
  in real mode (~1 line per protocol: cassandra, cockroachdb, swiftpaxos,
  tiga) — the one small, unavoidable per-protocol touch, since bridge-network
  implicit connectivity must become explicit published-port connectivity.

### 5. Latency: reuse the existing toggle, don't build a new one

Set `latency_simulation=0` in the real-mode `exp.config` (the flag and the
`if not config["latency_simulation"]: return` guard in `emulate_latency.py`
already exist). `distance.py`/`theoretical_latency_approximation.py` and the
`cdf.py`/`conflict.py` theoretical-optimum overlays need **no changes** — they
already compute a geography-based bound independent of whether the
underlying latency was emulated or real, so they become the natural
observed-vs-theoretical comparison for real WAN measurements.

### 6. Client/replica co-location — no conceptual change

`--network container:${nearby_database}` stays exactly as-is: client and
replica for DC *i* are still meant to run on the *same* real host. The only
fix is in `run_ycsb`/`start_container`'s call site: resolve the YCSB
container's Docker context from `${nearby_database}`'s name (its colocated
replica), not from the YCSB container's own name (which has no index of its
own in the registry).

### 7. Bind-mounts — the one non-dispatch proxy

`tiga/cluster.sh:70` and `run_benchmarks.sh:144` bind-mount local
files (`config-ycsb.yml`, the YCSB `.docker` env-file) into a container,
assuming shared filesystem with the Docker daemon. Add one
`stage_file(name, local_path)` function: no-op in simulated mode, `scp`
(or equivalent) to the target host in real mode. Used at these 2-3 call
sites only.

## Files touched (complete list, no new files beyond the registry columns)

- `latencies.csv` — add `host,ssh_user` columns.
- `exp.config` — add `deploy_mode=simulated|real`; set
  `latency_simulation=0` for real deployments (flag already exists).
- `utils.sh` — add `node_context`, `drun`/`dexec`/`dinspect`/`dlogs`/
  `dstop`/`dkill`, `stage_file`; make `get_container_ip` mode-aware.
- `cassandra/cluster.sh`, `cockroachdb/cluster.sh`,
  `swiftpaxos/cluster.sh`, `tiga/cluster.sh` — swap raw
  `docker <verb>` calls for `d*` wrappers; add `-p` publish flag per
  protocol's `_start_cluster`.
- `run_benchmarks.sh` — `start_network`/`stop_network` become
  per-host (create local bridge on each host once, idempotent, same call
  site); YCSB context resolution uses the nearby replica's name; use
  `stage_file` before the `config-ycsb.yml`/env-file bind-mounts.
- `fault_tolerance.sh`, `restore_tc.py` — swap raw `docker`/
  `docker.from_env()` for `d*`/`client_for`; no logic changes.
- `emulate_latency.py`, `cassandra/create_new_node.py`,
  `start_cassandra_cluster.py`,
  `cassandra/start_cassandra_data_centers.py`,
  `cassandra/cleanup_cassandra_cluster.py` — swap
  `docker.from_env()` for `client_for(name)`.

Not touched: any per-protocol create-keyspace/create-table logic, YCSB
workload invocation, `parse_ycsb_to_csv.sh`, fast-path/breakdown scripts, or
any plotting script (`cdf.py`, `conflict.py`, `closed_economy.py`, `ycsb.py`,
`swap.py`, etc.) — all consume `results/`/`logs/` files exactly as today.

## Infra-level items (not code, but required alongside the above)

- Provision N real hosts (VMs from any IaaS, or bare-metal), one per
  simulated DC, in genuinely distant locations; install Docker on each;
  create a Docker context per host from the orchestrator machine.
- Open the specific ports each protocol needs between hosts (Cassandra
  gossip/CQL, CockroachDB 26257, swiftpaxos's custom port, Tiga's RPC port)
  via cloud firewall/security groups, or put a lightweight mesh (WireGuard/
  Tailscale) across the hosts to avoid per-port-per-pair firewall
  management, especially if hosts span providers.
- `gcp.csv`-based container CPU/memory limits only make sense if targeting
  known cloud VM shapes — skip/adapt for bare-metal.

## Future option (not planned yet): Kubernetes rewrite

If the project later needs many more DCs, self-healing, or native scheduling,
`GCP/context.sh` (multi-cluster kubectl wrapper), `GCP/clusters.sh` (region
table), and `GCP/fed-start.sh`/`fed-stop.sh` (real multi-region GKE
provisioning) are reusable as a *pattern*. This would require re-deriving all
of `protocol-function.sh`'s readiness/leader-discovery/seed-wiring logic for
the 7 current protocols and solving cross-region pod networking (the legacy
harness's Redis-based approach was tied to `vcd`/`epaxos` telemetry, not
phase synchronization, and doesn't directly transfer). Worth a separate,
dedicated plan if/when this becomes the priority.

## Verification

1. **Simulated-mode regression:** with `deploy_mode=simulated` (registry
   `host` columns empty), run `cdf.sh --test` end-to-end and confirm
   identical behavior/output to today (all `d*` wrappers resolve to empty
   context → today's local `docker` calls).
2. **Two-host smoke test:** provision 2 cheap VMs in different regions,
   populate `latencies.csv`'s `host`/`ssh_user` columns, set
   `deploy_mode=real`, `latency_simulation=0`; run a 3-node
   `cdf.sh --test --protocols="accord"` (or `cassandra-paxos`, the simplest
   protocol) and confirm: containers land on the correct real hosts (`docker
   --context <ctx> ps` per host), replicas can reach each other on published
   ports, YCSB client colocated with its nearby replica completes a load+run
   cycle, and `results/cdf.pdf` is produced from real (not emulated) latency.
3. **Fault-injection check:** run `fault_tolerance.sh --test` against the
   2-host setup and confirm the slowdown/crash still land on the correct
   remote leader container and `restore_tc.py` correctly reverses the tc
   rule via the remote context.
4. Compare observed latency against `distance.py`'s haversine-based
   theoretical bound for the same two real locations, as a sanity check that
   real WAN latency roughly tracks geography.
