# Infrastructure providers

An *infra provider* tells the benchmark suite **where containers run**. The
default provider, `simulation.sh`, runs every container on the local Docker
daemon — the historical behavior of this repository. Other providers place one
container per remote machine, provisioned from an IaaS, and reach them through
SSH-backed Docker contexts.

The provider is selected in `exp.config`:

```
infra=simulation      # or gcp, aws, …
```

`utils.sh` sources `infra/${infra}.sh` and exposes the wrappers described below.
**No code outside `infra/` may call a provider tool** (`gcloud`, `aws`, `ssh`,
`scp`) or `docker` directly.

## Node indices

Nodes are numbered `1..N`, where

```
index = (dc - 1) * nodesperdc + k
```

`dc` is the 1-based row of `latencies.csv` (so `dc=1` is `Hanoi` by default) and
`k` is the 1-based node within that DC. Index **0** is reserved for the
*orchestrator* — the machine running the benchmark scripts. Containers that
belong to the orchestrator rather than to any node (the demo visualization) map
to 0, and `infra_context 0` must always return the local daemon.

`node_index_of <container-name>` (in `utils.sh`, mirrored in `infra.py`) maps a
container name to its index. Database containers are named `${city}${k}`
(`Hanoi1`, `NewYork2`), so the mapping is a reverse lookup on the city name —
the numeric suffix is the intra-DC index, *not* the node index.

## The registry

`latencies.csv` is the single source of truth. Columns `lat,lon,loc` describe
the simulated geography and are authored by hand. Columns
`region,host,ssh_user,net_device` describe the real machines:

| Column | Meaning |
| --- | --- |
| `region` | Provider zone closest to `loc`, authored by hand (e.g. `europe-west9-a`). |
| `host` | Routable address of the machine. **Empty means simulation.** Written by `infra_provision`. |
| `ssh_user` | SSH user for the Docker context. Written by `infra_provision`. |
| `net_device` | Primary interface for `tc` (`eth0` locally, `ens4` on GCE, `ens5` on EC2). |

Rows are indexed by DC. With `nodesperdc > 1`, every node of a DC shares that
DC's row, so a real provider must extend the registry (or key its machines on
the node index) before it can place more than one node per DC.

Read and write it with `registry_field <dc> <column>` and
`registry_set <dc> <column> <value>` from `utils.sh`.

## The contract

Every provider must define all of the following.

| Function | Contract |
| --- | --- |
| `infra_is_real` | Exit 0 if containers run on remote machines, 1 for the local daemon. This is the predicate every mode-dependent branch tests; it must be cheap, as it is called on each Docker operation. |
| `infra_provision <n>` | Create `n` machines through the IaaS — one per node, in the region of each node's registry row — wait for SSH, install Docker, and write `host`, `ssh_user` and `net_device` back into the registry. Must be idempotent. |
| `infra_teardown` | Destroy everything `infra_provision` created: machines, firewall rules, Docker contexts, and any `tc` state left on a host. Must clear the registry columns it wrote. |
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

- `infra_all_indices` — every provisioned node index (registry rows with a
  non-empty `host`).
- `infra_bootstrap <n>` — `infra_provision`, then `infra_open_ports` for the
  union of all protocol ports, then `pull_images` on each machine.
- `host_aliases` — `--add-host name:ip` for every container name in the
  deployment, for providers that drop Docker's embedded DNS.

## Adding a provider

1. Copy `simulation.sh` to `infra/<name>.sh` and implement the twelve functions.
2. Fill the `region` column of `latencies.csv` with zones for your provider.
3. Set `infra=<name>` and `latency_simulation=0` in `exp.config`.
4. Run `./cdf.sh --test --protocols=cassandra-paxos` against two machines
   first — it is the simplest protocol and exercises seeds, `get_container_ip`
   and the YCSB co-location in one pass.
