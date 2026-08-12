This repository contains a set of scripts to benchmark Cassandra replication protocols with the Yahoo! Cloud Serving Benchmark (YCSB).
It also offers a way to compare it against
- the [SwiftPaxos](https://github.com/imdea-software/swiftpaxos) library which implements a basic replicated key-value store several state-of-the-art protocols such as Paxos, Egalitarian Paxos, and SwiftPaxos;
- the [cockraochDB](https://github.com/cockroachdb/cockroach) distributed data store; and
- the Tiga suite, which bundles the Tiga, Calvin, Detock and Janus transactional protocols.

## Overview

The benchmark suite uses the following repos:
- [YCSB](https://github.com/otrack/YCSB) (`cassandra5` branch)
- [Apache Cassandra](https://github.com/otrack/cassandra/tree/testing6) (`testing6` branch)
- [Cassandra Docker Library](https://github.com/otrack/cassandra-docker-library)
- [SwiftPaxos](https://github.com/imdea-software/swiftpaxos) (`container` branch)
- [CockroachDB](https://github.com/otrack/cockroachdb) (`master` branch)

Everything runs as Docker containers, and all the images are published on Docker Hub under the
`0track/` namespace (see `exp.config`).
The `pull_images` step of every experiment fetches them, so the build instructions below can be skipped.

## Requirements

- Docker (the scripts run `docker` without `sudo`; containers are started with `NET_ADMIN`/`NET_RAW` so that latency can be emulated)
- Python 3 with `docker`, `pandas`, `numpy`, `matplotlib` and `pyyaml`
- A LaTeX distribution providing `pdflatex` and `pgfplots` (the plots are compiled from generated TikZ code)
- Java 11+, only if you want to rebuild the artifacts yourself

## Building artifacts (can be skipped)

The instructions that follow work for Java 11+.

### YCSB
``` bash
git clone --single-branch -b cassandra5 https://github.com/otrack/YCSB
cd YCSB
./bin/image.sh cassandra-cql swiftpaxos
```

### SwiftPaxos
``` bash
git clone --single-branch -b container https://github.com/imdea-software/swiftpaxos
cd swiftpaxos
./bin/image.sh
```

### Cassandra
``` bash
git clone https://github.com/otrack/cassandra/
cd cassandra
git checkout testing6
ant artifacts -Dant.gen-doc.skip=true -Dno-checkstyle=true
```

### Cassandra Docker Library
``` bash
git clone https://github.com/otrack/cassandra-docker-library/
cd cassandra-docker-library/5.1-accord
cp path/to/cassandra/build/apache-cassandra-5.1-SNAPSHOT-bin.tar.gz ./cassandra-bin.tgz 
docker build -t user/cassandra-accord:latest .
```

## Benchmarking

At a high-level, the benchmark creates a set of replicas and clients.
These are spread across several locations to simulate datacenters.
By default there is one data replica and one client per datacenter; this can be changed with the
`nodesperdc` parameter (SwiftPaxos only supports `nodesperdc=1`).
The locations are taken, in order, from `latencies.csv`; the WAN delay between two of them is
derived from their coordinates and enforced with the Linux traffic shapping tool (tc).
Replicas and clients are running in Docker containers.
The client share the same network interface as the nearby replica.

### Quick start

``` bash
# a single experiment, in a reduced setting that fits the local machine
./cdf.sh --test

# every experiment, one after the other (--test is passed by default)
./run-all.sh

# redraw the plots from the data already present under logs/
./run-all.sh --dry-run
```

Before executing the benchmarks in a full setting, you will need to fix the configuration parameters
that are defined in the file `exp.config`:

| Parameter | Meaning |
| --- | --- |
| `debug` | Print the debug traces of the scripts. |
| `*_image` | The Docker image used for each system; all of them are pulled before an experiment starts. |
| `network_name` | The Docker bridge network the containers are attached to. |
| `latency_simulation` | Enable the emulation of the WAN delays with tc. |
| `machine` | The GCP machine type whose CPU/memory limits are applied to each container (see `gcp.csv`). |
| `records` / `threads` / `maxexecutiontime` | The YCSB record count, client threads and duration of a run (in seconds). |
| `nodesperdc` | The number of replicas per datacenter. |
| `accord.*` / `cockroachdb.*` | Per-system tuning knobs (e.g., ephemeral reads, lease holder placement). |

### Experiments

Every experiment is a `<name>.sh` script that runs the benchmark and a `<name>.py` script that turns
the results into a plot or a table.

| Experiment | Description |
| --- | --- |
| `cdf.sh` | Computes the CDF of the latency distribution at one replica across several (standard) YCSB workloads. |
| `ycsb.sh` | Compares the average latency of each protocol over the YCSB workloads A to D, as a grouped bar chart. |
| `conflict.sh` | Plots the average latency across all clients when changing a fixed conflict rate for updates. |
| `closed_economy.sh` | Runs a closed economy workload (banking transactions) on transaction-supporting protocols, varying the number of nodes. |
| `swap.sh` | Runs a workload that atomically swaps S items per transaction, with S varying from 1 to 8, for 1 and 50 clients per site. |
| `latency_throughput.sh` | Generates a classical latency vs throughput graph by increasing the number of clients by a factor of 2 (1, 2, 4, 8, ..., up to 128) to demonstrate the hockey stick effect (where latency increases and throughput plateaus/degrades as the system saturates). |
| `fault_tolerance.sh` | Injects a 400ms slowdown then a crash on the first replica, and plots the throughput over time (mimics Figure 6 of the CockroachDB SIGMOD'20 paper). |
| `ephemeral.sh` | Illustrates the benefit of activating ephemeral reads in Accord, as a LaTeX table of the speed-up over workloads A to D. |

`run-all.sh` executes all of them in sequence and stops at the first failure.

Each experiment accepts the following flags:
- `--test` shortens the run and right-sizes the containers so that the experiment fits on the local machine.
- `--dry-run` skips the run and only redraws the plot from the data already available under `logs/`.
- `--protocols=LIST` overrides the (comma-separated) list of protocols to evaluate.

The protocols are listed in `protocols.csv`, together with the color and the name used for them in
the plots. Not all of them are meaningful for every experiment: the transactional ones
(`closed_economy.sh`, `swap.sh`) only run Accord and CockroachDB, and `ephemeral.sh` only runs Accord.

The results of the benchmarks are PDF plots created under `results/`.
The logs of a benchmark execution are created under `logs/<experiment>/`.
Please be careful that any new invocation of a benchmark cleans up the logs of the previous runs.

### Adding a system

Each system lives in its own directory (`cassandra/`, `cockroachdb/`, `swiftpaxos/`, `tiga/`) and
exposes the same interface to `run_benchmarks.sh`, namely a `cluster.sh` defining
`<system>_start_cluster`, `<system>_get_hosts`, `<system>_get_port`, `<system>_get_node_count` and
`<system>_cleanup_cluster`, plus a `<system>_fast_path.sh` script reporting the ratio of operations
that took the fast, medium and slow paths.
Adding a system amounts to providing these, then registering its protocols in `protocols.csv`.

## Demo

`demo.sh` runs the closed economy workload on Accord and streams the messages exchanged by the
replicas to a live visualization (a world map with the message flow and the account balances)
served on `http://localhost:3000`.
The web application is under `live-viz/`; see [docs/demo.md](docs/demo.md) for the details.
