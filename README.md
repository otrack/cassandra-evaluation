This repository contains a set of scripts to benchmark Cassandra replication protocols with the Yahoo! Cloud Serving Benchmark (YCSB).
It also offers a way to compare it against
- the [SwiftPaxos](https://github.com/imdea-software/swiftpaxos) library which implements a basic replicated key-value store several state-of-the-art protocols such as Paxos, Egalitarian Paxos, and SwiftPaxos; and 
- the [cockraochDB](https://github.com/cockroachdb/cockroach) distributed data store.

## Overview 

The benchmark suite uses the following repos:
- [YCSB](https://github.com/otrack/YCSB) (`cassandra5` branch)
- [Apache Cassandra](https://github.com/otrack/cassandra/tree/testing6) (`testing6` branch)
- [Cassandra Docker Library](https://github.com/otrack/cassandra-docker-library) 
- [SwiftPaxos](https://github.com/imdea-software/swiftpaxos) (`container` branch)
- [CockroachDB](https://github.com/otrack/cockroachdb) (`master` branch)

There are two implementations of the benchmarks, one for Docker and another for Google Cloud Platform (GCP).
In what follows, we detail the instruction for the Docker implementation.

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
ant artifacts -Dant.gen-doc.skip=true -Dcheckstyle.skip=true
```

### Cassandra Docker Library
``` bash
git clone https://github.com/otrack/cassandra-docker-library/
cd cassandra-docker-library/5.1-accord
cp path/to/cassandra/build/apache-cassandra-5.1-SNAPSHOT-bin.tar.gz ./cassandra-bin.tgz 
docker build -t user/cassandra-accord:latest .
```

## Benchmarking

As mentioned before, there are two possible environments
- `Docker` uses containers 
- `GCP` uses Google Cloud Platform

Below, we explain how to use the Docker one.

### Docker

At a high-level, the benchmark creates a set of replicas and clients.
These are spread across several locations to simulate datacenters.
Currently, there is one data replica and one client per datacenter.
WAN is simulated thanks to the Linux traffic shapping tool (tc).
Replicas and clients are running in Docker containers.
The client share the same network interface as the nearby replica.

There are several benchmarks:
- `cdf.sh` computes the CDF of the latency distribution at one replica across several (standard) YCSB workloads.
- `conflict.sh` is plotting the average latency across all clients when changing a fixed conflict rate for updates.
- `closed_economy.sh` runs a closed economy workload (banking transactions) on transaction-supporting protocols.
- `latency_throughput.sh` generates a classical latency vs throughput graph by increasing the number of clients by a factor of 2 until a hockey stick effect is observed (where both latency and throughput degrade).
The results of the benchmarks are PDF plots created under `results/`.
The logs of a benchmark execution are created under `logs/`.
Please be careful that any new invocation of a benchmark cleans up the logs of the previous runs.

Before executing the benchmarks, you will need to fix the configuration parameters that are defined in the file `exp.config`.
