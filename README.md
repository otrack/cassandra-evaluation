This repository contains a set of scripts to benchmark Apache Cassandra 5 with the Yahoo! Cloud Serving Benchmark (YCSB).
Below, we provide a brief how-to guide.

## Overview 

The benchmark suite uses the following forks:
- [YCSB](https://github.com/otrack/YCSB) (`cassandra5` branch)
- [Apache Cassandra](https://github.com/otrack/cassandra/tree/testing5) (`testing5` branch)
- [Cassandra Docker Library](https://github.com/otrack/cassandra-docker-library) 

## Building artifacts

### YCSB

``` bash
git clone https://github.com/otrack/YCSB
cd YCSB
git checkout cassandra5
mvn -pl site.ycsb:cassandra-binding -am clean install
```

### Cassandra
``` bash
git clone https://github.com/otrack/cassandra/
cd cassandra
git checkout testing5
ant artifacts -Dant.gen-doc.skip=true
```

### Cassandra Docker Library
``` bash
git clone https://github.com/otrack/cassandra-docker-library/
cd cassandra-docker-library/5.1-accord
cp path/to/cassandra/build/apache-cassandra-5.1-SNAPSHOT-bin.tar.gz ./cassandra-bin.tgz 
docker build -t user/cassandra-accord:latest .
```

## Benchmarking

There are two possible environments
- `Docker` uses containers 
- `GCP` uses Google Cloud Platform

Below, we explain how to use the docker one.

### Docker

The benchmark relies on containers to emulate a geo-distributed system.
The structure of the `Docker`directory is as follows:
- `start_cassandra_data_centers.py` deploys a geo-distributed system (one Cassandra node per DC site)
- `load_ycsb.sh` executes the load phase of YCSB (insert data in Cassandra)
- `run_ycsb.sh` executes the run phase of YCSB (do CRUD operations against Cassandra)
- `run_benchmark.sh` deploys the Cassandra instances, inject latency in the system load YCSB then run it
- `run_all.sh` invokes the previous script

Typically, a call to `run_benchmark.sh`is of the following form:

	./run_benchmarks.sh SERIAL 1 accord 3 3 c 1 100

This means that 
- `SERIAL` we benchmark Accord 
- `1` a single client thread is used
- `accord` the Accord image is used (more on this below)
- `3` the system starts with 3 Cassandra nodes
- `3` it ends with 3 Cassandra nodes (no new node is added after a round of experiments)
- `c` the benchark runs workload c in YCSB (a read-only workload) 
- `1` there is a single item in the dataset
- `100` the client(s) execute in the run phase 100 operations

#### Running the benchmark

Edit `ACCORD_CASSANDRA_IMAGE` in `start_cassandra_data_centers.py` to indicate the name of your Docker image (e.g., `user/cassandra-accord:latest`)
Launch the benchmark with `run_all.sh`.

### Going further

Instead of Accord, it is possible to use the two other replication protocols in Cassandra (namely, QUORUM and LOCAL).
For this, one needs to compile a "standard" version of Cassandra.
This requires the following steps:
- edit `conf/cassandra.yaml` and set `enabled` in the `accord` section to `false`
- recompile Cassandra with ant
- create a new Docker image, e.g., after copying the tarball, `docker build -t user/cassandra:latest .`
- change the `NORMAL_CASSANDRA_IMAGE` in `start_cassandra_data_centers.py` appropriately
