# Accord Latency/Throughput Scaling Analysis

This document follows on from [executor-lock-contention.md](executor-lock-contention.md). That analysis fixed Accord's executor-lock bottleneck (`accord.queue_shard_count`). This one follows `latency_throughput.sh` as successive bottlenecks were removed, on 2026-10-03. Each step names the bottleneck found, the evidence, and the fix or the hypothesis it ruled out. Section 9 lists the best settings so far and the changes needed to apply them.

All runs use 3 nodes (Hanoi, Lyon, NewYork, one per DC, `e2-highcpu-16` shape) and YCSB `ConflictWorkload` with θ=0.001, update-only, 100k records. They run in the local simulation with emulated WAN delays: Hanoi↔Lyon 44 ms, Hanoi↔NewYork 64 ms and Lyon↔NewYork 30 ms one way. Clients run in closed loop: each YCSB thread waits for its previous operation before sending the next.

---

## 1. Summary

| Step | Configuration | Peak observed | What limited it |
| :--- | :--- | :--- | :--- |
| Earlier baseline | 5 nodes, default config (2026-09-17) | 5,796 ops/s at 275 clients/DC | (5 nodes, not comparable) |
| §2 | 3 nodes, `queue_shard_count=16` | 5,184 ops/s at 183 clients/DC | CQL request threads (§6) |
| §7 | + `native_transport_max_threads=1024` | 6,903 ops/s at 183, no errors | Container memory at 275 (§8) |
| §8 | + swap disabled in the containers | 7,889 ops/s at 275, with ~150 failures and a Hanoi stall | Container memory: page cache (§8.2) |

Things investigated and ruled out as limits at this load, in order:
- CPU and lock contention in the executors;
- netem queue drops;
- TCP windows, buffers and Cassandra's internode queues;
- Accord's fast/slow paths and waits for dependencies;
- GC pauses.

---

## 2. Sweep with 16 executor shards

`latency_throughput.sh --protocols=accord`, with `CASSANDRA_JVM_OPTS="-Dcassandra.config.allow_system_properties=true -Dcassandra.settings.accord.queue_shard_count=16"`:

| Clients/DC | Total ops/s | Avg latency | Hanoi / Lyon / NewYork ops/s |
| ---: | ---: | ---: | :--- |
| 16 | 665 | 71 ms | 170 / 248 / 247 |
| 36 | 1,493 | 72 ms | 381 / 557 / 555 |
| 81 | 3,338 | 72 ms | 853 / 1,243 / 1,242 |
| 122 | 4,924 | 74 ms | 1,239 / 1,841 / 1,844 |
| **183** | **5,184** | 105 ms | 1,306 / 1,933 / 1,945 |
| 275 | 5,097 | 161 ms | 1,277 / 1,916 / 1,904 |

- **Latency is flat until 122 clients.** Lyon and NewYork stay at about 62 ms and Hanoi at about 90 ms, one round trip to the nearest quorum.
- **Then throughput plateaus.** At that point:
  - the Cassandra nodes use only 7–8.4 of their 16 cores, and the host is 74% idle;
  - there's no iowait;
  - node memory reaches 12.2–12.3 GiB of the 12.8 GiB container limit.

For comparison, Tiga at 3 nodes keeps about 110 ms of latency up to 15,165 ops/s, at 620 clients/DC.

---

## 3. CPU profile at 183 clients: no saturated thread, no lock contention

The run was profiled with async-profiler (`CASSANDRA_PROFILER=1`). Single points were run outside the sweep with a helper that calls `run_benchmark` with the sweep's arguments and writes to `logs/latency_throughput_profile/`.

- **Lock parking is gone.** Time parked on `AccordExecutor.lock` is about 1 s per minute, against 722–998 s in the Calvin baseline. Queue wait has a 2–3 ms median.
- **No thread is saturated.** From wall-clock samples:
  - executor threads are runnable at most about 20% of the time;
  - the 4 busy `Messaging-EventLoop` threads, the internode links, about 35–40%.
- **Flame graphs** (built with `jfr/collapse.py` and async-profiler 2.8.3's `FlameGraph`) are in `logs/profiles/latency_throughput_profile/flame/{Lyon1,Hanoi1}_{cpu,wall}.html`.
  - Wall-clock time: `AccordExecutor` threads spend about 86% of it idle in `AccordExecutorSyncSubmit.awaitExclusive`, called from the worker loop. `Messaging-EventLoop` threads are about 92% idle.
  - Executor CPU is spread over many components. Journal saves take 12–14%, the local YCSB row read 8–11%, park/wake in the SYNC submission model about 10%, PreAccept 7–9%, executor bookkeeping about 8%, cache maintenance about 7%, and cache misses 5–6%.

## 4. Emulated WAN: netem queue limit (fixed, but not the bottleneck)

- **The emulator drops packets above its default queue size.** `emulate_latency.py` installed `netem delay` with no `limit`, which means a 1,000-packet queue per link. At 183 clients, links carry about 20–25k packets/s (about 20 MB/s). The 44 ms and 64 ms links held about 800–1,400 packets and dropped a few hundred of them (`netem_stats.sh`).
- **Fix:** `limit 100000` on every netem qdisc (`NETEM_LIMIT` in `emulate_latency.py`). Drops went to 0 and Lyon/NewYork p99 improved slightly (154/177 → 130/119 ms), but **throughput stayed the same** (5,283 → 5,254 ops/s).

## 5. Internode TCP (`ss -ti`, `nstat`): not the bottleneck

Every busy internode connection is **`app_limited`**, with almost nothing waiting to be sent (`notsent` 0–34 KB):
- the congestion window is 1,000–2,300 segments;
- the send buffer never limits a connection (no `sndbuf_limited`);
- the receive window limits it ≤0.3% of the time.

Retransmissions are 0.005–0.006% of segments and are spurious (duplicate-ACK duplicates caused by reordering), with no timeouts. Cassandra simply had nothing more to send.

## 6. Accord's dependency path and metrics: the protocol isn't the bottleneck

### 6.1 Code reading
The code was read at the Accord commit pinned by the `benchmarking` branch of `~/cassandra` (`modules/accord` @ `446b2504`):

- **A replica executes a transaction only after its dependencies are applied.** On `Stable`, the replica builds `WaitingOn` from the transaction's dependencies (`Commands.commit` → `maybeExecute`). It executes only once every dependency that executes earlier has been applied locally.
- **Blocked transactions wait passively.** They register a listener (`NotifyWaitingOn`); the progress log steps in only after `fetch_txn = 2s*attempts`.
- **The client is answered before the write is applied.** On the replica fast path, the result is reported before `applyDirect` (`replicaExecuteFastApply`), and `Persist` calls the client callback immediately.
- **`ConflictWorkload` rarely creates conflicts between clients.** A thread writes its own key with probability 1−θ and a single shared key with probability θ.

### 6.2 Metrics
`AccordCoordinatorMetrics`, `AccordReplicaMetrics` and `AccordExecutorMetrics` were read through `nodetool sjk mxdump`, at 81 vs 183 clients (Lyon):

| | 81 clients | 183 clients |
| :--- | ---: | ---: |
| Fast / medium / slow paths | 65,783 / 5 / 0 | 103,287 / 3 / 0 |
| Coordinator PreAccept, mean | 68.5 ms | 70.5 ms |
| Coordinator execution done, mean | 74.2 ms | 74.3 ms |
| Executor wait-to-run, mean | 2.7 ms | 1.9 ms |
| Client-observed avg latency | 63 ms | 90 ms |

Accord's own latency barely changes while the client's rises by about 27 ms. The extra time is added **before** Accord starts coordinating.

## 7. The bottleneck: CQL request threads

- **Each Accord transaction holds a CQL request thread until it completes.** `TransactionStatement.execute` calls `AccordService.coordinate(...)`, which is `coordinateAsync(...).awaitAndGet()`.
- **There are only 128 such threads.** `native_transport_max_threads` defaults to 128. With 183 clients per node, 55 requests wait for a thread. That wait happens before the `txnId` is assigned, so Accord's metrics don't include it.
- **The numbers match.** By Little's law, 128 threads ÷ ~66 ms ≈ 1,940 tx/s for Lyon and NewYork (observed 1,946/1,951), and 128 ÷ ~91 ms ≈ 1,400 for Hanoi (observed 1,337). The predicted queueing delay is about 28 ms, against 27 ms observed.

**Fix:** `native_transport_max_threads=1024`.

| 183 clients/DC | Hanoi | Lyon | NewYork | Total |
| :--- | :--- | :--- | :--- | ---: |
| 128 threads | 1,337/s · 131 ms | 1,946/s · 90 ms | 1,951/s · 90 ms | 5,234 |
| **1024 threads** | **1,846/s · 95 ms** | **2,558/s · 69 ms** | **2,499/s · 70 ms** | **6,903** |

## 8. Next limit: container memory (275 clients, 1024 threads)

### 8.1 With swap allowed (default)
- **Results:** 6,288 ops/s. Hanoi collapses to 86 ops/s with 3.2 s latency for about 20 s. There are 1,210 timeouts and about 2,400 invalidations across the nodes. Logs show `Preempted`, `Exhausted` and `Recover`.
- **Memory:** every container reaches its `memory.max` of 12.8 GiB (80% of the 16 GB shape). The kernel reclaims continuously and swaps (Docker allows `memory.swap.max` = the memory limit). Memory pressure reaches 11.7% on Lyon, and host iowait 12–38%.
- **Not GC:** no allocation stalls, and heap occupancy stays at 30–63%.
- **How memory is used:** ZGC maps its 8 GiB heap as shared memory, so cgroups count it as "file" memory. Heap, about 2.9 GiB of native memory and a growing page cache together reach the limit.

### 8.2 With swap disabled (`memswap_limit = mem_limit`)

| 275 clients/DC | Hanoi | Lyon | NewYork | Total |
| :--- | :--- | :--- | :--- | ---: |
| Swap allowed | 628/s · 378 ms | 2,925/s · 89 ms | 2,735/s · 90 ms | 6,288 |
| **Swap disabled** | 917/s · 260 ms | **3,509/s · 75 ms** | **3,463/s · 73 ms** | **7,889** |

- **Lyon and NewYork recover.** Memory pressure falls to ≤3.9%, and swap stays at 0.
- **Hanoi still collapses the moment its container reaches the limit.** At 16:07:47 the kernel drops about 1 GiB of file pages in one go, and Hanoi runs at about 570/s for 30 s. Its replica `Stable` p99 is still about 4 s, with 163 timeouts and 50 invalidations.
- **Lyon and NewYork degrade in the last 20 s.** The host then reads 56–108 MB/s back from disk with 9–12% iowait: the page cache, now capped, evicts SSTables and journal segments that are still needed.

**Open:** whether a smaller heap (it is at most 63% used) or a larger container limit removes the stalls at 275 clients.

---

## 9. Best Accord settings so far, and how to apply them

| Setting | Value | Applies to | Section |
| :--- | :--- | :--- | :--- |
| `accord.queue_shard_count` | 16 (default `max(1, cores/8)` = 2) | Accord | [executor-lock-contention](executor-lock-contention.md) |
| `native_transport_max_threads` | 1024 (default 128) | Every Cassandra-based protocol (accord and Paxos block a thread per request) | §7 |
| Container swap | disabled (`memswap_limit = mem_limit`) | Every Cassandra container | §8 |
| netem queue `limit` | 100000 (default 1000) | Every protocol (emulator) | §4 |
| `accord.cache_size` | default (10% of heap); 3 GiB made things worse | Accord | [executor-lock-contention](executor-lock-contention.md) |

Changes to the scripts:

1. **`emulate_latency.py`** (done): `NETEM_LIMIT = 100000`, appended as `limit {NETEM_LIMIT}` to both `netem delay` commands.
2. **`cassandra/start_cassandra_data_centers.py`** (done): `run_kwargs['memswap_limit'] = mem_limit` next to `mem_limit`.
3. **`exp.config`** (done): the generic Cassandra flags, for every Cassandra-based protocol:
   ```
   cassandra.jvm_opts=-Dcassandra.config.allow_system_properties=true -Dcassandra.settings.native_transport_max_threads=1024
   ```
4. **Accord-only flags** (done):
   - `accord.jvm_opts=-Dcassandra.settings.accord.queue_shard_count=16` in `exp.config`;
   - in `start_cassandra_data_centers.py`, `accord_jvm_opts(protocol)` appends it after `cassandra.jvm_opts` for `accord` nodes only. `ACCORD_JVM_OPTS` in the environment overrides it for one run;
   - `create_cassandra_cluster` now receives the protocol.
   
   Checked on one-node clusters started through `cassandra_start_cluster`. Paxos nodes get 1024 native-transport threads and no `accord.*` option. Accord nodes get 1024 threads and 16 queue shards. Both have swap disabled.
5. **Rerun the affected experiments** (to do): all accord sweeps, and the Paxos runs as well, since `native_transport_max_threads` and the swap/netem changes affect them too.

---

## 10. Artifacts

| What | Where |
| :--- | :--- |
| Sweep with 16 shards (3 nodes) | `logs/latency_throughput/`, `results/latency_throughput.*` |
| Single-point runs (profiled, netem, metrics) | `logs/latency_throughput_profile/` |
| JFR captures, GC logs, flame graphs | `logs/profiles/latency_throughput_profile/` |
| Analysis scripts | `docs/analysis/accord/jfr/` (`jfrcpu.py`, `excl.py`, `park.py`, `spans.py`, `collapse.py`) |
| `ss`/`nstat` captures, metric dumps, cgroup/vmstat/GC samples | Session scratchpad only, not kept |

`logs/` and `results/` are git-ignored and exist only on the machine that ran the experiments.
