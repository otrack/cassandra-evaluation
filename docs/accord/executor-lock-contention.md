# Accord Contention Analysis (Calvin micro-benchmark)

This document records why Accord performed poorly in `calvin_ubench.sh` and how it was fixed. The investigation ran on 2026-10-03. It found that the Accord executor lock, not the protocol or the hardware, was the bottleneck. Raising `accord.queue_shard_count` from its default of 2 to 16 increased throughput about 4.5× and cut average latency about 4×.

---

## 1. Symptom

In the first sweep (CI ∈ {0.0001, 0.01}, 16 → 54 clients per DC, 3 DCs: Hanoi, Lyon, NewYork), Accord plateaued at about **120 tx/s per DC** at **both** contention indexes:

| Clients/DC | CI=0.0001 tput/DC | CI=0.0001 avg lat. | CI=0.01 tput/DC | CI=0.01 avg lat. |
| :---: | :---: | :---: | :---: | :---: |
| 16 | 53–83 | 178–280 ms | 98–115 | 131–154 ms |
| 24 | 107–122 | 185–210 ms | 116–121 | 186–194 ms |
| 36 | 122–129 | 261–278 ms | 110–123 | 276–307 ms |
| 54 | 77–121 | 416–592 ms | — | — |

Under the same setup, Tiga reached about 2,000–3,000 tx/s per DC at CI=0.0001. Some observations ruled out simple explanations:

- **Contention on hot records was not the cause.** The ceiling does not change between CI=0.0001 (10,000 hot records) and CI=0.01 (100 hot records).
- **The protocol path is fine at low load.** p1–p50 latency was 95–115 ms, one WAN round trip, as in the single-key and swap runs. The average was pulled up by the tail: p99 was 1.6–4 s.
- **Cost appeared to scale with keys per transaction.** Single-key YCSB-A peaks at about 1,200–1,300 ops/s per DC. A Calvin transaction touches 10 keys, and 120 tx/s × 10 keys gives roughly the same number of key-operations per second.
- **At 54 clients the system collapsed during the run.** Throughput fell to 12–16 tx/s, with 10 s timeouts. Node logs show `ACCORD_*_RSP` messages dropped after their timeout expired, `accord.coordinate.Timeout` errors, and `BEGIN_RECOVER` traffic.

---

## 2. Method

Each experiment reruns a single point, **CI=0.0001, 36 clients per DC**, with async-profiler attached to every replica:

```bash
CASSANDRA_PROFILER=1 \
CASSANDRA_JVM_OPTS="-Dcassandra.config.allow_system_properties=true -Dcassandra.settings.accord.<key>=<value>" \
./calvin_ubench.sh --protocols=accord --ci=0.0001 --clients=36
```

- **Applying settings for one run.** Cassandra applies any `-Dcassandra.settings.<yaml key>` property when `cassandra.config.allow_system_properties=true`. Each setting was confirmed in `system_views.settings` and in the startup log line `Detected JVM property cassandra.settings...`.
- **What the profiler records.**
  - The profiler options are `event=ctimer,interval=1ms,wall=10ms,lock=1ms`. The capture covers the 30 s warm-up and the 60 s measured run.
  - Each node's JFR file and `gc.log` land in `logs/profiles/calvin_ubench/`.
  - `docker stats` was sampled every ~2 s alongside.
- **Profiler overhead.** All profiled runs are slower than unprofiled ones, so compare runs with each other, not with unprofiled numbers.
- **Analysis.** The JFR files were analysed with `jfr print` and the scripts in [`jfr/`](jfr/):

| Script | Input (`jfr print --events ...`) | Output |
| :--- | :--- | :--- |
| `jfrcpu.py` | `jdk.ExecutionSample --stack-depth 200` | On-CPU samples by thread pool, top frames, and inclusive Accord/Cassandra frames. Wall-clock samples are excluded. |
| `excl.py` | `jdk.ExecutionSample --stack-depth 200` | Inclusive frames of the samples running in the executor's exclusive (locked) section. |
| `park.py` | `jdk.ThreadPark --stack-depth 12` | Total park time, grouped by thread pool and first non-JDK frame. |
| `spans.py` | `profiler.Span` | Duration statistics of the `DebugExecution` spans, per tag (`AccordTaskQueued`, `AccordExecutorCriticalSection`, per-key tasks, …). |

Each script takes a `HH:MM:SS.mmm` window, in the JFR's UTC time, and reads from stdin. For example:

```bash
jfr print --events jdk.ThreadPark --stack-depth 12 <node>.jfr | python3 park.py 07:11:07.000 07:12:10.000
```

`profiler.Span` events are kept only when a sample hits the span. That biases them towards long spans, so their percentiles are upper bounds. Use them to compare runs, not as absolute values.

---

## 3. Baseline: what Accord waits on (default configuration)

Run `20261003090407463065836`, profiled: **80–85 tx/s per DC**, average latency 406–430 ms, p99 about 2.1 s.

### 3.1 Not CPU, not GC

- **CPU.** Each Cassandra node used about 5–10 of its 16 cores, and the YCSB clients were nearly idle.
- **GC.** Generational ZGC runs 8–10 s concurrent cycles and collects 2–3 GB per minor cycle. The GC log nevertheless shows **no allocation stalls**, and safepoint pauses are negligible. GC is not what blocks transactions.

### 3.2 Executor-lock contention

| Signal (63 s window) | Hanoi | Lyon | NewYork |
| :--- | :---: | :---: | :---: |
| Executor threads parked on `AccordExecutor.lock` | 722 s | 998 s | 983 s |
| Messaging event loops parked on `AccordExecutor.lock` | 63 s | 90 s | 88 s |
| Wait in the executor queue (`AccordTaskQueued`) p50 / p99 | 181 ms / 2.5 s | 54 ms / 4.4 s | 53 ms / 3.3 s |
| Per-key task execution p50 / p99 | 0.27 / 3.4 ms | 0.28 / 3.5 ms | 0.29 / 3.5 ms |

- **Threads blocked on the lock.** On average, 11–16 of the 32 `AccordExecutor` threads per node are parked on the executor lock.
- **The default shard count is small.** The default `queue_shard_model` is `THREAD_POOL_PER_SHARD`. In that model `queue_shard_count` defaults to `max(1, cores/8)`, which is **2 shards** on a 16-core node, each with 16 threads (`AccordExecutor[0..1, 0..15]`). Every per-key task on a node therefore goes through one of only two locks.
- **The lock also blocks the network.** Messaging event loops take the same lock when they submit work, so about one Netty event-loop thread per node is blocked at all times. Incoming messages are delayed, which explains the expired `ACCORD_PRE_ACCEPT_RSP` and `ACCORD_APPLY_RSP` messages, the coordinator timeouts and the recovery traffic seen at 54 clients.
- **Tasks wait far longer than they run.** A task runs in under a millisecond but waits tens to hundreds of milliseconds in the queue. The queueing is the latency.

### 3.3 Work done while holding the lock (Hanoi)

About 40% of on-CPU samples run inside the exclusive section. The main items:

| Activity under the lock | Share of locked CPU samples |
| :--- | :---: |
| Preaccept / apply (`Commands.preaccept`, `Commands.apply`) | ~9% each |
| Dependency calculation (`SaferCommandStore.visitForKey` → `DepsCalculator`) | ~10% |
| Journal serialisation (`AccordJournal.saveCommand` → `CommandChangeWriter`) | ~10% |
| Updating `commands_for_key` (`SafeCommandStore.updateCommandsForKey`) | ~9% |
| Cache eviction (`AccordCache.shrinkOrEvict`) | ~8% |

Outside the lock, about 9% of all CPU went to loading `commands_for_key` back from SSTables (`CommandsForKeyAccessor.load`). With 1M uniformly chosen keys, a key's entry is almost never cached.

### 3.4 Why Calvin is hit harder than YCSB-A or swap

Each Calvin transaction touches 10 random keys out of 1M, so it creates about 10× the per-key work of a single-key transaction, and all of it passes through the same two locks per node. Most of those keys also miss the `commands_for_key` cache. Adding clients only lengthens the queue, so throughput stays flat while latency grows.

---

## 4. Experiments

All runs use CI=0.0001 with 36 clients per DC and the profiler on. The run IDs are the `accord_3_calvin_<id>` file prefixes in `logs/profiles/calvin_ubench/`.

| Configuration | Run ID | Tput/DC (tx/s) | Avg latency | p99 latency | Executor lock park (Hanoi) | Event-loop lock park (Hanoi) | Node CPU (of 16 cores) |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| Default (2 shards, cache ≈ 819 MiB) | `20261003090407463065836` | 80–85 | 406–430 ms | ~2.1 s | 722 s | 63 s | ~5–10 |
| `accord.cache_size=3072MiB` | `20261003094356880251515` | 60–67 | 503–565 ms | 3.4–3.7 s | 541 s | — | — |
| `accord.queue_shard_count=8` | `20261003100054826591431` | 263–372 | 93–131 ms | 0.68–0.91 s | 28 s | 11 s | ~11.6–13 |
| `accord.queue_shard_count=16` | `20261003122535505844995` | **286–396** | **87–120 ms** | **0.61–0.73 s** | **11 s** | **8 s** | ~12.4–14.6 |

### 4.1 Larger `commands_for_key` cache: worse

`accord.cache_size` defaults to 10% of the heap, which is about 819 MiB with the 8 GiB heap.

- **The cache itself worked.** At 3 GiB, loads of `commands_for_key` from disk fell from 8.5% to 2.8% of CPU, and eviction disappeared from the profile.
- **But the locked sections got longer.** Their p50 rose from 0.43 ms to 2.46 ms. Dependency calculation's share of locked CPU nearly doubled, from 10% to 19%, and queue wait went up with it.
- **Probable cause, not verified.** Per-key `commands_for_key` entries that stay cached are trimmed of old transactions less often, since the trimming happens when an entry is reloaded from disk. Entries therefore grow, and every dependency calculation scans more of them.
- **GC load rose.** Heap still in use after a minor GC went from 24–48% to 55–73%, and a major ZGC cycle took 15 s. There were still no allocation stalls.

Cache misses were a symptom, not the cause.

### 4.2 More executor shards: the fix

With `queue_shard_model=THREAD_POOL_PER_SHARD`, the total of 32 threads per node is split across the shards: 8 shards give 4 threads each, and 16 shards give 2 threads each. Each shard has its own lock.

- **8 shards: about 4× throughput.** Time parked on the lock fell about 25×, and the event loops are mostly no longer blocked. Latency drops to about one WAN round trip.
- **16 shards: about 7% more.** Lock parking becomes negligible, around 1% of the baseline.

---

## 5. Remaining limits (16 shards)

- **CPU-bound.** Nodes run at 78–91% of their 16 cores, and the YCSB clients are idle.
- **GC is the largest overhead.** ZGC workers take about 23% of on-CPU samples (young 16%, old 7%). The allocation rate, roughly a few MB per transaction for rows holding one INT, is the next thing to investigate, for example by adding `alloc` to `cassandra.profiler.options`.
- **Unexplained oscillation.** Throughput alternates between fast and slow 10 s intervals, about 270–540 tx/s for Lyon and NewYork.
- **Hanoi is slower.** Hanoi is consistently 25–30% below the other two DCs.
- **Overhead from the test configuration.**
  - The Cassandra JVM runs with assertions enabled (`-ea`), so Accord's invariant checks run on every operation.
  - The YCSB binding sends each transaction as an unprepared `SimpleStatement`, so a 20-statement `BEGIN TRANSACTION … COMMIT` is parsed on every call. CQL parsing was not prominent in the profiles.

---

## 6. Recommendations

1. **Make 16 shards the default for Accord runs**, by adding the following to `cassandra.jvm_opts` in `exp.config`:
   ```
   -Dcassandra.config.allow_system_properties=true -Dcassandra.settings.accord.queue_shard_count=16
   ```
   As a rule, size `queue_shard_count` so that each shard has about 2 threads, rather than relying on `cores/8`.
2. **Rerun the full `calvin_ubench.sh` sweep without the profiler**, to obtain the actual Pareto front. The 36-client point is no longer saturated.
3. **Rerun the other Accord experiments with the same setting** (latency/throughput, swap, closed economy, conflict…), so that comparisons across systems stay fair.
4. **Leave `accord.cache_size` at its default.**

---

## 7. Notes for reproducing

- **The script deletes earlier logs.** `calvin_ubench.sh` runs `rm -f logs/calvin_ubench/*${p}*` and rebuilds `results/calvin_ubench.*` from whatever `.dat` files remain. Back up both before running a single-point experiment, and restore them afterwards. After each run above, its `.dat` files were moved to `logs/profiles/calvin_ubench/ycsb/` and the previous sweep was restored.
- **The data is local to this machine.** `logs/` and `results/` are git-ignored, so the JFR captures, GC logs and YCSB outputs referenced here exist only on the machine that ran them.
- **Timestamps differ between files.** JFR and `gc.log` timestamps are UTC, while YCSB `.dat` timestamps follow the client container's clock.
