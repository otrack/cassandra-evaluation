# Accord CPU Cost on the Calvin Micro-benchmark (Low Contention)

This document follows on from [executor-lock-contention.md](executor-lock-contention.md) and [latency-throughput-scaling.md](latency-throughput-scaling.md). With the executor-lock and CQL-thread limits removed, the question is: **what still makes Accord slow on `calvin_ubench.sh` at low contention (CI=0.0001)?**

The investigation ran on 2026-10-03/04, and §8 compares the results with high contention (CI=0.01). It used the sources of the image under test: `~/cassandra` (branch `benchmarking`, commit `1b7ebc1d52`), Accord at `446b2504`, and the image built from `cassandra-docker-library/7.0-accord`.

## 1. Summary

- **Accord is CPU-bound, not latency- or contention-bound.**
  - Peak throughput is about **1,210 tx/s in total**, 405 per DC, at 54 clients/DC.
  - Nodes burn **~11–12 ms of CPU per transaction on each replica**.
  - Each node replicates every key, so the 16 cores of a node cap the whole cluster near 16 / 11.5 ms ≈ 1,400 tx/s.
  - Latency stays at one WAN round trip until the cores saturate.
- **The cost grows with the command stores a transaction touches**, not just with its keys.
  - A 1-key transaction costs 2.3 ms; a 10-key one costs 11.8 ms.
  - The data fit ~1.0 ms per transaction plus ~1.5 ms per command store touched. A 10-key transaction touches 7.6 of the 16 stores on average.
- **Found a bug: Accord reloads `commands_for_key` about 85 times per transaction per node, for 10 keys.**
  - `Apply`'s synchronous path submits, in every command store, a continuation whose context declares *all* the transaction's keys.
  - The keys the store does not own load as empty, are evicted on release, and reload for every such task.
  - A one-line slice of the context (§6) removes ~85% of these reads. The fixed build reaches **1,385 tx/s at 121 clients/DC (+14%)**, and no longer collapses at 81 clients.
- **GC is the largest single cost, at 26–28% of CPU.** Generational ZGC is the image's default. Switching to G1 cuts CPU per transaction by ~11–20% and raises the stock image's peak from 1,214 to 1,547 tx/s (+27%), with a lower p99 (§7).
- **Both fixes together** reach **1,764 tx/s (+45%)** at ~7.7 ms of CPU per transaction per replica.
- **High contention (CI=0.01) is limited by something else (§8).**
  - It plateaus near 1,150 tx/s while the nodes still have spare CPU, probably because hot-key transactions must apply one after the other.
  - With ZGC, the two peaks happen to be close, which is why low contention did not seem to scale better.
- **Ruled out:**
  - JVM assertions (`-ea`);
  - the chunk cache (`file_cache_enabled`);
  - a larger Accord cache (`accord.cache_size=3GiB`);
  - fewer or more command stores (fewer is much worse, 32 is +8%).

| Configuration | Peak observed | CPU per txn per node (36 clients) |
| :--- | :--- | :---: |
| Stock image, ZGC (16 shards, 1024 CQL threads) | 1,214 tx/s at 54 clients; collapses to 806 at 81 | ~11.8–12.2 ms |
| ApplyLink patch, ZGC | 1,385 tx/s at 121 clients; 1,344 at 182 | ~10.8 ms |
| Stock image, G1 | 1,547 tx/s at 81 clients; 1,526 at 121 | ~10.7 ms |
| **ApplyLink patch, G1** | **1,764 tx/s at 121 clients (+45%)**; 1,661 at 182 | ~8.2 ms (at 54 clients) |

---

## 2. Setup and method

- **Cluster.** `calvin_ubench.sh --protocols=accord --ci=0.0001`: 3 DCs (Hanoi, Lyon, NewYork), one `e2-highcpu-16` node each (16 cores, 8 GiB heap, 12.8 GiB container), RF=3, 1M records.
  - Each transaction reads 10 keys, one hot (10,000 hot keys) and nine cold, and then updates all ten.
  - The YCSB client sends one `BEGIN TRANSACTION … COMMIT` with 10 `LET` reads, an `IF` and 10 `UPDATE`s.
- **Defaults.** All runs use the settings committed in `exp.config` (`accord.queue_shard_count=16`, `native_transport_max_threads=1024`, swap off), unless stated otherwise.
- **CPU per transaction.** `jfr/cpu_loop.sh` samples each node's cgroup `cpu.stat` every 5 s. `jfr/cpu_per_txn.py` divides each node's CPU over the last 60 s of the run by the total throughput.
  - All the numbers in this document are unprofiled unless stated otherwise.
  - Run-to-run noise is about ±5%.
- **Profiles.** async-profiler through `CASSANDRA_PROFILER=1` with these options:
  - `event=ctimer,interval=1ms` for CPU, categorised by `jfr/categories.py`;
  - `alloc=1m` for allocation, aggregated by `jfr/alloc.py`;
  - method instrumentation (`event=<Class.method>,interval=20`) to count and attribute calls.
- **Accord metrics.** Dumped with `nodetool sjk mxdump` every 15 s. Table read counts come from `nodetool tablestats system_accord ycsb` every 30 s. The live heap was inspected with `jcmd GC.class_histogram`.

---

## 3. Baseline: CPU-bound at ~12 ms per transaction

Sweep at CI=0.0001 with the stock image:

| Clients/DC | Total tx/s | Avg latency (Hanoi / Lyon / NY) | Node CPU (of 16) | CPU/txn per node |
| ---: | ---: | :--- | :--- | :---: |
| 36 | 1,112 | 117 / 83 / 85 ms | 12.3 / 13.2 / 13.2 | 11.0–11.8 ms |
| **54** | **1,214** | 133 / 116 / 125 ms | 13.6 / 14.0 / 13.8 | 11.2–11.6 ms |
| 81 | 806 | 1,299 / 176 / 178 ms | — | — |

- **Throughput hits a CPU wall.** At 54 clients the nodes use 14 of 16 cores; at 81 the system degrades, with Hanoi's latency reaching 1.3 s.
- **For comparison, Tiga reaches 2,000–3,000 tx/s per DC** under the same setup.
- **Cost per transaction is the measure of interest.** Below saturation, at 36 clients, throughput is set by the closed loop (36 clients / ~100 ms); every configuration below is therefore compared on CPU per transaction at 36 clients.

---

## 4. What does not help

All runs at 36 clients/DC, stock image, one parameter changed at a time:

| Change | Total tx/s | CPU/txn (Lyon) | Verdict |
| :--- | ---: | ---: | :--- |
| Baseline (two runs) | 1,094–1,112 | 11.8–12.2 ms | — |
| `-da` (no assertions; the image runs with `-ea`) | 1,084 | 12.1 ms | no effect |
| `file_cache_enabled=true` (chunk cache) | 1,005 | 12.0 ms | no effect |
| `accord.cache_size=3072MiB` (default ≈ 819 MiB) | 1,053 | 11.5 ms | within noise; `commands_for_key` hit rate 35% → 46% |
| `command_store_shard_count=8`, `queue_shard_count=8` | 550 | 14.7 ms | 2× worse |
| `command_store_shard_count=4`, `queue_shard_count=4` | 233 | 21.2 ms | 5× worse |
| `command_store_shard_count=32` (16 queue shards) | 1,144 | 12.4 ms | same at 36 clients; 1,314 tx/s at 54 (+8%), now fully CPU-bound (15.4/16 cores) |

- **Each command store is a serial lane.** A store is an `ExclusiveExecutor`: it runs one task at a time. With 4 or 8 stores, the lanes saturate long before the CPU, and queueing pushes latency to 150–560 ms.
- **Past 16 stores, CPU is the limit again.**

---

## 5. Where the CPU goes

### 5.1 Cost versus transaction size

`calvin.txnsize` ∈ {1, 2, 5, 10}, one hot key per transaction, 36 clients:

| Keys/txn | Command stores touched (expected, of 16) | Total tx/s | CPU/txn (Lyon) |
| ---: | ---: | ---: | ---: |
| 1 | 1.0 | 1,327 | 2.2 ms |
| 2 | 1.9 | 1,250 | 3.8 ms |
| 5 | 4.4 | 1,227 | 8.2 ms |
| 10 | 7.6 | 1,112 | 11.8 ms |

- **Least-squares fit:**
  - Against command stores touched: **1.0 ms + 1.46 ms × stores**. It predicts 2.5 / 3.9 / 7.5 / 12.2 ms.
  - Against keys: 1.75 ms + 1.06 ms × keys. It predicts 2.8 / 3.9 / 7.0 / 12.3 ms, a worse fit.
- **The per-store model fits better.** Most of the work is per command store a transaction touches:
  - a command, with its journal records and cache entries, per store;
  - one task per message per store (§5.3);
  - per-store dependency calculation and replies.
- **A single-key transaction already costs ~2.2 ms of CPU per replica.**

### 5.2 CPU and allocation profile

On-CPU samples, Lyon, 16 shards, measured window (`jfr/categories.py`; each sample is counted once, in the first matching category):

| Category | Stock | ApplyLink patch |
| :--- | ---: | ---: |
| GC (ZGC workers and barriers) | 26.1% | 27.9% |
| `commands_for_key` loads (system table reads) | 12.3% | 6.3% |
| Executor machinery, locks, park/unpark | 11.9% | 9.6% |
| Accord protocol logic (preaccept, apply, commit…) | 10.3% | 12.1% |
| Data reads (`TxnNamedRead`) | 5.9% | 7.2% |
| Journal writes | 5.2% | 6.3% |
| Data writes | 3.8% | 4.7% |
| Dependency calculation | 3.2% | 3.5% |
| `commands_for_key` update | 3.1% | 3.7% |
| Command loads (journal reads) | 3.1% | 2.6% |
| Messaging, CQL, compaction, JIT, other | 15.1% | 16.1% |

- **Allocation.**
  - The stock image allocates **~1.3 MB per transaction per node** (~1.3 GB/s at ~1,000 tx/s). ZGC runs a young collection about every 0.7 s.
  - By category: `commands_for_key` loading and inflating 30%; data writes 12%; inbound messaging 9%; CQL parsing and coordination 8%.
- **The single largest allocation site is `BufferManagingRebufferer.<init>`, 12.6%.**
  - `system_accord.commands_for_key` is created with `NoopCompressor`, whose preferred buffer type is `ON_HEAP`.
  - `BufferPool.get()` pools only off-heap buffers, so every SSTable read of that table allocates a fresh chunk-sized `byte[]`.

### 5.3 The anomaly: ~85 `commands_for_key` reads per transaction

`nodetool tablestats` at 16 clients/DC (525 tx/s) counted these local reads per second on Lyon:

| Table | Reads/s | Per transaction |
| :--- | ---: | ---: |
| `system_accord.commands_for_key` | 44,900 | **~85** |
| `ycsb.usertable` | 4,100 | 7.8 |
| `ycsb.usertable` writes | 5,500 | 10.5 |

The Accord cache counters, at 36 clients, agree:
- **Requests:** ~73 per transaction.
- **Hit rate:** 35%.
- **Misses:** ~48 per transaction.

The cache is not too small:
- **Turnover.** The live heap held 384k cached `commands_for_key` entries and 148k cached commands. At the observed insertion rate an entry survives ~20 s, while a transaction lasts under a second.
- **Size.** Tripling the cache barely changed the hit rate.

Two method-instrumentation profiles found the source:

1. **`AccordExecutor.load`, sampled 1 call in 20.**
   - ~80k loads/s on Lyon, ~83 per transaction.
   - 91% are `commands_for_key` loads, set up in `SafeTask.setupKeyLoadsExclusive`.
   - 83% are submitted as *consequences* of a completed task (`Task.submitConsequencesExclusive`), not by an incoming message.
   - 10% come from range scans (`RangeTxnAndKeyScanner`).
2. **`SafeTask.<init>`, 1 in 20.**
   - ~45 tasks per transaction per node.
   - 72% come from `MapReduceCommandStores.applyAsyncInternal`, one per message per command store.
   - 26% come from **`Apply$ApplyLink.apply`**.

The cause in the code (`accord/messages/Apply.java`):

```java
// submit(): with READY_TO_EXECUTE, each command store runs applyDirect() synchronously
return node.commandStores().mapReduceConsume(minEpoch, maxEpoch, this.overrideWithSynchronousApply(this::applyDirect));

// applyDirect(): writes first, then the state-machine update as a continuation ...
return written.then(head -> new ApplyLink(head, commandStore, participants));

// ApplyLink.apply(): ... whose context is the whole message
return commandStore.continuationChain(Apply.this, safeStore -> { ... });
```

- **Normal path versus synchronous path.** The normal path, `MapReduceCommandStores.applyAsyncInternal`, slices the context to the store's ranges (`slice(ranges, Minimal)`). The synchronous override ignores the ranges, and `Apply.this.keys()` is the transaction's whole scope: all 10 keys, since every node replicates every key.
- **What each `ApplyLink` task does.** In each of the ~7.6 stores a transaction touches, the task therefore acquires `commands_for_key` for the ~8.7 keys that store does not own:
  - Those keys have no row in that store's partition of `commands_for_key`, so the load returns `null`. Half the table's reads touch 0 SSTables.
  - `AccordCache.release` evicts an entry that is loaded and `null` immediately (`evict = node.is(LOADED) && node.isNull()`).
  - So the next task reloads it.
- **Why the cost is per store and grows faster than the keys (§5.1).** That is 7.6 × 8.7 ≈ 66 extra loads per transaction, plus each store's own cold keys, matching the ~75–85 measured.
- **Why this hurts low contention specifically.** The keys are spread uniformly over 1M, so a transaction's keys almost always span many command stores, and each cold key misses the cache once anyway.

---

## 6. Fix: slice ApplyLink's context to the store

The patch is [`patches/accord-applylink-slice-keys.patch`](patches/accord-applylink-slice-keys.patch), applied to Accord `446b2504`:

```java
ExecutionContext context = slice(storeRanges.allBetween(minEpoch, maxEpoch), Routables.Slice.Minimal);
return written.then(head -> new ApplyLink(head, commandStore, participants, context));
// ApplyLink.apply(): commandStore.continuationChain(context, ...)
```

- **How it was tested.** `accord-core` was rebuilt with Gradle. Its jar replaced `lib/cassandra-accord-6.0-alpha3-SNAPSHOT.jar` in a local image `0track/cassandra-accord:applyslice`.
  - All other classes were bytecode-identical to the image's jar, which confirms the image matches `446b2504`.
  - The runs used a copy of `calvin_ubench.sh` that skips `pull_images`, with `:latest` re-tagged to the patched image and restored afterwards.

| Metric (36 clients/DC) | Stock | Patched |
| :--- | ---: | ---: |
| `commands_for_key` cache requests per txn | ~73 | ~32 |
| `commands_for_key` cache hit rate | 35% | 78% |
| `commands_for_key` table reads per txn | ~85 (at 16 clients) | ~11 |
| CPU/txn (Lyon) | 11.8–12.2 ms | 10.8 ms |

Sweep with the patch (ZGC):

| Clients/DC | Total tx/s | Avg latency (H / L / NY) | Node CPU | CPU/txn |
| ---: | ---: | :--- | :--- | ---: |
| 36 | 1,145 | 117 / 81 / 81 ms | 11.9–12.3 | 10.4–10.8 ms |
| 54 | 1,259 | 134 / 106 / 110 ms | 13.3–14.3 | 10.6–11.4 ms |
| 81 | 1,317 | 157 / 160 / 200 ms | 13.5–14.2 | 10.2–10.8 ms |
| **121** | **1,385** | 298 / 240 / 225 ms | 13.4–14.1 | 9.7–10.2 ms |
| 182 | 1,344 | 430 / 324 / 429 ms | 13.2–13.8 | 9.8–10.3 ms |

- **Higher peak.** It rises 14%, from 1,214 to 1,385 tx/s.
- **No collapse.** Throughput degrades gracefully past saturation, where the stock image collapsed at 81 clients.
- **Remaining loads.** The ~11 table reads left per transaction are about one per cold key, which is expected with 1M uniformly chosen keys.

---

## 7. GC: G1 instead of generational ZGC

- **ZGC's CPU share.** The image's `jvm21-server.options` selects generational ZGC with uncompressed oops. ZGC keeps pauses short at the price of concurrent CPU (marking, relocation, barriers), and that CPU is exactly what Accord lacks here.
- **The test.** `CASSANDRA_JVM_OPTS` appended `-XX:-UseZGC -XX:-ZGenerational -XX:+UseG1GC -XX:+UseCompressedOops -XX:MaxGCPauseMillis=100`. Later flags win, and `VM.info` confirmed `g1 gc`, compressed oops.

| Clients/DC | Collector | Total tx/s | Avg latency (H / L / NY) | CPU/txn (Lyon) | p99 / max (Lyon) |
| ---: | :--- | ---: | :--- | ---: | :--- |
| 36 | ZGC | 1,094–1,112 | 117 / 83 / 85 ms | 11.8–12.2 ms | — |
| 36 | G1 | 1,058 | 122 / 88 / 91 ms | 10.7 ms | — |
| 54 | ZGC | 1,214 | 133 / 116 / 125 ms | 11.6 ms | 763 ms / 5.1 s |
| 54 | G1 | **1,385** | 141 / 102 / 101 ms | 9.8 ms | **571 ms / 2.2 s** |

| 81 | G1 | **1,547** | 163 / 143 / 147 ms | 9.1 ms | — |
| 121 | G1 | 1,526 | 275 / 211 / 208 ms | 9.5 ms | — |

With the ApplyLink patch and G1 together:

| Clients/DC | Total tx/s | Avg latency (H / L / NY) | Node CPU | CPU/txn | p99 (Lyon) |
| ---: | ---: | :--- | :--- | ---: | ---: |
| 54 | 1,456 | 146 / 92 / 96 ms | 11.1–12.2 | 7.6–8.4 ms | 495 ms |
| 81 | 1,706 | 161 / 120 / 134 ms | 12.9–13.3 | 7.6–7.8 ms | 668 ms |
| **121** | **1,764** | 211 / 177 / 206 ms | 12.8–13.5 | 7.3–7.7 ms | 881 ms |
| 182 | 1,661 | 266 / 261 / 491 ms | 13.9–14.0 | 8.4 ms | 1,287 ms |

- **The two gains compound.** G1 lowers the CPU of every allocation; the patch removes allocation and work outright.
- **The peak rises 45% over the stock configuration**, with CPU per transaction down from ~11.5 ms to ~7.5 ms.
- **G1 can go into `exp.config` today.** It is a JVM flag in `cassandra.jvm_opts` (or `accord.jvm_opts` to keep it Accord-only). The ApplyLink fix needs a rebuilt Accord jar, so it belongs upstream or in the image build.

---

## 8. Comparison with high contention (CI=0.01)

Same settings as the low-contention runs (stock image, `exp.config` defaults), with `calvin_ubench.sh --protocols=accord --ci=0.01 --clients=36,54,81,121,182`, once with ZGC and once with G1.

| Clients/DC | CI=0.01, ZGC | CI=0.01, G1 | CI=0.0001, ZGC | CI=0.0001, G1 |
| ---: | :--- | :--- | :--- | :--- |
| 36 | 760 tx/s, 124–161 ms, 14.0 ms CPU/txn | 742 tx/s, 126–164 ms, 13.1 ms | 1,112 tx/s, 83–117 ms, 11.8 ms | 1,058 tx/s, 10.7 ms |
| 54 | 928, 151–193 ms | 911, 156–194 ms | **1,214**, 116–133 ms | 1,385 |
| 81 | **1,144**, 191–224 ms, 14.2 of 16 cores | 991, 215–273 ms | 806 (collapse) | **1,547** |
| 121 | fails: ~90% of requests time out (~5 s) | **1,168**, 286–311 ms, 12.6–13.1 of 16 cores | — | 1,526 |
| 182 | fails, as at 121 | 907, 557–584 ms | — | — |

Notes on the high-contention numbers:
- The ZGC sweep stopped scaling at 121 clients. On Lyon, only 137 transactions succeeded against 1,453 errors, all at about 5 s. The node logs were removed with the containers, so the cause is unknown.
- The G1 sweep ran without errors at every point, and its node logs show nothing beyond startup warnings.

**At high contention, Accord is not CPU-bound.**
- At the G1 peak, the nodes use only 12.6–13.1 of their 16 cores, while latency keeps rising with load.
- The coordinator metrics differ from low contention in two ways:
  - **Medium path.** 20% of transactions take it (no fast quorum, an extra commit step), against 0.4% at CI=0.0001.
  - **Apply latency.** The median is ~380 ms, against 74 ms at 36 clients and CI=0.0001. This figure accumulates over the whole sweep and is weighted towards its heaviest points.
  - Dependencies per transaction are similar: medians of 20 and 17.
- **Probable cause: per-hot-key execution chains.** This explanation is consistent with the numbers but was not measured directly.
  - Every transaction touches one of 100 hot keys, and transactions on the same hot key must apply in order.
  - A transaction can only read the hot key once its predecessor's writes have reached the replica. Each link of the chain therefore costs about one WAN round trip.
  - That caps a hot key near 10–15 tx/s, and 100 keys near 1,000–1,500 tx/s, which is where both sweeps plateau.

**Low and high contention are therefore limited by different things.**
- Low contention has no such chains, so it scales until the CPU runs out.
- With the stock ZGC configuration, the two limits happen to be close: 1,214 tx/s at CI=0.0001 against 1,144 tx/s at CI=0.01. So low contention appears not to scale better than high contention.
- The gap opens with G1 (1,547 against 1,168 tx/s) and with the ApplyLink patch (1,764 tx/s at CI=0.0001; not run at CI=0.01). These are the fixes that lower the CPU cost per transaction.
- In the original sweep (§1 of [executor-lock-contention.md](executor-lock-contention.md)), both workloads plateaued at ~120 tx/s per DC for a third reason: the two-shard executor lock, which capped them before either limit.

---

## 9. Remaining costs and next steps

Even with both fixes, a 10-key transaction costs ~7.5 ms of CPU per replica, against ~2.2 ms for a single-key one. This is the reason Accord stays well below Tiga at low contention. What is left, by size:

1. **GC and allocation, ~1.3 MB/txn.** Next steps:
   - Recycle on-heap read buffers for `NoopCompressor` tables, or give `commands_for_key` an off-heap-preferring compressor such as LZ4.
   - Cut allocation in `commands_for_key` inflate/deserialise and in `TxnWrite`/`PartitionUpdate` deserialisation.
   - Use prepared statements in the YCSB binding, since CQL lexing appears in the allocation profile.
2. **Per-command-store fan-out.** A transaction creates ~45 tasks per node, each with queueing, locking, cache acquire/release and a journal record.
   - Fewer stores serialise execution (§4), so the remedy is cheaper per-store work, not fewer stores.
   - Examples: batching a transaction's per-store tasks, or not issuing a journal lookup for a command that cannot exist yet. Every new transaction's first access in each store is a cache miss that schedules an `IOTaskLoad` against the journal.
3. **One load per cold key**, inherent to 1M uniformly random keys, which no realistic cache holds.
4. **Load-independent work.** At 16 clients, CPU per transaction is 18 ms against 12 ms at 36. Part of each node's CPU is spent regardless of load: compaction and flushes after the load phase, journal compaction, durability rounds.

---

## 10. Reproducing

- **Scripts in [`jfr/`](jfr/):**
  - `cpu_loop.sh` samples per-node cgroup CPU every 5 s;
  - `cpu_per_txn.py` turns those samples and the YCSB `.dat` files into CPU per transaction;
  - `categories.py` gives the CPU categories of §5.2 (its `gc` rule matches ZGC only);
  - `alloc.py` aggregates allocation samples (`jdk.ObjectAllocationInNewTLAB`, weighted by TLAB size);
  - `collapse.py` produces collapsed stacks for flame graphs.
- **Configuration changes for one run.** Use `ACCORD_JVM_OPTS` for Accord settings (e.g. `-Dcassandra.settings.accord.command_store_shard_count=32`). Use `CASSANDRA_JVM_OPTS` for JVM flags; it replaces `cassandra.jvm_opts` entirely, so it must repeat `-Dcassandra.config.allow_system_properties=true -Dcassandra.settings.native_transport_max_threads=1024`.
- **Instrumenting a method's calls.** `CASSANDRA_PROFILER=1 CASSANDRA_PROFILER_OPTIONS="event=org.apache.cassandra.service.accord.execution.AccordExecutor.load,interval=20"`.
  - The instrumentation matches every overload of the method, so count only the samples whose top frame is the overload of interest.
  - Instrumenting `SafeTask.<init>` shows where tasks are created.
- **The `.dat` files are git-ignored.** `calvin_ubench.sh` deletes `logs/calvin_ubench/*accord*` at the start of every run, so copy each point's outputs (with `cp -p`, since `cpu_per_txn.py` matches each run to its CPU samples by file mtime) before running the next.
