# Accord on the Calvin micro-benchmark: why it scales poorly, and why high contention looks better

**Data.**
- `results/calvin_ubench.csv`: the saturation phase of 2026-10-12/13. It ran on 5 DCs (Singapore, Frankfurt, Ashburn, Osasco, Tokyo) with **2 nodes per DC**, hence 2 partitions, and 10% multipartition transactions.
- `results/calvin_ubench_scale.csv`: an earlier scale phase on 3 DCs, with 1 and 2 nodes per DC.
- The client logs in `logs/calvin_ubench/`. They hold one YCSB output per DC and run, with a 10-second time series of throughput, average, p90, p99 and maximum latency.

**What is missing.**
- No Cassandra node logs, profiles or `nodetool` metrics were kept.
- Accord's fast/medium/slow-path ratios are 0 for every Accord run. `cassandra/cassandra_fast_path.sh` reads them over JMX after downloading jmxterm from GitHub inside each container. On that machine this failed silently, so the ratios are unknown, not zero.
- The statements below are therefore marked as:
  - **measured**, from the client logs;
  - **from the source**, with file and line in `~/Implementation/cassandra`;
  - **hypothesis**, consistent with the data but not verified.

**Short answer.**
1. **Most of the "contention paradox" is an artefact of the experiment design, not of Accord.**
   - Contention is fully confounded with the order of the runs: every low-contention run comes right after the dataset load.
   - In the scale phase, the two contention levels ran with different numbers of clients.
   - When the load is the same, low contention is as fast or faster, and reaches the higher peak: 746 against 622 tx/s.
   - What high contention really does is make the system **degrade gracefully** under overload. Low contention instead degrades through stalls of several seconds.
2. **Accord's capacity per node scales almost linearly with the number of partitions. What does not scale is latency.**
   - From 1 to 2 nodes per DC, most transactions lose Accord's optimised fast path, and their read goes to a remote replica. Their latency doubles or triples.
   - The benchmark is closed-loop, so throughput = clients / latency. Unless the number of clients is raised enough to compensate, throughput barely grows.
3. **The absolute capacity is low, at about 3,500–4,000 keys per second per node.** This is the untuned per-node budget already documented in `docs/accord/`. CockroachDB reaches 3.5× Accord's peak on the same 10 nodes.

---

## 1. Accord is a closed loop: throughput = clients / latency

The benchmark is closed-loop: each YCSB thread waits for its transaction before sending the next one. So, until saturation, throughput is set by latency (Little's law). For the 5-DC × 2 sweep (**measured**):

| CI | Clients per DC | Measured throughput | Clients × 5 / avg latency | Avg latency | p50 | p99 |
|---|---:|---:|---:|---:|---:|---:|
| 0.0001 | 16 | 209 | 219 | 366 ms | 376 | 1,660 |
| 0.0001 | 54 | 636 | 670 | 403 ms | 368 | 1,357 |
| 0.0001 | 81 | **746** | 789 | 514 ms | 458 | 1,764 |
| 0.0001 | 122 | 621 | 660 | 924 ms | 690 | 4,161 |
| 0.01 | 16 | 205 | 215 | 372 ms | 362 | 1,077 |
| 0.01 | 54 | 566 | 598 | 452 ms | 413 | 1,053 |
| 0.01 | 81 | **622** | 660 | 613 ms | 567 | 1,573 |
| 0.01 | 122 | 618 | 660 | 923 ms | 862 | 2,260 |

- **Two things set Accord's throughput.** One is the latency of an unloaded transaction (§2). The other is the load at which latency starts to grow (§4).
- **CockroachDB, for comparison.** Its latency is ~580 ms, higher than Accord's 370 ms. But it stays flat up to 183 clients per DC, so CockroachDB reaches 2,610 tx/s.

## 2. Latency: two modes, and the second does not depend on contention

**Measured:** Accord's latency percentiles per DC at 16 clients per DC (light load), with each DC's round trips to the other four (`infra/aws/latencies.csv`):

| DC | RTTs to the others, sorted (ms) | p1 | p20 | p30 | p40 | p50 | p90 | p99 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Ashburn | 91, **114**, 149, 221 | 118 | 122 | 127 | 202 | 290 | 427 | 1,498 |
| Frankfurt | 91, **162**, 203, 228 | 167 | 169 | 174 | 305 | 348 | 465 | 1,676 |
| Osasco | 114, **203**, 260, 334 | 209 | 212 | 220 | 390 | 406 | 591 | 1,882 |
| Singapore | 70, **162**, 221, 334 | 166 | 169 | 173 | 289 | 466 | 630 | 1,608 |
| Tokyo | 70, **149**, 228, 260 | 153 | 157 | 162 | 314 | 372 | 502 | 1,636 |

These rows are for CI=0.0001; CI=0.01 has the same structure.

- **Mode 1** covers 30–40% of transactions. Their latency is one round trip to the **2nd-nearest** DC plus ~5 ms, which is exactly a 3-of-5 quorum including the coordinator.
- **Mode 2** covers the remaining 60–70%. It is 2–3× slower, already at CI=0.0001 where conflicts are very rare.

**Why (from the source):**
- **Optimised fast path.** Accord gives the coordinator a "privileged" fast path only if it **replicates every key** of the transaction (`modules/accord/accord-core/.../topology/TopologyCollector.java:136-138`, `local/Node.java:728-737`). With rf=5 that fast quorum is 3: itself plus 2 (`topology/Shard.java:293-310`). The coordinator then reads locally (`coordinate/ExecuteTxn.java:232-250`, `ReadTracker.tryIfUniversal`). That is mode 1.
- **Who coordinates.** The client of each DC connects only to node 1 of its DC (`run_benchmarks.sh`: `nearby_database="${location}1"`). With 2 nodes per DC, node 1 owns one partition and node 2 the other.
- **Which transactions qualify.** 45% of transactions are single-partition on node 1's partition and can take the privileged path. The other 55% (45% on node 2's partition, 10% on both) cannot. That matches the 30–40% measured in mode 1, given that some privileged transactions also leave the fast path.
- **What the others cost.** They take the *unoptimised* fast path, whose quorum is 4 of 5: a round trip to the **3rd-nearest** DC instead of the 2nd. Their read then needs another round trip, because the coordinator is not a replica.
- **The read goes to a remote replica, not the one in the coordinator's own DC.**
  - Cassandra sorts the candidate replicas **nearest first**: same-DC nodes come before the others (`locator/NetworkTopologyProximity.java:38-51`, used through `service/accord/api/CompositeTopologySorter`).
  - Accord's `ReadTracker` contacts the candidates **from the end of the sorted list** (`coordinate/tracking/ReadTracker.java:398-423`).
  - The sorter by shard coverage that comes first is consistent with this, since it sorts "more intersections later" (`impl/SizeOfIntersectionSorter.java:61`). But it inverts the proximity order among replicas with equal coverage.
  - **Hypothesis to confirm:** this is a bug that makes a non-replica coordinator read from a remote replica even when the coordinator's own DC holds one. The order among remote DCs is not by latency; the comparator only distinguishes same DC and same rack.
- **The scale data agrees (measured, 3 DCs).**
  - With 1 node per DC (node 1 replicates everything), p1–p50 is 103–175 ms, mostly one round trip.
  - With 2 nodes per DC, p10 is still 97–100 ms but p50 jumps to 237–402 ms.
  - With 2 nodes per DC and 100% multipartition transactions (no transaction is ever privileged), even **p1 is 228–287 ms**, about 2.5–3 round trips.

**Consequence:** going from 1 to 2 nodes per DC roughly doubles the median latency. In a closed loop, doubling the nodes then gives about 1× more throughput with the same clients per node, not 2×.

## 3. Capacity per node: about 3,500–4,000 keys per second, and it does scale

**Measured and derived:**
- **3 DCs × 1 node, earlier runs:** ~350 tx/s at the plateau. Each node replicates every key, so each node handles 350 × 10 ≈ **3,500 keys/s**.
- **5 DCs × 2 nodes, this sweep:** 746 tx/s at the peak. A node's partition is touched by ~55% of transactions (45% single-partition plus the 10% multipartition), so each node handles ≈ 746 × 0.55 × 10 ≈ **4,100 keys/s**.
- So capacity per node is roughly constant, and **total capacity roughly doubles with 2 partitions** (746 against ~350).
- **Why the budget is so low.** It was documented before, in `docs/accord/executor-lock-contention.md` and `calvin-low-contention-cpu.md`. Nothing in `exp.config` changes the defaults:
  - `queue_shard_count = max(1, cores/8)` = 2 executor shards on 16 cores (`service/accord/AccordCommandStores.java:326-341`);
  - `native_transport_max_threads` = 128;
  - generational ZGC;
  - and ~45 executor tasks per 10-key transaction on each node (one per command store and per message, `AccordCommandStores`/`AccordExecutor`).

  With 16 shards, 1,024 CQL threads, G1 and the ApplyLink patch, the same 3-DC benchmark reached 1,764 tx/s, 5× more.
- **Multipartition transactions do not benefit from partitioning (measured, 3 DCs × 2, mp=1.0).** Both contention levels stay at ~340–360 tx/s. Every node takes part in every transaction (5 keys each). A large share of the per-transaction cost does not depend on the number of keys (~1 ms per transaction plus ~1.5 ms per command store, `calvin-low-contention-cpu.md` §5). So halving the keys per node does not halve its work. Their latency (2.5–3 round trips, §2) caps the closed loop as well.

## 4. The "high contention is more efficient" paradox

### 4.1 What the data actually shows

| Setup | Low contention (CI=0.0001) | High contention (CI=0.01) |
|---|---|---|
| 5 DCs × 2, saturation, same clients | peak **746** tx/s (81 clients) | peak 622 tx/s (81 clients) |
| 3 DCs × 1, scale | 347 tx/s at 36 clients | 361 tx/s at 81 clients |
| 3 DCs × 2, mp=0.1, scale | 436 at 48 clients, **358 at 72** | **747 at 108, 820 at 162** |
| 3 DCs × 2, mp=1.0, scale | 340 at 48, 362 at 72 | 338 at 108, 347 at 162 |

The paradox appears only in the 3 DCs × 2, mp=0.1 row. Two design flaws explain it.

**(a) Different numbers of clients.**
- The scale phase runs each contention index at the *peak* number of clients found by the saturation phase, scaled by the nodes per DC (`calvin_ubench.sh`, `scale_clients_for`).
- That saturation sweep stops at the first point where both latency and throughput worsen. One noisy point is enough (B5 in `calvin-ubench.md`).
- It recorded 24 clients for CI=0.0001 and 54 for CI=0.01. So with 2 nodes per DC, low contention ran with 48/72 clients and high contention with 108/162.
- At 48 clients, low contention is simply under-loaded. Its 436 tx/s is 144 clients / 0.32 s, at a latency still close to the unloaded one.

**(b) Contention is confounded with time since the load.**
- `calvin_ubench.sh` loads the dataset once per protocol, or once per number of nodes per DC in the scale phase. It then runs **all** CI=0.0001 points, followed by all CI=0.01 points.
- Low-contention runs therefore always start right after loading 1–2M records, while Cassandra and Accord flush and compact the load and its journal. High-contention runs start ~20 minutes later.
- **Measured** in the 5 × 2 sweep: at the lightest load (16–24 clients per DC), where contention cannot matter, low contention has the worse tail:
  - p99 is 1.66–2.28 s against 1.06–1.08 s;
  - in some 10-second windows, p99 reaches 2–4 s.
- In the scale phase, the 72-client low-contention run was the second run after the load. Its tail (p90 1.2–1.4 s, p99 3.5–4.1 s) has that shape.

### 4.2 What high contention really changes (measured, same clients)

- **Fewer transactions in mode 1.** At 16 clients, mode 1 ends around p30 at CI=0.0001 but around p20 at CI=0.01.
  - **From the source:** a replica refuses the fast-path vote when it has already witnessed a later timestamp on one of the keys (`local/CommandStore.java:580-595`). With rf=5 privileged, this costs at least one extra round (medium path).
  - The earlier 3-DC study measured 20% medium path at CI=0.01 against 0.4% at CI=0.0001 (`calvin-low-contention-cpu.md` §8).
- **Higher median latency at moderate load:** 413 against 368 ms at 54 clients, 567 against 458 ms at 81.
  - **From the source:** a replica answers a read only once every dependency of the transaction has been *applied* locally (`local/Commands.java:1046-1095`).
  - A transaction on a hot key therefore waits for its predecessor's Apply, which is sent only after that predecessor has executed (`coordinate/CoordinationAdapter.java:290-296`). Each link of the per-hot-key chain costs about one WAN hop.
- **A bounded tail under overload.**
  - At 122 clients per DC, average latencies are the same (924 against 923 ms).
  - But low contention is unstable:
    - p99 grows from 2.1 to 5.1 s during the run;
    - one DC at a time stalls: Ashburn's throughput falls from 140 to ~60 tx/s for 20 s with transactions up to 8.7 s, then Osasco and Tokyo, with maxima up to 9.9 s;
    - failures appear (0.12%).
  - High contention stays steady, with a p99 of ~2 s and maxima of 2–3.7 s.

**Hypothesis for the stability difference: hot keys act as backpressure.**
- **At high contention:** the dependency chains on 100 hot keys per partition raise latency gradually as the load grows. In a closed loop, that slows the clients down before the nodes saturate.
- **At low contention:** there is no such brake. Latency stays flat until the executors saturate, and then queues grow without limit.
- **Amplification (from the source):** after a few seconds the timeouts kick in.
  - A replica waiting on a dependency fetches its state after 2 s (`fetch_txn`).
  - Replicas of the home shard start recovery after 5 s (`recover_txn`).
  - Transactions older than the request timeout are rejected at PreAccept (`service/accord/api/AccordAgent.java:263-266, 333-367, 435-449`).
  - All of these add coordination work exactly when the node is overloaded. The 8–10 s maxima and the stall moving from DC to DC fit this, but **without node logs this is not verified**.

## 5. What to change, in order

1. **Fix the experiment before drawing conclusions about contention.**
   - Interleave the contention indexes, or reload the data before each contention index.
   - Add an idle period after each load (until `nodetool compactionstats` shows nothing pending).
   - Repeat each point at least 3 times.
   - Run both contention indexes at the **same** clients per DC in the scale phase, for instance with `--scale-clients`.
   - Make the saturation stop rule require two consecutive degradations.
2. **Keep the evidence.**
   - Copy each node's `system.log` and `gc.log` before the containers are removed.
   - Fix `cassandra_fast_path.sh`: ship jmxterm in the image, or use `nodetool sjk mxdump`, as in `docs/accord/`. Then the fast/medium/slow ratios are known per run.
   - Run with `CASSANDRA_PROFILER=1` on one overloaded point at each contention index.
3. **Confirm and fix the read-replica choice.**
   - Trace one transaction coordinated by a non-replica, and see which node serves `StableThenRead`.
   - If it is remote, invert the proximity order for Accord, or pick the read candidates from the start of the list. This alone should remove about one WAN round trip from 55% of transactions with 2 nodes per DC.
4. **Let the coordinator be a replica.** Route each transaction to a node replicating its home partition: either a token-aware client (one session per partition), or the YCSB client sending each transaction to the node that owns its home partition. Every single-partition transaction would then get the privileged fast path and a local read, as with 1 node per DC.
5. **Apply the per-node tuning already found:** `accord.queue_shard_count=16`, `native_transport_max_threads=1024` and G1, through `cassandra.jvm_opts` in `exp.config`, and check whether the image has the ApplyLink patch. This raises the ~3,500–4,000 keys/s budget per node, and with it the load at which the low-contention instability appears.

After steps 1–2, rerun the 5-DC saturation at both contention indexes with the same client counts, in a random order. If high contention still beats low contention at equal load, investigate the overload behaviour (recovery and fetch traffic) using the node logs.
