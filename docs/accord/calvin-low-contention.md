# Accord on calvin_ubench at low contention (CI=0.0001)

Setup: `calvin_ubench.sh --ramp`, 5 DCs x 1 node, Accord, 1 partition, 10% multipartition,
simulated WAN. Each node is an emulated `e2-highcpu-16` (16 vCPUs, 12.8 GB container, 8 GB heap) on a
96-core host. Throughput is the total over the 5 DCs. Everything below was measured on 2026-10-20.

## Symptom

With the stock image, throughput peaks at about 575 tx/s (about 115 per DC) from 36 clients per site on,
and latency then grows in proportion to the number of clients (queueing). The YCSB clients are not the
limit: their containers use 25-75% of one core.

## Findings

1. **Executor lock contention (largest effect).** The nodes use only 5-9 of their 16 cores and
   the Accord executor threads are mostly parked on the `AccordExecutor` `ReentrantLock`. The default
   `accord.queue_shard_count` (-1, i.e. cores/8 = 2 shards on 16 cores) serialises the 16 command stores
   behind two locks. With 16 shards the peak goes from about 575 to about 1190 tx/s, and the nodes become
   CPU-bound (about 16 cores at 81 clients per site). Raising `native_transport_max_threads` to 1024 made no
   difference.
2. **ZGC makes throughput collapse under load.** With 16 shards and ZGC, throughput falls to 500-800 tx/s
   beyond 81 clients per site. With G1 it does not collapse: about 1400-1500 tx/s up to 413 clients in
   40 s stages.
3. **Apply's synchronous continuation loads keys it does not own.** `Apply.ApplyLink` used `Apply.this` as its
   context, which declares every key of the transaction in every command store; the foreign keys are loaded
   (as empty) and evicted again. Slicing the context to the store's ranges
   (`cassandra/tuned-image/accord-applylink-slice-keys.patch`) is worth about 10% on top of G1, but this is
   within the run-to-run noise of a single run. The bug is still present at Accord commit 446b250.
4. **What limits the tuned image now: heap pressure.** At 183 clients per site the nodes are CPU-bound.
   About 54% of the CPU samples are in the Accord executors (dependency calculation in
   `SaferCommandStore.visitForKey`, CommandsForKey loads and saves), and about 22% in G1's own threads
   (remembered-set work and concurrent marking) plus 8% in the JIT. The GC log of one node shows about
   0.7 MB allocated per transaction, young pauses of 150-240 ms with a 3-4 GB live set, 52 concurrent mark
   cycles, and bursts of evacuation failures that drive the heap to its 8 GB ceiling and end in full GCs of
   1.5-2.9 s. These match the dips in per-DC throughput (for example 49 then 370 ops/s in consecutive
   10 s windows). Accord's executor queues also grow in bursts of 100-500k waiting tasks, mostly
   `IOTaskLoad` and `IOTaskSave`.

## Result

CI=0.0001, 5 DCs, total tx/s (40 s stages, one run per cell, so +-10% noise):

| clients/site | 36 | 54 | 81 | 122 | 183 | 275 | 413 |
|---|---|---|---|---|---|---|---|
| stock (2 shards, ZGC) | 544 | 463 | 554 | 574 | - | 392 | - |
| 16 shards | 792 | 961 | 1187 | 819 | 508 | 817 | - |
| 16 shards + G1 | 789 | 1193 | 1206 | 1372 | 1490 | - | - |
| 16 shards + G1 + ApplyLink patch | - | 1099 | 1502 | 1284 | 1404 | 1499 | 1471 |

Over a longer stage (150 s at 183 clients per site) the tuned image gives about 1130 tx/s (1124 and 1156
in two runs). The heap pressure of finding 4 builds up over a few minutes (the first full GC came after
about 3 minutes in the profiled run), so the 40 s numbers above are probably optimistic.

## What was tried without effect

All with the tuned image; short-stage runs vary by +-15% between identical runs, so only large effects
are visible.

- `-XX:+UseCompressedOops` (the image's `-XX:-UseCompressedOops` is a ZGC workaround), and a later
  G1 marking threshold (`InitiatingHeapOccupancyPercent=70`, no adaptive IHOP, `ParallelRefProcEnabled`):
  no measurable change.
- `memtable_heap_space=512MiB`: 1132 tx/s against 1124-1156 for the baseline.
- `-Xms10g -Xmx10g`: throughput fell to 112 tx/s. The cause was not investigated; the heap plus the 2 GB
  direct-memory cap leaves little of the 12.8 GB container, which is a plausible reason.

## What is in the repository

- `cassandra/tuned-image/` builds `0track/cassandra-accord:tuned` from `0track/cassandra-accord:latest`:
  `queue_shard_count: 16` in `cassandra.yaml`, G1 instead of generational ZGC in `jvm21-server.options`,
  and the ApplyLink patch applied to Accord commit 446b250 (only the two `Apply` classes are replaced in the
  image's Accord jar). Run `cassandra/tuned-image/build.sh`.
- `exp.config` points `accord_cassandra_image` at `:tuned`.
- Profiling: `cassandra.profiler=1` in `exp.config` (or `CASSANDRA_PROFILER=1`), then
  `cassandra/jfr-analysis/run.sh <Tool> logs/profiles/calvin_ubench/<run>_<DC>1.jfr`. The profiler costs
  throughput (918 against about 1400 tx/s at 183 clients in one comparison), so compare profiled runs only
  with profiled runs.

## Next steps

Reduce the allocation per transaction (CommandsForKey load/inflate is the largest share) or the size of
the live set: profile allocations at 183 clients, then consider cache sizing, G1 region and young-generation
settings, and why the 10 GB heap collapsed.
