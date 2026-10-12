# Investigating Accord latency outliers with async-profiler

> The AWS 5-region investigation, the cross-node correlation tools and the `DX` distributed execution tracing are
> handed over in [accord-latency-handover.md](accord-latency-handover.md).

This note covers what we're investigating, how profiles are captured, what we have found so far, the changes made
and proposed as a result, and how to use the scripts that analyse the recordings.

## What we're investigating

The Accord benchmarks (`cdf.sh`, `conflict.sh`, ...) show latency outliers we can't explain from the YCSB
output alone. The aim is to find out what each replica is doing while those outliers happen, and to separate:

- **costs inherent to Accord** (ordering conflicting transactions);
- **costs of the implementation** (data structures, serialisation, executor handoff);
- **costs of the environment** (GC, heap sizing, the profiler itself).

We capture async-profiler recordings, which include the `profiler.Span` events Cassandra's `DebugExecution`
emits around executor work, on every replica during the YCSB run phase.

## Capturing profiles

Set `cassandra.profiler=1` in `exp.config`, or use `CASSANDRA_PROFILER=1` for a single run. The setting must be on
when the cluster is created.

```bash
CASSANDRA_PROFILER=1 ./conflict.sh --protocols=accord
CASSANDRA_PROFILER=1 CASSANDRA_PROFILER_OPTIONS='event=ctimer,interval=1ms,wall=10ms,lock=1ms,jfrsync=profile' ./cdf.sh --protocols=accord
```

- **Node flags.** `cassandra/start_cassandra_data_centers.py` starts each node with
  `-Dcassandra.async_profiler.enabled=true`, `-Dcassandra.async_profiler.unsafe_mode=true`,
  `-Daccord.debug_execution=true`, `-Daccord.debug_execution_report=<exp.config, default false>` and
  `-XX:+DebugNonSafepoints`.
- **Extra flags.** `cassandra.jvm_opts` (or `CASSANDRA_JVM_OPTS`) adds arbitrary JVM flags, for example the
  `-D` switches below that turn individual optimisations off for A/B runs.
- **Capture window.** `cassandra/profiler.sh` starts and stops the profiler around the YCSB *run* phase of each
  `run_benchmark` call, through `nodetool profile execute`. `nodetool profile start` can't be used: it accepts only
  one event, and refuses to run when the host's `kernel.perf_event_paranoid` is above 1.
- **Output.** Captures are copied off the nodes before cleanup to `logs/profiles/<experiment>/<run>_<node>.jfr`,
  next to `<run>_<node>.gc.tgz` (the node's GC log, which records time-to-safepoint). The default options are in
  `exp.config` (`cassandra.profiler.options`).

### Things to know about the recordings

- **Spans are a sample.** `DebugExecution` used `Span.endIfProfiled`: a span is recorded only if the profiler
  sampled the ending thread while it was open. So recorded spans are biased towards long ones; use them to find
  examples, not to count. `-Daccord.debug_execution_spans=all` records every span.
- **`ctimer` samples are coarser than configured.** On HZ=250 kernels (Debian's default, as far as we know), a
  CPU-timer sample fires about every 4 ms of CPU time, however small `interval` is. Treat each sample as ~4 ms.
  Wall samples are accurate: each running thread is sampled every 10 ms.
- **Idle threads are batched.** Wall samples of parked threads are recorded as one event with a `samples` count,
  and a stale stack can outlive the idle period. Add `nobatch` for exact thread states (files ~20× larger).
- **`perf_events` is unavailable on our hosts** (`perf_event_paranoid=3`), so `ctimer` and `wall` are used, which
  means no kernel frames. `--cap-add PERFMON` doesn't help: the image's entrypoint drops to the `cassandra` user,
  which clears the capability.
- **`jfrsync=profile`** merges JDK events (safepoints, VM operations, `jdk.ZPageAllocation` with commit sizes)
  into the same file.

### Span tags

| Current tags (`benchmarking-bottlenecks`, `benchmarking-spans`) | Legacy tags | Meaning |
|---|---|---|
| `Queued <label>` | `AccordTaskQueued` | task creation → it starts running (loading, waiting for its store, waiting for a thread) |
| `Head <label>` | — | the task becoming next on its command store → it starts running (store-to-thread handoff) |
| `<label>` | `[table\|tid\|<store>,<exec>,<node>]` for store tasks, else class name | the task running |
| `AccordExecutorCriticalSection` | same | the executor lock held |
| `AccordExecutorParkedWithWork` | — | a thread parked while tasks were queued but not pollable |

The label format is `<kind>[ s<store>]`. For store tasks the kind is the request (e.g. `Accept s24`); for other
tasks it's the task class (`PlainChain s24`, `IOTaskLoad`). The analysis tools understand both naming schemes.

## Findings so far

### 1. `cdf.sh`: whole-JVM stalls of 1.1–1.3 s (ZGC heap growth)

- **Symptom.** `Run_PlainChain` spans longer than 1 s on all five replicas.
- **Bursts, not slow tasks.** `LongSpans` showed they came in 2–3 bursts per node. In each burst ~64 executor
  threads started within ~50 ms of each other and all finished within ~2 ms.
- **Heap commit in every burst.** Every executor thread was in `ZSyscall::fallocate` ←
  `ZPhysicalMemoryBacking::commit_numa_interleaved` ← `ZPageAllocator::alloc_page`.
- **A safepoint spread it to the whole JVM.** The VM thread sat in `SafepointSynchronize::begin` throughout, and
  the Netty threads were blocked at the safepoint.
- **Cause.** The nodes ran with `-Xms2g -Xmx15g`, and ZGC grew the committed heap ~2 GB at a time (5.3 → 7.3 →
  9.6 GB), the allocating threads committing it themselves via `fallocate` on the heap's memfd. The commits were
  serialised (~1 CPU-second per burst), and a pending safepoint froze every other thread until they finished.
- **Fix.** `-Xms = -Xmx` (`-XX:+AlwaysPreTouch` is already set), applied for the later conflict runs: they show
  `-Xms15g -Xmx15g`. Note that this checkout's `start_cassandra_data_centers.py` still uses
  `xms_gb = min(2, xmx_gb)`.

### 2. `conflict.sh`: work queueing behind one hot command store

- **Symptom.** `AccordTaskQueued` median 17–34 ms, p99 113–155 ms, while store tasks run for a median of ~0.3 ms.
- **One store does almost all the work.** About 90% of store-task executions on every replica are for command
  store 24, which holds the workload's hot keys. A store runs one task at a time.
- **The executor isn't short of threads.** While 100–200 tasks are queued, only 1–2 of 64 executor threads are
  busy, waiting for the executor lock is ~3%, and the host is ~17% busy.
- **Only the hot store's tasks wait.** They queue for a median of 19–34 ms; other stores' tasks and off-store
  tasks for ~0.1–0.3 ms.
- **Executor throughput levels off.** While backlogged, the executor starts tasks at a flat ~2,400/s (a lower
  bound) on all three replicas.

Where store 24's time goes during backlog (thread-seconds per second; the three nodes agree within ~0.02):

| Component | Share of store time | Detail |
|---|---|---|
| between tasks | ~0.20–0.22 | handoff (poll/park/unpark ~0.035, lock ~0.02), `DebugExecution` CPU-clock reads ~0.04, wall-sampler artefacts |
| journal (`SafeTask.save` → `AccordJournal.saveCommand`) | 0.18 | serialisation (mostly deps) 25–37%, **reply sent inline from `onDurable`** 27–30%, segment copy 16–19%, in-memory index ~10% |
| `CommandsForKey` update | 0.175 | `computeInfoAndAdditions`, `NotifySink`, `Pruning.newPruneBefore`: scans of the hot key's transactions |
| deps calculation (`CommandsForKey.visit`) | 0.145 | mostly Accept, then PreAccept: scans every transaction before the target |
| other message logic | 0.19 | |
| `updateSize` (executor lock held) | 0.045 | O(n) heap-size recomputation of the hot key's `CommandsForKey` on every release |
| other cache bookkeeping | 0.026 | |
| reads | ~0.002 | reads run off the store (`PlainChain` → `TxnNamedRead`) |

- **Throughput and latency model.** YCSB is closed-loop, so while the store is saturated, removing a fraction *s* of
  its time raises its throughput ceiling by 1/(1−*s*) and cuts latency by ~*s*. Gains combine as 1/(1−Σ*s*), until
  something else becomes the bottleneck.
- **Reads go to SSTables needlessly.** 41–47% of read time is in SSTable code (`pread`, LZ4, CRC). `isRowComplete`
  only stops at the memtable if the row has primary-key liveness newer than the next SSTable, and `UPDATE` never
  writes that. This was added by CASSANDRA-16671: the coordinator merges several replicas' responses, and a newer
  deletion of the queried cell on another replica would otherwise make an existing row vanish. A single,
  unreconciled replica doesn't need it; Accord reads already set `rowFilter().needsReconciliation() == false`.
- **The reads don't explain the store's idle time.** A read is running in 14% of store-idle ticks against 10–11%
  of busy ticks; chance accounts for most of that.
- **No safepoint and no evidence of a missed wake-up.** In ticks where every executor thread looks parked despite
  a backlog, no thread is at a safepoint, executor CPU samples are as dense as in busy ticks, and there's no burst
  of work afterwards. So these are most likely a wall-sampler artefact; the new park check verifies this directly.

### Corrections made along the way

- **Executor CPU.** An early reading of "0.43 executor cores, the store idle more than half the time" came from
  counting `ctimer` samples as 1 ms. With ~4 ms per sample, the store's tasks are CPU-bound for ~80% of the time.
- **`Wait_` spans.** `DebugTask.onRunning` ended `runningAtSpan` instead of `waitingAtSpan`, so no `Wait_*` spans
  were recorded until that was fixed (`0b4a29a2faf`).

## Changes

### Benchmark scripts (this repository)

- `cassandra/profiler.sh`: starts and stops captures, copies the JFRs and GC logs, and checks the host's perf
  settings. It waits only for its own jobs, because a bare `wait` also waited for the never-ending `dlogs -f` log
  followers.
- `run_benchmarks.sh`: brackets each run phase with a capture.
- `start_cassandra_data_centers.py`: adds the profiler and debug JVM flags, plus `cassandra.jvm_opts`.
- `exp.config`:
  - `cassandra.profiler`, `cassandra.profiler.options`;
  - `accord.debug_execution_report` (default `false`, saving ~4% of executor CPU);
  - `cassandra.jvm_opts`.
- `utils.sh`: `config()` now keeps values that contain `=`.

### Cassandra (local branch `benchmarking-bottlenecks`, on `origin/benchmarking`)

The span commits (`7fbbcaa710a`, `974d173e880`) are also ported onto the current `benchmarking`, with the DX
tracing, as `benchmarking-spans`; see [accord-latency-handover.md](accord-latency-handover.md) section 4.

| Commit | Change | Turn off with |
|---|---|---|
| `d51a6b38241` | Defer `AccordCacheEntry` size estimates while other tasks reference the entry; estimate once on the last release | `-Daccord.cache.defer_size_updates=false` |
| `cb296741c6a` | `SerializePacked`: no `Supplier` allocated per element for the range check | — |
| `2cba132465a` | Journal callbacks (the reply) for already-durable records run on `AccordJournal-durable-callbacks`, not the store thread | `-Daccord.journal.offload_durable_callbacks=false` |
| `7fbbcaa710a` | Span labels `<kind> s<store>`; `Queued`/`Head` spans; `-Daccord.debug_execution_spans=all` | — |
| `974d173e880` | Debug: log a missed wake-up; `AccordExecutorParkedWithWork` span | only with `accord.debug_execution` |

These were tested with an overlay build on top of the stale jars (unit tests, two in-JVM dtests, and unit tests
run with the profiler attached), so the image needs rebuilding (`ant artifacts`) before a run.

### Proposed, not implemented

1. **Sticky store threads.** After a store task completes, the same thread takes the store's next head directly,
   within a bound (N tasks or ~0.5 ms), still waking another thread for other work. `Head … s<store>` spans will
   show whether the handoff is worth this.
2. **`updateSize` delta.** Make the final estimate cheap by relying on the identity of carried-over `TxnInfo`
   elements and `missing` arrays; it complements the deferral.
3. **Per-command-store durability queue.** Journal serialisation and writes would run in a per-store queue
   scheduled by the `AccordExecutor`, outside the store's exclusive execution: ordered per store, with no global
   writer thread. It needs loads to see pending writes and the in-memory index update to move with the write.
4. **`CommandsForKey.visit`.** Skip the elided committed prefix (the existing `TODO` about skipping to the earliest
   undecided transaction); let Accept reuse PreAccept's result. Prune hot keys more eagerly, which shrinks every
   per-key cost.
5. **Reads.** Relax `isRowComplete` for unreconciled Accord reads: no liveness required, but at least one queried
   cell must be live at `nowInSec`. Or write with `INSERT` in the workload.
6. **Inline execution** for this workload. Count store passes per transaction with `spans=all` first.

## Distributed execution tracing

The executor spans show what one node is doing, but not which remote event a slow transaction is waiting for.
`DebugDistributedExecution` (Cassandra branch `debug-distributed-execution`) adds spans for each step of a
transaction's execution on every node, tagged with its TxnId, so the captures of all nodes can be joined:

```bash
CASSANDRA_PROFILER=1 CASSANDRA_DX=32 CASSANDRA_DX_KEYS=8 ./cdf.sh --protocols=accord
cassandra/jfr-analysis/run.sh TxnLatency --skip 30 logs/profiles/cdf/<run>_*.jfr     # where the tail is (also <run>_txnlatency.txt)
cassandra/jfr-analysis/run.sh LinkDelays --bucket 1000 logs/profiles/cdf/<run>_*.jfr # one-way delay per link, over time
cassandra/jfr-analysis/run.sh TxnTrace --slowest 10 logs/profiles/cdf/<run>_*.jfr   # also written to <run>_txntrace.txt
cassandra/jfr-analysis/run.sh TxnTrace --key <token> logs/profiles/cdf/<run>_*.jfr  # one key's history
```

- **Sampling.** `accord.debug_distributed_execution=N` traces one transaction in N, chosen from the TxnId alone, so
  every node traces the same transactions. `accord.debug_distributed_execution_keys=K` records every transaction's
  interactions with one key in K, so a sampled key's history is complete. Both settings need a running capture:
  nothing is built otherwise.
- **Tags.** All tags start with `DX`, then a role, an event and the TxnId:

  | Tag | Meaning |
  |---|---|
  | `C <phase> <txn>` | coordinator span per coordination phase (Client, PreAccept, Propose, Stabilise, Execute, Persist, recovery, progress log) |
  | `E <event> <txn> …` | coordinator events (PreAccepted, Executing with path and deps count, Executed, Durable, Preempted, Timeout, …) |
  | `R <event> <txn> s<store>` | replica state changes (PreAccepted, Accepted, Committed, Stable, ReadWaiting, ReadStarted, PreApplied, Applied, rejections) |
  | `M Send/Recv/Reply/Resp <type> <txn> n<peer>` | request send and receive, reply send and response receive |
  | `Q`/`T <reason> <txn> s<store>` | command store task queued / running (needs `accord.debug_execution`, which profiling sets) |
  | `K <status> <txn> k<key> s<store>` | the transaction's update of that key's `CommandsForKey` |
  | `K Unblock <waiter> k<key> s<store> by <trigger>` | the key no longer holds up the waiter; the trigger is the transaction whose update released it |
  | `K BlockedOn <dep> k<key> s<store> <status> <until> by <trigger>` | the key cannot progress until `dep` reaches `status` |

- **Wait chains.** `TxnTrace` follows `Unblock` events. On each replica, the waiter's last release names its
  trigger, then the trigger's own last release earlier on that replica names the next, up to `--depth`. A release
  "by itself" means nothing earlier held the transaction on that key: it was waiting to become Stable, not on a
  dependency.
- **Caveats.**
  - Messages delivered to the local node bypass the message sink, so they have no `M` events.
  - Cross-node offsets are only as good as the hosts' clock sync.
  - Every event has a unique tag string, so keep N and K well above 1 for long runs on busy clusters.

## Analysis tools (`cassandra/jfr-analysis`)

Small single-file tools built on `jdk.jfr.consumer`, with no dependencies. `run.sh` compiles them (JDK 17+;
`$JAVA_HOME` or the `PATH`) and runs one; set `JFR_HEAP` for a larger heap.

```bash
cassandra/jfr-analysis/run.sh SpanStats logs/profiles/conflict/*.jfr
```

Times are seconds from the recording's start. "Deep backlog" means at least 50 queued spans open at that moment.

| Tool | Arguments | Question it answers |
|---|---|---|
| `SpanStats` | `<jfr>...` | Per span tag (store numbers folded together): count, p50/p99/p999/max, number >100 ms and >1 s, which threads emit it. Start here. |
| `LongSpans` | `<ms> <jfr>...` | Are long spans independent, or one shared stall? Groups overlapping spans into clusters. |
| `Stores` | `<jfr>...` | Which command stores do the work: store-task runs per store, by count and summed duration. |
| `QueueByStore` | `<jfr>` | Whose tasks wait: queue time by store and by kind (legacy tags: approximate, by pairing). |
| `Timeline` | `<jfr> <bucketMs> [from to]` | Queue depth against executor threads idle / busy / waiting for the lock, per bucket, with the stores run. |
| `Drain` | `<jfr>` | Task dispatch rate while backlogged (a plateau means a saturated serial resource). |
| `HotPath` | `<jfr>` | During backlog: executor time by task kind, phase and component (journal, `CommandsForKey`, deps, ...). |
| `Matrix` | `<jfr>` | Store tasks only: protocol step × component. |
| `Cfk` | `<jfr>` | Inside `CommandsForKey` work: the innermost method and the protocol step that called it. |
| `Reads` | `<jfr>` | Durations of read spans, split by whether they touched SSTables. |
| `Gaps` | `<jfr>` | Per 10 ms sampler tick: is a store task running? If not, why not? |
| `Idle`, `Idle3` | `<jfr>` | Ticks where no executor thread is visibly running: what are the threads doing, and is it real? |
| `Window` | `<jfr> <from> <to> <threadRegex> <depth> [top]` | Top stacks in a time window, by state (CPU / RUNNABLE / SLEEPING). |
| `Phases` | `<jfr> <threadPrefix> [from to]` | Executor samples by the call made from the loop body (prepare / run / complete / lock / idle). |
| `Drill` | `<jfr> <anchorFrame> <n> [frameRegex]` | Call chains below a frame, e.g. `Drill f.jfr SafeTask.runMayThrow 12 'journal\.'`. |
| `Seq` | `<jfr> <thread> <fromSec> <n>` | The raw span sequence on one thread, with µs timestamps. |
| `Rate` | `<jfr>` | How often the wall sampler visits each thread, and how much is batched. |
| `TxnTrace` | `[--slowest N \| --txn <id> \| --key <key>] [--min-ms M] [--depth D] [window] <jfr>...` | All nodes' `DX` spans joined per transaction: timeline, then wait chains (who released it on each key, and who released them). |
| `TxnLatency` | `[--slow-ms M] [--excess ms] [--top N] [--keys N] [--bucket ms] [window] <jfr>...` | Where the `DX` tail is: per coordinator and kind, by execution path and key; messaging delay against waiting; the slowest TxnIds. |
| `LinkDelays` | `[--bucket ms] [--excess ms] [window] <jfr>...` | One-way message delay per directed link from the `M` events: clock-sync check, backlogs, retransmissions. |
| `NetEvents` | `[--bucket ms] [--links regex] [--top N] [--from ..] [--to ..] <jfr>...` | The JFR diagnostic events (`cassandra.profiler.jfr_events`): per connection queue, backpressure, TCP_INFO; per node TCP counters, executor queues, GC pauses, CPU throttling. |

`[window]` is `--from HH:MM:SS[.sss] --to HH:MM:SS[.sss]` (UTC) and/or `--skip <s>` after the first Client span. The
executor tools ignore `DX` spans, and `SpanStats` folds them into their shape.

### Interpreting the outputs

- **`SpanStats`.** A tight band near the maximum (e.g. p99 1.23 s against a max of 1.24 s) suggests one systematic
  cause; a long tail suggests contention. `emitting threads` tells you which pool does the work.
- **`LongSpans`.** `ends spread` of a few ms across dozens of threads means a shared stall (safepoint, GC, a lock)
  released them together. Many clusters of one thread each means genuinely slow tasks.
- **`Stores` / `QueueByStore`.** One store with most of the run time, and the only one with long queues, means
  the work is serialised on that store. Check the busy share before blaming the executor.
- **`Timeline` / `Drain`.**
  - `busy` stuck at 1–2 while `queued` is high means a serial bottleneck; `lockwait` high means executor-lock
    contention.
  - `idle` is a lower bound, because parked threads are sampled sparsely.
  - A dispatch rate that's flat across nodes while backlogged is the serial resource's ceiling.
- **`HotPath` / `Matrix`.**
  - Values are thread-seconds per second of backlog. For `STORE task` rows, their sum is the store's busy fraction
    (1.00 would be always busy), and 1 minus it is the time between store tasks.
  - The ceiling gain from removing a component *s* is 1/(1−*s*). Use the `wall` column; `cpu` counts `ctimer`
    samples as 1 ms and must be multiplied by ~4.
- **`Gaps` / `Idle` / `Idle3`.**
  - Compare how often something (e.g. a read) appears in store-idle ticks with how often it appears in busy ticks.
    Only the excess is causal.
  - `Idle3` checks idle ticks against executor CPU samples within ±1 ms. If CPU samples are as dense in idle ticks
    as in busy ones, the "idle" is a sampling artefact.
- **`Window` / `Phases` / `Drill`.**
  - `WALL RUNNABLE` is on-CPU or in a syscall; `SLEEPING` is blocked, and weighted by batch size.
  - Stacks that end in a bare libc frame under `ctimer` are kernel time; we can't see past the syscall.
  - "outside loop" usually means the stack walk failed, often inside a ZGC barrier during serialisation.

### General caveats

- Wall samples are the most reliable time measure (10 ms per running thread). `ctimer` samples need the ×~4
  correction; spans are length-biased unless `spans=all`.
- Parked threads are batched, so their recorded state can lag reality; use `nobatch` when it matters.
- Cassandra threads always have Netty's `FastThreadLocalRunnable` at the root of the stack, so classify a sample by
  the frames *below* the phase frame, not anywhere in the stack.
