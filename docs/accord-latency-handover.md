# Handover: Accord latency outliers on AWS (5 regions)

This continues [accord-profiling.md](accord-profiling.md), which covers the earlier conflict/cdf findings,
the capture mechanics and the original `jfr-analysis` tools. This note records:

- what the AWS `cdf.sh` runs showed;
- the tools added for correlating events across nodes;
- the distributed execution tracing (`DX` spans) added to Cassandra;
- what is still open.

## 1. Setup and runs

The setup is `cdf.sh` (YCSB workload A, zipfian over 10k keys), with 5 DCs × 1 node on m5.4xlarge, one in each of
Ashburn (us-east-1), Frankfurt (eu-central-1), Osasco (sa-east-1), Singapore (ap-southeast-1) and
Tokyo (ap-northeast-1). There is one YCSB client per DC, co-located with its node and coordinating through it.
Latency is real WAN latency: `latency_simulation=0`.

| Run id (`accord_5_a_…`) | What changed | Headline |
|---|---|---|
| `20261001091221621624852` | accidentally `--test` (10 s per client, 1k records) | the clients barely overlapped; not usable |
| `20261001165922846094947` | 10 YCSB threads per client | server max ~540 ms; Osasco's client saw 709 ms |
| `20261002131402471305684` | 50 threads per client, `prepare-on-all-nodes=false`, `-Xms=-Xmx=31g`, `ActiveProcessorCount=16` | outliers up to ~700 ms, all server-side |
| `20261006101912590525506` | CFK serialisation moved out of the executor lock, executor prioritisation tweaks | no real change; differences are variability |

`logs/` currently holds only the last AWS run; the earlier ones were overwritten. `results/profiles/` now holds the first
run with `DX` tracing, a simulated one (section 5).

## 2. Findings

### Configuration and harness

1. **`-XX:ActiveProcessorCount=112` on 16-vCPU hosts (fixed by the user from the third run on).**
   - `start_cassandra_data_centers.py` looks the machine up in `gcp.csv`. `m5.4xlarge` isn't there, but the loop
     variable `gcp_row` leaks: it keeps the last row (`c2d-standard-112`).
   - The same miss gave the default `-Xms2g -Xmx4g`. With that heap, Singapore hit a 657 ms ZGC allocation stall
     just after the second run, the same commit-under-safepoint mechanism as finding #1 in `accord-profiling.md`.
   - Check that the fix guards on a *matched* row.
2. **Prepare round trip in the first measured operations (fixed).**
   - The YCSB fork opens a new driver session for the measured phase and prepares statements lazily.
   - Driver 4 waits for the PREPARE on *all* nodes, so the first operation paid an extra round trip to the farthest
     region: Osasco 709 ms = ~333 ms prepare + 376 ms execution.
   - `run_benchmarks.sh` now passes `-Ddatastax-java-driver.advanced.prepared-statements.prepare-on-all-nodes=false`.
3. **Client start-up is staggered (open).**
   - The clients start 38–43 s apart, more than the 30 s warmup.
   - When a client starts, its 50 threads fire at once, on a coordinator that has never coordinated before (cold
     JIT). That causes a burst of 600–880 ms requests on every node. Osasco's start was in warmup; Tokyo's fell
     inside the others' measured windows (Singapore 690 ms in the third run, Frankfurt 861 ms in the fourth).
   - Fix by starting every client before anyone measures, or by a warmup longer than the start spread + 30 s.
4. **`fast_path_ratio.dat` is always zero (open).** `cassandra_fast_path.sh` probably can't reach JMX. The
   `DX E Executing … <path>` events give the fast/medium/slow path per transaction instead.
5. **Run variability.** Single runs differ by tens of ms at p99.9. Compare tails over 2–3 runs per variant.

### What the profiles show

1. **GC and safepoints are clean** in every run after the heap fix: pauses under 1 ms.
2. **JIT compilation dominates the JVM's CPU** (~60–75% of samples). Bursts reach ~6 cores for a second, but the
   hosts are only ~6% busy, so this isn't a latency cause.
3. **Server and client agree.** Server-side request latency, rebuilt from wall samples (`RequestLatency`), equals
   the client's minus a constant ~11 ms. So every outlier from the third run on is inside Cassandra.
4. **The median is one round trip to a simple majority.** Ashburn 100–115, Frankfurt/Singapore ~150, Tokyo ~140,
   Osasco ~190 ms.
5. **Measured one-way delays (ms).** These come from the durability stalls propagating out from the coordinating
   node, and agree with published AWS round-trip times:

   | Pair | One-way |
   |---|---|
   | Ashburn–Frankfurt | 47 |
   | Ashburn–Osasco | 55 |
   | Ashburn–Tokyo | 75 |
   | Ashburn–Singapore | 104 |
   | Frankfurt–Singapore | 80 |
   | Frankfurt–Osasco | 100 |
   | Frankfurt–Tokyo | 112 |
   | Osasco–Tokyo | 130 |
   | Osasco–Singapore | 163 |
   | Singapore–Tokyo | 36 |

   They are in `py/xcorr.py` and `py/worstcase.py`.
6. **Worst-case bounds.** Computed as in `AccordYcsbLoadTest.computeWorstLatencies`, whose test matrix holds
   one-way delays for the Hanoi/Lyon/… synthetic setup. `py/worstcase.py` gives worst2 of 510 / 580 / 583 / 643 / 605 ms
   for Ashburn / Frankfurt / Osasco / Singapore / Tokyo.
   - With 10 client threads, observed maxima were within the bounds.
   - With 50 threads, the third run's server-side maxima exceed worst2: Ashburn +30, Frankfurt +7, Osasco +64,
     Singapore +47, Tokyo +22 ms.
7. **The tail is contention.**
   - Requests above 2.5× the median go from 0.6% with one client to 3.8% with all five. A client running alone has
     almost no tail.
   - The > 600 ms requests are nearly all *reads*, at Singapore, Tokyo and Osasco.
8. **Slow requests are released by remote events** (`py/xcorr.py`).
   - Cross-correlating slow request completions between nodes gives peaks at exactly ±the one-way delay.
   - The signs give an order: Osasco → Ashburn → Frankfurt, Tokyo, Singapore. For example, 95% of Singapore's
     > 500 ms reads finish 104 ± 15 ms after a slow Ashburn request finishes, against a 32% baseline.
   - Interpretation: reads wait on Ashburn-coordinated writes (Ashburn has the most throughput), which in turn wait
     on Osasco transactions. These are chains of dependencies across regions, which the single-conflict bound
     doesn't model.
   - This is inferred from timing only; the DX tracing (section 4) is meant to confirm it.
9. **Executor stalls from the shard-durability flush.**
   - Every node had 7–16 events in which all 32 executor threads stopped for 10–54 ms.
   - The cause: when the durability round's exclusive sync point is applied locally,
     `markExclusiveSyncPointLocallyApplied` → `AccordCommandStore.ensureDurable` serialised every modified
     CommandsForKey under `lockCaches()`, i.e. the executor lock.
   - The trigger is the coordinator's `DurabilityQueue` critical section (`ShardScheduler.success` →
     `DurabilityQueue.submit` → `ExecuteSyncPoint` → send `ApplyThenWaitUntilApplied`). The stall starts on the
     coordinator and then on each replica one one-way delay later. The `ensureDurable` work is never nested inside
     the `DurabilityQueue` monitor.
   - Requests overlapping a stall were 2.3× more likely to exceed 2.5× p50, which accounts for about 11% of the
     > 500 ms tail.
   - In the fourth run (serialisation out of the lock):
     - critical sections fell from 13–22 ms to 5–7 ms;
     - executor queue wait went from p50 3–4 ms / p99 12–18 ms to 2.5–3 / 10–14 ms;
     - stalls are still there but shorter (mean 11–15 ms, max 28–47 ms), more frequent, and the same total time.
       They are now bursts of `IOTaskSave` (32–40% of executor CPU inside a stall, against under 0.5% outside),
       with more executor-lock contention.
     - Next options: cap how many saves run at once, or run them below transaction tasks.
10. **Images still emit the legacy span tags** (`AccordTaskQueued`, `[table|tid|…]`), so the `Queued`/`Head`
    labels from `benchmarking-bottlenecks` can't be used to attribute queue wait by kind of task. The span changes
    are now ported onto `benchmarking` (with the DX commit) as branch `benchmarking-spans` (section 4); build images
    from it.

## 3. Tools (`cassandra/jfr-analysis`)

Run any tool with `cassandra/jfr-analysis/run.sh <Tool> <args>`, using JDK 17+ (`JAVA_HOME=/usr/lib/jvm/applejdk-21`
here). Times are absolute UTC (`HH:MM:SS.sss`) unless stated. The original tools are described in
`accord-profiling.md`; these are the additions.

| Tool | Arguments | Use |
|---|---|---|
| `RequestLatency` | `<jfr> [longMs]` (`CSV=1`, `HIST=1`) | **Server-side client latency** from wall samples of `SharedPool` workers parked in `AccordResult.awaitAndGet`: read/write percentiles and requests above `longMs`. With `CSV=1` it prints every request as `startMs,durMs,kind` (input to `xcorr.py`). It's a lower bound with ~10 ms resolution, and captures most requests. |
| `InFlight` | `<jfr>` | Concurrent client requests per second (Little's law; it showed the 10-thread clients). |
| `CpuByThread` | `<jfr>...` (`QUIET=1`) | CPU samples per second by category (executor, messaging, JIT, GC, …). Finds JIT/GC bursts and coordinator activity. |
| `Activity` | `<bucketMs> <from> <to> <jfr>...` | App-thread CPU per bucket for all nodes side by side. Finds stalls and bursts shared across the cluster. |
| `FramesPerSecond` | `<jfr> <classRegex>` | Per second, samples whose stack contains matching classes, e.g. `'^accord\.coordinate\.(CoordinateTransaction\|CoordinateEphemeralRead)$'` to see when each node coordinates (client start/stop). |
| `FrameClasses` | `<jfr> <classRegex> [event]` | Totals of the same, e.g. to check for Recover/Invalidate frames. |
| `WindowAbs` | `<jfr> <from> <to> <threadRegex> <depth> <top> [event]` | Top stacks in an absolute time window (`jdk.ExecutionSample` or `profiler.WallClockSample`). |
| `TopStacks` | `<jfr> <threadRegex> <depth> [event]` | Top stacks over the whole recording, e.g. the anonymous `[tid]` threads, which are C2 compiler threads. |
| `FindFrame` | `<jfr> <frameSubstring> [from to]` | Every sample whose stack contains a frame, with time and thread. |
| `ThreadSamples` | `<jfr> <thread> <from> <to>` | Raw samples of one thread, with batch counts; shows how parks map to requests. |
| `ExecutorStates` | `<jfr> <from> <to>` | Executor threads idle / waiting for the lock / running, from wall samples. Wall samples of idle threads can be stale: check CPU samples too. |
| `CriticalSections` | `<jfr> <minMs>` (`-Dd=<frames>`) | Executor-lock holds of at least `minMs` and what the holder was doing (this is how `ensureDurable` was found). |
| `StallWork` | `<stallsFile> <node> <jfr>` | Executor CPU inside a node's stall windows against outside, by kind of work (`IOTaskSave`, store task, lock, …). |
| `DurabilityQueueSamples` | `<jfr>` | Samples inside `DurabilityQueue`, whether `ensureDurable` is nested in them, and monitor contention. |
| `LongParks` | `<jfr>` (`-Dms=N`) | Long `ThreadPark`/`JavaMonitorEnter` events on app threads, and thread counts per pool. |
| `StoreSpanRate` | `<jfr>...` | Store-task spans per second (crude activity measure). |
| `TxnTrace` | `[--slowest N \| --txn <id> \| --key <key>] [--min-ms M] [--depth D] [window] <jfr>...` | **Joins `DX` spans across nodes** (section 4). |
| `TxnLatency` | `[--slow-ms M] [--excess ms] [--top N] [--keys N] [--bucket ms] [window] <jfr>...` | **Where the `DX` tail is**: Client-span percentiles per coordinator and kind (KW/KE/…), the slow ones (default ≥ 2.5× the group's median) and how many had one of their own messages delayed (`msg%`), slow share by execution path and by key, and the slowest TxnIds for `TxnTrace --txn`. |
| `LinkDelays` | `[--bucket ms] [--excess ms] [window] <jfr>...` | **One-way delay per directed link** from the `M` events. The p50 should equal the link's latency (it checks clock sync). `--bucket` shows backlogs (a climb, then a linear drain) and retransmission-like spikes. |
| `NetEvents` | `[--bucket ms] [--links regex] [--top N] [--from ..] [--to ..] <jfr>...` | **The JFR diagnostic events** (§4, "JFR diagnostic events"): per outbound connection the queue, unwritable time, backpressure intervals, throughput, cwnd/RTT/retransmits/loss state and send buffer; per node the TCP counter deltas, Accord executor queue, GC pauses and container CPU throttling; `--bucket` for a per-connection timeline. |

`[window]` is `--from HH:MM:SS[.sss] --to HH:MM:SS[.sss]` (UTC) and/or `--skip <s>` (seconds after the first Client
span), e.g. `--skip 30` to leave out client start-up. The `DX` tools share `Dx.java` (scanning, TxnIds, node ids,
message pairing). The executor tools ignore `DX` spans (`Tags.isDx`). Before that, `Stores`, `Timeline`, `QueueByStore` and
`LongSpans` counted a traced transaction's `DX T … s<n>` spans as store tasks. `SpanStats` folds `DX` tags into
their shape (`DX T PreAccept <KW> s<store>`).

Python helpers live in `cassandra/jfr-analysis/py/`:

- `xcorr.py <dir> [nodes]` takes `<dir>/<Node>.csv` files written by `CSV=1 RequestLatency`. For each pair of nodes
  it prints the peak of the cross-correlation of slow-request completions, next to the one-way delay. A peak at
  +one-way means the row's slow requests finish one hop after the column's, i.e. they were released by an event at
  the column node.
- `worstcase.py` prints the per-site worst-case bounds for the one-way matrix it contains.

### Recipes

**Server-side latency per node, and the per-pair release order:**

```bash
T=cassandra/jfr-analysis/run.sh; P=results/profiles; R=accord_5_a_<run>
for n in Ashburn Frankfurt Osasco Singapore Tokyo; do
  $T RequestLatency $P/${R}_${n}1.jfr 500            # percentiles + requests > 500 ms
  CSV=1 $T RequestLatency $P/${R}_${n}1.jfr > /tmp/xc/$n.csv
done
python3 cassandra/jfr-analysis/py/xcorr.py /tmp/xc
```

To restrict to each client's measured window, take the window from the client's `.dat` file
(`Starting test` / `0 sec` timestamp + 60 s). The client logs' times are UTC, like the JFRs.

**Stalls across the whole executor (and their spread across nodes):**

```bash
for f in $P/${R}_*.jfr; do $T LongSpans 5 $f; done | grep -E 'threads=(1[6-9]|[23][0-9])'
```

A cluster that spans most executor threads, with start/end spread of a few ms, is a stall. Its absolute time is the
recording start plus its offset; the recording start is printed in each file's header line. Then:
- `StallWork` shows what ran inside the stalls;
- `CriticalSections` shows which lock holds caused them;
- `DurabilityQueueSamples` shows the trigger.

Stalls that start on one node and reach the others one one-way delay later come from a durability round.

**Start and stop of each client:** use `FramesPerSecond` with the coordinator-class regex above on every node.

**With `DX` spans: tail first, then drill down:**

```bash
$T TxnLatency --skip 30 $P/${R}_*.jfr             # where the tail is; slowest TxnIds
$T LinkDelays --bucket 1000 $P/${R}_*.jfr          # was it the network/messaging? (and are the clocks in sync)
$T TxnTrace --txn <hlc> --depth 8 $P/${R}_*.jfr    # one transaction's timeline and wait chains
```

If `TxnLatency`'s `msg%` is high, messages were late: `LinkDelays` shows which links and when. Then check the nodes'
`StatusLogger` lines (`MessagingService n/a <large>/<small>` is the outbound backlog) and netem/TCP on the hosts. If
`msg%` is low, transactions were waiting: look at the paths and keys, then `TxnTrace` on the slowest.

**Pitfalls:**
- **CPU-sample weight.** On these AWS hosts a `ctimer` sample is ~1–1.4 ms of CPU, *not* the ~4 ms assumed in
  `accord-profiling.md`. Calibrate against `jdk.CPULoad`.
- **Recording starts.** The nodes' recordings start up to ~17 s apart; align by absolute time, not offset.
- **Load.** `machineTotal` includes the co-located YCSB client; it uses about 0.1 core.

## 4. Distributed execution tracing (`DX` spans)

### Cassandra changes

They're committed in `cassandra/cassandra` as `69972c39213` on branch `debug-distributed-execution` (on top of
`refine`, `9872f54c827`) and as `58a065df377` on `benchmarking`. Accord-core is untouched.

| File | Change |
|---|---|
| `service/accord/debug/DebugDistributedExecution.java` (new) | sampling, tag formats, the wrappers, the trigger thread-local |
| `config/CassandraRelevantProperties.java` | `ACCORD_DEBUG_DISTRIBUTED_EXECUTION` (`accord.debug_distributed_execution`, int, default 0) and `…_KEYS`. Enum constants must stay alphabetical; a startup check enforces it. |
| `service/accord/api/AccordAgent.java` | `trace()` wraps each coordination phase's `Tracing` in a span; `coordinatorEvents()` and `replicaEvents()` are wrapped |
| `service/accord/AccordService.java` | `coordinateAsync`: Client span |
| `service/accord/AccordMessageSink.java` | `M Send` (both `send`s) and `M Reply` (`reply`) |
| `service/accord/AccordVerbHandler.java` / `AccordResponseVerbHandler.java` | `M Recv` / `M Resp` (message ids map replies back to TxnIds) |
| `service/accord/debug/DebugExecution.java` | `Q`/`T` spans for store tasks of traced transactions |
| `service/accord/execution/SaferCommandsForKey.java` | `K` events: overrides `callback` and `registerUnmanaged`, and wraps the `NotifySink` from `overrideSink()` |

**Executor span labels (branch `benchmarking-spans`).** `benchmarking-bottlenecks` sits on a `benchmarking` that has
since been rebased, so its span commits were ported onto the current `benchmarking`, on top of the DX commit:

| Commit | Change |
|---|---|
| `8b8c69a96a1` (from `7fbbcaa710a`) | `TaskLabels`: run spans `<kind> s<store>` (e.g. `PreAccept s20`), `Queued <label>` (was `AccordTaskQueued`), new `Head <label>`; `-Daccord.debug_execution_spans=all`. The DX `Q`/`T` spans are emitted next to `Queued`/run from the same starts; a task that runs twice no longer re-emits its DX `Q` span. |
| `9bac5485069` (from `974d173e880`) | `AccordExecutorParkedWithWork` spans, and a rate-limited warning for a lock-loop thread parking while a task could be polled (a missed wake-up) |

The other `benchmarking-bottlenecks` commits are optimisations and were not ported: deferred `AccordCacheEntry`
size estimates, `SerializePacked` without a `Supplier` per element, and journal callbacks off the store. They were
checked like the DX commit:
- an overlay of every source changed since the jars (144 files), including the base commits;
- `TaskLabelsTest` (4/4);
- `FullAccordCQLTest.testAcceptTransactionWithUpdatesToSamePrimaryKeyButDifferentColumns` with the profiler and DX on,
  with and without `spans=all`.

Every new tag was emitted and the DX `Q`/`T` counts matched (15/15). `compiletest.sh`'s `-sourcepath` also
wrote production classes into `build/test/classes`; they were deleted.

### Flags

- **`-Daccord.debug_distributed_execution=N`** traces one transaction in N. The choice is a hash of the TxnId's
  HLC and node, so every node picks the same transactions. `1` traces all of them.
- **`-Daccord.debug_distributed_execution_keys=K`** records every transaction's interactions with one key in K
  (by `RoutingKey.hashCode`, which is the table plus token hash and the same on every node).
- **When anything is recorded.** Spans exist only while an async-profiler recording runs (`Span.start()` returns 0
  otherwise), and no tag is built unless the transaction or key is sampled *and* a recording is running. Spans are
  unconditional (`Span.end`/`Span.emit`), so they aren't biased towards long ones.
- **`Q`/`T` spans** also need `accord.debug_execution=true`; profiling sets it.

### Tags

Every tag starts with `DX `.

| Tag | Emitted |
|---|---|
| `C <CoordinationKind> <txn>` | coordinator span per phase: Client, PreAccept, Propose, Stabilise, Execute, Persist, recovery kinds, HomeProgress/WaitProgress, … |
| `E <event> <txn> [extra]` | coordinator events: PreAccepted, Accepted, Executing `<path> deps=<n>`, Executed, Durable, Preempted, Timeout, Failed, LocalExecution, … |
| `R <event> <txn> s<store> [extra]` | replica events: PreAccepted, Accepted, Committed, Stable, ReadWaiting, ReadStarted, PreApplied, Applied, Reject… |
| `M Send\|Recv\|Reply\|Resp <type> <txn> n<peer>` | message events. Local delivery bypasses the sink, so it has no `M` events. |
| `Q`/`T <reason> <txn> s<store>` | store task queued (creation → run) / running |
| `K <SaveStatus> <txn> k<key> s<store>` | the transaction's update of that key's `CommandsForKey`; `K Unmanaged<mode>` for unmanaged registrations |
| `K Unblock <waiter> k<key> s<store> by <trigger\|->` | the key released the waiter. The trigger is the transaction whose update was running on that thread; `-` means e.g. a RedundantBefore update. |
| `K BlockedOn <dep> k<key> s<store> <status> <until> by <trigger>` | the key waits for `dep` to reach `status` |

### Harness

These changes are committed in this repository as `173aaa1`; `docs/` and `cassandra/jfr-analysis/` are still untracked.

- **Settings.** `exp.config` has `accord.debug_distributed_execution`, `accord.debug_distributed_execution_keys` and
  `accord.debug_distributed_execution_report` (default 10). `CASSANDRA_DX`, `CASSANDRA_DX_KEYS` and
  `CASSANDRA_DX_REPORT` override them for a single run.
- **Launch.** `start_cassandra_data_centers.py` (`distributed_execution_opts`) passes the flags only when profiling
  and only when non-zero. Like `cassandra.profiler`, they take effect when the cluster is created.
- **Capture.** `profiler.sh` logs the settings when a capture starts. After copying the captures, it writes
  `<run>_txnlatency.txt` (`TxnLatency --top <report>`) and `<run>_txntrace.txt` (`TxnTrace --slowest <report>`) over
  all nodes. That needs `cassandra/jfr-analysis` next to `profiler.sh` and a JDK 17+ on the machine running the
  scripts. The first DX run had no summary because `jfr-analysis/` (untracked) wasn't on that machine.
  `--slowest` picks start-up outliers if the capture includes client start; rerun with `--skip`.
- **Image.** The AWS image must be rebuilt from the branch (`ant artifacts`).

```bash
CASSANDRA_PROFILER=1 CASSANDRA_DX=32 CASSANDRA_DX_KEYS=8 ./cdf.sh --protocols=accord
cassandra/jfr-analysis/run.sh TxnTrace --slowest 10 logs/profiles/cdf/<run>_*.jfr
cassandra/jfr-analysis/run.sh TxnTrace --txn 1790867183887743 logs/profiles/cdf/<run>_*.jfr
cassandra/jfr-analysis/run.sh TxnTrace --key <token> logs/profiles/cdf/<run>_*.jfr
```

Suggested starting values on AWS are N≈32 and K≈8–16. Every event has a unique tag string, so N=K=1 is for short
runs only.

### Reading `TxnTrace`

**Timeline.** For each transaction it prints every event on every node, relative to the start of its Client span:
offset, duration, node, thread, tag. `*` stands for the transaction itself.

**Wait chains.** Then, for each replica (node and store), it prints:
- the last `Unblock` of the transaction there: when it happened, how long after its `K Stable` on that replica,
  and the trigger, with the trigger's own update that caused it (e.g. `its Applied at +7.29ms`);
- then the trigger's last release earlier on that replica, and so on, up to `--depth`.

"Released by itself" means nothing earlier held it on that key: it was waiting to become Stable, not on a
dependency.

**Key sampling only.** With only key sampling on there are no Client spans. The tool then lists the N longest waits
between Stable and the release instead.

**Things to check:**
- **Replica execution needs only Stable.** In theory replica execution only needs the coordinator's Stable. Check
  that the time from `R Stable` / `K Stable` to `K Unblock` is short when the release is "by itself", and see who
  holds it when it isn't.
- **Multiple releases.** A transaction can be released more than once on a key (to execute, then to apply). The
  chain takes the last one before the next step; look at the full timeline if a chain looks odd.
- **Clocks.** Cross-node offsets depend on clock sync (about 1 ms on AWS). Send/receive pairs give the real
  one-way delays.

### JFR diagnostic events (`benchmarking-spans`, `cde6531002d`)

DX shows *that* a message was late, not *where*. These events show the connection's state:

| Event | Period | Content |
|---|---|---|
| `cassandra.net.Outbound` | 100 ms (`.jfc`) | per peer × connection type: `pendingCount/Bytes` (Cassandra's queue), `flushingBytes` (handed to Netty, not yet flushed), `writable`, sent/overloaded/expired/error counts, connection attempts, TCP_INFO (`tcpRtt`, `tcpRto`, `tcpSndCwnd`, `tcpSndSsthresh`, `tcpUnacked`, `tcpLost`, `tcpTotalRetrans`, `tcpCaState`, …) and `soSndBuf`/`soRcvBuf` |
| `cassandra.net.Inbound` | 100 ms | per inbound connection: received counts, read throttling, TCP_INFO (`tcpRcvSpace`, `tcpRcvRtt`, …) |
| `cassandra.net.InboundPeer` | 100 ms | per peer: received but not yet processed (`scheduledCount/Bytes`), capacity in use |
| `cassandra.net.Backpressure` | each interval | an `EventLoopDelivery` unwritable (flushing above the high water mark) until it drains; queue at start/end, TCP_INFO at start |
| `cassandra.os.Tcp` | 100 ms | the container's TCP counters (`/proc/net/snmp`, `/proc/net/netstat`): retransmitted segments, RTO timeouts, loss probes, fast retransmits, drops |
| `cassandra.os.Pressure` (`9e194779f2f`, `1815902b430`) | 100 ms | the container cgroup's PSI stall totals (cpu/memory/io, some/full), `memory.current`, swap, `memory.events`, major faults, dirty/writeback; the host's major faults, swap-ins/outs, direct reclaim, CPU steal (total and most of one CPU per period) |
| `cassandra.os.Threads` (`1815902b430`) | 100 ms | JVM threads in uninterruptible sleep (`D`) and running, with each `D` thread's kernel wait channel |
| `cassandra.accord.Executor` | 100 ms | per executor: waiting / loading / running tasks, cache size |

- **Enabling.** `-Dcassandra.jfr.diagnostic_events=true` registers the hooks, in `MessagingService` (removed on
  shutdown, so dtest instances have their own). They are recorded only while a JFR recording enables them.
  - The harness uses async-profiler's `jfrsync` with `cassandra/diagnostics.jfc`.
  - The `.jfc` also enables GC pauses, safepoints ≥ 2 ms, `jdk.NetworkUtilization` and the container CPU and
    throttling events.
  - It disables JFR's own samplers and Cassandra's per-operation `cassandra.accord.cache.*` events, which default
    to enabled.
- **Naming.** Events carry `local`/`peer` addresses and Accord ids (`peerAccordId` = DX `nN`).
- **Harness.** `cassandra.profiler.jfr_events=1` (default; `CASSANDRA_JFR_EVENTS` overrides it) does three things:
  - `start_cassandra_data_centers.py` passes the flag;
  - `profiler.sh` copies the `.jfc` into each container and adds `jfrsync=`;
  - it writes `<run>_<node>.netstat.txt` with `tc -s qdisc` (netem drops) and the TCP counters at the start and end
    of each capture.
- **Checked:**
  - `ConnectionTest` 11/11, with and without the flag;
  - `FullAccordCQLTest` with `jfrsync` on JDK 11 and JDK 21: all events recorded, TCP_INFO populated over loopback;
  - a JDK 11-written merged file reads only with JDK 11's `jfr`, while JDK 21 writes and reads fine. The nodes run 21.

### How it was verified

These are local checks; `/tmp` is not persistent, so redo the overlay to re-test.

1. **Build the overlay.** The prebuilt jars are older than the branch, so put every changed source into the overlay
   explicitly. A partial overlay produced a `ClassFormatError` from mixing old and new classes. The
   `cassandra-run-tests` skill's `bin/overlay.sh` does the compile:
   ```bash
   J=$(ls build/apache-cassandra-*.jar); A=$(ls build/lib/jars/*accord*.jar)
   ( find src/java -name '*.java' -newer $J; find modules -path '*src/main/java*' -name '*.java' -newer $A;
     echo src/java/org/apache/cassandra/service/accord/debug/DebugDistributedExecution.java ) | sort -u > /tmp/all.txt
   OVERLAY=/tmp/overlay-dx $SK/bin/overlay.sh $(cat /tmp/all.txt)
   ```
2. **Run a dtest with the profiler attached.** This container is aarch64, so use the arm64 library from
   `lib/async-profiler-4.5.jar`:
   ```bash
   OVERLAY=/tmp/overlay-dx EXTRA_ARGS="-Daccord.debug_distributed_execution=1 -Daccord.debug_distributed_execution_keys=1 \
     -Daccord.debug_execution=true -agentpath:/tmp/linux-arm64/libasyncProfiler.so=start,event=ctimer,interval=10ms,wall=50ms,jfr,file=/tmp/dt.jfr" \
     $SK/bin/rundtest.sh org.apache.cassandra.distributed.test.accord.FullAccordCQLTest \
       testAcceptTransactionWithUpdatesToSamePrimaryKeyButDifferentColumns
   cassandra/jfr-analysis/run.sh TxnTrace --slowest 2 /tmp/dt.jfr
   ```
   In-JVM dtests share one JVM: `TxnTrace` takes node names from the `nodeN_` thread prefix.
3. **Results.**
   - The dtest passes with and without the flags.
   - Every tag kind is emitted.
   - Sampling at N=16 picked 6,249 of 100,000 TxnIds.
   - `AccordMetricsTest` fails identically without the changes ("keyspace system_metrics does not exist"), so it
     says nothing either way.

## 5. First DX run (simulated, `accord_5_a_20261008141911621920469`)

The setup is `cdf.sh` with `latency_simulation=1`: 5 Docker nodes on one host (Hanoi, Lyon, NewYork, Rotterdam,
SaoPaulo; netem delays from `emulate_latency.py`), `CASSANDRA_DX=1`, `CASSANDRA_DX_KEYS=1024`. It used the
`benchmarking` image (DX, legacy executor tags). The captures are in `results/profiles/`, with the nodes' logs as
`accord_node<id>.log` (node id N = DX `nN`).

- **Cost of DX=1.**
  - Every transaction is traced: ~4.7M DX spans per node per 100 s.
  - Each JFR is 440 MB instead of ~20 MB; 350 MB of it is the constant pool of unique tag strings.
  - ~8% of the JVM's CPU samples are in `DebugDistributedExecution`.
  - The tools still handle all five nodes in ~30 s with an 8–9 GB heap.
  - Use N≈32 for long runs. At DX=1 every traced transaction's `K` events are recorded, so key sampling wasn't
    exercised.
- **The `M` events are trustworthy.** `LinkDelays` medians equal the netem delays to within 0.2 ms on all 20 links
  (e.g. SaoPaulo→Hanoi 84.2 ms for 84 ms).
- **The 2.2–2.4 s outliers were messaging, not Accord.**
  - All of them were coordinated by SaoPaulo, in the first ~12 s, when its client started (12:22:26).
  - Rotterdam→SaoPaulo and Lyon→SaoPaulo climb to 2.2–2.6 s, then drain linearly over ~11 s.
  - Hanoi→SaoPaulo reaches ~450 ms; NewYork→SaoPaulo stays at 37 ms. So it's per connection, not SaoPaulo's receive
    side.
  - Rotterdam's `StatusLogger` at 12:22:31 shows `MessagingService n/a 0/2729`: 2,729 small messages queued outbound.
  - It coincides with the client start, a JIT peak, and a ~4.4 s concurrent ZGC cycle on every node.
  - Probably TCP congestion-window collapse under netem, not yet confirmed: collect `tc -s qdisc` drops
    (`netem_stats.sh`) and `ss -ti` cwnd/retransmits during a run.
  - Every link also has occasional maxima of +360–680 ms, roughly proportional to RTT. They look like retransmissions.
- **The steady-state tail is a hot-key convoy** (`TxnLatency --skip 15`).
  - The maximum is 339 ms. Of the slow transactions (≥ 2.5× median), only 0–0.3% per group had a late message.
  - Slow by path: MEDIUM writes 46%, ephemeral reads 3.5%, FAST writes 2%.
  - One key (`tid:1b:2599699261351934742`) accounts for 63% of the slow transactions, and five keys for 94%.
  - The wait chains show how. Writes on the hot key take the MEDIUM path (three WAN round trips). Each then waits
    100–150 ms after its own Stable for a write that *started later* to become Stable. Ephemeral reads that took
    such writes as dependencies wait for their Apply.
- **Tool gaps.**
  - A chain stops at a trigger that was released by becoming Stable. It doesn't follow why that took long (the
    trigger's own coordination).
  - In convoys, depth-4 chains resolve within ~1 ms without reaching the root; a convoy summary (N released within
    X ms, root Y) would help.
  - Without message ids, repeated messages of one type for one transaction are paired in time order: ~1.8% of `M`
    events stay unpaired.
  - In timelines, `K BlockedOn * …` doesn't say whether `*` is the waiter or the dependency. It's the dependency;
    the waiter isn't named.

### Second simulated run (`accord_5_a_20261008162336095457514`, DX=16): what the profiles rule out

- **The start-up backlog reproduces.** Rotterdam→SaoPaulo reached ~1.8 s and NewYork→Hanoi ~1.3 s, on different
  links from the first run.
  - All the links affected in either run are 90–170 ms RTT; the 3–6 ms links never are.
  - The delay plateaus for ~10 s and then drains.
- **Not the JVMs:**
  - safepoints ≤ 7 ms on every node (GC logs);
  - no gap ≥ 20 ms in any node's sampling, so no CFS throttling or process freeze;
  - host CPU (`machineTotal`, 96 hardware threads) ≤ 77%, and only 7–39% when the backlog had already reached 1.4 s;
  - the receiving event loops (the thread delivering `M Recv` for that peer) and the sending ones use ≤ 5–7% of a
    core throughout.
  - So messages sat in the sender's `OutboundConnection` queue (the earlier run's StatusLogger: 2,729 pending) with
    the event loop idle: the socket wasn't draining.
- **Steady-state spikes** (+150–290 ms, ≈ the 200 ms minimum RTO plus RTT) come in clusters on a single link each,
  so they look like per-connection TCP loss recovery rather than a host-wide stall.
- **Connection timing.** The internode connections were established before `emulate_latency.py` added the netem
  delays. At 14:26:13 both ends of each SaoPaulo connection log it within 0–2 ms; reconnections at 14:28:36 show the
  ~37 ms one-way delay.
- **Leading hypothesis.** The connections then sit idle until the clients start, so Linux restarts slow start
  (`tcp_slow_start_after_idle`). If the client-start burst loses packets, cwnd collapses. Congestion avoidance then
  grows it by ~1 segment per RTT, i.e. over tens of seconds on a 90–170 ms RTT link and far faster on a 6 ms one.
  That fits the backlog appearing only on long-RTT links, its plateau and its ~10 s drain.
- **Still undecided, between:**
  - (a) loss → cwnd collapse;
  - (b) a window/buffer limit (cwnd or autotuned buffers sized for the zero-latency path, at ~window/RTT);
  - (c) netem itself.

  The JFR diagnostic events and the netstat snapshots distinguish them. The run's auto-summary also failed again:
  `jfr-analysis/` isn't on the run machine.

### Third simulated run (`accord_5_a_20261008220048136496055`, DX=16, JFR diagnostic events)

The image was `benchmarking-spans` (new executor tags), and every capture had the JFR events and netstat snapshots.
`SaoPaulo1.gc.tgz` is missing; the cause is unknown.
- **Start-up backlog (20:04:03–11, before YCSB's measured phase at 20:04:30) is solved.**
  - netem dropped nothing.
  - TCP retransmits were 0.005% of segments, all fast retransmits matched by a D-SACK (spurious, from mild
    reordering).
  - On the worst connection (Rotterdam→SaoPaulo small: queue ≤ 2,724, 37 backpressure intervals, 7.7 s) every
    interval had unacked = cwnd.
  - At the first traffic, RTT jumped from 0.1 ms (measured before netem) to 89 ms, and `ssthresh` was set to 16 with
    no loss. That is CUBIC's HyStart leaving slow start because RTT rose far above the stale minimum.
  - cwnd then took ~5 s to reach the ~180 segments that 2.7 MB/s needs at 94 ms. It's an artefact of applying netem
    under connections that had already measured a near-zero RTT.
  - Fix: apply netem before the nodes connect, or reconnect afterwards.
- **The measured phase's 1.0–1.3 s maxima are one cluster-wide event at 20:04:36.7.**
  - All five JVMs had a 0.5–0.8 s time-to-safepoint or all-threads handshake: Hanoi 493, Lyon 702, NewYork 530,
    Rotterdam 708, SaoPaulo 770 ms. All ended within 20:04:37.63–37.73.
  - Meanwhile CPU was near zero and executor queues empty. PreAccept/GetEphemeralReadDeps work started at ~36.70
    completed only after the stall.
  - Separate JVMs ending together means a host-level cause.
  - `mlockall(MCL_CURRENT)` succeeds on every node, so the pre-touched heap can't be paged out. Memory mapped later
    can be, though: the Accord journal segments in `/tmp`, SSTable mmaps, later direct allocations. The containers
    allow swap (25.6 GB limit plus 25.6 GB swap; host 187.5 GB for 5 nodes and 5 clients).
  - Which thread held up each safepoint isn't recorded. Thread-level "silence" in the samples isn't evidence: idle
    threads' wall samples are batched.
  - Next run, `-XX:+SafepointTimeout -XX:SafepointTimeoutDelay=200` names the threads (with OS id and CPU time) in
    `gc.log`. `cassandra.os.Pressure` records the container's PSI stall times, memory events, major faults and the
    host's swapping every 100 ms.
- **`.jfc` fix:** under `jfrsync`, async-profiler leaves `jdk.JVMInformation`, `jdk.InitialSystemProperty`,
  `jdk.CPULoad`, `jdk.CPUInformation`, `jdk.OSInformation` and `jdk.GCHeapSummary` to JFR. They're now enabled, with
  `jdk.ZAllocationStall`, `jdk.ZPageAllocation`, `jdk.ContainerMemoryUsage`.

### Fourth simulated run (`accord_5_a_20261009010807291318538`): synchronised start and the new instruments

- **Synchronised start works.**
  - All five clients started at 23:11:30.12–30.79, with launches 13–15 s ahead.
  - Their measured phases began within 48 ms of each other (23:12:02.460–.508), against 3.5 s before and 38 s on AWS.
- **New instruments all working:**
  - the recording context is back in the JFR;
  - `cassandra.os.Pressure` is recorded, with real PSI values;
  - `SafepointTimeout` is on.
- **Measured-phase maxima 0.49–0.81 s** (previous run 1.26–1.38 s). There was no cluster-wide stall; one smaller one at
  23:12:47.6 (Hanoi 308 ms time-to-safepoint, SaoPaulo 70 ms, same moment).
  - `SafepointTimeout` named the threads: `AccordExecutor[0,4]`, `[0,7]`, `[0,10]`, `[0,13]`.
  - Each was mid-task, then went **unsampled for ~364 ms** with no CPU time. All resumed together at 47.952 in an array
    copy under `Journal.asyncWrite`.
  - Over the same interval the container's I/O, memory and CPU pressure, its major faults, CPU throttling and the
    host's swap/reclaim counters were all flat. So it's not I/O, paging, CPU quota or swapping.
  - What fits: a hypervisor descheduling vCPUs (the simulation host is a 96-vCPU VM running 10 JVMs). It could also
    be a kernel wait PSI doesn't count.
  - `1815902b430` adds host CPU steal time (`cassandra.os.Pressure`) and `cassandra.os.Threads` (JVM threads in `D`
    state with their kernel wait channel) to tell these apart.
- **`jfrsync` cost:** each capture start triggers `RedefineClasses` VM operations, 72–181 ms on every node, at capture
  start and again ~17 s later. That's JFR instrumenting its Java-level event classes. They fell in warmup here. The
  0.5–0.8 s stalls appeared only in runs with `jfrsync`, but this run's evidence doesn't implicate JFR itself; worth
  checking on AWS with `NetEvents`.

## 6. Open items and next steps

1. Rebuild the AWS image from `benchmarking-spans` (DX plus the executor span labels). Run with
   `CASSANDRA_DX=32 CASSANDRA_DX_KEYS=8`, then use `TxnLatency --skip 30` and `TxnTrace` on the > 600 ms reads to
   confirm (or refute) the Osasco → Ashburn → Asia chains in §2.8.
2. Client start-up stagger (§2.3): **implemented, not yet run.**
   - `run_benchmark` picks a start time `ycsb.start_delay` seconds ahead (default 90 on real machines, 15 simulated;
     `YCSB_START_DELAY` overrides it).
   - Each client runs the image's own entrypoint through `_ycsb_start_at_script` (in `run_benchmarks.sh`, passed inline as `sh -c`), which waits until then, so the launcher
     waits only for its `Scheduled YCSB start` line.
   - The clients' `docker logs` show `YCSB starting at <time>`. Their `.dat` files' `0 sec` lines should then agree
     to within JVM start-up (~1 s), not ~40 s.
   - A warning is logged if launching overran the delay.
3. Durability stalls (§2.9): cap or deprioritise the `IOTaskSave` burst after `ensureDurable`.
4. Simulated runs (§5):
   - The start-up HyStart/cwnd backlog ends before YCSB's measured phase, so it only distorts the warmup. It exists
     because `run_benchmarks.sh` applies netem after the load phase (deliberately, for a fast load), under
     connections that measured a near-zero RTT; their minimum RTT is remembered for `net.ipv4.tcp_min_rtt_wlen`
     (300 s). To remove it: reconnect the internode connections after `emulate_latency`, lower
     `tcp_min_rtt_wlen` in the containers, or keep the warmup longer than ~10 s after the last client starts.
   - For the cluster-wide safepoint stalls, rerun and read `gc.log` (the `SafepointTimeout` thread lists) next to
     `NetEvents --bucket 100` (per-node PSI and major faults).
   - If page faults are confirmed: disable swap for the containers (`memswap_limit = mem_limit`), move the journal off
     `/tmp`, or give the containers headroom.
   Hot-key MEDIUM-path convoys dominate the steady-state tail there.
5. Possible DX extensions:
   - a waiting-on-dependency edge for non-key waits;
   - coordinator-side per-replica reply latency without the id maps (and message ids for exact pairing);
   - client-operation ↔ TxnId correlation (return the TxnId to the driver);
   - JFR custom events instead of string tags, if the tag volume is a problem (it is at DX=1: §5);
   - in `TxnTrace`: follow a trigger released by its own Stable into its coordination, and summarise convoys.
6. Fix `cassandra_fast_path.sh` (JMX), and keep `gcp.csv` lookups from silently falling back.
7. Commit `cassandra/jfr-analysis` (and `docs/`), so the run machine has the tools `profiler.sh` uses. Consider
   folding the absolute-time tools (`WindowAbs`, `Activity`) into the relative-time originals.
