# The Calvin micro-benchmark in YCSB: implementation quality and critique

**Scope.** The Calvin micro-benchmark as implemented in this repository's YCSB fork
(`~/Implementation/YCSB`) and as driven by `calvin_ubench.sh`, `run_benchmarks.sh`,
`parse_ycsb_to_csv.sh` and `calvin_ubench.py` in `~/Implementation/cassandra-evaluation`.

**Reference.** Thomson, E., Bieniusa, E., Da Silva, R., et al. "Fast Database Writes at Scale."
SIGMOD 2012. §6.2 (microbenchmark) and Figures 5 and 6. The companion analysis of the paper's
scalability figures — including the digitised Figure 5 curves and a fitted cost model — is in
[`calvin-scalability-analysis.pdf`](calvin-scalability-analysis.pdf), sources in
[`calvin-scalability-analysis.tex`](calvin-scalability-analysis.tex).

---

## 0. Verdict in one paragraph

The *workload generator* is good work: it is a faithful, well-parameterised, well-tested
reimplementation of the paper's microbenchmark, and several of its design decisions
(per-machine scoping of the contention index, constant records-per-partition, partitioner that
mirrors each system's placement) are exactly right and non-obvious. The *harness* around it is
also careful and unusually well documented. The problems are not in the transaction generator —
they are in three places: (i) the record is 4 bytes where Calvin's was 100, so the memory regime
is gone and no parameter can restore it; (ii) the deployment is a 3-datacentre, geo-replicated,
WAN-emulated system, which means *every* transaction is distributed and the paper's central
result — that the distributed penalty is confined to multipartition transactions — is not
testable in this configuration; and (iii) the failure rate, which is the single most diagnostic
quantity in a contention experiment, is computed correctly by the parser and then discarded by
the plotting script. There is also one outright arithmetic bug in the scale figure's y-axis.

---

## 1. What is being reproduced

The paper's §6.2 microbenchmark, in full:

> each transaction reads 10 records of its home partition, one of them hot; a multipartition
> transaction reads 5 records, one of them hot, on each of 2 partitions; it checks that the sum of
> their counters is non-negative and increments each counter.

The contention index `C` is *per machine*: "the fraction of the total 'hot' records that are
updated when a transaction executes at a particular machine." At `C = 0.0001` a transaction picks
1 of 10 000 hot records, so at most 1000 transactions per machine can be in flight; at `C = 0.01`,
at most 100. Figure 5 varies `n` from 1 to 100 machines at two contention levels and two
multipartition proportions.

The paper specifies the *number* of records touched, the hot/cold split and the contention
index. It never states the total record count, the record size, the key encoding, the
replication factor or the client model.

## 2. How this review was done

Claims below are backed by one of:

* **E** — empirical: a probe compiled and run against the built classes (`core/target/classes`),
  or the TestNG suite executed (17/17 pass, `TestCalvinWorkload`).
* **R** — read: source and shell scripts, with `file:line` citations.
* **T** — traced: the full measurement path from `ClientThread` through `DBWrapper` to
  `Client.exportMeasurements` and `parse_ycsb_to_csv.sh`, to establish what the reported
  throughput and latency actually count.
* **P** — provenance: the paper's own figures, digitised from the PDF content streams.

---

## 3. Part A — the YCSB workload (`CalvinWorkload`, `CalvinPartitioner`)

### 3.1 What it gets right

These are not trivial, and several would be easy to get wrong.

**The contention index is scoped per machine, not globally** (`CalvinWorkload.java:126`):

```java
hotRecords = Math.round(1.0 / contentionIndex);
```

so `C = 0.0001 → H = 10 000` hot records *in each partition*, not 10 000 in total. This is the
correct reading of the paper, it was verified independently against two separate codebases, and
getting it wrong is the single easiest way to make a Calvin replication look bad. `workloadcalvin`
documents it explicitly.

**`mod` reproduces Calvin's placement**: key number `i` on partition `i mod N`
(`CalvinPartitioner.java:92-111`), with `size(p) = (recordCount - p + N - 1) / N` and
`keyNum(p, i) = p + i * N`. Checked for `recordCount = 10^6, N = 3`: sizes 333 334 / 333 333 /
333 333, maximum key numbers 999 999 / 999 997 / 999 998 — exact partition, no overlap.

**One hot record per partition is deterministic at the paper's settings.** `nextHotCount`
(`:236-240`) computes `h = 0.1 × 10 = 1.0`, so `floor = 1` and the fractional part is `0.0`; the
branch `random.nextDouble() < 0.0` is never taken. Every transaction, single- or
multipartition, touches exactly one hot record per partition. This matters and it holds.

**Multipartition structure matches the paper**: `txnSize / parts.length` records per partition
(`:271`), i.e. 5 per partition on 2 partitions for a `k = 10` transaction; 1 hot and 4 cold on
each. Distinct partitions are drawn by rejection sampling (`:250-262`), and `Arrays.sort` on the
key list (`:290`) gives deadlock-free lock ordering for lock-based stores, which is a real
portability concern that was thought about.

**The partitioner mirrors the system's own placement**, which is what makes the comparison fair:
`murmur3` slices Cassandra's token ring into `N` equal arcs and is paired with
`cassandra.fixed_tokens=1`; `range` splits into contiguous key blocks, forces zero padding so
string order equals numeric order (`:178-180`), and is paired with `cockroachdb.partitions=N`;
`tiga` reproduces Tiga's own hash (`CalvinPartitioner.java:82`). `configure_deployment`
(`calvin_ubench.sh:262-284`) sets the corresponding store-side knobs. The zero-padding fix is
applied *before* the partitioner is constructed (`:182` comes after `:178-180`), which is the
order-dependent detail that is easy to get wrong.

**Validation of the configuration is strict and legible.** `init` rejects a hot pool smaller than
the hot records a transaction needs, a cold pool smaller than the cold records it needs
(`:187-196`), a span larger than the partition count, a `txnsize` not divisible by the span, and
more hot records per partition than records per partition (`:161-175`). Each throws a message
naming the offending quantity. It also forces `mpProportion = 0` when there is a single
partition (`:157-160`), matching the paper's observation that a multipartition transaction is
meaningless with one machine.

**The effective configuration is echoed** as `[CONFIG]` lines (`:198-204`). This is worth more
than it looks: it is the reason this review could reconstruct exactly what ran, without guessing
at defaults.

**The test suite is strong** — 17 tests, executed here and all passing: key generation, the
fractional hot count, the contention index, both pool-size rejections, all four partitioners,
the Murmur3 token, the Tiga key, token slicing, range padding, single- and multi-partition
structure, the home partition, and the single-partition-ignores-multipartition rule.

**Where the harness gets the parameters right.** `calvin_ubench.sh:414-419` parses the
saturation and scale log directories separately, and `records = 1 000 000` per partition is
recomputed as `recordcount = records * npd` at every scale point, with the dataset reloaded per
`npd` (`do_create_and_load=1` inside the `npd` loop, `scale_phase`). Holding the per-partition
pool constant while the partition count grows is the paper's convention and it is what makes the
contention index mean the same thing at every `n`.

### 3.2 Defects and gaps

Ranked by consequence for the experiment, not by severity in the abstract.

#### A1 — The record is 4 bytes; Calvin's was 100, and no parameter can change it

`ClosedEconomyWorkload.buildValues()` (`:457-463`):

```java
HashMap<String, ByteIterator> values = new HashMap<>();
values.put(DEFAULT_FIELD_NAME, new StringByteIterator("0"));
return values;
```

It never touches `fieldLengthGenerator` or `fieldCount`. `CalvinWorkload` does not override it
(zero occurrences of `buildValues`, `fieldLength` or `ByteIterator` in the file). The schema
agrees: `cassandra/ycsb.sh:51-54` creates `usertable (y_id VARCHAR PRIMARY KEY, field0 INT)` for
`CalvinWorkload`, with the comment "Numeric counters (the transactions update them with += 1)".

So `-p fieldlength=4000` and `-p fieldcount=1` (`run_benchmarks.sh:336`, with
`fieldlength=${fieldlength:-4000}` at `:207-208`) are inert on this path. Measured: `buildValues()`
returns a single 1-byte payload, stored as a 4-byte integer.

| | Calvin C++ | this harness |
|---|---|---|
| value | `kRecordSize = 100` B, 8 touched | `field0 INT`, **4 B** |
| key | `IntToString(k)`, bare decimal, 1–10 B | `user00013310`, 11 B |
| records per partition | 10⁷ | 10⁶ |
| declared payload per partition | **1.00 GB** | **~15 MB** |

That is roughly 10× fewer records and 25× smaller records, i.e. ~250× less data per machine, on
hardware (`machine=e2-highcpu-16`) that already has 2× Calvin's core count and more RAM than the
paper's 7 GiB. The working set lands much closer to the last-level cache, so the memory-hierarchy
term that dominates the fitted $D_\infty \approx 2.4$–$19.8$ ms in the companion analysis is
substantially absent.

This is not a defaults complaint that a `--fieldlength` fixes: the workload pins the record to one
scalar and the schema pins the column to `INT`. Both would have to change. The shape Calvin used —
a record large enough that reading and forwarding it costs realistically, with a small modified
prefix — is reproducible by padding `field1..fieldN` on insert and leaving `field0` as the
counter; `fieldcount` is already plumbed through the whole stack and simply unused.

Note that the *hot pool fraction* also differs: `H = 10⁴` out of 10⁶ records per partition is 1%,
where Calvin's C++ had `H = 10⁴` out of 10⁷, i.e. 0.1%. The contention index itself — the x-axis
of Figure 5 — is identical, so the x-axis is right; only the amount of cold data backing it up
differs, which feeds back into A1.

#### A2 — `isConsistent` cannot detect a lost update

`CalvinWorkload.java:316-319`:

```java
return countedSum >= 0 && countedSum % txnSize == 0 && countedSum <= expectedSum(count);
```

A lost update makes `countedSum` *smaller* than `expectedSum`, and the check is `<=`, so it
passes. The upper bound is documented as deliberate (`:307-309`, "some of the `count` attempted
transactions may abort"), but as written it also tolerates exactly the corruption the validation
exists to catch. `countedSum >= 0` is tautological (counters start at 0 and only increment) and
`% txnSize == 0` is the one term with real content — it does catch a transaction that committed
some of its records, which is worth keeping. The upper bound should be an equality modulo the
number of observed aborts, or the abort count should be threaded into `expectedSum`.

#### A3 — No retry on a failed transaction

`doTransactionReadModifyWrite` (`:297-305`) calls `checkAndIncrement` once and returns
`.isOk()`. A rejected transaction becomes `Status.ERROR` and is gone. Neither remaining atomic
binding is the paper's blocking model:

* `CassandraCQLClient:877-948` builds Accord's `BEGIN TRANSACTION … LET … IF … END IF … COMMIT
  TRANSACTION` at `SERIAL` consistency — optimistic, rejects on conflict.
* `CockroachDBFlavor` issues one atomic CTE in an explicit CockroachDB transaction — optimistic,
  raises a retry error on serialization failure.

Under contention, which is the entire subject of the experiment, the measured behaviour is
therefore dominated by the *abort rate of an optimistic protocol*, not by Calvin's deterministic
scheduling. That is a legitimate thing to measure — but it is a different experiment, and its
most important output (the abort rate) is discarded downstream. See C1.

#### A4 — Per-transaction allocation on the hot path

`doTransactionReadModifyWrite` allocates, per transaction: a `long[10]`, a `String[10]`, a
`HashSet<Long>` with boxed `Long` values, and one fresh `String` per key from
`buildKeyName` (`:221-230`; with `zeroPadding <= 1` this delegates to `CoreWorkload.buildKeyName`,
i.e. a string concatenation). At the paper's ~26 000 transactions/s/machine that is ~500 000
short-lived `String`s per second per node, plus ~10 boxed `Long`s and 2 arrays per transaction.
At the harness's much higher node throughput this is the client, not the store, that will run out
of allocator throughput — and YCSB runs the client in the same container budget as everything
else (`ycsb_cpus=8`). The transaction itself is the unit of work the paper's model is built on,
so this is a real per-transaction constant competing with the store's serialisation cost.

#### A5 — `ThreadLocalRandom.current()` per transaction (`:299`)

Correct and appropriate for a load generator. Noted only because `Random` is threaded through
`nextHotCount`/`nextTransactionPartitions`/`nextTransactionKeyNums`, which makes those methods
deterministically testable — a genuine virtue — and the production path throws that away by
passing a non-seedable source. The test suite depends on the injectable form, so this is a
trade-off, not an oversight.

#### A6 — `maxCold` is validated against the single-partition requirement even when the workload is multipartition (`:161-175`)

`maxCold = txnSize - floor(hotFraction × txnSize)` = 9, and the cold-pool check at `:192` requires
9 cold records per partition. A multipartition transaction with span 5 needs only `10/5 - 1 = 1`
cold record per partition, but a configuration with 1–8 cold records per partition is rejected.
Note the asymmetry: `maxHot` *is* checked against `txnSize / mpSpan` when `mpProportion > 0`
(`:171`), `maxCold` is not. Harmless at the shipped settings, and it only ever rejects rather
than silently misbehaves — but it is an inconsistency.

#### A7 — `Hashed` allocates `byte[recordCount]` and does two full passes at `init` (`CalvinPartitioner.java:170-186`)

At `recordcount = 4 × 10⁶` that is 4 MB and 8 × 10⁶ `String` constructions and hashes, per client
JVM, at startup — including one client per datacentre, three times, on every scale point where the
dataset is reloaded. Correct, and it buys an exact key list per partition (which is what makes the
hot pool well-defined for a hash partitioner at all). It is load-time, not run-time. Worth knowing
the cost if `--records` is ever raised.

#### A8 — The three bindings are not equally faithful, and nothing says so

| binding | `checkAndIncrement` | used by the harness |
|---|---|---|
| `CassandraCQLClient` (Accord) | atomic, multi-partition, per-record predicate | yes (`accord`) |
| `CockroachDBFlavor` (via `JdbcDBClient`) | atomic, single CTE, sum predicate | yes (`cockroachdb-opt`) |
| `TigaClient` | native, atomic | yes (`tiga`) |
| `DB` default | **not atomic**: `k` reads then `k` updates, no `start()`/`commit()` | only as a fallback |

The Accord/Cassandra predicate is `∀i: c_i ≥ 0` rather than `Σc_i ≥ 0` (`:902-911`). Because
counters start at 0 and only ever increment, the two are tautologically equivalent *in this
microbenchmark*, so this is a spec-fidelity difference and not a behavioural one — but it is a
difference, and it means "same transaction" is not literally the same predicate across protocols.

The `DB` default is a trap for anyone who adds a protocol: `JdbcDBClient` falls back to it when
the flavor returns null, and it will silently produce lost updates under concurrency. The harness
avoids it by restricting `protocols` to the three that implement the operation atomically
(`calvin_ubench.sh:146`, with the comment "The transactional systems whose YCSB client implements
checkAndIncrement"). That is the right call and it is documented — but the safety depends on a
list in a shell script rather than on anything in the code.

#### A9 — Test coverage has two specific holes

**The conservation assertion is a tautology.** `TestCalvinWorkload.runWorkload` (`:363-411`)
asserts `finalSum == txnSize × opsPerClient × numClients` after 4 concurrent client threads. It
runs against `BasicTransactionalDB`, whose `start()` acquires a `static final ReentrantLock`
(`BasicTransactionalDB.java:33,53-55`) and whose `commit()` releases it. `ClientThread` brackets
every transaction with `db.start()`/`db.commit()`, so the entire test runs under a **global
mutex** — the transactions are serialised, there is no concurrency to lose an update to, and the
assertion cannot fail for the reason it exists. The test does validate the workload (key
selection, distinctness, hot/cold structure, partitioner, expected-sum accounting), and 17/17 pass
when executed. But no test exercises atomicity under interleaving.

**The atomic binding path is untested.** `JdbcDBClientTest.checkAndIncrementTest` (`:456-485`)
drives `checkAndIncrement` through the JDBC transaction, i.e. through the *default non-atomic*
implementation, and asserts `Status.OK` twice. `CockroachDBFlavor`'s single-CTE path and
`CassandraCQLClient`'s Accord transaction are not covered by any test in the tree.

#### A10 — `mod` — Calvin's own placement — is never exercised

`calvin_partitioner` (`calvin_ubench.sh:252-259`) maps `accord → murmur3`, `cockroachdb* → range`,
`tiga* → tiga`, and only the `*)` fallback yields `mod`. All three live protocols take an explicit
branch, so `CalvinPartitioner.Mod` is dead code in this harness. That is correct for the protocols
under test, and it is worth stating in the figure caption: these are comparisons *against*
Calvin's design, not measurements of Calvin.

---

## 4. Part B — the harness (`calvin_ubench.sh`, `run_benchmarks.sh`)

### 4.1 What it gets right

The scripts are unusually well commented, and the comments are accurate — I checked several
against the code. `calvin_ubench.sh:3-31` states the microbenchmark, both phases, the partition
mapping and the parameter defaults in prose before any code runs, which is exactly the document a
reader needs. `run_benchmarks.sh:158-167` explains *why* the YCSB container is CPU-capped
(driver connection pools sized from `availableProcessors()` opened ~96 channels per node against a
500 ms init-query budget on a 96-core host) — a real incident, correctly diagnosed and fixed.
`:316-325` similarly explains a driver-level prepare-on-all-nodes behaviour change and the
225–325 ms first-interval effect it caused. `warmupexecutiontime` is applied to every run phase
unless the caller overrides it (`:130-133`).

Specific correctness wins worth recording:

* The client ramp is `c = (3c+1)/2`, i.e. ×1.5 with integer rounding, matching
  `latency_throughput.sh` as the header claims.
* `npd = 1` breaks out of the `mp` loop (`:395`) and `plot_scale` reuses that single point for
  every `mp` line (`calvin_ubench.py:204-218`) — correct, because with one node per DC every
  transaction is single-partition and the proportion has no effect.
* `drop_unsound_rows` (`utils.py:23-67`) excludes non-positive measurements, prints the count and
  names a few offending configurations on stderr. Its docstring states the reason precisely:
  "keep such rows out of means, sums and medians, where they are indistinguishable from a
  slow-but-working system." That is the right instinct, applied at the wrong layer (see C1).
* The dataset is reloaded per `npd` — necessary, since changing `cassandra.fixed_tokens` or
  `cockroachdb.partitions` moves records — but *not* per `mp`, avoiding four redundant reloads per
  scale point.
* `--test` mode halves `records` to 20 000 and explains why that is still sufficient: at
  `CI = 0.0001` the hot pool is 10 000 records and the cold pool must still supply 9 per
  transaction (`:199-208`). I verified the arithmetic: 20 000 − 10 000 = 10 000 ≥ 9. ✓

### 4.2 Defects

#### B1 — The deployment is geo-replicated and WAN-emulated, so every transaction is distributed

This is the most consequential property of the harness, and it is not stated as a limitation
anywhere.

* `nodes=3` is the **datacentre count**, not the node count (`calvin_ubench.sh:164` passes it as
  `run_benchmark`'s `num_dcs` argument, `run_benchmarks.sh:66`, resolved at `:397`).
* `cassandra_create_keyspace` (`cassandra/ycsb.sh:5-23`) builds
  `NetworkTopologyStrategy` with **one replica per DC**: `'Hanoi': 1, 'Lyon': 1, 'NewYork': 1`.
  Three replicas total, one in each DC. (`durable_writes = false`, which is appropriate for a
  microbenchmark and consistent with Calvin's in-memory assumption.)
* `cockroachdb_create_usertable` (`cockroachdb/ycsb.sh:20-26`) sets
  `num_replicas = num_dcs = 3`, one per DC.
* `latency_simulation=1` and `emulate_latency` apply `tc` to the WAN links; the RTT/OWD matrix in
  `docs/tiga.md` is 89/128/60 ms between the three sites.
* `checkAndIncrement` in the Cassandra binding hard-codes
  `setConsistencyLevel(QUORUM)` (`:929`), and QUORUM over 3 replicas across 3 DCs is a cross-DC
  round trip. The consistency levels configured by `run_benchmarks.sh:275-285`
  (`SERIAL`) do not apply to this operation.
* All client traffic originates in DC 1: `--network container:${nearby_database}` with
  `nearby_database="${first_dc}1"`, and for Cassandra/Tiga a single contact point
  (`run_benchmarks.sh:282`). CockroachDB spreads the client across its local gateways and one
  remote backup (`:244-260`).

So a "single-partition transaction" — 1 hot and 9 cold records in one partition, entirely local
in Calvin — is a 10-row atomic transaction whose replicas span three continents. And DC 1 carries
strictly more load than DC 2 and DC 3: it hosts the client, and (for accord and Tiga) it is the
coordinator for every transaction.

Consequence: the paper's headline mechanism — that the distributed penalty is incurred *only* by
multipartition transactions, which is why 10%-distributed and 100%-distributed curves diverge so
sharply in Figure 5 — cannot be observed here, because there is no configuration in which a
transaction is not distributed. The `mp` axis still varies the *number of participants per
transaction* and that remains a meaningful comparison, but it is not the paper's comparison.
Figure 5's abscissa is also "number of machines in the data centre", and here `scale_values` varies
nodes *per DC* inside a fixed 3-DC deployment, so the total machine count runs 3 → 6 → 12.

This is a property of the shared harness (the geo deployment is used by `closed_economy.sh`,
`conflict.sh`, `cdf.sh` and others), so it is not a defect in `calvin_ubench.sh` specifically — but
`calvin_ubench.sh` is the one script whose whole purpose is to reproduce a **single-datacentre**
figure, and it should say so, or offer a `--single-dc` mode.

#### B2 — The per-node y-axis of the scale figure is 3× too large

`calvin_ubench.py:270`:

```python
f.write(f"        {npd} {tput / npd if per_node else tput:.2f}\n")
```

`tput` is the sum of `[OVERALL] Throughput` over the datacentre rows (`:221`, grouped and summed);
`npd` is nodes per datacentre. So the plotted value is *aggregate per node across the whole
deployment*. The intent, stated in the module docstring at `:18` and in the axis label at `:257`,
is "the throughput per node of a data center" — Figure 5's quantity. The correct divisor is
`ndc × npd`, and `ndc` is available in the CSV as the `nodes` column, which `plot_scale` never
reads.

With `ndc = 3` the bottom row of the scale figure is inflated by exactly 3. The saturation figure's
x-axis has the same aggregation but says so honestly ("summed over the data centers", `:134`), so
the defect is confined to the scale figure's per-node row — which is precisely the row meant to be
compared against Figure 5.

#### B3 — Client count is scaled linearly while per-node capacity is falling

`scale_clients_for` (`:353-368`) takes the peak found at `saturation_nodesperdc = 1` and multiplies
by `npd`:

```bash
per_node=$(( (peak + saturation_nodesperdc - 1) / saturation_nodesperdc ))
echo $(( per_node * npd ))
```

Figure 5's own point is that per-node throughput *declines* with `n` — 26 079 tx/s at `n = 1` down
to 13 864 at `n = 100` at low contention, and 26 029 → 5 268 at 100% distributed. So a client
count that grows linearly in `n` walks the deployment up its own saturation curve as `n` grows. The
measured near-linearity will be worse than Calvin's by construction, and the harness cannot
distinguish that from a genuine scaling result.

The saturation sweep itself is a good idea and arguably better than Calvin's — Figure 5's y-axis
is a saturation quantity and the paper never says how it found that point. The problem is only that
it is run once, at one node count, and extrapolated. Running it per `npd` would remove the confound
at the cost of a longer sweep.

#### B4 — The peak is calibrated at 10% multipartition and applied to 100% (`:223`, `:305-348`, `:386`)

`saturation_mp` is the first element of `mp_values`, i.e. 0.1. The 100%-MP scale runs therefore
start from a client count chosen for the much cheaper configuration and are pushed past their own
saturation point. That is the right way to expose a slowdown — Figure 6 is a slowdown plot — but
combined with `plot_scale`'s `max` over the two client counts tried (`:221`) it means the 100%-MP
curve reports the *better* of two over-driven loads, which biases against seeing the penalty.

#### B5 — The Pareto stop condition is one-step and noise-sensitive (`:335`)

```bash
if [ "${prev_latency}" -ge 0 ] && [ "${latency}" -gt "${prev_latency}" ] \
   && [ "${tput}" -lt "${prev_throughput}" ]; then break; fi
```

A single noisy interval pair ends the ramp, so a higher peak that happened to follow a dip is
never measured. `peak_clients` is a proper argmax over the sweep and survives this, so the
*reported* peak is the best point seen — but the sweep may stop before the curve turns.

#### B6 — The stop criterion and the plotted latency are different statistics

`global_tput_latency` (`:238`) reports `max` over measurement intervals of `AverageLatency(us)`,
divided by 1000 and truncated by `int()`. `calvin_ubench.py:86` plots `p50`. The client count is
selected on a max-of-averages and the curve is a Pareto frontier of (throughput, median). A
max-of-average is the right choice for detecting saturation and the median is the right choice for
display, but the frontier can then extend past the true peak, and `int()` truncation is
gratuitous.

#### B7 — `replication_factor=3` is a dead parameter (`:165`)

It is threaded through `run_benchmark` into `cassandra_create_keyspace` and
`cockroachdb_create_usertable`, and **neither uses it**: the first hard-codes one replica per DC,
the second sets `num_replicas = num_dcs`. The effective replication factor is 3 either way, so
nothing is wrong today — but the parameter reads as a control and is not one.

#### B8 — The client is closed-loop; YCSB's intended-throughput measurement is unused

`ops_per_thread = 0` (`:166`) → `operationcount = 0` → each of the `clients` threads issues
transactions in a loop until `maxexecutiontime = 60`. Concurrency is therefore *imposed* as
`clients × ndc` in flight. Calvin's own client is open-loop: it issues at a fixed rate regardless
of completion. This is not a defect — the saturation phase is a legitimate substitute, and B3 is
the real problem — but it means "throughput" here is always "throughput at a closed-loop offered
concurrency".

Separately, `ClosedEconomyWorkload.doTransaction` (`:495-514`) never calls
`measurements.measure` / `measureIntended`; latency is measured per DB operation by `DBWrapper`.
So there is no intended-throughput series at all. YCSB's open-loop machinery is not dead in this
tree — `tiga_openloop.sh` and the `tiga.openloop.*` properties exist and `run_benchmarks.sh:120`
already filters them for the load phase — it is simply not offered for this benchmark. Wiring it
in would let the contention-window argument be tested directly rather than inferred.

#### B9 — Smaller items

* `parse_ycsb_to_csv.sh` is invoked on `logs/calvin_ubench/*.dat`, which also contains
  `*_fast_path_ratio.dat` sidecars (`:414-416`). They do not match the parser's patterns so they
  are harmlessly skipped, but the glob is loose.
* `scale_phase` runs `base` and `1.5 × base` clients for every `(npd, ci, mp)` combination:
  3 × 2 × 2 × 2 = 24 runs per protocol, 72 total, at 60 s plus warm-up each. With the saturation
  phase (3 protocols × 2 CIs × up to 12 client counts) this is a long campaign; there is no resume
  or checkpoint. `--phase=saturation|scale` helps.
* The saturation figure is produced only at `mp = 0.1`, which the header states, but the caption
  generated at `calvin_ubench.py:166-167` does not.

---

## 5. Part C — the measurement pipeline

This section exists because the numbers in both figures are only as meaningful as the path that
produced them, and that path turns out to discard the most diagnostic quantity in a contention
experiment.

### C1 — The failure rate is computed correctly and then thrown away

Traced end to end:

1. `DBWrapper.measure` (`DBWrapper.java:187-202`) gives a failed operation **its own measurement
   name**:

   ```java
   if (result == null || !result.isOk()) {
     measurementName = (reportLatencyForEachError || latencyTrackedErrors.contains(...))
         ? op + "-" + result.getName() : op + "-FAILED";
   }
   ```

   With the defaults (`reportlatencyforeacherror=false`, `latencytrackederrors` unset) a rejected
   transaction lands in the `tx-readmodifywrite-FAILED` bucket. It is *not* silently mixed into
   the latency histogram, which is good.

2. `Client.exportMeasurements` (`:241-243`):

   ```java
   long failedOpsCount = Measurements.getMeasurements().getFailedOperationsCount();
   double throughput = 1000.0 * (opcount - failedOpsCount) / (runtime);
   ```

   so `[OVERALL] Throughput` **excludes** failed operations. Throughput and latency therefore
   describe the same population — successful transactions — and are consistent with each other.
   (I initially assumed the opposite and checked; the code is right.)

3. `parse_ycsb_to_csv.sh` (`:174-192`, `:206-211`) reads both buckets, emits only successful
   operations as `op` rows, and computes the error rate correctly:

   ```awk
   succ = (op in op_ops) ? op_ops[op] : 0
   fail = (op in op_fail) ? op_fail[op] : 0
   failed_pct = fail / (succ + fail) * 100
   ```

   The column lands in the CSV header (`:14`).

4. `calvin_ubench.py` filters `df['op'] == 'tx-readmodifywrite'` (`:82`, `:180`) and reads `tput`
   and `p50`. **`failed_pct` is never read.** Neither figure reports it, and the peak-client
   selection in `calvin_ubench.sh` ignores it too (`:328-343` compares only throughput and
   latency).

The consequences are concrete:

* A run in which 90% of transactions are aborted and one in ten commits is reported as a
  low-throughput run with no visible explanation. On a latency/throughput curve it is
  indistinguishable from a system that got slower for some other reason. **For an experiment whose
  entire independent variable is the contention index, the error rate is the first thing one
  wants to see, and it is one column away.**
* The saturation ramp is driven by `peak_throughput = max(tput)`. Since throughput counts commits
  only, and rejected transactions still consume a client slot, a full network round trip (or
  several) and a coordinator lease, the ramp will keep increasing `clients` — past the point where
  the store is mostly aborting — because aborts do not reduce throughput until they dominate.
  The Pareto stop is on the same two metrics, so nothing else intervenes.

The fix is small and uses machinery that already exists: pass
`-p reportlatencyforeacherror=true` (or `-p latencytrackederrors=<status names>`) so failed
transactions contribute to the latency histogram, and plot `failed_pct` as a third series or as a
second y-axis panel. `drop_unsound_rows` should additionally treat a high `failed_pct` as a reason
to flag the row rather than to plot it unremarked.

### C2 — What the latency actually is

`calvin_ubench.py:86` uses `p50`, which `parse_ycsb_to_csv.sh` extracts per operation from the
`[op]` percentile block, in milliseconds (`int(val/1000 + 0.5)` at `:167`). With C1's fix these
would become the latency of *all* attempted transactions; today they are the latency of the
successful ones, i.e. a survivorship-biased sample that gets *more* biased as contention rises,
since the transactions that succeed are the ones that found their hot records free. Combined with
A3 (no retry) the reported latency is therefore an optimistic estimate of user-visible latency
under contention, while the reported throughput is a pessimistic estimate of useful work. Those
two biases push in opposite directions, which is the worst case for interpreting the curve.

### C3 — Latency is measured at the client, in one datacentre, for a cross-DC transaction

Each datacentre runs its own YCSB client in its own network namespace and writes
`${base}_${dc}.dat`; `global_tput_latency` sums throughput and averages latency across them. That
is a defensible aggregation for a geo benchmark. Two caveats: for Cassandra/Accord and Tiga all
client threads connect to a single node in DC 1, so DC 1's client and DC 1's coordinator are both
on the critical path while DC 2 and DC 3 are passive replicas; and the average of per-DC median
latencies is not itself a median, so the figure's y-axis ("Median latency … averaged over the data
centres", `:148` with the throughput x-axis at `:134`) is a mean of medians. Both should be in the caption.

---

## 6. Can this harness reproduce Figure 5?

| Figure 5 property | reproduced? | why |
|---|---|---|
| per-node throughput falls with `n` | yes | 3 scale points, linearly scaled clients (B3 biases the magnitude) |
| near-linear scaling of total throughput | partly | 3 points (`n = 1, 2, 4`) cannot distinguish near-linear from sublinear-with-a-knee; the paper's knee is at `n ≈ 10`–20 |
| contention index per machine | **yes** | `H = round(1/C)` per partition; exact |
| 1 hot + 9 cold, or 1 + 4 on each of 2 partitions | **yes** | verified |
| constant records per partition across `n` | **yes** | `recordcount = records × npd`, reloaded per `npd` |
| degradation from SP → MP transactions (Figure 6) | **no** | every transaction is cross-DC in a 3-DC deployment (B1) |
| absolute per-node throughput | **no** | 4-byte records, ~250× less data per machine, 2× the cores, 3 replicas (A1, B1) |
| memory-hierarchy sensitivity | **no** | working set far closer to LLC (A1) |
| latency/throughput shape at saturation | partly | closed-loop client (B8); latency is survivorship-biased (C2); error rate discarded (C1) |

**Summary.** The workload generator is faithful enough to be trusted. The harness measures a
*different system* from the one in Figure 5 — geo-replicated rather than single-datacentre, with
a different record size and a different client model — so it can compare protocols against each
other under a Calvin-like transaction mix, and it can show how each behaves as contention rises,
but its numbers should not be placed on Figure 5's axes.

---

## 7. Recommended changes, in order

**Cheap, and they change conclusions**

1. Plot `failed_pct`. It is already in the CSV. Add it as a series in the scale figure and as a
   guard in the saturation ramp. *(C1)*
2. Pass `-p reportlatencyforeacherror=true` so latency covers attempted transactions. *(C2)*
3. Divide the per-node row by `ndc × npd`, reading `ndc` from the CSV's `nodes` column. *(B2)*
4. Add a `--single-dc` mode: `nodes=1`, no `emulate_latency`, RF=1. That is the configuration
   Figure 5 describes, and it is a small change to `configure_deployment` plus the schema creation.
   *(B1)*

**Moderate**

5. Give the record a real size: add `calvin.valuelength` (default 100, matching Calvin's
   `kRecordSize`) padding the record with `field1..fieldN` on insert while `field0` stays the
   counter. Requires widening the schema. This is the only way to restore the memory regime. *(A1)*
6. Run the saturation sweep per `npd`, or at least at `npd ∈ {1, max}`, instead of extrapolating
   linearly. *(B3)*
7. Fix `isConsistent` to detect lost updates, and thread the abort count into `expectedSum`. *(A2)*
8. Calibrate the peak at both `mp` values. *(B4)*

**Housekeeping**

9. Either remove `replication_factor` or make the schema creation honour it. *(B7)*
10. Add `zeropadding` and the per-partition pool size to the `[CONFIG]` echo. *(A-ergonomics)*
11. Fix the `maxCold` validation to use `txnSize / mpSpan - hot` when `mpProportion > 0`, matching
    what `maxHot` already does. *(A6)*
12. Pool or reuse the per-transaction key structures in `doTransactionReadModifyWrite`, and
    pre-compute key names once per partition. *(A4)*
13. Add a test that exercises `checkAndIncrement` against a store with per-key rather than global
    locking, so the conservation assertion can fail. *(A9)*
14. State in both figure captions that `mod` is never exercised, that the saturation figure is
    10%-MP only, and that the per-DC latency is a mean of medians. *(A10, B9, C3)*

---

## 8. Corrections to earlier criticism of mine

An earlier review of `CalvinWorkload` in isolation produced twelve findings. Reading the harness
showed that most of them were artefacts of reviewing defaults that the driver sets explicitly, and
two were simply wrong. They are withdrawn here rather than quietly dropped.

| # | Earlier claim | Status | Reason |
|---|---|---|---|
| P1 | `readmodifywriteproportion` defaults to 0 → 0 Calvin transactions | **withdrawn** | `workloads/workloadcalvin` sets `1.0`; `recordcount` is also passed explicitly |
| P2 | `contentionindex=0.01` realises `C = 0.02` | **withdrawn** | requires `h ≥ 2`; at `0.1 × 10` the hot count is exactly 1 |
| P3 | 29.9% of transactions touch no hot record | **withdrawn** | same cause. Also: `testFractionalHotCount` shows the fractional case is a *deliberate, tested* feature, so this was never a bug |
| P4 | 9,943/20,000 asymmetric multipartition transactions | **withdrawn** | same cause; 0/20,000 at the shipped settings |
| P5 | Only `CockroachDBFlavor` and `TigaClient` can run multipartition configs | **wrong** | Accord goes through `CassandraCQLClient` with Accord's `BEGIN TRANSACTION` extension; there are three atomic bindings and the harness lists exactly those three |
| P6 | Cassandra uses a per-record predicate, not the sum | **downgraded** | True, but inert here: counters start at 0 and only increment, so `∀i: c_i ≥ 0 ⟺ Σc_i ≥ 0` |
| P7 | No retry on serialization failure | **upheld, and worse** | Confirmed, and it now has a measurement consequence I had not seen (C1) |
| P8 | `isConsistent` uses `<=`, so lost updates pass | **upheld** | A2 |
| P9 | Per-transaction allocation in the hot path | **upheld** | A4 |
| P10 | `ThreadLocalRandom` in the hot path | **withdrawn as a defect** | Correct for a load generator; the injectable `Random` is what makes the tests possible (A5) |
| P11 | No defensive check that keys land in distinct partitions | **withdrawn** | Rejection sampling at `:250-262` and the `HashSet` at `:284` already guarantee it |
| P12 | Contention index is global, not per machine | **withdrawn** | `:126` is per partition; this was the one thing the implementation got most right |

The general lesson, recorded because it applies to the next review: a workload's *defaults* are
not its behaviour. The defaults of `CalvinWorkload` on their own are unusable — `init` throws on a
small `recordcount` with the default `hotrecords=1000`, `mpproportion` defaults to 0, and the
contention-index and hot-fraction parameters interact in ways the names do not reveal. Every one
of those becomes unreachable once the driver is read.

---

## 9. Limits of this review

* **No run was performed.** Every claim about the harness is from reading the scripts and from the
  configuration artefacts in the repository. I did not execute a deployment, so the empirical
  statements about the workload (A1's byte counts, the `h = 1` determinism, the `Mod` arithmetic,
  17/17 tests) are verified, but the statements about *results* — B3's bias direction, C1's effect
  on the curves — are reasoned from the code, not measured. The `results/calvin_ubench.csv` and
  `.tex` in the tree were not analysed; doing so, and checking `failed_pct` against the throughput
  collapse, would turn B3 and C1 from arguments into measurements.
* **Only the three live protocols were examined.** `cockroachdb-bad` exists in the scripts and was
  not reviewed; `AccordDBFlavor` was looked for and does not exist in this YCSB fork.
* **The Calvin C++ figures are lineage evidence, not the 2012 artifact.** `~/Implementation/` has
  the later geo-replicated CalvinDB, not the SIGMOD '12 prototype. `kRecordSize = 100`,
  `kDBSize = 10⁷` and the 8-byte modified prefix come from that tree. The self-consistency check
  (10⁷ × 100 B = 1 GB against the paper's stated 7 GiB) is weak but real corroboration; the
  specific numbers should be treated as the modern implementation's, not the prototype's.
* **Tiga's engine source is absent.** Only `ycsb-jni-1.0.jar` is present, so `TigaClient`'s
  atomicity and shard mapping were taken on trust from the binding's surface, and the `tiga`
  partitioner's `TIGA_KEY_SPACE = 2000005` (`CalvinPartitioner.java:44`) could not be checked
  against the engine. It is self-consistent with `recordcount = 4 × 10⁶` at `npd = 4` — each
  partition receives ≈10⁶ keys, and the hot pool is the first `H` keys of that partition's own
  precomputed list (`CalvinPartitioner.java:177-185`), so it is well defined whatever the engine
  does — but the agreement of the two hashes is an assumption.
* **`docs/tiga.md` is the only place the WAN latency matrix is written down**, and it was derived
  from `latencies.csv` coordinates rather than measured. B1's magnitude depends on it.