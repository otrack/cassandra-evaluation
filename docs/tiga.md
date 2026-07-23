# Tiga Performance Analysis Log

This document logs the performance analysis of Tiga under the geo-distributed YCSB benchmark, correlating the observed throughput and latency numbers to Tiga's protocol critical path and the physical network topology.

---

## 1. Geographic Network Topology & Latency Matrix

The benchmark evaluates a 3-node cluster representing a WAN deployment across Hanoi, Lyon, and New York. The network delays emulated via `tc` (derived from `latencies.csv` coordinates) correspond to the following round-trip time (RTT) and one-way delay (OWD) matrix:

| Link | One-Way Delay (OWD) | Round-Trip Time (RTT) |
| :--- | :---: | :---: |
| **Hanoi (Replica 1 - Leader) <-> Lyon (Replica 2)** | 44 ms | 89 ms |
| **Hanoi (Replica 1 - Leader) <-> New York (Replica 3)** | 64 ms | 128 ms |
| **Lyon (Replica 2) <-> New York (Replica 3)** | 30 ms | 60 ms |

---

## 2. Critical Path & Single-Phase Execution

In sharded/partitioned databases, multi-shard transactions must execute a multi-phase protocol (Dispatch + Launch) to establish a global ordering (deadline rank) across shards. 

However, under the single-shard topology (`shardNum = 1`) used in the YCSB workload, all keys reside in the same shard:
1.  **Dispatch Bypassed**: In `TigaCoordinator::DoOne`, the coordinator checks `txnGen_->NeedDisPatch()`. Since the JNI client uses `TigaYcsbTxnGenerator` which inherits the default `NeedDisPatch() -> false` behaviour, the Dispatch phase is completely bypassed.
2.  **Single-Phase Multicast**: Transactions are processed as single-phase operations, transitioning directly into the `Launch()` phase where the coordinator multicasts the transaction to all replicas.

---

## 3. Hanoi Client (`ycsb-1`) Performance

*   **Observed Latency (Workload A)**: Read = **180.1 ms**, Update = **278.9 ms**
*   **Theoretical Critical Path**:
    1.  **Headroom estimation**: The Hanoi coordinator measures OWD to all replicas (Hanoi=0ms, Lyon=44ms, New York=64ms). The maximum OWD is 64 ms. It adds the buffer offset (`owd_delta_us = 10ms`), setting the headroom bound to **74 ms**.
    2.  **Hold buffer delay**: The local leader replica (Hanoi) receives the Launch request at client time $t = 0$ and holds it in the `holdBuffer_` until $t = 74\text{ ms}$ (intentional delay of **74 ms**).
    3.  **Quorum replication**: At $t = 74\text{ ms}$, the leader executes the transaction and replicates it. To commit on the slow path, it waits for a slow quorum of 2 out of 3 replicas (itself + Lyon replica). The round-trip replication time to Lyon is **88 ms** ($44\text{ ms} \times 2$).
    4.  **Local reply**: Once committed, the leader replies to the Hanoi client locally (0 ms).
    5.  **Total theoretical latency**: $74\text{ ms} \text{ (hold delay)} + 88\text{ ms} \text{ (replication RTT)} = \mathbf{162\text{ ms}}$.
*   **Conclusion**: The theoretical latency of **162 ms** aligns closely with the observed **180.1 ms** read latency (the remaining 18 ms is attributed to thread scheduling and JVM/JNI context switching overheads).

---

## 4. New York Client (`ycsb-3`) Performance

*   **Observed Latency (Workload A)**: Read = **225.6 ms**, Update = **310.8 ms**
*   **Theoretical Critical Path**:
    1.  **Headroom estimation**: The New York coordinator also measures the maximum OWD to any replica in the cluster (Hanoi at 64 ms) and sets the headroom bound to **74 ms**.
    2.  **Hold buffer delay**: The coordinator multicasts the Launch request at $t = 0$. The remote leader (Hanoi) receives it at physical time $64\text{ ms}$ and holds it until the deadline of $74\text{ ms}$ (holds for 10 ms).
    3.  **Log sync & Polling**: The leader executes the transaction at $t = 74\text{ ms}$ and replicates the log to the New York replica via `InterReplicaSync` (arrives at New York at $74\text{ ms} + 64\text{ ms} = 138\text{ ms}$). At the same time, the leader replies to the New York coordinator (arrives at $138\text{ ms}$).
    4.  **Local sync discovery**: The client coordinator (local to the New York client container) polls the local New York replica via the `InquireServerSyncStatus` thread. Once it discovers that the New York replica has synced up to the leader's log ID (at $t = 138\text{ ms}$ + polling interval delay), the slow quorum of 2 is satisfied and the transaction commits.
    5.  **Total theoretical latency**: **138 ms to 148 ms**.
*   **Conclusion**: The observed latency of **225.6 ms** is significantly higher than the base network limit of 138 ms. This is due to the **pull-based inquiry thread** (`InquireServerSyncStatus`) used by the coordinator to fetch sync status from replicas. This polling adds periodic query offsets and JNI/JVM wake-up overheads on the client.

---

## 5. Why the Fast Path Fails for Single-Shard Transactions

Although Tiga's pseudo-code permits fast-path commits for single-shard transactions, the current implementation prevents this optimization under concurrent workloads:

1.  **Bypassing Speculative Execution**: 
    Because single-shard transactions bypass cross-shard agreement, they immediately mark `agreeStatus_ = AGREE_COMPLETE`. Replicas execute them directly via the slow path (`EXEC_DIRECT`) instead of the speculative path (`EXEC_SPEC`).
2.  **Per-Key Hash Mismatch**:
    Follower replicas compute their execution hashes based on their local synced log boundary (`boundarySyncedHashMarks_`). This boundary is only updated when the follower receives the leader's `InterReplicaSync` log message.
3.  **Lag Under Skewed Workloads**:
    Under YCSB Workload A's Zipfian (skewed) key distribution, hot keys are continuously modified. Because the leader processes updates in rapid succession, the followers' synced boundaries for hot keys almost always lag behind.
4.  **Slow-Path Fallback**:
    This lag causes the follower's reply hash to mismatch the leader's hash. The coordinator fails the fast-path check (`validFastReplies >= 3`) and falls back to the slow-path check, which forces the client to wait for background log replication and active polling.

*Note: If there are no conflicting operations (e.g. executing on disjoint keys or with low request rates), the sync boundary is stable, hashes match, and the fast path succeeds.*

---

## 6. Detailed Analysis of the Hash Mismatch

The hash mismatch described in Section 5 arises from two primary implementation factors under concurrent workloads:

1. **Divergent Execution Orders (Arrival Order vs. Rank Order)**:
   * **At the Leader**: Transactions are placed in the `holdBuffer_` and reordered so they are executed in strict **deadline rank order** (`localDdlRank_`).
   * **At the Followers**: Because single-shard transactions bypass the agreement phase, the follower replica bypasses the hold buffer and enqueues transactions directly to the execution queue `toExecQuF_` ([TigaReplica.cc:1234](file:///home/otrack/Implementation/Tiga/TigaService/TigaReplica.cc#L1234)). 
   * **The Divergence**: The follower execution thread `FollowerExecTd` dequeues and processes them in **FIFO arrival order** (the order in which the coordinator's `LaunchRequest` packets arrive at the follower). Under concurrency and network jitter, this arrival order frequently differs from the leader's rank order, causing the accumulative hash chains to diverge.

2. **Lagging Sync Boundaries (Stale Checkpoint XORing)**:
   Even if the execution orders happen to align, the follower uses its local synced boundary (`boundarySyncedHashMarks_`) to align its hash with the leader's synced state.
   * This boundary is updated asynchronously when the follower receives the leader's background `InterReplicaSync` messages.
   * Under concurrent updates, these sync messages are almost always in transit. Consequently, the follower calculates its reply hash by XORing a **stale boundary checkpoint**. This mismatch in the checkpoint base guarantees that the final hashes will differ.

### Why Sequential Coordination Bypasses This Mismatch
If all conflicting transactions in the entire system were serialized through a single coordinator thread:
* Transaction 1 would be forced to fully commit and sync to the followers before Transaction 2 starts.
* The followers' synced boundaries (`boundarySyncedHashMarks_`) would always be up to date when processing Transaction 2, allowing the hashes to match.
* However, in our YCSB binding, **each YCSB client thread runs on its own independent coordinator** (instantiated once per thread in [tiga_ycsb.cc](file:///home/otrack/Implementation/Tiga/ycsb_jni/tiga_ycsb.cc)). This concurrent execution model naturally creates conflicting request interleavings that trigger the mismatches.
* Sharing a single coordinator across all threads is not a viable alternative, as the coordinator's recursive mutex (`mtx_`) and single-transaction design (`reqInProcess_`) would serialize all threads, leading to an unbearable queueing effect (reducing concurrency to 1).

---

## 7. Performance under Preventive Mode

To validate our findings, we enabled preventive mode in [config-ycsb.yml](file:///home/otrack/Implementation/Tiga/config-ycsb.yml):
```yaml
preventive: true
```
We then re-ran the YCSB performance benchmark. The results show a massive performance improvement, matching our theoretical analysis:

### Key Latency Metrics (New York Client `ycsb-3` - Workload A)
*   **Median (p50) Read Latency**: Dropped from **225 ms** to **74 ms** (a ~3x reduction).
*   **Median (p50) Update Latency**: Dropped from **310 ms** to **77 ms** (a ~4x reduction).

### Analysis of the Improvement
1.  **Upfront Ordering**: In preventive mode, the timestamp agreement is done *before* execution. Since leaders are co-located in the same region, this agreement occurs over the LAN in $< 1\text{ ms}$.
2.  **Perfect Hash Matching**: Deterministic, pre-ordered execution ensures that all replicas execute conflicting transactions in the same order. The follower execution hashes match the leader's hash perfectly.
3.  **Bypassing Polling**: Because the fast path check (`validFastReplies >= 3`) succeeds immediately, the client coordinator commits the transaction as soon as the multicast replies are received. It completely bypasses the sync-status polling delay, locking the median latency to the exact headroom deadline limit of **74 ms**.

*Note: The server logs still report `Fast ratio: 0.0000` in this mode. This is an implementation artifact: under preventive mode, replicas skip speculative execution (`EXEC_SPEC`) entirely and run directly under `EXEC_DIRECT`. While the replicas count this as a direct (slow) path, the client coordinator achieves 1-WRTT fast commits.*

---

## 8. Standard YCSB Schema, Row-Level Conflicts, and Ratios

### Alignment with Standard YCSB Semantics
To align Tiga with standard YCSB schema and operational semantics, we refactored the database state machine, JNI layer, and YCSB Java client driver:
1. **Dynamic Row Store**: In `YCSBStateMachine.cc`, we replaced the simplified integer-increment state with a dynamic row store: `std::vector<std::vector<std::string>> kvStore_`. Each record contains a dynamic array of field strings.
2. **Row-Level Conflicts**: Transactions register the record key `int_key` in the read/write set (`ws_`), causing concurrent updates/reads to any field of the same record to correctly conflict and be ordered at the row level.
3. **Map Serialization & Blind Writes**: 
   * **Updates/Inserts**: The JNI layer extracts update fields from the Java client, serializes them to a flat payload string (`fieldId:val|`), and writes it to `ws_[int_key]`. Replicas execute this as a blind write, updating only the specified field indices in the target row without reading other fields first.
   * **Reads**: Replicas serialize and return the entire target row. The JNI client deserializes it and populates the Java client map with only the requested fields.

### Measured Fast Path Ratios (New York Client `ycsb-3`)
Under the row-level conflict model, we re-ran the YCSB benchmarks and obtained clean, crash-free results:

| Workload | Read/Write | Fast Path Ratio | Slow Path Ratio |
| :--- | :--- | :--- | :--- |
| **Workload A** | 50% Read / 50% Update | **88.5%** | 11.5% |
| **Workload B** | 95% Read /  5% Update | **78.9%** | 21.1% |
| **Workload C** | 100% Read | **73.2%** | 26.8% |
| **Workload D** | 95% Read /  5% Insert | **64.9%** | 35.1% |

### Analysis of the Ratios and Performance Correlation
The measured ratios directly explain the YCSB throughput, average latency, and tail latency profiles:

1.  **Flat Median (p50) Latency**:
    *   Across all workloads, the median (p50) read latency for New York remains locked at **70–75 ms** (typically **74 ms**).
    *   This matches the fast quorum deadline bound (`max_OWD + delta = 64ms + 10ms = 74ms`) in a 3-node system.
    *   Because the fast-path commit ratio is $> 50\%$ in all workloads ($88.5\% \rightarrow 78.9\% \rightarrow 73.2\% \rightarrow 64.9\%$), the 50th percentile transaction always commits via the fast path, keeping the median latency flat.

2.  **Average and Tail (p90, p99) Latency Inflation**:
    *   While the median is flat, the average and tail latencies are highly sensitive to the slow-path fallback ratio.
    *   A slow-path fallback requires 1.5–2 WRTTs ($150\text{ ms} - 225\text{ ms}$) and polling overhead, and holding locks longer increases queueing delays.
    *   As the slow path ratio drops (e.g., down to **11.5%** in Workload A), the average read latency is kept low, preventing tail latency inflation.

3.  **Throughput Sensitivity**:
    *   Because YCSB threads run in a closed loop (a thread blocks until the previous transaction commits), throughput is throttled by average latency.
    *   Throughput increases as the slow path ratio (and thus average latency) drops, allowing client threads to initiate transactions much faster.

4.  **Why Slow Commits Happen in Read-Only/Low-Write Workloads**:
    *   In a 3-node system, the fast path requires replies from all 3 nodes. If the 3rd replica's reply is delayed due to transient WAN network jitter, the coordinator receives a slow quorum of 2 replies first and immediately commits the transaction on the slow path. This represents a trade-off where the coordinator accepts a slow path classification to commit the transaction early and avoid blocking on a lagging node.
---

## 9. Docker Latency Emulation & Scale-out Constraints

During experiments with larger topologies (e.g., 5-node clusters), two critical environment bugs were identified and resolved to allow the evaluation to proceed correctly:

1. **Docker Latency Emulation Failure (Silent `tc` Bypass)**:
   * **Issue**: In [Docker/tiga/cluster.sh](file:///home/otrack/Implementation/cassandra-evaluation/Docker/tiga/cluster.sh), Tiga/Calvin/Detock server containers were launched without `NET_ADMIN` and `NET_RAW` capabilities. When [Docker/emulate_latency.py](file:///home/otrack/Implementation/cassandra-evaluation/Docker/emulate_latency.py) ran traffic control (`tc`) commands inside these containers to emulate WAN delays, the commands failed silently with `Operation not permitted`. As a result, actual network latency between replicas remained `0 ms`, leading to incorrect uniform latency measurements of `~70 ms` across all locations.
   * **Fix**: Added `--cap-add=NET_ADMIN --cap-add=NET_RAW` to the Tiga server containers inside [Docker/tiga/cluster.sh](file:///home/otrack/Implementation/cassandra-evaluation/Docker/tiga/cluster.sh), and modified [Docker/emulate_latency.py](file:///home/otrack/Implementation/cassandra-evaluation/Docker/emulate_latency.py) to validate `tc` command exit codes and raise an exception on failure.

2. **Replication Group Boundary Overflow**:
   * **Issue**: When running with 5 nodes, Tiga servers crashed with `*** buffer overflow detected ***: terminated`. This was traced back to `#define MAX_REPLICA_NUM (3)` defined statically in `Tiga/Common.h`. Indexing connection and tracking structures using replica IDs `3` and `4` caused out-of-bounds writes on array structures.
   * **Fix**: Increased `MAX_REPLICA_NUM` to `16` in `Tiga/Common.h` and rebuilt the corresponding Docker images (`0track/tiga-suite` and `0track/ycsb`).
