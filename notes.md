nodelay (https://www.datastax.com/blog/performance-doubling-message-coalescing)

inconsistent parameters in default cassandra.yaml (e.g., recover_delay)

# 21.10.2025

- configure QUORUM to access everybody (table level)

# 18.12

- native (graalVM) Cassandra binary?

# 8.02 - copilot

This task adds under /Docker an experiment that runs the closed economy workload in YCSB.
This workload is described here: https://github.com/otrack/YCSB/blob/cassandra5/core/src/main/java/site/ycsb/workloads/ClosedEconomyWorkload.java
In detail, 
- the experiment follows the same pattern as what cdf.sh and conflict.sh
- the workload is already available in the Docker image of YCSB; 
  its name is wokrloads/workload-ce.
- there is no other workload to run 
- the number of nodes is variables, from 3 to 5
- because the workload relies on transactions, only two protocols supports it: accord and cockroachdb 
- as the other experiemtns, one expects a plot that informs about the results;
  more precisely this plot should (i) on the x axis varies the number of nodes in the system, 
  and (ii) on y axis mentions the number of transactions (i.e., read-modify-write operations 
  that transfer some money between two bank accounts)
  for each system, and each number of replicas, there is one histogram 
  this plot is similar to Fig. 9 in https://arxiv.org/pdf/2104.01142.
  
Please create an appropriate PR to do this job.

# 09.02 - copilot

Apply the following changes:
- In closed_economy.sh, set the default number of records to 100,000.
- In closed_economy.sh, add a fixed total number of threads.
This number is split evently among all the YCSB clients.
There is always at least one thread per YCSB client---this is also the default value.
- In run_benchmarks.sh, set YCSB_THREADS to 1 when it is the load phase of YCSB (parameter workload_type in the run_ycsb function).
- When creating closed_economy.tex with closed_economy.py, add the theoretical best-case and worst-case latencies for Accord.
These are computed as follows: 
For some fixed n (ie., the total of nodes in the system), there are two parameters: f and e.
Paramter f is always set to a minority of nodes, that is (n-1)/2.
Parameter e is the largest value that satisfies the following inequation: n = max(2e+f-1, 2f+1).
A fast quoum is any n-e nodes.
A slow quorum is any n-f nodes.
To apply a transaction, Accord executes a commit phase followed by an execute phase.
In all cases, the execute phase takes one round-trip to the closest two nodes.
For the best-case latency, the commit phase takes one round-trip to the closest fast quorum. 
For the worst-case latency, the commit phase takes threee round-trip to the closest slow quorum.

# 09.02 - copilot

There is something incorrect in closed_economy.py.
For instance, I have the following result in df_rmw:

1        accord      3       ce            NaN      Hanoi  tx-readmodifywrite        1   0.00  268  268  268  268  ...   268   268   268   268   268   268   268   268   268   268   268   268
3        accord      3       ce            NaN       Lyon  tx-readmodifywrite        1   0.00  201  201  201  201  ...   201   201   201   201   201   201   201   201   201   201   201   201
6        accord      3       ce            NaN    NewYork  tx-readmodifywrite        1   0.84  137  137  137  137  ...   355   355   355   355   356   356   356   356   356   356   356   356

However, the throughput for accord with 3 replicas, as computed in closed_economy.tex is less than 1 tx/s.
This does not make sense since YCSB client observe on average 249ms for a transaction.

Correct the script and also make sure that it is good for the other replication protocols as well.

# 09.02 - copilot

Again, there is a problem.
Look at the following extract of the closed_economy.csv file:

1        accord      3       ce            NaN      Hanoi  tx-readmodifywrite        1   0.00  268  268  268  268  ...   268   268   268   268   268   268   268   268   268   268   268   268
3        accord      3       ce            NaN       Lyon  tx-readmodifywrite        1   0.00  201  201  201  201  ...   201   201   201   201   201   201   201   201   201   201   201   201
6        accord      3       ce            NaN    NewYork  tx-readmodifywrite        1   0.84  137  137  137  137  ...   355   355   355   355   356   356   356   356   356   356   356   356
...
45  cockroachdb      3       ce            NaN      Hanoi  tx-readmodifywrite        1   5.37  183  183  183  183  ...   186   186   186   186   186   187   187   188   188   197   228   256
48  cockroachdb      3       ce            NaN       Lyon  tx-readmodifywrite        1   1.38  716  716  716  716  ...   719   719   719   719   719   719   720   720   721   722   772  1440
51  cockroachdb      3       ce            NaN    NewYork  tx-readmodifywrite        1   1.04  956  956  956  956  ...   959   959   959   959   960   960   962   963   964   996   998  1946
...
69  cockroachdb      7       ce            NaN    Beijing  tx-readmodifywrite        1   1.44  690  690  690  690  ...   692   692   692   693   693   693   693   693   694   695   699  1638
72  cockroachdb      7       ce            NaN      Hanoi  tx-readmodifywrite        1   1.21  822  822  822  822  ...   824   824   824   824   824   825   826   826   827   831   835   860
75  cockroachdb      7       ce            NaN       Lyon  tx-readmodifywrite        1   2.38  414  414  414  414  ...   416   417   417   417   418   419   420   422   425   428   433  1172
78  cockroachdb      7       ce            NaN     Mumbai  tx-readmodifywrite        1   1.26  786  786  786  786  ...   788   788   788   788   789   789   790   790   791   792   799  1752
81  cockroachdb      7       ce            NaN    NewYork  tx-readmodifywrite        1  17.43   52   52   52   52  ...    54    55    55    55    55    58    60    61    62    62    66   764
84  cockroachdb      7       ce            NaN  Rotterdam  tx-readmodifywrite        1   2.53  390  390  390  390  ...   392   392   392   392   393   393   397   398   399   402   404  1106
87  cockroachdb      7       ce            NaN      Texas  tx-readmodifywrite        1   4.93  198  198  198  198  ...   200   200   200   200   200   200   201   206   207   209   212   908

With 3 ndoes, the average latency of YCSB for cockroachdb is 623ms, while accord has 249ms.
Roughly speaking, we should have less than 3 tx/s for cockroachdb and more than 12 tx/s for accord in this setting.
The closed_economy.tex computed by closed_economy.py reports 3 tx/s for accord.
Now, when using 7 nodes, the average latency for cockroachdb is around 480ms, leading to around 14 tx/s.
However, the plot reports 4 tx.s with 7 nodes for this storage engine.

# 09.02 - copilot

The script closed_economy.py is calculating th throughput plot on the y-axis.
In fact this is not the most appropriate because it requires to approximate it from the latency.
Instead, use the percentiles of the latency distribution to indicate per replication protocol and per system size the average latency.
In detail, you should
- replace the y-axis with average latency (in ms)
- given a system of n nodes, 
(i) compute the average latency over the n clients,
(ii) also include the main percentiles (e.g., P90, P95, and P99) in the plot.
This should replace the bar which is currently displayed, while keeping a histogram-like plot.

# 17.02 - copilot

In the Casssandra implementation, this is not a two-phase transaction which is being used. I want something in the vein of the following code: https://github.com/pmcfadin/awesome-accord/blob/main/examples/inventory/transaction.cql.  Notice that cecause there is no support for this in the latest Java driver, the corresponding CQL query has to be written by hand.

# 17.02 - copilot

In the JDBC client, one should also use a two-phase transaction where possible. To implement this, modify the jdbc module as follows:
- add a createTransfertStatement to DefaultDBFlavor which returns null
- in the JDBC client use the this statement if it is not null and otherwise rely on the default implementation provided in site.ycsb.DB
- add a CockroachDBFlavor class, child of DefaultDBFlavor and in this class add a two-phase transaction which relies on the syntax offered by CockroachDB. (For this, you might use 

# 20.02 - copilot

Add the capability to bound the resources of the database containers in a manner of a public cloud platform when using containers.
More precisely, under /Docker, you should:
- create a file called gcp.csv with the following columns: name, vcpus, memory.
Each entry in this file should correspond to the resources provided by the Google Cloud Platform for a given machine type at the cloud provider. 
For instance, n1-standard corresponds to 4 vCPUs and 15GB RAM.
Use the data made publicly available by GCP to populate this file.
- in run_benchmark.sh, addd a parameter to limit the cpu and memory resources of a database container.
To define such limits, use the machine type as provided in the entry "machine=" in the file exp.config.
If no such information is provided, then there is no limit for the container.

# 20.02 

possible improvements
- YCSB: swap among N instead of 2
- performance breakdown for Accord and CRDB
- add a fault-tolerance exp.

# 22.02 - copilot

The goal of this work is to add a fault-tolerance experiment.
This experiment should mimic the one appearing in Section 6.2 (Figure 6) in the paper entitled "CockroachDB: The Resilient Geo-DistributedSQL Database", published at SIGMOD'20 (industry track).
In detail,
- the experiment is similar to the already existing ones (e.g., cdf.sh, conflict.sh), relying on the same tool to start/stop a cluster and a bunch of YCSB clients
- the experiment should last X (configurable) minutes during which two events occurs.
The first event happens at time X/4.
It is a slowdown of database-node1 by adding 400ms of latency to reach the other databases (implemented using the tc tool).
The second event happens at time 3X/4.
The site database-node1 crashes (by executing a docker kill command.)
- the plot output by the experiment is as follows:
the x axis is a timeline
the y axis is the aggregated throughput of the YCSB client (as observed with the -s command in YCSB)
- the experiment is available in a new script called fault-tolerance.sh

# 22.02 - copilot

-> draft, not used

The two events, slowdown and failure, should not impact database-node1 (and ycsb-1) but instead the location where the leader is.
This implies that before injecting the event, the script should look for the leader.
In the case of cockroachdb, it requires to look into 
For swiftpaxos-paxos, this information can be found in the log, as the library output a message "I am the leader".

# 24.02 - copilot

Do the following improvement to Docker/cdf.py: 
Replace the vertical line in gray representing the optimum with a small dash on the x axis at the right value. 
Below the dash, write a small "Q" in gray. Do this for all the sub-figures in the figure. 
In the caption of the figure, replace "optimum" with "closest quorum".

# 25.02

disable data durability

a few bugs:

1. 
INFO  [AccordExecutor[5,2]] 2026-02-25T06:57:09,371 ShardDurability.java:263 - Successfully completed 1/3 cycle of durability scheduling covering range 1b255f4d-ef25-40a6-0000-000000000012:(-Inf,-2305843009213693955]. Completed in 88327s (vs 112s target).

This looks _very_ long. 
The experiment did not run in 88327s.

2. 
ERROR [AccordScheduled:1] 2026-02-25T07:14:12,579 JVMStabilityInspector.java:72 - Exception in thread Thread[AccordScheduled:1,5,AccordScheduled]
java.lang.RuntimeException: Timed out waiting for epoch when processing message from 1 to Node{2} message (from:/10.100.15.2:7000, type:IMMEDIATE verb:ACCORD_PRE_ACCEPT_REQ)
	at org.apache.cassandra.service.accord.AccordVerbHandler.lambda$doVerb$0(AccordVerbHandler.java:73)
	at accord.utils.async.AsyncResults$AbstractResult.notify(AsyncResults.java:87)
	at accord.utils.async.AsyncResults$AbstractResult.trySetResult(AsyncResults.java:118)
	at accord.utils.async.AsyncResults$AbstractResult.tryFailure(AsyncResults.java:131)
	at accord.utils.async.AsyncResults$SettableResult.tryFailure(AsyncResults.java:226)
	at accord.topology.TopologyManager$FutureEpoch.timeout(TopologyManager.java:573)
	at accord.impl.AbstractTimeouts$Stripe$Registered.onExpire(AbstractTimeouts.java:104)
	at accord.impl.AbstractTimeouts$Stripe.unlock(AbstractTimeouts.java:196)
	at accord.impl.AbstractTimeouts.maybeNotify(AbstractTimeouts.java:269)
	at org.apache.cassandra.concurrent.ExecutionFailure$1.run(ExecutionFailure.java:138)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Unknown Source)
	at java.base/java.util.concurrent.FutureTask.runAndReset(Unknown Source)
	at java.base/java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(Unknown Source)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)
	at io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
	at java.base/java.lang.Thread.run(Unknown Source)
Caused by: accord.coordinate.EpochTimeout: Timeout waiting for epoch 19
	at accord.coordinate.EpochTimeout.timeout(EpochTimeout.java:31)
	... 12 common frames omitted

ERROR [MetadataFetchLogStage:1] 2026-02-25T07:14:04,849 JVMStabilityInspector.java:72 - Exception in thread Thread[MetadataFetchLogStage:1,5,MetadataFetchLogStage]
java.lang.IllegalStateException: null
	at accord.utils.Invariants.createIllegalState(Invariants.java:76)
	at accord.utils.Invariants.illegalState(Invariants.java:81)
	at accord.utils.Invariants.illegalState(Invariants.java:96)
	at accord.utils.Invariants.require(Invariants.java:236)
	at accord.topology.TopologyManager$Epochs.<init>(TopologyManager.java:296)
	at accord.topology.TopologyManager.onTopologyUpdate(TopologyManager.java:748)
	at accord.local.Node.onTopologyUpdateInternal(Node.java:351)
	at accord.local.Node.onTopologyUpdate(Node.java:371)
	at accord.impl.AbstractConfigurationService.reportTopology(AbstractConfigurationService.java:435)
	at org.apache.cassandra.service.accord.AccordConfigurationService.reportTopology(AccordConfigurationService.java:448)
	at accord.impl.AbstractConfigurationService.reportTopology(AbstractConfigurationService.java:446)
	at accord.topology.TopologyManager$TopologyRange.forEach(TopologyManager.java:937)
	at org.apache.cassandra.service.accord.AccordConfigurationService.lambda$fetchTopologyAsync$9(AccordConfigurationService.java:429)
	at org.apache.cassandra.utils.concurrent.ListenerList$CallbackBiConsumerListener.run(ListenerList.java:267)
	at org.apache.cassandra.concurrent.ImmediateExecutor.execute(ImmediateExecutor.java:140)
	at org.apache.cassandra.utils.concurrent.ListenerList.safeExecute(ListenerList.java:190)
	at org.apache.cassandra.utils.concurrent.ListenerList.notifyListener(ListenerList.java:181)
	at org.apache.cassandra.utils.concurrent.ListenerList$CallbackBiConsumerListener.notifySelf(ListenerList.java:274)
	at org.apache.cassandra.utils.concurrent.ListenerList.lambda$notifyExclusive$0(ListenerList.java:148)
	at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:242)
	at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:235)
	at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:225)
	at org.apache.cassandra.utils.concurrent.ListenerList.notifyExclusive(ListenerList.java:148)
	at org.apache.cassandra.utils.concurrent.ListenerList.notify(ListenerList.java:113)
	at org.apache.cassandra.utils.concurrent.AsyncFuture.trySet(AsyncFuture.java:103)
	at org.apache.cassandra.utils.concurrent.AbstractFuture.lambda$map$0(AbstractFuture.java:342)
	at org.apache.cassandra.concurrent.ImmediateExecutor.execute(ImmediateExecutor.java:140)
	at org.apache.cassandra.utils.concurrent.ListenerList.safeExecute(ListenerList.java:190)
	at org.apache.cassandra.utils.concurrent.ListenerList.notifyListener(ListenerList.java:181)
	at org.apache.cassandra.utils.concurrent.ListenerList$RunnableWithExecutor.notifySelf(ListenerList.java:369)
	at org.apache.cassandra.utils.concurrent.ListenerList.lambda$notifyExclusive$0(ListenerList.java:148)
	at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:242)
	at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:235)
	at org.apache.cassandra.utils.concurrent.IntrusiveStack.forEach(IntrusiveStack.java:225)
	at org.apache.cassandra.utils.concurrent.ListenerList.notifyExclusive(ListenerList.java:148)
	at org.apache.cassandra.utils.concurrent.ListenerList.notify(ListenerList.java:113)
	at org.apache.cassandra.utils.concurrent.AsyncFuture.trySet(AsyncFuture.java:103)
	at org.apache.cassandra.utils.concurrent.AbstractFuture.trySuccess(AbstractFuture.java:144)
	at org.apache.cassandra.utils.concurrent.AsyncPromise.trySuccess(AsyncPromise.java:117)
	at org.apache.cassandra.net.MessageDelivery.lambda$sendWithRetries$1(MessageDelivery.java:99)
	at org.apache.cassandra.net.MessageDelivery$1Request.onResponse(MessageDelivery.java:170)
	at org.apache.cassandra.net.ResponseVerbHandler.doVerb(ResponseVerbHandler.java:88)
	at org.apache.cassandra.net.InboundSink.lambda$new$0(InboundSink.java:103)
	at org.apache.cassandra.net.InboundSink.accept(InboundSink.java:123)
	at org.apache.cassandra.net.InboundSink.accept(InboundSink.java:52)
	at org.apache.cassandra.net.InboundMessageHandler$ProcessMessage.run(InboundMessageHandler.java:457)
	at org.apache.cassandra.concurrent.ExecutionFailure$1.run(ExecutionFailure.java:138)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)
	at io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
	at java.base/java.lang.Thread.run(Unknown Source)


# 25.02 - copilot

The emulation is outputing suspicious kernel events:

2026-02-25T08:41:03+01:00 homer kernel: htb: netem qdisc 20: is non-work-conserving?
2026-02-25T08:41:03+01:00 homer kernel: htb: too many events!
2026-02-25T08:41:03+01:00 homer kernel: htb: netem qdisc 30: is non-work-conserving?
2026-02-25T08:41:04+01:00 homer kernel: htb: netem qdisc 30: is non-work-conserving?
2026-02-25T08:41:04+01:00 homer kernel: htb: too many events!
2026-02-25T08:41:04+01:00 homer kernel: htb: netem qdisc 10: is non-work-conserving?
2026-02-25T08:41:04+01:00 homer kernel: htb: netem qdisc 10: is non-work-conserving?
2026-02-25T08:41:04+01:00 homer kernel: htb: too many events!
2026-02-25T08:41:04+01:00 homer kernel: htb: netem qdisc 20: is non-work-conserving?

Investigate the cause and propose a fix, by changing the current traffic shaping rules to do network emulation.
In particular,
- remove the 100mbit rate limit in the htb root class
- possibly use tbf in lieu of htb
The end goal is to mimick cross-DC links between the different locations.
These connections are usually high-latency but do not have bandwidth limits.

# 26.02 - copilot

The script Docker/parse_ycsb_to_csv.sh is very slow when there are multiple files to parse.
The goal of this task is to improve its overall performance.
To this end, you will use more appropriate algorithmic constructs and also make it parallel where possible.
For instance, it looks possible to use GNU parallel and execute the loop "file in "$@"; do" concurrently.
Of course, this requires to be careful when outputing each line, so that they do not get mixed up.

# 01.03 - copilot

The goal of this work is to add a plot that provides a latency breakdown to Docker/cdf.sh.
The breakdown is by protocol and leverage the tracing capabilities of the underlying database (argument "-p db.tracing=true" to run_benchmark.sh in line 38 of cdf.sh).
Notice that, for the moment, only cockroachdb offers this.

The breakdown plot is implemented in a new Python script called cdf-breakdown.py.
This script output one plot for each protocol (so currently, only cockroachdb, but the logic should be generic enough).
For a protocol, there is one bar for each city.
For each city, the script analyzes the content of the corresponding log of the city, e.g., logs/cockroachdb_3_a_20260301111952773348329_NewYork.dat.
It uses the tracing output to decompose the time spent in each request.
This decomposition depends on the protocols but it should be along the following axes: processing, execution, commit, and ordering.
Where,
1) Processing is the time spent in computing some internal logic to execute the request (e.g., the query plan);
2) Execution is the actual time to execute the statement; 
3) Commit is the time to commit the request; and 
4) Ordering corresponds to the time spent to order the request wrt. the other concurrent requests.
The total time should correspond to the average end-to-end latency to complete the request.

For instance, enclosed is the result of tracing the following statement "UPDATE usertable SET field0 = $1 WHERE ycsb_key = $2" on cockroachdb.

One can observe that 
1) A client connected to SQL node n3 executed an UPDATE against table "usertable" (table ID 104) for a single primary-key row (ycsb-style key "user7109110581138159243").
2) The SQL layer on n3 planned and executed a (local) DistSQL flow that sent a Get to store/node n1, read the current row, then sent a Put + EndTxn (parallel commit) back to n1.
3) The KV work (Get + Put + EndTxn) was processed on n1 via Raft. The system attempted a 1PC, but the operation required consensus and went through Raft; the transaction ultimately committed (parallel-commit attempt resulted in explicit commit).
4) The whole SQL statement took ~0.35s; ComponentStats show ~129ms of KV time (gRPC/kv request), and additional time was spent in Raft/replication and the node/node RPC round-trips.

Below is a non-overlapping decomposition of the client-observed end-to-end latency for the UPDATE in the enclosed trace. 
I show the time windows I used (with the exact log lines that mark their start/end), the numeric durations, and a short explanation of what each axis includes. The four axes are mutually exclusive and sum to the total client-perceived latency.

Summary (final numbers):
* Total (client-perceived): 09:29:19.212192 → 09:29:19.561421 ≈ 0.349229 s (349.2 ms)
* Processing (planning, bind): 0.000369 s (0.37 ms)
* Execution (build/run DistSQL flow, KV Get, local compute + prepare Put): 0.129904 s (129.9 ms)
* Ordering (sequencing / latch acquisition + Raft ordering / consensus): 0.089672 s (89.7 ms)
* Commit (transport to leaseholder, apply/ack round-trips, response transfer — excluding ordering): 0.129284 s (129.3 ms)

These four values sum to the total: 0.000369 + 0.129904 + 0.089672 + 0.129284 ≈ 0.349229 s.

# 01.03 - copilot

In Docker/latency_throughput.sh, given a protocol, the script run all the values of the threads variable.
This is not useful if the performance degrades too much.
Add a verification in the while loop (line 35) which check that the performance did not degrade too much.
In case the average latency of the previous experiment is above 1s then there is no need to continue further and the loop is interrupted, moving to the next (if any) protocol.ma

# 01.03 - copilot

In Docker, add a new experiment that runs YCSB workloads A to D.
This experiment is implemented as a bash script called ycsb.sh.
It is similar to the other ones, e.g., cdf.sh, conflict.sh, etc.
The script outputs the total throughput across all YCSB clients, as a bar chart:
On the x axis, for each of the workload (A,B,..), there is a group of bars, one per protocol.
The first group of bars mention the protocol in small and below this mention, there is the name of the workload (A).
The other groups simply mentions the workload.
These mentions should all be horizontally aligned.
There are no other keys in the figure.
The y axis is the throuhgput in command per second.
The caption of the figure should explain it, as it is the case with cdf.sh.

Correct the script to plot on the y-axis the latency (not the throughput) in ms.
The latency should be averaged over all the clients in the system.
This requires to extract for each workload the operation which are executed (that is, either insert, update, or get).
For each such operation, the YCSB log mention the average latency.
For instance, "[UPDATE], AverageLatency(us), 217213.92" indicates that it took around 217ms to execute an update on average at that client.
Be careful not to take into account the clean-up operation.

# 05.03 - copilot

The Latex plots produced by the experiments in /Docker, e.g., cdf.sh, conflict.sh, ycsb.sh, etc, use different color schemas.
As a consequence, acrosss plots, the same protocol may appear with different colors which is difficult to follow for the reader.
This task should 
- Use a unified color schema linking the protocols listed in /Docker/protocols.txt to a well-defined color.
To do this, you may change protocols.txt into a csv file whose second entry is the chosen color for the protocol.
The Accord protocol must be in red.
- In each plot, when the performance of a protocol is illustrated, the corresponding curve/points should use the appropriate color.
- Remove the mention of the colors from all the generated plots but the one created by cdf.sh.
In this plot, at the top of it, you should list the protocols together with the chosen color.

# 06.03 - copilot

In parse_ycsb_to_csv.sh, the script counts failures as follows:

    # Extract per-operation failure counts: "[OP], Failures, VALUE"
    /^\[[^]]+\], Failures,/ {
        split($0, a, ",")
        op_field = a[1]
        gsub(/^\[|\].*$/, "", op_field)
        op_lower = tolower(op_field)
        if (op_lower != "cleanup") {
            val = a[3]
            gsub(/^[ \t]+|[ \t]+$/, "", val)
            if (val ~ /^[0-9]+$/) op_fail[op_lower] = val + 0
        }
    }

This is not the correct way. To understand why, look at the following extract of a YCSB client log:

[OVERALL], RunTime(ms), 116824
[OVERALL], Throughput(ops/sec), 0.8559884955146203
[TOTAL_GCS_G1_Young_Generation], Count, 3
[TOTAL_GC_TIME_G1_Young_Generation], Time(ms), 28
[TOTAL_GC_TIME_%_G1_Young_Generation], Time(%), 0.023967677874409368
[TOTAL_GCS_G1_Old_Generation], Count, 0
[TOTAL_GC_TIME_G1_Old_Generation], Time(ms), 0
[TOTAL_GC_TIME_%_G1_Old_Generation], Time(%), 0.0
[TOTAL_GCs], Count, 3
[TOTAL_GC_TIME], Time(ms), 28
[TOTAL_GC_TIME_%], Time(%), 0.023967677874409368
[UPDATE-FAILED], Operations, 81
[UPDATE-FAILED], AverageLatency(us), 1219410.172839506
[UPDATE-FAILED], MinLatency(us), 587776
[UPDATE-FAILED], MaxLatency(us), 1902591
[UPDATE-FAILED], 1stPercentileLatency(us), 588287

It informs us that operations that failed are report as [FOO-FAILED], Operations, XXX
where FO0 is the name of the operation and XXX the number of times it failed.

As a consequence, to compute the ratio of failed operation, one should capture this line to count the number of time an operation of type FOO failed.
Then, summing this value with the total number of time it succeeded gives us easily a ratio, which is the value we need to report.

# 06.03 - copilot

The reporting for Cassandra is nice but there is a slight glitch:
for the extreme conflict rate, 0 and 1, the failure ratio are not quite visible.
To fix this, you should slightly move on the right the one for 0 and on the left the one for 1.

Also, the failure ratio is interesting, so let's generalize this to all protocols.
However, if there is no failures, then 0% can be omitted from the plot.
In /Docker, the conflict.sh experiment report that some clients might fail.
The goal of this task is to report such an information in the plot created in conflict.py.
In detail, 
- parse_ycsb_to_csv.sh computes also the percentage of failed operations (from the total of operations executed by the YCSB client)
This is a an additional column of the csv file, after p100, called "failed".
- when some clients fail the dot of the corresponding protocol is painted with a crossing pattern
- for Cassandra, the ratio of failed operations is reported above each of its dot in the plot

# 10.03 - copilot

This task adds under /Docker an experiment that runs the swap workload in YCSB.
This workload is described here: https://github.com/otrack/YCSB/blob/cassandra5/core/src/main/java/site/ycsb/workloads/SwapWorkload.java.
In detail, 
- the experiment is implemented in a bash script called swap.sh
- it follows the same pattern as other experiments, e.g., closed_economy.sh, cdf.sh, and conflict.sh
- the workload is already available in the Docker image of YCSB (under wokrloads/workloadsw)
- there is no other workload to run 
- the number of nodes is 5 and the replication factor is set 3
- parameter S in the SwapWorkload varies from 3 to 8
- because the workload relies on transactions, only two protocols are evaluated: accord and cockroachdb
- as with other experiements, the script outputs a Latex plot that informs about the results;
  more precisely this plot should (i) on the x axis varies the number of swapped items, ie., parameter S, from 3 to 8
  and (ii) on the y axis mentions the total throughput of the system.

# 10.03 - copilot

This task adds a flag --test to the experiment scripts in /Docker (that is cdf.sh, closed_economy.sh, conflict.sh, ephemeral.sh, fault_tolerance.sh, latency_throughput.sh, ycsb.sh and swap.sh).
When raised, this flag
1) changes the default 600s for an experiment to run (parameter maxexecutiontime) into 60s.
2) right sizes the container size to fit the machine on which the experiment is launched.
In detail, assume that the experiment runs on a machine M with c cpus and g gigabytes of memory.
Suppose further that this experiment uses k database containers.
When the flag is raised, the machine specification (machine=...) in exp.config is ignored.
Instead, the script picks a specification s with s.c cpus and s.g gigabytes of memory such that c <= s.c * k and g <= s.g * k.

# 10.03 - copilot

In the experiment /Docker/conflict.sh, the conflict rate is set from 0.0 to 1.0 by step of 0.1.
This task should change this conflict rate to from 0.0 to 0.1 by step of 0.01.
There are two files impacted by this change.
First, the script launching the experiment itself, cdf.sh.
Second, the Python script, cdf.py, in charge of creating the plot output by cdf.sh.

# 10.03 - copilot

This task consists in two parts.
First, change the default time maxexecutiontime to 120s when the "--test" flag is set.
This should impact all the experiments scripts under /Docker, i.e., cdf.sh, closed_economy.sh, ephemeral.sh, conflict.sh, fault_tolerance.sh, latency_throughput.sh, swap.sh, and ycsb.sh.
Second, create a new script /Docker/run-all.sh that run all the abvoe scripts one after the other.
By default, this script use the "--test" flag everywhere.
When moving to the next script, it should track that the previous one went well.
If not, it takes care of clearing all the docker containers still running and related to the previous experiment, and re-executes the faulty script.

# 12.03 - copilot

Make the following modifications to the plotting scripts:
- ycsb.py should plot the median latency (not the average) on the y axis.
Additionally, each bar should include the standard deviation, presented as a small solid line 
centered horizontally at the tip of the bar.
The scale of the y axis should be 0 to 500 ms.
- conflict.py latency_throughput.py, closed_economy.py and swap.py should all plot the median and not the average latency.
- regarding closed_economy.py and swap.py, reduce the width of the figures, because currently there is a lot of blank space.
The two figures should fit side-by-side on a column in a two-column paper.
You may remove the labels for the swap.py plot because they are the exact same as in closed_economy.py.

# 17.03 - copilot

Some replication protocols use special paths to execute client commands.
The goal of this work is to update the experiments in /Docker to return this information.
More precisely,

- Create a script swiftpaxos/swiftpaxos_fast_path.sh to extract such an information for the protocols implemented by the swiftpaxos library.
This script is modelled after accord_fast_path.sh.
It returns the ratio of fast path by fetching the log of the container passed as an argument.
In the log, the replication protocol ouputs information in the following form: 

2026/03/17 13:26:50 weird 0; conflicted 1200; slow 0; fast 17329
2026/03/17 13:26:51 weird 0; conflicted 1309; slow 0; fast 17331
...

Only the last line matters because it contains the most recent observation.
The script extracts ratios that read as follows:

Fast ratio: 0.927
Medium ratio: 0.0
Slow ratio: 0.073
Ephemeral ratio: 0.0

The fast ratio corresponds to fast / (slow + fast).
The slow path ratio is computed similarly.
The other two ratios are not computed by swiftpaxos, hence their values is set to 0.0.

- Create a script cockroachdb/cockroachdb_fast_path.sh that always returns:

Fast ratio: 0.0
Medium ratio: 0.0
Slow ratio: 1.0
Ephemeral ratio: 0.0

- In Docker/run_benchmark.sh, at the bottom of run_benchmark() call the scripts to fetch the ephemeral/fast/medium/slow path ratios for each of the container.
For each such ratio, compute the average over all the containers.
Then, these ratios are stored under ${output_file}_fast_path_ratio.dat.

# 17.03

In Docker/fault_tolerance.sh, the script slows down then crashes database-node1.
This is arbitrary.
Instead, we want that such events are applied to a leader of the replication protocol.

For each of the replication protocol familly, i.e., cassandra, cockraochdb, and swiftpaxos, add a function family_get_leaders() in family/cluster.sh.
This function takes as input the protocol used in the family.
It returns the container hosting a leader (there might be many) of the replication protocol.
More precisely,
- for cockroachdb, it returns all the containers which one lease holder for a range of the usertable table (created in cockroachdb/ycsb.sh).
- for swiftpaxos, if the protocol used is paxos, it should return the leader by parsing the log of all the containers, looking for the message "I am the leader"
- for cassandra, this can be database-node1, as previously

# 17.03

The Docker/run_benchmark.sh script now reports the ratio of fast/medium/slow/ephemeral paths for each protocol.
(See the comment "Compute special execution path ratios" and lines below it in the script.)
Add this information to the cvs file extracted from the logs with parse_ycsb_to_csv.sh.
It can be added in the form of four columns "fast_path,medium_path,slow_path,ephemeral_path" after the "failed" column.

# 17.03

When injecting a slowdown (event 1) in /Docker/fault_tolerance.sh, the previous tc policies should be saved and later restored when the slowdown disappears (event 2).

# 18.03

Make the following improvements to the plotting scripts:
- cdf.pdf: remove the bottom plot about average tail latency. [DONE]
- ycsb.pdf: cap the y-axis to 400ms (instead of the present 500ms) [DONE]

# 19.03

For all the experiments under /Docker (that is cdf.sh, closed_economy.sh, ephemeral.sh, etc., including run-all.sh which runs them all) add a new parameter "--protocols".
This parameter takes a list of protocols that override the $protocols variable in these scripts.
For instance, callling

> ./cdf.sh --protocols="accord swiftpaxos-paxos" --test

runs the cdf experiment in test mode using for only the accord and swiftpaxos-paxos prootocols.

Notice that when "--dry-run" is passed, the experiment is skipped in full so this new parameter is just ignored.

# 24.03

The objective of this task is to create a plot of the performance breakdown for Cockroachdb and Accord when executing Docker/closed_economy.sh.
For this, you should leverage two prior scripts: 
- breakdown.py which already computes a breakdown for CockroachDB (in this script, the part for Accord is *not* working)
- cassandra/cassandra_breakdown.sh which computes a breakdown for Accord
Please follow these steps carefully to do the task:
1) Remove the use of breakdown.py in cdf.sh and ignore the tracing parameter for accord/cassandra in run_benchmark.sh 
2) Move breakdown.py to cockroachdb/cockroachdb_breakdown.py. In this script, remove the part related to Accord and the ploting part which is of no use now. The script should now output something similar to cassandra/cassandra_breakdown.sh, i.e., five lines of the following form: city,fast_commit,slow_commit,ordering,execution. Notice that, since there is no fast path in the protocol, it should be always set fast_commit to 0. The slow commit time is simply equal to commit time which is retrieved from the tracing capabilities of Cockroachdb/Cockroachdb_Breakdown, as previously computed in breakdown.py.
3) In closed_economy.sh, the call to run_benchmark should not clean-up the cluster. Instead, when the experiment ends, the script call the appropriate script to compute the performance breakdown and store this information under results/closed_economy/breakdown.csv. Please add an appropriate header to this file. Do not forget to raise the tracing flag when calling run_benchmark.sh so that cockroachdb collect useful information for the performance breakdown at the clients.
4) Adjust the code of closed_economy.py to add another plot on the right of the existing one. This plot is a stacked histogram similar in spirit to what breakdown.py was previously  doiung (so you may re-use the old code there). It shows time spent in each phase by the two protocols. To have something readable, report the average time across all DCs (cities) spent in each phase for the first experiment, i.e., nodes=replication_factor=3.

# 25.03

Let's improve the script Docker/closed_economy.sh. 
The end goal is twofolds: add an optimal cockroachdb deployment in the comparison, and better present the results.
For this,
- Add a function cockroachdb_fix_lease_holder to cockroachdb/cluster.sh. 
This function fixes the lease holder at the best location (assuming there is a single data range), and using as input the total number of nodes.
First, the script computes the best Paxos leader location using the distance among peers--this is provided by the functions defined in distance.py and emulate_latency.py.
Then, it alters the table "usertable" which is created by cockroachdb/ycsb.sh using an appropriate statement.
For instance, if Hanoi is the best location, then it the statement should be:
ALTER TABLE usertable
  CONFIGURE ZONE USING
    lease_preferences = '[["+region=Hanoi"]]';
- Use the function above to place ideally the lease holder in cockroachdb when executing Docker/closed_economy.sh.
For this, the cleanest way is to add a new parameter in exp.config called cockroachdb.best_location.
By default it is set to false.
This parameter is read is cluster.sh.
If it is set (i.e., true) then the function above is called right after creating the table to place ideally the lease holder.
In closed_economy.sh, the script should change exp.config to have cockroachdb.best_location set to true.
- The plot Docker/closed_economy.py is in charge of showing the result of the experiment.
In the right figure, for the moment, we only display the result for 3 sites.
Add also the result for 5 and 7.
The computation is the very same: one takes the average of the commit, ordering, and execute phase across the various nodes.

# 25.03

Some more changes are needed in the closed economy experiment:
- The closed_economy.sh experiment should evaluate cockroachdb in two flavors:
when the lease holder is pinned at the best location and holds all ranges which is the current setting,
but also in another set-up when cockroachdb decides for everything using default settings.
These two flavors should be reported as "CockroachDB" and "CockroachDB*" in the plots.
To implement this, change the scripts closed_economy.sh and closed_economy.py appropriately.
- Also, the plots computed by closed_economy.py should fit on a single line, which is currently not the case.
Moreover, the y axis of the second plot (breakdown one) should be in millisecond, as the first one.

# 25.03 - copilot

A handful of small changes are needed in the Docker experiments:
- The caption of the figures should always list protocols in the same order. Currently, this is not the case.  It should always be the order defined in protocols.csv.
- In all plotting scripts, Accord should be plotted last so that it(s) curve(s) overwrite the others'. 
- In ycsb.sh, the bar for standard deviation are in different colors. Please always use black everywhere. This is readable.
- To remove blanks, shrink the height of the plots computed in the following experiments:
ycsb (~25% less), latency_throughput (~10% less), closed_economy (~25% less), swap (~50% less) and fault_tolerance (~30% less).
- Use a Latex tiny font everywhere but captions.

# 25.03 - copilot

One more change is needed in the closed economy experiment (Docker/closed_economy.sh).
Please follow the instructions below.

Currently the number of client is one per DC. This is necessary to have a detailed breakdown with CockroachDB. Indeed, when tracing is on, this outputs a lot of data. Nonetheless, it would be interesting to add another set of runs where the number of clients is the default one, that is as defined in exp.config (currently, threads=10 in this file). 

For these additional runs, we also run all the three algorithms (Accord, CockroachDB, and CockroachDB*). They are plotted together with the ones for a single client per DC in the right-hand side figure. To separate the two set of runs, draw a vertical dashed line between them. Change also the y-axis label of the figure. For the first set of plots, it should read as "client/DC=1", while the second set of runs should be "client/DC=X", where X is the value read from exp.config. 

For thesse additional, we do not provide a breakdown because, as said above, this is not practical. Consequently, desactivate tracing for cockroachdb and do not run the breakdown script for Accord.

# 25.03 - meeting

TODO:
add commit time to Fig. 9
mail to B. what are the needed metrics to plot slow/fast paths
Fig. 11: worst/best-case for crdb
double-check Fig. 13 (try w. a bigger data set and theta=0.0) [FEDOR]
intro: add some citations about commit+ordering protocols
related work: explain Tiga

# 25.03 - copilot

In closed_economy.py, the color of each stack should be the one of the corresponding protocol.
Fix this by emitting one `\addplot` per (bar position, phase), each containing a single coordinate.
Each `\addplot` uses `fill=<protocol_color>` directly so every bar gets its protocol's color.
The `pattern color` still uses the phase color so patterns differentiate commit/ordering/execution.
This avoids the colormap/scatter approach which does not work with `ybar stacked` in pgfplots.

# 27.03 - copilot

In closed_economy.sh, three protocols are compared: Accord, CockroachDB, and CockroachDB+.
CockroachDB+ stands for the best possible placement for CockroachDB: the best location in the system (quorum-wise) owns all leases.
The goal of this task is to replace CockroachDB with CockroachDB-:
this time, the worth location owns all leases.

To do this task, you should proceed as follows:
- Add a boolean argument to cockroachdb_fix_lease_holder() in cluster.sh.
If this argument is set, then the best lease holder is chosen, and otherwise the worst.
- Add a protocol named "cockroachdb-bad" to protocols.csv with a green-ish color.
Similarly to cockroachdb-opt, filter out this protocol from the protocols variable used in the cdf, ycsb, conflict, and latency_throughput.
- Replace cockroachdb with cockroachdb-bad in closed_economy.sh

# 27.03 - copilot

The script Docker/swap.sh executes an experiment during which clients run continuously transactions to swap atomically the content of S records (actually, just the first field of each record).
The script that plots the results of this experiment is Docker/swap.sh.
For the moment, this script outputs a single figure.
The goal of this task is to include a new figure, in which the performance of each database is brokendown.
This figure is very similar to the right-hand plot created by Docker/closed_economy.py.
Please follow carrefully the following steps to do this task:
- Change swap.sh to compute a performance breakdown after each run.
These results are stored under the directory results/swap/, in a file called breakdown.csv.
This file is similar to results/swap/breakdown.csv and consists in entries of the following form: 
protocol,S,city,fast_commit,slow_commit,commit,ordering,execution, where S is the number of items swapped during the run.
- In Docker/swap.py, add a new figure on the right of the existing one.
This figure is a bar plot detailing the performacne breakdown of the two protocols compared in swap.sh.
For each value of S, a breakdown is presented for the two protocols.
Below each such group, add a mention of the value of S.
Additionally add a legend to the x axis that reads "breakdwown".

# 27.03 - copilot

Currently, the swap experiment runs with a single client per site.
The breakdown is computed only for this case.
I want that you add another case where we run instead 50 clients per site.
The result of this experiment should appear as a dashed line on the left-hand plot computed in swap.py.

# 27.03 - copilot

In Docker/conflict.sh, only the end-to-end latency of Accord appears.
The goal of this task is to add an additional line plotting the commit latency.
For this, follow carefully the following steps:
- Add a new protocol accord-cmt to protocol.csv.
This protocol will be used nowhere else than in conflict.sh, so be careful in filtering it out elsewhere in variable protocols.
- To retrieve the commit time in Accord, use function compute_breakdown() in cassandra/cassandra_breakdown.sh.
Important: the time reported here is computed over the full life time of the server.
Thus for each value of theta, the server needs to be restarted and refilled with data.
As a consequence, the usual loop "for t in ${thetas}" in conflict.sh needs to be adjusted appropriately.

# 28.03 - copilot 

Make the following modifications:
- Add a back-up host to CockroachDB in run_benchmark.
A back-up can be found by using variable hosts until line 171 who stores the IP address of all the database containers separated by commas
(e.g., 172.18.0.2,172.18.0.3,172.18.0.4,172.18.0.5,172.18.0.6).
A backup can be added by passing an address of the following form:
jdbc:postgresql://host1:26257/db?cockroachdb=true&sslmode=disable,jdbc:postgresql://host2:26257/db?cockroachdb=true&sslmode=disable
- For every experiment script in Docker, before starting the experiment ensure that the host holds the latest version of the container images defined in exp.config.
Also, remove this computation from run-all.sh 

# 28.03

TODO:
(Fig. 8: add commit latency for Accord) [IN-PROGRESS]
Fig. 9: look for hokey limit using 2* then finer-grain search [DONE] 
Fig. 11: side-by-side breakdown [DONE]
add a latency-aware policity in the Java Cassandra driver
Fig. 12: jdbc cannot recover? [DONE]
Fig 11: single breakdown, the rest are just identical

# 28.03

In YCSB, tracing is now working when there are multiple clients per thread: just a single one is activating traces. The goal of this task is to leverage this feature to enhance the experimental results of Docker/swap.sh.
To achieve this, please carefully follow the steps below:
- In swap.sh, merge the logic of the two loops when there is a single client (phase 1) and 50 clients (phase 2).
- In every run of the benchmark, activate tracing in swap.sh.
- Compute a performance breakdown as previously at the end of the run.
Because now there might be a variable number of clients add a column right after S and entitled "clients" in results/swap/breakdown.csv
In other words, the header should now be: protocol,S,clients,city,fast_commit,slow_commit,commit,ordering,execution.
Update the computation of the performance breakdown to also mention the number of clients.
- Be careful that between #clients=1 and #clients=50 Accord needs to be restarted.
This comes from the fact that the internal metrics are not reset.
- In the figure created by swap.py, the right-hand site plot should now report four groups of bars:
1) one for #client=1, S=1 with a performance breakdown of CockroachDB and Accord
2) one for #client=50, S=1 with a performance breakdown of CockroachDB and Accord
3) one for #client=1, S=8 with a performance breakdown of CockroachDB and Accord
4) one for #client=50, S=8 with a performance breakdown of CockroachDB and Accord

# 30.03

In /Docker/latency_throughput.sh, the experiment stops when the latency is above 500ms.
Instead, it should look for the Pareto front and stops when both latency and throughput degrade wrt. the previous increase in the number of clients.
Implement such a change in the script.

# Agent

The script demo.sh runs the closed economy workload in YCSB using Accord, a new replication for Apache Cassandra. 
The workload executes in a system of 3 geo-distributed replicas, with one colocated YCSB client per replica.
YCSB clients use the tracing capability of the Java CQL driver to see how replicas exchange messages.
Logs present under ./logs/demo/accord*.dat illustrate the output produced by the tracing mechanism.

The goal of this work is to create a live representation of the messages exchanged among replicas by the Accord protocol.
The representation is displayed as a webpage in a server running in a separate container (as the clients and replica ones).
It parses the logs of the YCSB clients.
It is implemented entirely in javascript using Apache ECharts and uses nodejs.

# Agent

Implement the following improvements:
- Put a world map in the background of the live visualizer.
- Put the datacenters in their actual locations on the world map. To do this, use latencies.csv which includes the latitude and longitude of each DC.
- Support any number of datacenters in the visualizer.
- Distinguish the type of messages sent between replicas using colors and add an appropriate legend at the top left-hand side of the visualizer.
- On the top right-hand side, add the state of the replicated database. To consult it, you may use statement of the form "SELECT * FROM ycsb.usertable" and either (a) call cqlsh at one of the replica, or (b) execute a CQL query from JS but this might be harder to code. Because this is a closed economy workload, add the currency (€) of each account. Moreover, add also the total sum of the accounts below all accounts. The state of the database is updated continuously every second.
- When a transaction is executed at a datacenter, illustrate it as a transfer(X, Y) where X and Y are the two accounts being access.
- Map each account to a robot (see live-viz/robots). When a robot receives some money, it gets happy and its icon slightly blink.

# Agent

Implement the following things:
- add a slow-motion mode where the transferts are executed slowly one-by-one and their corresponding messages takes more more time tro travel (say a split-second).
- when launching the demo with demo.sh open the corresponding webpage in the webbrowser
- add a DEMO.md explaining how to launch the demo 
- git add all the files which were created for the demo thus far
