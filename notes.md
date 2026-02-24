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
