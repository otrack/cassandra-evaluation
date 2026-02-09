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

