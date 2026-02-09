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
