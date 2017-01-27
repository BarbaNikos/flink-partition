# flink-partition
The past year I have been studying the effects of stream partitioning in performance. 
To that end, I want to develop a number of real-world applications on Apache Flink, and 
measure the effect in performance of choosing different partitioning algorithms. In this 
study, the applications range from simple group-by aggregates to complex analytical queries with 
multiple stages of processing.

# System Infrastructure
I am using Apache Flink version 1.1.4 on a single node setup. The motivation behind this 
setup is to isolate the effect of partitioning to performance and take-out any additional 
costs that come with distributed processing (i.e., network costs, coordination overheads etc.).

# Benchmarks
## TPC-H
## ACM DEBS 2015 Challenge
## Google cluster monitoring dataset