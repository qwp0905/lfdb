# Benchmarking

## Why Benchmarking Matters

Storage engine performance cannot be improved reliably through intuition alone.
Every optimization needs an objective measurement that shows whether it
improved the system, merely moved the bottleneck, or introduced a regression
under another workload.

However, an arbitrary metric is not a useful target. Optimizing for an
unrealistic benchmark can produce a fast benchmark implementation without
producing a better storage engine. A benchmark must therefore model workloads
that resemble how the engine is expected to be used in practice.

A useful benchmark should define:

- the shape and size of stored records
- the ratio of reads, inserts, updates, and scans
- the distribution used to select keys
- the number of concurrent clients
- the transaction and durability boundaries
- the relationship between the working set and available cache
- how the database state changes throughout the workload

Each benchmark should answer a concrete performance question. A sequential
insert benchmark measures append behavior, while a highly contended update
benchmark measures coordination under write conflicts. These results describe
different properties and must not be treated as interchangeable measures of
overall performance.


## Why YCSB

LFDB uses YCSB-style (Yahoo Cloud Serving Benchmark) workloads as its primary application-level benchmark
because they describe common storage access patterns through a small and
understandable set of operations.

The workloads combine point reads, updates, inserts, read-modify-write
operations, and short range scans under configurable concurrency and key-access
distributions. This makes them useful for observing not only raw operation
costs, but also cache behavior, write contention, transaction coordination,
tree traversal, WAL processing, and background maintenance under sustained
load.

In particular, skewed key distributions are important. Real workloads rarely
access every key uniformly. A small subset of records is often accessed or
updated much more frequently than the rest, making contention and locality
central parts of storage engine performance. Recent-key and range-scan
workloads additionally model append-oriented and account- or timeline-oriented
access patterns.

YCSB is used as a common workload vocabulary rather than as a claim that every
application behaves identically. LFDB's scenarios are documented individually,
and engine-specific or durability-specific behavior is measured with separate
benchmarks where YCSB is insufficient.

Detailed workload definitions are maintained by the YCSB project:

- [YCSB project](https://github.com/brianfrankcooper/YCSB)
- [Core workloads](https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads)


## Methodology

TODO


## Environment

TODO


## Results

### Concurrency Scaling

TODO

### Dataset Scaling

TODO

### Cache Scaling

TODO