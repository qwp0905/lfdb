# Architecture

## Introduction

LFDB is a high-performance storage engine designed for high-concurrency workloads. As its name suggests, LFDB aims to be `lock-free`. While it is not entirely mutex- or latch-free, it is designed to behave as close to that model as possible.


## Core Concept

- Strict ACID compliance with Snapshot Isolation
- Transaction support across multiple tables
- Transactional DDL
- Fully zero-copy reads in small values
- Writes never block reads, and all writes are serialized at row granularity
- Latch-coupling-free tree mutation based on a B-link tree
- Fast performance and durability through a fully lock-free WAL
- High-performance S3-FIFO-based cache with Direct I/O for consistent performance across different operating systems and disks
- Online compaction to minimize disk fragmentation without interruption
- Runtime-overhead-free timeout management built on a high-performance timing wheel timer


### ACID Capabilities

LFDB provides full transactions that strictly comply with the ACID model. This applies to all operations within a single transaction, including single-table operations, multi-table operations, DDL, and DML.

> Atomicity

In LFDB, all writes and DDL operations within a single transaction are handled atomically. Regardless of whether the transaction involves a single table, multiple tables, or DDL, all operations performed within the transaction are either fully applied on commit or fully discarded on rollback. The same guarantee holds even if the system is interrupted due to events such as a power failure: any uncommitted operations are discarded.

> Consistency

LFDB is a general-purpose key-value engine and does not impose special constraints. However, if a key or value that is too large is provided, the request is rejected immediately.

> Isolation

LFDB provides Snapshot Isolation. Every transaction creates its own snapshot at the time it starts and performs reads and writes based on that snapshot. If multiple uncommitted transactions attempt to write to the same row, all transactions except the winner immediately receive a WriteConflict error, allowing them to open a new transaction and retry right away.

> Durability

LFDB achieves durability through its WAL. For every write request, it records WAL entries at the granularity of modified disk data blocks. These records are persisted to disk when the transaction commits, and all committed writes can be safely recovered after an unexpected shutdown by reading the WAL files on restart and validating each WAL record using its embedded CRC32 checksum.
In addition, LFDB uses checkpoints to reuse WAL files whose contents have already been flushed to disk, reducing replay time. WAL record writing is lock-free across all paths, and fsync syscalls for commits from multiple transactions are grouped together, so the runtime cost of maintaining durability is kept very low.


### Concurrency Model

LFDB is designed to handle high-concurrency workloads through the following design points:

> Lock free WAL

LFDB’s WAL is designed to be `lock-free`, using no mutexes at all. Before writing a WAL record, each insert request obtains the offset within the WAL buffer where it will write its data through an atomic operation. This allows data to be written without memory conflicts, and the WAL buffer is safely written to disk in order during commit through CAS. The WAL consists of multiple fixed-size segments and scales according to request frequency. In addition, when a WAL buffer or segment becomes full, it is replaced quickly and safely through atomic CAS, and its pointers are safely reclaimed through epoch-based GC.
The fsync syscalls for WAL segments that occur on commit requests are grouped and processed by the fsync thread, distributing the cost of commits.

> B-Link tree indexing

LFDB uses a B-link tree index, which is slightly different from a traditional B+ tree. A B-link tree is an improved variant of the B+ tree that always maintains a right sibling pointer and a high key in each node. With this structure, if an index traversal encounters a key that exceeds the node’s high key, it can move to the right sibling, allowing index reads without latch coupling. In addition, during tree node splits, height-based optimistic locking allows writes involving multiple splits to proceed concurrently without latching the root node, while reads can continue safely at the same time through right moves.

- [[Efficient Locking for Concurrent Operations
on B-Trees]](https://dl.acm.org/doi/pdf/10.1145/319628.319663)

> Copy on write

All write operations in LFDB never block reads and are performed using copy-on-write at the data-block level. All internal writes are serialized at the single-block granularity; data is written to a copied block, and the block is then atomically replaced, ensuring that reads are not affected at all.

> Block cache

LFDB uses a high-performance block cache based on a modern algorithm to reduce I/O costs. It is built on the S3-FIFO algorithm, enabling efficient cache slot management. By default, the cache is sharded into 64 shards, configurable, to distribute mutex contention. Cache block replacement operations that involve I/O, such as eviction, are performed without holding the mutex for the cache shard, and are designed to be serialized at the individual cache-block level.

- [[S3 FIFO Algorithm]](https://s3fifo.com/)

> Transaction timeout

LFDB uses a timing-wheel-based timer to manage a very large number of transaction timeouts without runtime overhead. Transaction timeouts can be configured at 1 ms granularity and are registered with the timer when a transaction starts. Timeout handling is performed by a separate timer thread, so it has no impact on runtime performance.

> Zero copy read

LFDB reads do not copy any data blocks when the size is small. Through copy-on-write, LFDB does not modify blocks registered in the block cache directly; instead, it only replaces blocks, enabling safe reads without copying data blocks. See the `VecRef` struct for details. However, because VecRef holds a reference to the underlying data block, keeping it alive for a long time may increase memory pressure.

> Buffered io

All disk writes in LFDB are performed asynchronously on separate threads. Disk writes requested around the same time are buffered and sorted per file, then issued through the pwritev syscall. Like WAL group commit, this distributes the cost of disk writes and provides logical async I/O. Support for io_uring on Linux is planned for the future.


### Disk management

LFDB manages disk space through the following design points:

> MVCC / Background GC

In LFDB, data is written by version into disk space separate from the B-tree index. Each transaction reads data based on the snapshot created when it started and searches for the version that is visible to it.
Version chains that can no longer be observed due to transaction lifecycle progression are reclaimed by a periodically running background GC.

> Page allocation

Pages reclaimed by the background GC are stored in a free list and reused. However, the actual disk space is not released, so disk usage grows monotonically until compaction is performed.

> Online auto compaction

When the number of blocks reclaimed by the background GC exceeds a threshold, compaction is triggered automatically. Compaction is performed through a copy-and-remove process, and when compacting a large table, disk usage may temporarily increase significantly until all data has been moved into the new file. This does not cause any interruption. It may slightly reduce runtime read performance, but it does not affect writes at all; in fact, write performance may improve. Automatic compaction can also be disabled.
