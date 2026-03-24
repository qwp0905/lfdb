# LFKV-DB

Lock-Free Key-Value Storage Engine implemented in Rust.

A persistent, ACID-compliant embedded key-value store built for **high-concurrency workloads**. Unlike single-writer embedded databases (e.g. BoltDB, LMDB), it supports **concurrent reads and writes from multiple threads simultaneously** — with no writer lock and no reader starvation — through a combination of B-link tree indexing, MVCC snapshot isolation, and a lock-free WAL.

## Features

- **Blink Tree Index** - Lock-free concurrent B-tree variant with right-link pointers for non-blocking traversal and range scans
- **MVCC** - Snapshot isolation through version chains; readers never block writers
- **Lock-Free WAL** - CAS-based append with atomic bit-packed offset, epoch-based buffer reclamation, and segment preloading/reuse
- **Group Commit** - Batches multiple fsync calls per segment to amortize I/O cost
- **Buffer Pool** - 2-tier LRU (old/new) page cache with sharded locking and atomic pin-based eviction control
- **Direct I/O** - OS page cache bypass for predictable I/O performance
- **Garbage Collection** - Queue-based version cleanup and periodic leaf cleanup that reclaim obsolete versions and freed pages
- **Crash Recovery** - Automatic WAL replay on startup with redo of committed transactions
- **Custom Thread Infrastructure** - Work-stealing thread pool, oneshot channels, and buffered execution modes, all with panic safety

## Usage

```rust
use lfkv_db::EngineBuilder;

let engine = EngineBuilder::new("./data")
    .buffer_pool_memory_capacity(128 << 20) // 128MB
    .build()?;

// Start a transaction
let mut tx = engine.new_transaction()?;

// Insert
tx.insert(b"key1".to_vec(), b"value1".to_vec())?;
tx.insert(b"key2".to_vec(), b"value2".to_vec())?;

// Read
let value = tx.get(b"key1")?;

// Range scan [start, end)
let mut iter = tx.scan(b"key1", b"key3")?;
while let Some((key, value)) = iter.try_next()? {
    // process data
}

// Full table scan
let mut iter = tx.scan_all()?;
while let Some((key, value)) = iter.try_next()? {
    // process data
}

// Delete
tx.remove(b"key1")?;

// Commit (or auto-aborts on drop)
tx.commit()?;
```

## Architecture

```
                    ┌──────────────────┐
                    │      Engine      │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  TxOrchestrator  │
                    │  (coordination)  │
                    └──┬──┬──┬──┬──┬───┘
                       │  │  │  │  │
         ┌─────────────┘  │  │  │  └──────────────┐
         │        ┌───────┘  │  └───────┐         │
         │        │          │          │         │
┌────────▼──────┐ │ ┌────────▼───────┐  │ ┌───────▼───────┐
│  Buffer Pool  │ │ │   Free List    │  │ │    Garbage    │
│  2-tier LRU   │ │ │  (page alloc)  │  │ │   Collector   │
│  sharded lock │ │ └────────────────┘  │ │   2-process   │
└───────┬───────┘ │                     │ └───────────────┘
        │  ┌──────▼────────┐  ┌─────────▼───────┐
        │  │      WAL      │  │     Version     │
        │  │  lock-free    │  │    Visibility   │
        │  │  CAS append   │  │     (MVCC)      │
        │  └──┬─────┬──────┘  └─────────────────┘
        │     │     │
┌───────▼─────▼┐ ┌──▼─────────────┐
│DiskController│ │  WAL Segments  │
│ (async I/O)  │ │  + Preloader   │
│  Direct I/O  │ │  + Checkpoint  │
└──────────────┘ └────────────────┘
```

### Modules

| Module | Description |
|--------|-------------|
| `engine` | Bootstrap, configuration, lifecycle management |
| `cursor` | Blink tree traversal, CRUD operations, range iterator, GC pipeline |
| `transaction` | Transaction orchestrator, MVCC visibility, free list |
| `buffer_pool` | 2-tier LRU page cache with sharded locking, atomic pin, dirty page tracking |
| `wal` | Lock-free CAS-based WAL, segment preloading/reuse, group commit, replay |
| `disk` | Async disk I/O controller with Direct I/O, page object pool |
| `thread` | Work-stealing thread pool, oneshot channel, buffered/timeout execution modes |
| `serialize` | Page-level binary serialization with type tag validation |

### Transaction Lifecycle

1. `Engine::new_transaction()` allocates a transaction ID and registers it as active
2. All reads check MVCC visibility - only committed versions are visible to other transactions
3. Writes append version records to data entries; write-write conflicts return `WriteConflict` error
4. `commit()` writes a commit record to WAL and calls `fsync` via group commit
5. `drop` auto-aborts uncommitted transactions

### Write-Ahead Logging

- **Lock-free append**: Atomic bit-packed `u64` (24-bit pin counter + 40-bit offset) enables concurrent writes without locks
- **CAS buffer rotation**: When a 16KB buffer fills, one thread wins the CAS to swap in a new buffer; others retry
- **Epoch-based reclamation**: Replaced buffers are safely freed via crossbeam epoch GC
- **Segment preloading**: Background thread pre-allocates the next segment; completed segments are reused via rename
- **Group commit**: Batches fsync calls per segment to amortize disk I/O
- **Checkpoint**: Periodically flushes buffer pool and advances the recovery point
- **Replay**: On startup, redoes committed inserts after the last checkpoint; incomplete transactions are marked as aborted

### Garbage Collection

Two background processes collaborate to reclaim obsolete versions and freed pages:

- **Queue-based version cleanup** - Transactions mark entries with old versions into a queue. A dedicated process drains the queue in bulk, removes obsolete version records, and releases emptied pages back to the free list.
- **Periodic leaf cleanup** - A single thread periodically scans B-tree leaves, detects entries that have become empty, removes them from leaf nodes, and releases their pages.

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.
