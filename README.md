# LFDB

Lock-Free Key-Value Storage Engine implemented in Rust.

A persistent, ACID-compliant embedded key-value store built for **high-concurrency workloads**. Unlike single-writer embedded databases (e.g. BoltDB, LMDB), it supports **concurrent reads and writes from multiple threads simultaneously** — with no writer lock and no reader starvation — through a combination of B-link tree indexing, MVCC snapshot isolation, and a lock-free WAL.

## Usage

### Open

The path passed to `EngineBuilder` is used as a dedicated data directory. The engine creates WAL segments and per-table data files inside it. Use a directory that is not shared with other applications.

```rust
use lfdb::EngineBuilder;

let engine = EngineBuilder::new("./data")
    .buffer_pool_memory_capacity(128 << 20) // 128MB
    .build()?;
```

### Create Table

```rust
let mut tx = engine.new_tx()?;
tx.open_table("users")?;
tx.commit()?;
```

### Read & Write

```rust
let mut tx = engine.new_tx()?;
let users = tx.table("users")?;

users.insert(b"key1".to_vec(), b"value1".to_vec())?;

let value = users.get(b"key1")?; // returns Option<Vec<u8>>

users.remove(b"key1")?;

tx.commit()?;
```

### Scan

```rust
let tx = engine.new_tx()?;
let users = tx.table("users")?;

// Range scan [start, end)
let mut iter = users.scan(b"key1", b"key3")?;
while let Some((key, value)) = iter.try_next()? {
    // process
}

// Full scan
let mut iter = users.scan_all()?;
while let Some((key, value)) = iter.try_next()? {
    // process
}

tx.commit()?;
```

### Drop Table

```rust
let mut tx = engine.new_tx()?;
tx.drop_table("users")?;
tx.commit()?;
```

`drop_table` is transactional. The drop only takes effect after `commit()` — until then, other transactions still see the table through their own snapshot. On `abort()` (or implicit abort via drop), the table remains intact.

The physical data file is removed in the background after commit, once any in-flight cursors on the table have finished.

**Restrictions:**
- Concurrent drops on the same table return `WriteConflict`. The first transaction to call `drop_table` wins; the second must retry.
- Re-opening the same name after a drop within the same transaction is not allowed and returns `TableAlreadyDropped`. Drop and re-create must happen in separate transactions.
- A table created and dropped within the same transaction is never persisted — neither commit nor abort leaves anything on disk.

### Multi-Table Transaction

```rust
let mut tx = engine.new_tx()?;
let users = tx.table("users")?;
let orders = tx.table("orders")?;

users.insert(user_id.clone(), user_data)?;
orders.insert(order_id, order_data)?;

tx.commit()?; // atomic across both tables
```

### Metrics

```rust
let m = engine.metrics();
println!("uptime: {}ms", m.uptime_ms);
println!("cache hit: {}", m.buffer_pool_cache_hit);
println!("get p99: {}µs", m.operation_get_latency_micros_p99);
```

## Configuration

| Option | Description |
|--------|-------------|
| `wal_file_size` | Size limit of a single WAL segment file. When exceeded, a new segment is created. |
| `wal_segment_flush_delay` | Maximum time to wait before triggering a checkpoint for WAL segment reuse. |
| `wal_segment_flush_count` | Maximum number of commits to buffer before triggering a checkpoint for WAL segment reuse. |
| `checkpoint_interval` | Hard timeout for checkpoint execution — runs regardless of WAL segment pressure. |
| `group_commit_count` | Maximum commits buffered per WAL segment. Larger values improve write throughput but increase potential data loss on crash in high-latency IO environments. |
| `buffer_pool_memory_capacity` | Total memory allocated to the buffer pool. Since the engine uses Direct I/O and bypasses the OS page cache, a larger buffer pool is critical for performance. |
| `buffer_pool_shard_count` | Number of buffer pool shards. More shards reduce lock contention but shrink each shard's capacity, increasing eviction frequency. |
| `gc_trigger_interval` | Interval at which leaf merge runs. Run more frequently when removes are heavy, less frequently when removes are rare. |
| `gc_thread_count` | Number of GC threads. In write-heavy workloads with frequent WAL segment rotation, increasing this can improve write throughput. |
| `transaction_timeout` | Maximum lifetime of a transaction before it is automatically aborted. |

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
│  Buffer Pool  │ │ │  TableMapper   │  │ │    Garbage    │
│  2-tier LRU   │ │ │  ┌───────────┐ │  │ │   Collector   │
│  sharded lock │ │ │  │TableHandle│ │  │ │   2-process   │
└───────────────┘ │ │  │ FreeList  │ │  │ └───────────────┘
                  │ │  │ DiskCtrl  │ │  │
   ┌──────────────┘ │  └───────────┘ │  │
   │                └────────────────┘  │
   │                                    │
┌──▼──────────────┐  ┌──────────────────▼┐
│      WAL        │  │     Version       │
│  lock-free      │  │    Visibility     │
│  CAS append     │  │     (MVCC)        │
└────────┬────────┘  └───────────────────┘
         │
┌────────▼────────┐
│  WAL Segments   │
│  + Preloader    │
│  + Checkpoint   │
└─────────────────┘
```

### Characteristics

- **Embedded, single process**: the database is opened by one process at a time. `Engine` can be wrapped in an `Arc` and shared freely across threads within the same process.
- **Key ordering**: keys are compared as raw bytes in lexicographic order.

### Transaction Lifecycle and Isolation

LFDB provides **Snapshot Isolation**. Each transaction reads from a consistent snapshot taken at the moment it starts — concurrent writes by other transactions are invisible until they commit, and only to transactions that begin after the commit. The same rules apply uniformly to every operation, including DDL such as `open_table` and `drop_table`.

**Visibility rules:**
- A transaction always sees its own writes
- Only committed transactions' writes are visible to others
- A transaction started before a commit will never see that commit's writes — even if the commit completes before the transaction ends

**Write conflicts:**
A `WriteConflict` is returned immediately whenever two concurrent transactions race on the same resource. It applies uniformly to every mutating operation:

- **Row writes** — two transactions inserting or removing the same key
- **Table creation** — two transactions calling `open_table` with the same name on a table that does not yet exist
- **Table drop** — two transactions calling `drop_table` on the same table

In every case the loser gets `WriteConflict` at the call site (not at commit). The application is responsible for retrying.

Optimistic locking is straightforward — read, compute, write, and retry on conflict:

```rust
loop {
    let mut tx = engine.new_tx()?;
    let table = tx.table("accounts")?;
    let current = table.get(&key)?;
    let next = compute_next(current);
    match table.insert(key.clone(), next) {
        Ok(_) => {},
        Err(Error::WriteConflict) => continue,
        Err(e) => return Err(e),
    }
    tx.commit()?;
    break;
}
```

**Auto-abort:**
A transaction automatically aborts on drop if `commit()` was not called. There is no need to explicitly call `abort()` on error paths.

**Timeout:**
A transaction that exceeds its configured timeout is automatically aborted. Any subsequent operation returns `TransactionClosed`.

### Crash Recovery

LFDB recovers automatically on restart — no manual intervention is required.

On startup, the engine replays the WAL and redoes all committed transactions since the last checkpoint. Uncommitted transactions (those that were in-flight at the time of the crash) are treated as aborted and their writes are invisible.

**Durability guarantee**: `commit()` returns only after the WAL record has been fsynced to disk. If `commit()` returned `Ok`, the transaction will survive a crash.

## Limitations

- **Key size**: maximum 256 bytes
- **Value size**: maximum 65,536 bytes (64 KB)
- **Heavy removes**: compaction is not implemented. Disk space freed by removes is returned to the per-table free list and reused for new writes, but each table's data file never shrinks. Heavy delete workloads will cause disk fragmentation over time. Tune `gc_trigger_interval` to reduce empty leaf nodes and maintain scan performance, but this does not recover fragmented disk space. Dropping a table deletes the entire file immediately.

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.
