use std::sync::atomic::AtomicU64;

/**
 * WAL log sequence id.
 *
 * `LogId` orders WAL records and defines replay boundaries. It is independent
 * from transaction ids.
 */
pub type LogId = u64;
pub type AtomicLogId = AtomicU64;
pub const LOG_ID_BYTES: usize = LogId::BITS as usize >> 3;

/**
 * Transaction/version id.
 *
 * `TxId` is used for ownership and visibility decisions. It is not a WAL
 * position and does not share ordering semantics with `LogId`.
 */
pub type TxId = u64;
pub const TX_ID_BYTES: usize = TxId::BITS as usize >> 3;
pub type AtomicTxId = AtomicU64;

/**
 * Monotonic WAL segment generation.
 *
 * This identifies the WAL segment generation and also acts as the boundary used
 * by WAL sync/commit coordination. A commit must be able to wait for sync
 * durability through the required previous generation.
 */
pub type SegmentGeneration = u64;

// Sized to hold at least 2 base pages (base page = 4KB) with room for headers.
pub const WAL_BLOCK_SIZE: usize = 16 << 10; // 16kb
