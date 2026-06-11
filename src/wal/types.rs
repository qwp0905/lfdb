use std::sync::atomic::AtomicU64;

pub type LogId = u64;
pub type AtomicLogId = AtomicU64;
pub const LOG_ID_BYTES: usize = LogId::BITS as usize >> 3;
pub type TxId = u64;
pub const TX_ID_BYTES: usize = TxId::BITS as usize >> 3;
pub type AtomicTxId = AtomicU64;

// Sized to hold at least 2 base pages (base page = 4KB) with room for headers.
pub const WAL_BLOCK_SIZE: usize = 16 << 10; // 16kb
