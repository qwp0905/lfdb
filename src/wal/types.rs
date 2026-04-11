use std::sync::atomic::AtomicU64;

pub type LogId = u64;
pub type AtomicLogId = AtomicU64;
pub const LOG_ID_BYTES: usize = LogId::BITS as usize >> 3;
pub type TxId = u64;
pub const TX_ID_BYTES: usize = TxId::BITS as usize >> 3;
pub type AtomicTxId = AtomicU64;

pub type SegmentGeneration = u64;
