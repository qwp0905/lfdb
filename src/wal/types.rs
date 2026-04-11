pub type LogId = u64;
pub const LOG_ID_BYTES: usize = LogId::BITS as usize >> 3;
pub type TxId = u64;
pub const TX_ID_BYTES: usize = TxId::BITS as usize >> 3;

pub type SegmentGeneration = u64;
