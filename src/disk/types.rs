use std::sync::atomic::AtomicU64;

/// Logical disk block pointer used by the storage layer.
pub type Pointer = u64;
pub const POINTER_BYTES: usize = Pointer::BITS as usize >> 3;
pub type AtomicDiskPointer = AtomicU64;
