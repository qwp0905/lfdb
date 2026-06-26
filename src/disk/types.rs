/// Logical disk block pointer used by the storage layer.
pub type Pointer = u64;
pub const POINTER_BYTES: usize = Pointer::BITS as usize >> 3;
