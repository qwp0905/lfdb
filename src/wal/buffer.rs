use std::{
  cell::{Cell, UnsafeCell},
  mem::MaybeUninit,
  sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

use super::{FsyncResult, SegmentGeneration, WALSegment, WAL_BLOCK_SIZE};
use crate::{
  disk::{PageRef, PendingIO, Pointer},
  utils::{ExclusivePin, SBox, SharedToken},
};

/**
 * Shared segment ownership state for buffers in the same WAL segment.
 *
 * Multiple `LogBuffer`s can write blocks that belong to one segment, but the WAL
 * manager eventually needs to recover ownership of that segment for reuse.
 * `MaybeUninit` allows the segment to be moved out exactly once by
 * `take_segment` instead of being dropped with the shared state.
 */
struct SegmentState {
  /**
   * Shared across all buffers within the same segment. Raw pointer allows exclusive
   * ownership transfer via take_segment() when the last buffer finishes — required
   * for segment reuse. It must taken after pin is empty.
   */
  segment: MaybeUninit<WALSegment>,
  /**
   * pin for using segment
   * it must taken with segment pointer.
   */
  pin: ExclusivePin,
  /**
   * rotated and written complete data block count for current segment
   */
  rotated_count: AtomicU64,
  /**
   * flag which segment has been taken to check drop.
   */
  taken: Cell<bool>,
}
impl SegmentState {
  const fn new(segment: WALSegment) -> Self {
    Self {
      segment: MaybeUninit::new(segment),
      pin: ExclusivePin::new(),
      rotated_count: AtomicU64::new(0),
      taken: Cell::new(false),
    }
  }
}
impl Drop for SegmentState {
  fn drop(&mut self) {
    if self.taken.get() {
      return;
    }
    unsafe { self.segment.assume_init_drop() };
  }
}

const BITS: u64 = 40;
const MASK: u64 = (1 << BITS) - 1;

/**
 * A single WAL block (16KB page) being filled concurrently by multiple writers.
 * Writers atomically claim a slot and sequence number via a single fetch_add on offset,
 * then write their record independently.
 */
pub struct LogBuffer {
  /**
   * entry pin (24bit) + offset (40bit), packed into one AtomicU64.
   * A single fetch_add atomically reserves a position in the block and increments
   * the in-flight writer count. The offset portion can grow up to ~4000 bytes per
   * record, so 40 bits is sufficient; 24 bits accommodates the concurrent writer count.
   */
  offset: AtomicU64,
  /**
   * data block for current pointer to store wal records.
   * must mark records count before write to disk.
   * records count can be obtained from pinning entry.
   */
  entry: UnsafeCell<PageRef<WAL_BLOCK_SIZE>>,
  /**
   * written complete count for data entry which has valid offset
   */
  commit_count: AtomicU32,
  /**
   * disk pointer for current data block
   */
  segment_ptr: Pointer,

  segment_state: SBox<SegmentState>,
  /**
   * current generation for current segment
   */
  generation: SegmentGeneration,
}
impl LogBuffer {
  pub fn init_new(
    entry: PageRef<WAL_BLOCK_SIZE>,
    segment: WALSegment,
    generation: SegmentGeneration,
  ) -> Self {
    Self::new(
      entry,
      0,
      SBox::new(SegmentState::new(segment)),
      generation,
      0,
    )
  }
  /**
   * if segment is not full, then copy pointers and recreate buffer
   */
  pub fn init_next(&self, entry: PageRef<WAL_BLOCK_SIZE>, offset: usize) -> Self {
    Self::new(
      entry,
      self.segment_ptr + 1,
      self.segment_state.clone(),
      self.generation,
      offset,
    )
  }

  const fn new(
    entry: PageRef<WAL_BLOCK_SIZE>,
    segment_ptr: Pointer,
    segment_state: SBox<SegmentState>,
    generation: SegmentGeneration,
    offset: usize,
  ) -> Self {
    Self {
      offset: AtomicU64::new(offset as u64),
      entry: UnsafeCell::new(entry),
      commit_count: AtomicU32::new(0),
      segment_ptr,
      segment_state,
      generation,
    }
  }

  pub fn pin_segment(&self) -> Option<SharedToken<'_>> {
    self.segment_state.pin.try_shared()
  }

  /**
   * Atomically reserves a write slot and returns (offset, ready).
   * ready is the number of writers that claimed a slot before this call.
   * Since each writer appends exactly one record, ready also equals the number
   * of records already in the block — used by flush callers to write the correct
   * record count header and to wait for all prior writers to finish.
   */
  pub fn reserve_append(&self, len: usize) -> (usize, u32) {
    let prev = self
      .offset
      .fetch_add(((len as u64) & MASK) | (1 << BITS), Ordering::Release);
    ((prev & MASK) as usize, (prev >> BITS) as u32)
  }
  pub fn append_at(&self, record: &[u8], offset: usize) {
    unsafe { (*self.entry.get()).copy_from(record, offset) };
  }
  pub fn load_committed_append(&self) -> u32 {
    self.commit_count.load(Ordering::Acquire)
  }
  pub fn sync_segment(&self) -> FsyncResult {
    debug_assert!(!self.segment_state.taken.get());
    unsafe { self.segment_state.segment.assume_init_ref() }.fsync()
  }
  pub fn flush_block(&'static self) -> PendingIO {
    debug_assert!(!self.segment_state.taken.get());
    unsafe { self.segment_state.segment.assume_init_ref() }
      .write_async(self.segment_ptr, unsafe { &*self.entry.get() })
  }
  /**
   * to complete writing data to entry
   */
  pub fn commit_append(&self) {
    self.commit_count.fetch_add(1, Ordering::Release);
  }

  pub const fn get_pointer(&self) -> Pointer {
    self.segment_ptr
  }

  /**
   * to drop segment and pin
   * it should be call when nothing to refer this segment
   */
  pub fn take_segment(&self) -> WALSegment {
    debug_assert!(self.segment_state.pin.is_exclusive());
    debug_assert!(!self.segment_state.taken.get());
    self.segment_state.taken.set(true);
    unsafe { self.segment_state.segment.assume_init_read() }
  }
  pub fn increase_rotated_count(&self) {
    self
      .segment_state
      .rotated_count
      .fetch_add(1, Ordering::Release);
  }
  /**
   * Returns true when all prior blocks in the segment have been written to disk.
   * written_count increments after each block rotation completes its disk write,
   * so segment_ptr <= written_count + 1 means blocks 0..segment_ptr-1 are persisted.
   */
  pub fn prev_blocks_rotated(&self) -> bool {
    self.segment_ptr <= self.segment_state.rotated_count.load(Ordering::Acquire) + 1
  }
  pub const fn get_generation(&self) -> SegmentGeneration {
    self.generation
  }
}

unsafe impl Send for LogBuffer {}
unsafe impl Sync for LogBuffer {}
