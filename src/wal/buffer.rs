use std::{
  ptr::copy_nonoverlapping,
  sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
};

use super::{FsyncResult, WALSegment, WAL_BLOCK_SIZE};
use crate::{
  disk::PageRef,
  error::Result,
  utils::{ToRawPointer, UnsafeBorrow, UnsafeDrop, UnsafeTake},
};

const U16_MASK: u32 = 0xFFFF;

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
   * data block for current index to store wal records.
   * must mark records count before write to disk.
   * records count can be obtained from pinning entry.
   */
  entry: PageRef<WAL_BLOCK_SIZE>,
  /**
   * writtne complete count for data entry which has valid offset
   */
  commit_count: AtomicU32,
  /**
   * disk index for current data block
   */
  segment_index: usize,
  /**
   * pin for using segment
   * it must taken with segment pointer.
   */
  segment_pin: *const AtomicUsize,
  /**
   * Shared across all buffers within the same segment. Raw pointer allows exclusive
   * ownership transfer via take_segment() when the last buffer finishes — required
   * for segment reuse. It must taken after pin is empty.
   */
  segment: *const WALSegment,
  /**
   * rotated and written complete data block count for current segment
   */
  written_count: *const AtomicUsize,
  /**
   * current generation for current segment
   */
  generation: usize,
}
impl LogBuffer {
  const BIT: u64 = 40;
  const MASK: u64 = (1 << Self::BIT) - 1;
  /**
   * default offset for entry to write records len
   */
  const OFFSET_BYTE: u64 = 2;

  pub fn init_new(
    entry: PageRef<WAL_BLOCK_SIZE>,
    segment: WALSegment,
    generation: usize,
  ) -> Self {
    Self::new(
      entry,
      0,
      AtomicUsize::new(0).to_raw_ptr(),
      segment.to_raw_ptr(),
      AtomicUsize::new(0).to_raw_ptr(),
      generation,
    )
  }
  /**
   * if segment is not full, then copy pointers and recreate buffer
   */
  pub fn init_next(&self, entry: PageRef<WAL_BLOCK_SIZE>) -> Self {
    Self::new(
      entry,
      self.segment_index + 1,
      self.segment_pin,
      self.segment,
      self.written_count,
      self.generation,
    )
  }

  fn new(
    entry: PageRef<WAL_BLOCK_SIZE>,
    segment_index: usize,
    segment_pin: *const AtomicUsize,
    segment: *const WALSegment,
    written_count: *const AtomicUsize,
    generation: usize,
  ) -> Self {
    Self {
      offset: AtomicU64::new(Self::OFFSET_BYTE),
      entry,
      commit_count: AtomicU32::new(0),
      segment_index,
      segment_pin,
      segment,
      written_count,
      generation,
    }
  }

  pub fn pin_segment(&self) {
    self
      .segment_pin
      .borrow_unsafe()
      .fetch_add(1, Ordering::Release);
  }
  /**
   * Atomically reserves a write slot and returns (offset, ready).
   * ready is the number of writers that claimed a slot before this call.
   * Since each writer appends exactly one record, ready also equals the number
   * of records already in the block — used by flush callers to write the correct
   * record count header and to wait for all prior writers to finish.
   */
  pub fn pin_entry(&self, len: usize) -> (usize, u32) {
    let prev = self.offset.fetch_add(
      ((len as u64) & Self::MASK) | (1 << Self::BIT),
      Ordering::Release,
    );
    ((prev & Self::MASK) as usize, (prev >> Self::BIT) as u32)
  }
  pub fn apply_record_count(&self, count: u32) {
    self.write_at(&((count & U16_MASK) as u16).to_le_bytes(), 0)
  }
  pub fn write_at(&self, record: &[u8], offset: usize) {
    let ptr = self.entry.as_ref().as_ptr() as *mut u8;
    let len = record.len();
    unsafe { copy_nonoverlapping(record.as_ptr(), ptr.add(offset), len) };
  }
  pub fn load_commit(&self) -> u32 {
    self.commit_count.load(Ordering::Acquire)
  }
  pub fn flush(&self) -> FsyncResult {
    self.segment.borrow_unsafe().fsync()
  }
  pub fn write_to_disk(&self) -> Result {
    self
      .segment
      .borrow_unsafe()
      .write(self.segment_index, &self.entry)
  }
  /**
   * to complete writing data to entry
   */
  pub fn commit_entry(&self) {
    self.commit_count.fetch_add(1, Ordering::Release);
  }
  pub fn unpin_segment(&self) {
    self
      .segment_pin
      .borrow_unsafe()
      .fetch_sub(1, Ordering::Release);
  }
  pub fn get_index(&self) -> usize {
    self.segment_index
  }

  /**
   * to drop segment and pin
   * it should be call when nothing to refer this segment
   */
  pub fn take_segment(&self) -> WALSegment {
    self.written_count.drop_unsafe();
    self.segment_pin.drop_unsafe();
    self.segment.take_unsafe()
  }
  pub fn load_offset(&self) -> usize {
    (self.offset.load(Ordering::Acquire) & Self::MASK) as usize
  }
  pub fn load_segment_pinned(&self) -> usize {
    self.segment_pin.borrow_unsafe().load(Ordering::Acquire)
  }
  pub fn increase_written_count(&self) {
    self
      .written_count
      .borrow_unsafe()
      .fetch_add(1, Ordering::Release);
  }
  /**
   * Returns true when all prior blocks in the segment have been written to disk.
   * written_count increments after each block rotation completes its disk write,
   * so segment_index <= written_count + 1 means blocks 0..segment_index-1 are persisted.
   */
  pub fn is_ready_to_flush(&self) -> bool {
    self.segment_index <= self.written_count.borrow_unsafe().load(Ordering::Acquire) + 1
  }
  pub fn get_generation(&self) -> usize {
    self.generation
  }
}

unsafe impl Send for LogBuffer {}
unsafe impl Sync for LogBuffer {}
