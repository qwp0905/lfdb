use std::{
  cell::Cell,
  io,
  iter::repeat,
  mem::MaybeUninit,
  sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
};

use crossbeam::{
  channel::{bounded, unbounded, Receiver, Sender},
  queue::SegQueue,
  select,
  utils::Backoff,
};

use super::{FsyncResult, SegmentGeneration, WALSegment, WAL_BLOCK_SIZE};
use crate::{
  disk::{Page, PagePool, PageRef, PendingIO, Pointer},
  utils::{create_static_ref, AtomicBitmap, ExclusivePin, SBox, SharedToken},
};

type PendingBatch = (
  PendingIO,
  PageRef<WAL_BLOCK_SIZE>,
  Vec<Sender<io::Result<()>>>,
);
struct LogBufferBatch {
  occupied: AtomicBool,
  queue: SegQueue<(usize, Sender<io::Result<()>>)>,
  pending_recv: Receiver<PendingBatch>,
  pending_send: Sender<PendingBatch>,
  max_offset: Cell<usize>,
}
impl LogBufferBatch {
  fn new() -> Self {
    let (t, r) = unbounded();
    Self {
      occupied: AtomicBool::new(false),
      queue: SegQueue::new(),
      pending_recv: r,
      pending_send: t,
      max_offset: Cell::new(0),
    }
  }

  fn push_and_compete(&self, done: Sender<io::Result<()>>, offset: usize) -> bool {
    self.queue.push((offset, done));
    !self.occupied.fetch_or(true, Ordering::Release)
  }

  fn try_release(&self) -> bool {
    self.occupied.fetch_and(false, Ordering::Release);
    if self.queue.is_empty() {
      return true;
    }
    if self.occupied.fetch_or(true, Ordering::AcqRel) {
      return true;
    }
    false
  }

  fn append_batch(&self, task: PendingBatch) {
    self.pending_send.send(task).unwrap();
  }

  fn get_pending(&self) -> Receiver<PendingBatch> {
    self.pending_recv.clone()
  }

  fn drain_all(&self) -> impl Iterator<Item = (usize, Sender<io::Result<()>>)> + '_ {
    repeat(()).map_while(|_| self.queue.pop())
  }

  const fn get_max_offset(&self) -> usize {
    self.max_offset.get()
  }
  fn set_max_offset(&self, offset: usize) {
    self.max_offset.set(offset);
  }
}

pub struct BatchedWrite {
  current: Receiver<io::Result<()>>,
  pending: Receiver<PendingBatch>,
}
impl BatchedWrite {
  const fn new(
    current: Receiver<io::Result<()>>,
    pending: Receiver<PendingBatch>,
  ) -> Self {
    Self { current, pending }
  }

  fn handle_pending(&self, (pending, page, waiting): PendingBatch) {
    let result = pending.wait().map_err(|err| err.kind());
    let _ = page;
    for done in waiting {
      let _ = done.send(result.map_err(io::Error::from));
    }
  }

  pub fn wait(self) -> io::Result<()> {
    loop {
      select! {
        recv(self.current) -> v => return v.unwrap(),
        recv(self.pending) -> p => {
          let Ok(pending) = p else {
            return self.current.recv().unwrap();
          };
          self.handle_pending(pending);
        },
      }
    }
  }
}

struct WriteCompletion {
  queue: SegQueue<(Pointer, BatchedWrite)>,
  frontier: AtomicU64,
  bitmap: AtomicBitmap,
}
impl WriteCompletion {
  fn new(max_len: Pointer) -> Self {
    Self {
      queue: SegQueue::new(),
      frontier: AtomicU64::new(0),
      bitmap: AtomicBitmap::new(max_len as usize),
    }
  }
  fn wait_until(&self, threshold: Pointer) -> io::Result<()> {
    let backoff = Backoff::new();
    loop {
      let current = self.frontier.load(Ordering::Acquire);
      if threshold <= current {
        return Ok(());
      }
      let Some((ptr, batched)) = self.queue.pop() else {
        backoff.snooze();
        continue;
      };
      let result = batched.wait();
      self.bitmap.insert(ptr);
      if let Some(i) = (current..).take_while(|&i| self.bitmap.contains(i)).last() {
        self.frontier.fetch_max(i + 1, Ordering::Release);
      }
      result?;
    }
  }

  fn push(&self, ptr: Pointer, batch: BatchedWrite) {
    self.queue.push((ptr, batch));
  }

  fn drain(&self) {
    while let Some((ptr, batched)) = self.queue.pop() {
      let _ = batched.wait();
      self.bitmap.insert(ptr);

      let current = self.frontier.load(Ordering::Acquire);
      if let Some(i) = (current..).take_while(|&i| self.bitmap.contains(i)).last() {
        self.frontier.fetch_max(i + 1, Ordering::Release);
      }
    }
  }
}

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
  completion: WriteCompletion,
  /**
   * flag which segment has been taken to check drop.
   */
  taken: Cell<bool>,

  /**
   * current generation for current segment
   */
  generation: SegmentGeneration,
}
impl SegmentState {
  fn new(segment: WALSegment, generation: SegmentGeneration, max_len: Pointer) -> Self {
    Self {
      segment: MaybeUninit::new(segment),
      pin: ExclusivePin::new(),
      completion: WriteCompletion::new(max_len),
      taken: Cell::new(false),
      generation,
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
  entry: PageRef<WAL_BLOCK_SIZE>,
  /**
   * written complete count for data entry which has valid offset
   */
  commit_count: AtomicU32,
  /**
   * disk pointer for current data block
   */
  segment_ptr: Pointer,

  segment_state: SBox<SegmentState>,

  batch: LogBufferBatch,
}
impl LogBuffer {
  pub fn init_new(
    entry: PageRef<WAL_BLOCK_SIZE>,
    segment: WALSegment,
    generation: SegmentGeneration,
    max_len: Pointer,
  ) -> Self {
    Self::new(
      entry,
      0,
      SBox::new(SegmentState::new(segment, generation, max_len)),
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
      offset,
    )
  }

  fn new(
    entry: PageRef<WAL_BLOCK_SIZE>,
    segment_ptr: Pointer,
    segment_state: SBox<SegmentState>,
    offset: usize,
  ) -> Self {
    Self {
      offset: AtomicU64::new(offset as u64),
      entry,
      commit_count: AtomicU32::new(0),
      segment_ptr,
      segment_state,
      batch: LogBufferBatch::new(),
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
    unsafe { self.entry.copy_from_unchecked(record, offset) }
  }
  pub fn load_committed_append(&self) -> u32 {
    self.commit_count.load(Ordering::Acquire)
  }
  pub fn sync_segment(&self) -> FsyncResult {
    debug_assert!(!self.segment_state.taken.get());
    unsafe { self.segment_state.segment.assume_init_ref() }.fsync()
  }

  pub fn flush_and_forget(&self, page_pool: &PagePool<WAL_BLOCK_SIZE>) {
    let batch = self.flush_block_with(WAL_BLOCK_SIZE, page_pool);
    self.segment_state.completion.push(self.segment_ptr, batch);
  }

  pub fn flush_block_with(
    &self,
    offset: usize,
    page_pool: &PagePool<WAL_BLOCK_SIZE>,
  ) -> BatchedWrite {
    debug_assert!(!self.segment_state.taken.get());

    let (t, r) = bounded(1);
    let batched = BatchedWrite::new(r, self.batch.get_pending());

    if !self.batch.push_and_compete(t, offset) {
      return batched;
    };

    loop {
      let mut max_offset = self.batch.get_max_offset();
      let mut waiting = Vec::new();
      for (offset, done) in self.batch.drain_all() {
        max_offset = max_offset.max(offset);
        waiting.push(done);
      }
      self.batch.set_max_offset(max_offset);

      let mut page = page_pool.acquire();
      page.copy_from(self.entry.range(0..max_offset), 0);

      let static_ref = unsafe { create_static_ref::<Page<WAL_BLOCK_SIZE>>(&page) };
      let pending = unsafe { self.segment_state.segment.assume_init_ref() }
        .write_async(self.segment_ptr, static_ref);

      self.batch.append_batch((pending, page, waiting));
      if self.batch.try_release() {
        break;
      }
    }

    batched
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

  pub fn get_generation(&self) -> SegmentGeneration {
    self.segment_state.generation
  }

  pub fn wait_prev_blocks(&self) -> io::Result<()> {
    self.segment_state.completion.wait_until(self.segment_ptr)
  }

  pub fn drain_batch(&self) {
    self.segment_state.completion.drain();
  }
}

unsafe impl Send for LogBuffer {}
unsafe impl Sync for LogBuffer {}
