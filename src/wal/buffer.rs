use std::{
  cell::Cell,
  io,
  iter::repeat,
  mem::ManuallyDrop,
  sync::{
    atomic::{fence, AtomicBool, Ordering},
    Arc,
  },
};

use crate::{
  background::{oneshot, EventBus, Oneshot, OneshotFulfill},
  disk::Pointer,
  page::{Page, PageAllocator, PageRef},
  utils::{create_static_ref, AtomicRef, AtomicSBox, MpscQueue, SBox},
};

use super::{
  AppendCompletion, AppendTicket, BookingResult, FsyncResult, LogId, OffsetBooking,
  SegmentGeneration, SyncCompletion, WALSegment, WriteCompletion, WAL_BLOCK_SIZE,
};

pub struct WALSegmentRotated {
  pub last_log_id: LogId,
  pub segment: WALSegment,
}
impl WALSegmentRotated {
  const fn new(last_log_id: LogId, segment: WALSegment) -> Self {
    Self {
      last_log_id,
      segment,
    }
  }
}

pub struct SegmentBuffer {
  log_buffer: AtomicSBox<LogBuffer>,
  segment: ManuallyDrop<WALSegment>,
  write_completion: WriteCompletion,
  sync_completion: Arc<SyncCompletion>,
  last_log_id: Cell<Option<LogId>>,
  generation: SegmentGeneration,
  event_bus: Arc<EventBus>,
}
impl SegmentBuffer {
  pub fn new(
    segment: WALSegment,
    entry: PageRef<WAL_BLOCK_SIZE>,
    log_id_offset: LogId,
    max_len: Pointer,
    sync_completion: Arc<SyncCompletion>,
    generation: SegmentGeneration,
    event_bus: Arc<EventBus>,
  ) -> Self {
    Self {
      log_buffer: AtomicSBox::new(LogBuffer::new(0, entry, log_id_offset, 0, 0)),
      segment: ManuallyDrop::new(segment),
      write_completion: WriteCompletion::new(max_len as usize),
      sync_completion,
      last_log_id: Cell::new(None),
      generation,
      event_bus,
    }
  }

  pub fn init_next(
    &self,
    segment: WALSegment,
    entry: PageRef<WAL_BLOCK_SIZE>,
    log_id_offset: LogId,
    max_len: Pointer,
  ) -> Self {
    Self::new(
      segment,
      entry,
      log_id_offset,
      max_len,
      self.sync_completion.clone(),
      self.generation + 1,
      self.event_bus.clone(),
    )
  }

  pub fn set_last_log_id(&self, log_id: LogId) {
    self.last_log_id.set(Some(log_id));
  }

  pub fn load_log_buffer(&self) -> AtomicRef<LogBuffer> {
    self.log_buffer.load()
  }
  pub fn store_log_buffer(&self, log_buffer: LogBuffer) -> SBox<LogBuffer> {
    self.log_buffer.store_and_load(log_buffer)
  }

  pub fn wait_completion(&self, pointer: Pointer) -> io::Result<()> {
    self.write_completion.wait_until(pointer)
  }

  pub fn sync(&self) -> FsyncResult {
    self.segment.fsync()
  }

  pub const fn get_generation(&self) -> SegmentGeneration {
    self.generation
  }
}
impl Drop for SegmentBuffer {
  fn drop(&mut self) {
    let segment = unsafe { ManuallyDrop::take(&mut self.segment) };
    let Some(last_log_id) = self.last_log_id.get() else {
      return;
    };
    self
      .sync_completion
      .register(self.generation, segment.fsync());
    self
      .event_bus
      .publish(WALSegmentRotated::new(last_log_id, segment));
  }
}

struct LogBufferBatch {
  occupied: AtomicBool,
  queue: MpscQueue<(AppendTicket, OneshotFulfill<io::Result<()>>)>,
  max_offset: Cell<usize>,
}
impl LogBufferBatch {
  fn new() -> Self {
    Self {
      occupied: AtomicBool::new(false),
      queue: MpscQueue::new(),
      max_offset: Cell::new(0),
    }
  }

  fn push_and_compete(
    &self,
    done: OneshotFulfill<io::Result<()>>,
    ticket: AppendTicket,
  ) -> bool {
    self.queue.push((ticket, done));
    if self.occupied.fetch_or(true, Ordering::Relaxed) {
      return false;
    }
    fence(Ordering::Acquire);
    true
  }

  fn try_release(&self) -> bool {
    self.occupied.fetch_and(false, Ordering::Release);
    if self.queue.is_empty() {
      return true;
    }
    if self.occupied.fetch_or(true, Ordering::Relaxed) {
      return true;
    }
    fence(Ordering::Acquire);
    false
  }

  fn drain_all(
    &self,
  ) -> impl Iterator<Item = (AppendTicket, OneshotFulfill<io::Result<()>>)> + '_ {
    repeat(()).map_while(|_| unsafe { self.queue.pop() })
  }

  const fn get_max_offset(&self) -> usize {
    self.max_offset.get()
  }
  fn set_max_offset(&self, offset: usize) {
    self.max_offset.set(offset);
  }
}

pub struct BatchedWrite {
  current: Oneshot<io::Result<()>>,
}
impl BatchedWrite {
  const fn new(current: Oneshot<io::Result<()>>) -> Self {
    Self { current }
  }

  pub fn wait(self) -> io::Result<()> {
    self.current.wait().unwrap()
  }
}

pub struct LogBuffer {
  pointer: Pointer,
  entry: PageRef<WAL_BLOCK_SIZE>,
  reserved_offset: OffsetBooking,
  batch: LogBufferBatch,
  append_completion: AppendCompletion,
  log_id_offset: LogId,
}
impl LogBuffer {
  fn new(
    pointer: Pointer,
    entry: PageRef<WAL_BLOCK_SIZE>,
    log_id_offset: LogId,
    offset: usize,
    order: u32,
  ) -> Self {
    Self {
      pointer,
      entry,
      reserved_offset: OffsetBooking::new(offset, order),
      batch: LogBufferBatch::new(),
      append_completion: AppendCompletion::new(order),
      log_id_offset,
    }
  }

  pub fn initialized(
    pointer: Pointer,
    entry: PageRef<WAL_BLOCK_SIZE>,
    log_id_offset: LogId,
    offset: usize,
  ) -> Self {
    Self::new(pointer, entry, log_id_offset, offset, 1)
  }

  pub fn reserve_append(&self, len: usize) -> BookingResult {
    self.reserved_offset.reserve(len)
  }
  pub const fn get_log_id_offset(&self) -> LogId {
    self.log_id_offset
  }
  pub fn append_at(&self, record: &[u8], ticket: &AppendTicket) {
    unsafe { self.entry.copy_from_unchecked(record, ticket.get_offset()) };
    self.append_completion.complete(ticket.get_order());
  }

  pub fn flush_block_with(
    &self,
    ticket: AppendTicket,
    upstream: &SegmentBuffer,
    allocator: &PageAllocator<WAL_BLOCK_SIZE>,
  ) -> BatchedWrite {
    let (o, f) = oneshot();
    let batched = BatchedWrite::new(o);

    if !self.batch.push_and_compete(f, ticket) {
      return batched;
    };

    loop {
      let mut page = allocator.allocate();

      let mut max_offset = self.batch.get_max_offset();
      let mut waiting = Vec::new();
      for (ticket, done) in self.batch.drain_all() {
        self.append_completion.wait_until(ticket.get_order());
        max_offset = max_offset.max(ticket.get_len() + ticket.get_offset());
        waiting.push(done);
      }
      self.batch.set_max_offset(max_offset);

      page.copy_from(self.entry.range(0..max_offset), 0);

      let static_ref = unsafe { create_static_ref::<Page<WAL_BLOCK_SIZE>>(&page) };
      let pending = upstream.segment.write_async(self.pointer, static_ref);
      pending.add_callback(create_cb(waiting, page));
      if self.batch.try_release() {
        return batched;
      }
    }
  }

  pub fn flush_and_forget(
    &self,
    ticket: AppendTicket,
    upstream: &SegmentBuffer,
    allocator: &PageAllocator<WAL_BLOCK_SIZE>,
  ) {
    debug_assert_eq!(ticket.get_offset() + ticket.get_len(), WAL_BLOCK_SIZE);
    let batch = self.flush_block_with(ticket, upstream, allocator);
    upstream.write_completion.register(self.pointer, batch);
  }

  pub const fn get_pointer(&self) -> Pointer {
    self.pointer
  }
}

const fn create_cb(
  waiting: Vec<OneshotFulfill<io::Result<()>>>,
  page: PageRef<WAL_BLOCK_SIZE>,
) -> impl FnOnce(&io::Result<()>) {
  move |result| {
    drop(page);
    let result = result.as_ref().map_err(|err| err.kind()).copied();
    for done in waiting {
      done.fulfill(result.map_err(io::Error::from));
    }
  }
}
