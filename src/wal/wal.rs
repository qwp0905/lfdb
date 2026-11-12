use std::{
  io,
  path::PathBuf,
  sync::{atomic::Ordering, Arc},
  time::Duration,
};

use crossbeam::{atomic::AtomicCell, utils::Backoff};

use crate::{
  background::{EventBus, ThreadPool},
  blob::BlobMetadata,
  disk::{IOPool, Pointer},
  metrics::{measure, MetricsRegistry},
  page::PageAllocator,
  table::TableId,
  utils::{error, info, AtomicRef, AtomicSBox},
  Error, Result,
};

use super::{
  replay, AppendTicket, AtomicWriteSeq, BatchedWrite, BookingResult, LogBuffer,
  LogCompletion, LogRecordUninit, RecordEncoding, ReplayResult, SegmentBuffer,
  SegmentPreload, SyncCompletion, TxId, WALFormatVersion, WALSegment, WriteSeq,
  WAL_BLOCK_SIZE,
};

pub struct WALConfig {
  pub max_file_size: usize,
  pub max_buffer_size: usize,
  pub segment_expiration: Duration,
  pub segment_min_count: usize,
}
impl WALConfig {
  pub const MIN_BUFFER_SIZE: usize = WAL_BLOCK_SIZE * 3;
}

pub struct WALFailed;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
  Available,
  Failed(io::ErrorKind),
}
impl State {
  fn is_available(&self) -> bool {
    matches!(self, Self::Available)
  }
}

const DEFAULT_ENCODING: RecordEncoding = RecordEncoding::Lz4;

/**
 * Lock-free, group-commit write-ahead log.
 *
 * Multiple threads append records concurrently into a shared 16KB block (LogBuffer)
 * by atomically reserving a slot via a single fetch_add. No mutex is held during
 * the write — contention is resolved only at block rotation via CAS.
 *
 * When a block fills up, the thread that crosses the threshold wins the CAS and
 * rotates to the next block (or a new segment if the current segment is full).
 * Rotated segments are fsynced asynchronously and queued for checkpoint.
 *
 * flush=true callers (commit, checkpoint) wait for all prior segment fsync to
 * complete before returning, guaranteeing durability across segment boundaries.
 */
pub struct WriteAheadLog {
  /**
   * Current log buffer.
   */
  buffer: AtomicSBox<SegmentBuffer>,

  sync_completion: Arc<SyncCompletion>,
  log_completion: LogCompletion,
  durable_write_seq: AtomicWriteSeq,

  /**
   * wal segment max size
   */
  max_len: Pointer,

  /**
   * A state of wal. If wal io fails, it switches to the failed state and requires a restart.
   */
  state: AtomicCell<State>,

  /**
   *  preload wal segment
   *  reuse synced + checkpoint complete segment
   */
  preloader: SegmentPreload,
  /**
   * preloaded data block.
   */
  allocator: PageAllocator<WAL_BLOCK_SIZE>,

  event_bus: Arc<EventBus>,
  metrics: Arc<MetricsRegistry>,
}
impl WriteAheadLog {
  fn new(
    config: WALConfig,
    event_bus: Arc<EventBus>,
    io_pool: Arc<IOPool>,
    metrics: Arc<MetricsRegistry>,
    last_write_seq: WriteSeq,
  ) -> Result<Self> {
    let max_len = config.max_file_size / WAL_BLOCK_SIZE;

    let buffer_allocator = PageAllocator::new(config.max_buffer_size / WAL_BLOCK_SIZE);
    let max_len = max_len as Pointer;
    let preloader = SegmentPreload::new(
      max_len,
      config.segment_expiration,
      config.segment_min_count,
      io_pool,
    );
    let sync_completion = Arc::new(SyncCompletion::new());

    let buffer = SegmentBuffer::new(
      preloader.load()?,
      buffer_allocator.allocate(),
      last_write_seq,
      max_len,
      sync_completion.clone(),
      0,
      event_bus.clone(),
    );

    Ok(Self {
      preloader,
      buffer: AtomicSBox::new(buffer),
      allocator: buffer_allocator,
      sync_completion,
      log_completion: LogCompletion::new(last_write_seq),
      durable_write_seq: AtomicWriteSeq::new(last_write_seq),
      state: AtomicCell::new(State::Available),
      max_len,
      event_bus,
      metrics,
    })
  }
  pub fn init(
    config: WALConfig,
    event_bus: Arc<EventBus>,
    io_pool: Arc<IOPool>,
    metrics: Arc<MetricsRegistry>,
  ) -> Result<Self> {
    Self::new(config, event_bus, io_pool, metrics, 0)
  }
  pub fn replay(
    config: WALConfig,
    event_bus: Arc<EventBus>,
    io_pool: Arc<IOPool>,
    metrics: Arc<MetricsRegistry>,
    init_thread: &ThreadPool,
    replay_version: WALFormatVersion,
  ) -> Result<(Self, ReplayResult)> {
    info!("start to replay wal segments version: {}", replay_version);

    let replay_result = replay(io_pool.clone(), init_thread, replay_version)?;

    info!(
      "wal replay result: last_write_seq {} last_tx_id {} redo {} segments {} last snapshot {:?}",
      replay_result.last_write_seq,
      replay_result.last_tx_id,
      replay_result.redo.len(),
      replay_result.segments.len(),
      replay_result.last_snapshot,
    );
    let this = Self::new(
      config,
      event_bus,
      io_pool,
      metrics,
      replay_result.last_write_seq,
    )?;
    Ok((this, replay_result))
  }

  /**
   * Transition WAL to failed state and publish the failure.
   *
   * WAL I/O failure is terminal for this WAL instance. Later callers receive
   * `WALFailed` with the kind that caused the transition, not their own; the
   * failure event only reports that this transition happened.
   */
  fn failover(&self, err: io::ErrorKind) -> Error {
    if let Err(State::Failed(prev)) = self
      .state
      .compare_exchange(State::Available, State::Failed(err))
    {
      return Error::WALFailed(prev);
    }

    error!("error occurs in wal: {err}");
    error!("it does not recover automatically, please drop engine and restart.");
    self.preloader.failover();
    self.event_bus.publish(WALFailed);
    Error::WALFailed(err)
  }
  const fn handle_failover(&self) -> impl FnOnce(io::Error) -> Error + '_ {
    |err| self.failover(err.kind())
  }

  fn append_in_block(
    &self,
    reserved: ReservedAppend,
    record: LogRecordUninit,
    flush: bool,
  ) -> io::Result<WriteSeq> {
    let ReservedAppend {
      buffer,
      ticket,
      segment,
    } = reserved;
    let pointer = buffer.get_pointer();
    let write_seq = buffer.get_write_seq_offset() + ticket.get_order() as WriteSeq;
    buffer.append_at(&record.init(write_seq), &ticket);
    if !flush {
      return Ok(write_seq);
    }

    let pending = buffer.flush_block_with(ticket, &segment, &self.allocator);
    drop(buffer);
    pending.wait()?;

    segment.wait_completion(pointer)?;
    self.wait_sync(segment)?;
    self
      .durable_write_seq
      .fetch_max(write_seq + 1, Ordering::Relaxed);
    Ok(write_seq)
  }

  fn wait_sync(&self, buffer: AtomicRef<SegmentBuffer>) -> io::Result<()> {
    let generation = buffer.get_generation();
    let done = buffer.sync();
    drop(buffer);
    done.wait()?;
    self.sync_completion.wait_until(generation)?;
    Ok(())
  }

  fn rotate_block(
    &self,
    reserved: ReservedAppend,
    overflow: AppendTicket,
    record: LogRecordUninit,
    flush: bool,
  ) -> io::Result<WriteSeq> {
    let ReservedAppend {
      buffer,
      ticket,
      segment,
    } = reserved;

    let write_seq = buffer.get_write_seq_offset() + ticket.get_order() as WriteSeq;
    let record = record.init(write_seq);
    let (available, remain) = record.split_at(ticket.get_len());
    debug_assert_eq!(available.len(), ticket.get_len());
    debug_assert_eq!(remain.len(), overflow.get_len());

    let pointer = buffer.get_pointer() + 1;
    let mut new_page = self.allocator.allocate();
    new_page.copy_from(remain, 0);

    let new_buffer =
      LogBuffer::initialized(pointer, new_page, write_seq, overflow.get_len());
    let new_buffer = segment.store_log_buffer(new_buffer);

    buffer.append_at(available, &ticket);
    buffer.flush_and_forget(ticket, &segment, &self.allocator);
    drop(buffer);

    if !flush {
      return Ok(write_seq);
    }
    let pending = new_buffer.flush_block_with(overflow, &segment, &self.allocator);
    drop(new_buffer);

    pending.wait()?;
    segment.wait_completion(pointer)?;
    self.wait_sync(segment)?;
    self
      .durable_write_seq
      .fetch_max(write_seq + 1, Ordering::Relaxed);
    Ok(write_seq)
  }

  fn rotate_segment(&self, reserved: ReservedAppend) -> Result {
    let ReservedAppend {
      buffer,
      ticket,
      segment,
    } = reserved;

    let pointer = buffer.get_pointer();
    let write_seq = buffer.get_write_seq_offset() + ticket.get_order() as WriteSeq;

    let new = match self.preloader.load() {
      Ok(v) => v,
      Err(Error::IO(err)) => return Err(self.failover(err.kind())),
      Err(err) => return Err(err),
    };

    let replacement =
      segment.init_next(new, self.allocator.allocate(), write_seq, self.max_len);
    self.buffer.store(replacement);

    buffer.flush_and_forget(ticket, &segment, &self.allocator);
    drop(buffer);

    if let Err(err) = segment.wait_completion(pointer + 1) {
      return Err(self.failover(err.kind()));
    };
    segment.set_last_write_seq(write_seq);
    Ok(())
  }

  fn append(&self, record: LogRecordUninit, flush: bool) -> Result<DurabilityGuard<'_>> {
    let len = record.len();
    let backoff = Backoff::new();

    loop {
      if let State::Failed(kind) = self.state.load() {
        return Err(Error::WALFailed(kind));
      }

      let segment = self.buffer.load();
      let buffer = segment.load_log_buffer();
      let (ticket, overflow) = match buffer.reserve_append(len) {
        BookingResult::Overflow => {
          drop(buffer);
          drop(segment);
          backoff.snooze();
          continue;
        }
        BookingResult::Available(ticket) => (ticket, None),
        BookingResult::Splitted {
          available,
          overflow,
        } => (available, Some(overflow)),
      };

      let reserved = ReservedAppend::new(segment, buffer, ticket);
      let Some(overflow) = overflow else {
        let write_seq = self
          .append_in_block(reserved, record, flush)
          .map_err(self.handle_failover())?;
        return Ok(DurabilityGuard::new(&self.log_completion, write_seq));
      };
      if reserved.buffer.get_pointer() + 1 < self.max_len {
        let write_seq = self
          .rotate_block(reserved, overflow, record, flush)
          .map_err(self.handle_failover())?;
        return Ok(DurabilityGuard::new(&self.log_completion, write_seq));
      }
      self.rotate_segment(reserved)?;
    }
  }

  fn try_flush(&self, write_seq: WriteSeq) -> io::Result<bool> {
    if self.durable_write_seq.load(Ordering::Relaxed) > write_seq {
      return Ok(true);
    }

    let segment = self.buffer.load();
    let generation = segment.get_generation();
    let segment_offset = segment.get_write_seq_offset();
    if segment_offset > write_seq {
      drop(segment);
      self.sync_completion.wait_until(generation)?;
      self
        .durable_write_seq
        .fetch_max(segment_offset, Ordering::Relaxed);
      return Ok(true);
    }

    let buffer = segment.load_log_buffer();
    let ptr = buffer.get_pointer();
    let buffer_offset = buffer.get_write_seq_offset();
    let mut frontier = buffer_offset;
    let mut pending = None;

    if buffer_offset <= write_seq {
      let Some(current) = buffer.current_append() else {
        return Ok(false);
      };

      frontier = buffer_offset + current.get_order() as WriteSeq;
      pending = Some(buffer.flush_block_with(current, &segment, &self.allocator));
    }

    drop(buffer);
    pending.map(BatchedWrite::wait).transpose()?;
    segment.wait_completion(ptr)?;

    self.wait_sync(segment)?;
    self
      .durable_write_seq
      .fetch_max(frontier, Ordering::Relaxed);
    Ok(true)
  }

  pub fn flush_until(&self, write_seq: WriteSeq) -> Result {
    let backoff = Backoff::new();
    loop {
      if let State::Failed(kind) = self.state.load() {
        return Err(Error::WALFailed(kind));
      }
      if !self.try_flush(write_seq).map_err(self.handle_failover())? {
        backoff.snooze();
        continue;
      }
      return Ok(());
    }
  }

  pub fn completed_write_seq(&self) -> WriteSeq {
    self.log_completion.get_frontier()
  }

  pub fn append_insert(
    &self,
    tx_id: TxId,
    table_id: TableId,
    ptr: Pointer,
    record_version: TxId,
    data: &[u8],
  ) -> Result<DurabilityGuard<'_>> {
    let record = LogRecordUninit::new_insert(
      tx_id,
      table_id,
      ptr,
      record_version,
      DEFAULT_ENCODING,
      data,
    );
    self.append(record, false)
  }
  pub fn append_blob_created(
    &self,
    metadata: BlobMetadata,
  ) -> Result<DurabilityGuard<'_>> {
    self.append(LogRecordUninit::new_blob_created(metadata), false)
  }

  pub fn checkpoint_and_flush(
    &self,
    durable_write_seq: WriteSeq,
    current_version: TxId,
    path: PathBuf,
  ) -> Result<DurabilityGuard<'_>> {
    self.append(
      LogRecordUninit::new_checkpoint(durable_write_seq, current_version, path),
      true,
    )
  }

  fn __commit_and_flush(&self, tx_id: TxId) -> Result<DurabilityGuard<'_>> {
    self.append(LogRecordUninit::new_commit(tx_id), true)
  }
  pub fn commit_and_flush(&self, tx_id: TxId) -> Result<DurabilityGuard<'_>> {
    measure!(self.metrics.wal_commit, self.__commit_and_flush(tx_id))
  }

  pub fn is_available(&self) -> bool {
    self.state.load().is_available()
  }

  pub fn reuse_segments(&self, segments: Vec<WALSegment>) {
    self.preloader.reuse(segments)
  }

  pub fn close(&self) {
    self.sync_completion.drain();
    if !self.state.load().is_available() {
      return;
    }
    self.preloader.close();
  }
}

unsafe impl Send for WriteAheadLog {}
unsafe impl Sync for WriteAheadLog {}

struct ReservedAppend {
  segment: AtomicRef<SegmentBuffer>,
  buffer: AtomicRef<LogBuffer>,
  ticket: AppendTicket,
}
impl ReservedAppend {
  const fn new(
    segment: AtomicRef<SegmentBuffer>,
    buffer: AtomicRef<LogBuffer>,
    ticket: AppendTicket,
  ) -> Self {
    Self {
      segment,
      buffer,
      ticket,
    }
  }
}

pub struct DurabilityGuard<'a> {
  completion: &'a LogCompletion,
  write_seq: WriteSeq,
}
impl<'a> DurabilityGuard<'a> {
  const fn new(completion: &'a LogCompletion, write_seq: WriteSeq) -> Self {
    Self {
      completion,
      write_seq,
    }
  }

  pub const fn get_write_seq(&self) -> WriteSeq {
    self.write_seq
  }
}
impl<'a> Drop for DurabilityGuard<'a> {
  fn drop(&mut self) {
    self.completion.complete(self.write_seq);
  }
}

#[cfg(all(test, not(loom)))]
#[path = "tests/wal.rs"]
mod tests;
