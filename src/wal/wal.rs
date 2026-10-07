use std::{io, path::PathBuf, sync::Arc};

use crossbeam::{atomic::AtomicCell, utils::Backoff};

use crate::{
  background::{EventBus, ThreadPool},
  blob::BlobMetadata,
  disk::{IOPool, Pointer},
  page::PageAllocator,
  table::TableId,
  utils::{error, info, AtomicRef, AtomicSBox},
  Error, Result,
};

use super::{
  replay, AppendTicket, BookingResult, LogBuffer, LogCompletion, LogId, LogRecordUninit,
  RecordEncoding, ReplayResult, SegmentBuffer, SegmentPreload, SyncCompletion, TxId,
  WALFormatVersion, WALSegment, WAL_BLOCK_SIZE,
};

pub struct WALConfig {
  pub max_file_size: usize,
  pub max_buffer_size: usize,
}
impl WALConfig {
  pub const MIN_BUFFER_SIZE: usize = WAL_BLOCK_SIZE * 4;
}

pub struct WALFailed;

#[derive(Clone, Copy, Debug)]
enum State {
  Available,
  Failed,
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
  buffer_allocator: PageAllocator<WAL_BLOCK_SIZE>,
  io_allocator: PageAllocator<WAL_BLOCK_SIZE>,

  event_bus: Arc<EventBus>,
}
impl WriteAheadLog {
  fn new(
    config: WALConfig,
    event_bus: Arc<EventBus>,
    io_pool: Arc<IOPool>,
    last_log_id: LogId,
  ) -> Result<Self> {
    let max_len = config.max_file_size / WAL_BLOCK_SIZE;

    let buffer_allocator =
      PageAllocator::new((config.max_buffer_size / WAL_BLOCK_SIZE) / 2);
    let io_allocator = PageAllocator::new((config.max_buffer_size / WAL_BLOCK_SIZE) / 2);
    let max_len = max_len as Pointer;
    let preloader = SegmentPreload::new(max_len, io_pool);
    let sync_completion = Arc::new(SyncCompletion::new());

    let buffer = SegmentBuffer::new(
      preloader.load()?,
      buffer_allocator.allocate(),
      last_log_id,
      max_len,
      sync_completion.clone(),
      0,
      event_bus.clone(),
    );

    Ok(Self {
      preloader,
      buffer: AtomicSBox::new(buffer),
      buffer_allocator,
      io_allocator,
      sync_completion,
      log_completion: LogCompletion::new(last_log_id),
      state: AtomicCell::new(State::Available),
      max_len,
      event_bus,
    })
  }
  pub fn init(
    config: WALConfig,
    event_bus: Arc<EventBus>,
    io_pool: Arc<IOPool>,
  ) -> Result<Self> {
    Self::new(config, event_bus, io_pool, 0)
  }
  pub fn replay(
    config: WALConfig,
    event_bus: Arc<EventBus>,
    io_pool: Arc<IOPool>,
    init_thread: &ThreadPool,
    replay_version: WALFormatVersion,
  ) -> Result<(Self, ReplayResult)> {
    info!("start to replay wal segments version: {}", replay_version);

    let replay_result = replay(io_pool.clone(), init_thread, replay_version)?;

    info!(
      "wal replay result: last_log_id {} last_tx_id {} redo {} segments {} last snapshot {:?}",
      replay_result.last_log_id,
      replay_result.last_tx_id,
      replay_result.redo.len(),
      replay_result.segments.len(),
      replay_result.last_snapshot,
    );
    let this = Self::new(config, event_bus, io_pool, replay_result.last_log_id)?;
    Ok((this, replay_result))
  }

  /**
   * Transition WAL to failed state and publish the failure.
   *
   * WAL I/O failure is terminal for this WAL instance. After the first failure,
   * later callers see `WALUnavailable`; the failure event only reports that this
   * transition happened.
   */
  fn failover(&self, err: io::ErrorKind) -> Error {
    if !self.state.swap(State::Failed).is_available() {
      return Error::WALUnavailable;
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
  ) -> io::Result<LogId> {
    let ReservedAppend {
      buffer,
      ticket,
      segment,
    } = reserved;
    let pointer = buffer.get_pointer();
    let log_id = buffer.get_log_id_offset() + ticket.get_order() as LogId;
    buffer.append_at(&record.init(log_id), &ticket);
    let pending = buffer.flush_block_with(ticket, &segment, &self.io_allocator);
    drop(buffer);

    if !flush {
      return Ok(log_id);
    };
    pending.wait()?;
    segment.wait_completion(pointer)?;
    self.wait_sync(segment)?;
    Ok(log_id)
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
  ) -> io::Result<LogId> {
    let ReservedAppend {
      buffer,
      ticket,
      segment,
    } = reserved;

    let log_id = buffer.get_log_id_offset() + ticket.get_order() as LogId;
    let record = record.init(log_id);
    let (available, remain) = record.split_at(ticket.get_len());
    debug_assert_eq!(available.len(), ticket.get_len());
    debug_assert_eq!(remain.len(), overflow.get_len());

    let pointer = buffer.get_pointer() + 1;
    buffer.append_at(available, &ticket);
    buffer.flush_and_forget(ticket, &segment, &self.io_allocator);
    drop(buffer);

    let mut new_page = self.buffer_allocator.allocate();
    new_page.copy_from(remain, 0);

    let new_buffer =
      LogBuffer::initialized(pointer, new_page, log_id, overflow.get_len());
    let new_buffer = segment.store_log_buffer(new_buffer);

    if !flush {
      return Ok(log_id);
    }
    let pending = new_buffer.flush_block_with(overflow, &segment, &self.io_allocator);
    drop(new_buffer);

    pending.wait()?;
    segment.wait_completion(pointer)?;
    self.wait_sync(segment)?;
    Ok(log_id)
  }

  fn rotate_segment(&self, reserved: ReservedAppend) -> Result {
    let ReservedAppend {
      buffer,
      ticket,
      segment,
    } = reserved;

    let pointer = buffer.get_pointer();
    let log_id = buffer.get_log_id_offset() + ticket.get_order() as LogId;
    let pending = buffer.flush_block_with(ticket, &segment, &self.io_allocator);
    drop(buffer);

    let new = match self.preloader.load() {
      Ok(v) => v,
      Err(Error::IO(err)) => return Err(self.failover(err.kind())),
      Err(err) => return Err(err),
    };

    let replacement =
      segment.init_next(new, self.buffer_allocator.allocate(), log_id, self.max_len);
    self.buffer.store(replacement);

    if let Err(err) = pending
      .wait()
      .and_then(|_| segment.wait_completion(pointer))
    {
      return Err(self.failover(err.kind()));
    };
    segment.set_last_log_id(log_id);
    Ok(())
  }

  fn append(&self, record: LogRecordUninit, flush: bool) -> Result<DurabilityGuard<'_>> {
    let len = record.len();
    let backoff = Backoff::new();

    loop {
      if !self.state.load().is_available() {
        return Err(Error::WALUnavailable);
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
        let log_id = self
          .append_in_block(reserved, record, flush)
          .map_err(self.handle_failover())?;
        return Ok(DurabilityGuard::new(&self.log_completion, log_id));
      };
      if reserved.buffer.get_pointer() + 1 < self.max_len {
        let log_id = self
          .rotate_block(reserved, overflow, record, flush)
          .map_err(self.handle_failover())?;
        return Ok(DurabilityGuard::new(&self.log_completion, log_id));
      }
      self.rotate_segment(reserved)?;
    }
  }

  pub fn durable_log_id(&self) -> LogId {
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
    last_log_id: LogId,
    current_version: TxId,
    path: PathBuf,
  ) -> Result<DurabilityGuard<'_>> {
    self.append(
      LogRecordUninit::new_checkpoint(last_log_id, current_version, path),
      true,
    )
  }

  pub fn commit_and_flush(&self, tx_id: TxId) -> Result<DurabilityGuard<'_>> {
    self.append(LogRecordUninit::new_commit(tx_id), true)
  }

  pub fn is_available(&self) -> bool {
    self.state.load().is_available()
  }

  pub fn reuse_segments(&self, segments: Vec<WALSegment>) -> Result {
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
  log_id: LogId,
}
impl<'a> DurabilityGuard<'a> {
  const fn new(completion: &'a LogCompletion, log_id: LogId) -> Self {
    Self { completion, log_id }
  }
}
impl<'a> Drop for DurabilityGuard<'a> {
  fn drop(&mut self) {
    self.completion.complete(self.log_id);
  }
}
