use std::{
  io::ErrorKind,
  mem::forget,
  path::PathBuf,
  sync::{atomic::Ordering, Arc, OnceLock},
};

use crossbeam::{
  atomic::AtomicCell,
  epoch::{Atomic, Collector, Guard, LocalHandle, Owned, Shared},
  utils::Backoff,
};

use crate::{
  background::EventBus,
  blob::BlobMetadata,
  disk::{IOPool, PagePool, Pointer},
  error, info,
  table::TableId,
  utils::SharedToken,
  Error, Result,
};

use super::{
  replay, AtomicLogId, LogBuffer, LogId, LogRecordUninit, RecordEncoding, ReplayResult,
  SegmentPreload, SyncQueue, TxId, WALFormatVersion, WALSegment, WAL_BLOCK_SIZE,
};

pub struct WALConfig {
  pub max_file_size: usize,
  pub max_buffer_size: usize,
}

pub struct WALSegmentRotated(WALSegment);
impl WALSegmentRotated {
  pub fn into_inner(self) -> WALSegment {
    self.0
  }
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

static COLLECTOR: OnceLock<Collector> = OnceLock::new();
thread_local! {
  static LOCAL: LocalHandle = COLLECTOR.get_or_init(Collector::new).register();
}
fn pin() -> Guard {
  LOCAL.with(LocalHandle::pin)
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
   * last log id (LSN)
   */
  last_log_id: AtomicLogId,
  /**
   * Current log buffer, managed via epoch GC. Epoch pinning guarantees the buffer
   * pointer remains valid for the duration of a guard — preventing use-after-free
   * when the buffer is rotated and the old one is deferred-destroyed.
   */
  buffer: Atomic<LogBuffer>,

  sync_queue: SyncQueue,
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
  preloader: Arc<SegmentPreload>,
  /**
   * preloaded data block.
   */
  page_pool: PagePool<WAL_BLOCK_SIZE>,

  event_bus: Arc<EventBus>,
}
impl WriteAheadLog {
  pub fn init(
    config: &WALConfig,
    event_bus: Arc<EventBus>,
    io_pool: Arc<IOPool>,
  ) -> Result<Self> {
    let max_len = config.max_file_size / WAL_BLOCK_SIZE;
    let page_pool = PagePool::new(config.max_buffer_size / WAL_BLOCK_SIZE);
    let max_len = max_len as Pointer;
    let preloader = SegmentPreload::new(max_len, io_pool, &event_bus);
    let buffer = LogBuffer::init_new(page_pool.acquire(), preloader.load()?, 0);

    Ok(Self {
      last_log_id: AtomicLogId::new(0),
      preloader,
      buffer: Atomic::new(buffer),
      page_pool,
      sync_queue: SyncQueue::new(),
      state: AtomicCell::new(State::Available),
      max_len,
      event_bus,
    })
  }
  pub fn replay(
    config: &WALConfig,
    event_bus: Arc<EventBus>,
    io_pool: Arc<IOPool>,
    replay_version: WALFormatVersion,
  ) -> Result<(Self, ReplayResult)> {
    let max_len = config.max_file_size / WAL_BLOCK_SIZE;
    let page_pool = PagePool::new(config.max_buffer_size / WAL_BLOCK_SIZE);
    let max_len = max_len as Pointer;
    info!("start to replay wal segments version: {}", replay_version);

    let replay_result = replay(&io_pool, replay_version)?;

    info!(
      "wal replay result: last_log_id {} last_tx_id {} redo {} segments {} last snapshot {:?}",
      replay_result.last_log_id,
      replay_result.last_tx_id,
      replay_result.redo.len(),
      replay_result.segments.len(),
      replay_result.last_snapshot,
    );

    let preloader = SegmentPreload::new(max_len, io_pool, &event_bus);
    let buffer = LogBuffer::init_new(page_pool.acquire(), preloader.load()?, 0);

    Ok((
      Self {
        last_log_id: AtomicLogId::new(replay_result.last_log_id),
        preloader,
        buffer: Atomic::new(buffer),
        page_pool,
        sync_queue: SyncQueue::new(),
        state: AtomicCell::new(State::Available),
        max_len,
        event_bus,
      },
      replay_result,
    ))
  }

  /**
   * Transition WAL to failed state and publish the failure.
   *
   * WAL I/O failure is terminal for this WAL instance. After the first failure,
   * later callers see `WALUnavailable`; the failure event only reports that this
   * transition happened.
   */
  fn failover(&self, err: ErrorKind) -> Error {
    if !self.state.swap(State::Failed).is_available() {
      return Error::WALUnavailable;
    }

    error!("error occurs in wal: {err}");
    error!("it does not recover automatically, please drop engine and restart.");
    self.preloader.failover();
    self.event_bus.publish(WALFailed);
    Error::WALFailed(err)
  }

  fn append_in_block(
    &self,
    reserved: ReservedAppend,
    record: LogRecordUninit,
    flush: bool,
  ) -> Result {
    let ReservedAppend {
      buffer_ptr: _buffer_ptr,
      guard: _guard,
      buffer,
      offset,
      commit_order,
      token,
      backoff,
    } = reserved;

    let log_id = self.last_log_id.fetch_add(1, Ordering::Release);
    buffer.append_at(&record.init(log_id), offset);
    if !flush {
      buffer.commit_append();
      return Ok(());
    }

    while commit_order > buffer.load_committed_append() {
      backoff.snooze();
    }
    buffer.commit_append();

    let done = buffer.flush_block();
    while !buffer.prev_blocks_rotated() {
      backoff.snooze();
    }
    if let Err(err) = done.wait().unwrap() {
      return Err(self.failover(err.kind()));
    };

    self.wait_sync(buffer, token)
  }

  fn wait_sync(&self, buffer: &LogBuffer, token: SharedToken) -> Result {
    let done = buffer.sync_segment();
    drop(token);

    if let Err(err) = self.sync_queue.wait_until(buffer.get_generation()) {
      return Err(self.failover(err.kind()));
    }

    done
      .wait()
      .unwrap()
      .map_err(|err| self.failover(err.kind()))
  }

  fn rotate_block(
    &self,
    reserved: ReservedAppend,
    record: LogRecordUninit,
    flush: bool,
  ) -> Result {
    let ReservedAppend {
      buffer_ptr,
      guard,
      buffer,
      offset,
      commit_order,
      token,
      backoff,
    } = reserved;

    let record = record.init(self.last_log_id.fetch_add(1, Ordering::Release));
    let (remain, overflow) = record.split_at(WAL_BLOCK_SIZE - offset);
    buffer.append_at(remain, offset);

    let mut new_page = self.page_pool.acquire();
    new_page.copy_from(overflow, 0);
    let Ok(new_buffer_ptr) = self.buffer.compare_exchange(
      buffer_ptr,
      Owned::new(buffer.init_next(new_page, overflow.len())),
      Ordering::Release,
      Ordering::Acquire,
      guard,
    ) else {
      unreachable!()
    };

    unsafe { guard.defer_destroy(buffer_ptr) };
    while commit_order > buffer.load_committed_append() {
      backoff.snooze();
    }

    if !flush {
      let result = buffer.flush_block().wait().unwrap();
      buffer.increase_rotated_count();
      return result.map_err(|err| self.failover(err.kind()));
    }

    let new_buffer = unsafe { &*new_buffer_ptr.as_raw() };
    let done = new_buffer.flush_block();

    let result = buffer.flush_block().wait().unwrap();
    buffer.increase_rotated_count();
    if let Err(err) = result {
      return Err(self.failover(err.kind()));
    };

    while !new_buffer.prev_blocks_rotated() {
      backoff.snooze();
    }
    if let Err(err) = done.wait().unwrap() {
      return Err(self.failover(err.kind()));
    };

    self.wait_sync(buffer, token)
  }

  fn rotate_segment(&self, reserved: ReservedAppend) -> Result {
    let ReservedAppend {
      buffer_ptr,
      guard,
      buffer,
      commit_order,
      mut token,
      offset: _,
      backoff,
    } = reserved;

    let new = match self.preloader.load() {
      Ok(v) => v,
      Err(Error::IO(err)) => return Err(self.failover(err.kind())),
      Err(err) => return Err(err),
    };
    let replacement =
      LogBuffer::init_new(self.page_pool.acquire(), new, buffer.get_generation() + 1);

    self
      .buffer
      .store(Owned::init(replacement), Ordering::Release);
    unsafe { guard.defer_destroy(buffer_ptr) };

    while commit_order > buffer.load_committed_append() {
      backoff.snooze();
    }

    let done = buffer.flush_block();
    loop {
      match token.try_upgrade() {
        Ok(t) => break forget(t),
        Err(t) => token = t,
      };
      backoff.snooze();
    }

    while !buffer.prev_blocks_rotated() {
      backoff.snooze();
    }
    if let Err(err) = done.wait().unwrap() {
      return Err(self.failover(err.kind()));
    };

    let segment = buffer.take_segment();
    self.sync_queue.push(segment.fsync());
    self.event_bus.publish(WALSegmentRotated(segment));
    Ok(())
  }

  fn append(&self, record: LogRecordUninit, flush: bool) -> Result {
    let len = record.len();
    let backoff = Backoff::new();

    loop {
      if !self.state.load().is_available() {
        return Err(Error::WALUnavailable);
      }

      let guard = pin();
      let buffer_ptr = self.buffer.load(Ordering::Acquire, &guard);
      let buffer = unsafe { &*buffer_ptr.as_raw() };

      let Some(token) = buffer.pin_segment() else {
        backoff.snooze();
        continue;
      };

      let (offset, commit_order) = buffer.reserve_append(len);
      if offset > WAL_BLOCK_SIZE {
        drop(token);
        backoff.snooze();
        continue;
      }

      let reserved = ReservedAppend {
        buffer_ptr,
        guard: &guard,
        buffer,
        token,
        offset,
        commit_order,
        backoff: &backoff,
      };

      if offset + len <= WAL_BLOCK_SIZE {
        return self.append_in_block(reserved, record, flush);
      }

      if buffer.get_pointer() + 1 < self.max_len {
        return self.rotate_block(reserved, record, flush);
      }

      self.rotate_segment(reserved)?;
      backoff.reset();
    }
  }

  pub fn current_log_id(&self) -> LogId {
    self.last_log_id.load(Ordering::Acquire)
  }

  pub fn append_insert(
    &self,
    tx_id: TxId,
    table_id: TableId,
    ptr: Pointer,
    record_version: TxId,
    data: &[u8],
  ) -> Result {
    self.append(
      LogRecordUninit::new_insert(
        tx_id,
        table_id,
        ptr,
        record_version,
        DEFAULT_ENCODING,
        data,
      ),
      false,
    )
  }
  pub fn append_blob_created(&self, metadata: BlobMetadata) -> Result {
    self.append(LogRecordUninit::new_blob_created(metadata), false)
  }

  pub fn checkpoint_and_flush(
    &self,
    last_log_id: LogId,
    current_version: TxId,
    path: PathBuf,
  ) -> Result {
    self.append(
      LogRecordUninit::new_checkpoint(last_log_id, current_version, path),
      true,
    )
  }

  pub fn commit_and_flush(&self, tx_id: TxId) -> Result {
    self.append(LogRecordUninit::new_commit(tx_id), true)
  }

  pub fn is_available(&self) -> bool {
    self.state.load().is_available()
  }

  pub fn close(&self) {
    self.sync_queue.drain();
    let guard = pin();
    let ptr = self.buffer.swap(Shared::null(), Ordering::Release, &guard);
    if !ptr.is_null() {
      unsafe { guard.defer_destroy(ptr) };
    }
    if !self.state.load().is_available() {
      return;
    }
    self.preloader.close();
  }
}

struct ReservedAppend<'a> {
  buffer_ptr: Shared<'a, LogBuffer>,
  guard: &'a Guard,
  buffer: &'static LogBuffer,
  token: SharedToken<'a>,
  offset: usize,
  commit_order: u32,
  backoff: &'a Backoff,
}
