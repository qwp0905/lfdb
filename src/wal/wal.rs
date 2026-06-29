use std::{
  io::ErrorKind,
  mem::forget,
  path::PathBuf,
  sync::{atomic::Ordering, Arc},
};

use crossbeam::{
  atomic::AtomicCell,
  epoch::{self, Atomic, Guard, Owned, Shared},
  utils::Backoff,
};

use crate::{
  background::EventBus,
  disk::{IOPool, PagePool, Pointer},
  error, info,
  table::TableId,
  utils::{SharedToken, UnsafeBorrow},
  Error, Result,
};

use super::{
  replay, AtomicLogId, LogBuffer, LogId, LogRecordUninit, ReplayResult, SegmentPreload,
  SyncQueue, TxId, WALSegment, WAL_BLOCK_SIZE,
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
pub struct WAL {
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
impl WAL {
  pub fn replay(
    config: &WALConfig,
    event_bus: Arc<EventBus>,
    io_pool: Arc<IOPool>,
  ) -> Result<(Self, ReplayResult)> {
    let max_len = config.max_file_size / WAL_BLOCK_SIZE;
    let page_pool = PagePool::new(config.max_buffer_size / WAL_BLOCK_SIZE);
    let max_len = max_len as Pointer;
    info!("start to replay wal segments");

    let replay_result = replay(&io_pool)?;

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
    buffer: &'static LogBuffer,
    record: LogRecordUninit,
    offset: usize,
    commit_order: u32,
    flush: bool,
    token: SharedToken,
    backoff: &Backoff,
  ) -> Result {
    let log_id = self.last_log_id.fetch_add(1, Ordering::Release);
    buffer.write_at(&record.init(log_id), offset);
    if !flush {
      buffer.commit_entry();
      return Ok(());
    }

    while commit_order > buffer.load_commit() {
      backoff.snooze();
    }
    buffer.commit_entry();

    let done = buffer.write_to_disk();
    while !buffer.is_ready_to_flush() {
      backoff.snooze();
    }
    if let Err(err) = done.wait().unwrap() {
      return Err(self.failover(err.kind()));
    };

    self.wait_sync(buffer, token)
  }

  fn wait_sync(&self, buffer: &LogBuffer, token: SharedToken) -> Result {
    let done = buffer.flush();
    drop(token);

    if let Err(err) = self.sync_queue.wait_until(buffer.get_generation())? {
      return Err(self.failover(err.kind()));
    }

    done
      .wait()
      .unwrap()
      .map_err(|err| self.failover(err.kind()))
  }

  fn rotate_block(
    &self,
    buffer_ptr: Shared<LogBuffer>,
    guard: &Guard,
    buffer: &'static LogBuffer,
    record: LogRecordUninit,
    offset: usize,
    commit_order: u32,
    flush: bool,
    backoff: &Backoff,
    token: SharedToken,
  ) -> Result {
    let mut record = record.init(self.last_log_id.fetch_add(1, Ordering::Release));
    let remain = record.split_off(WAL_BLOCK_SIZE - offset);
    buffer.write_at(&record, offset);

    let mut new_page = self.page_pool.acquire();
    new_page.range_mut(0..remain.len()).copy_from_slice(&remain);
    let new_buffer = buffer.init_next(new_page, remain.len());
    let Ok(new_buffer_ptr) = self.buffer.compare_exchange(
      buffer_ptr,
      Owned::new(new_buffer),
      Ordering::Release,
      Ordering::Acquire,
      guard,
    ) else {
      unreachable!()
    };

    unsafe { guard.defer_destroy(buffer_ptr) };
    while commit_order > buffer.load_commit() {
      backoff.snooze();
    }

    if !flush {
      let result = buffer.write_to_disk().wait().unwrap();
      buffer.increase_written_count();
      return result.map_err(|err| self.failover(err.kind()));
    }

    let new_buffer = new_buffer_ptr.as_raw().borrow_unsafe();
    let done = new_buffer.write_to_disk();

    let result = buffer.write_to_disk().wait().unwrap();
    buffer.increase_written_count();
    if let Err(err) = result {
      return Err(self.failover(err.kind()));
    };

    while !new_buffer.is_ready_to_flush() {
      backoff.snooze();
    }
    if let Err(err) = done.wait().unwrap() {
      return Err(self.failover(err.kind()));
    };

    self.wait_sync(buffer, token)
  }

  fn rotate_segment(
    &self,
    buffer_ptr: Shared<LogBuffer>,
    guard: &Guard,
    buffer: &'static LogBuffer,
    commit_order: u32,
    mut token: SharedToken,
    backoff: &Backoff,
  ) -> Result {
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

    while commit_order > buffer.load_commit() {
      backoff.snooze();
    }

    let done = buffer.write_to_disk();
    loop {
      match token.try_upgrade() {
        Ok(t) => break forget(t),
        Err(t) => token = t,
      };
      backoff.snooze();
    }

    while !buffer.is_ready_to_flush() {
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

      let guard = epoch::pin();
      let buffer_ptr = self.buffer.load(Ordering::Acquire, &guard);
      let buffer = buffer_ptr.as_raw().borrow_unsafe();

      let Some(token) = buffer.pin_segment() else {
        backoff.snooze();
        continue;
      };

      let (offset, commit_order) = buffer.reserve_entry(len);
      if offset > WAL_BLOCK_SIZE {
        drop(token);
        backoff.snooze();
        continue;
      }

      if offset + len <= WAL_BLOCK_SIZE {
        return self.append_in_block(
          buffer,
          record,
          offset,
          commit_order,
          flush,
          token,
          &backoff,
        );
      }

      if buffer.get_pointer() + 1 < self.max_len {
        return self.rotate_block(
          buffer_ptr,
          &guard,
          buffer,
          record,
          offset,
          commit_order,
          flush,
          &backoff,
          token,
        );
      }

      self.rotate_segment(buffer_ptr, &guard, buffer, commit_order, token, &backoff)?;
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
    data: Vec<u8>,
  ) -> Result {
    self.append(
      LogRecordUninit::new_insert(tx_id, table_id, ptr, record_version, data),
      false,
    )
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
    let guard = epoch::pin();
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
unsafe impl Send for WAL {}
unsafe impl Sync for WAL {}
