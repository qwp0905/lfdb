use std::{
  io::ErrorKind,
  mem::forget,
  path::PathBuf,
  sync::{atomic::Ordering, Arc},
};

use crossbeam::{
  atomic::AtomicCell,
  epoch::{self, Atomic, Owned},
  utils::Backoff,
};

use crate::{
  background::EventBus,
  disk::{IOPool, PagePool, Pointer},
  error, info,
  table::TableId,
  utils::UnsafeBorrow,
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

    let replay_result = replay(&page_pool, &io_pool)?;

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

  /**
   * ## lock freely append wal record.
   *
   * 1.  create uninitialized record by closure.
   *
   * 2.  load current buffer.
   *
   * 3.  pinning current segment in buffer.
   *
   * 4.  obtain offset and record count from buffer.
   *
   * 5.  is able to write in entry
   *   5-1. obtain log id and initialize record to a vector.
   *   5-2. write record vector and commit entry.
   *
   * 6.  if fsync required and able to write in entry
   *   6-1. wait commit for previous writes in entry.
   *   6-2. apply records count to entry and commit entry.
   *   6-3. wait previous writes in disk and fsync call.
   *   6-4. wait previous fsync and current fsync, then return.
   *
   * 7.  if obtained offset exceed the threshold(eg. WAL_BLOCK_SIZE), yield and move to 2 and retry.
   *
   * 8.  if obtained offset exceed the threshold at first, then start to rotate current buffer.
   *   8-1. if current buffer segment pointer has been exceed the threshold(eg. max len),
   *        then trying to rotate buffer with rotated segment.
   *
   * 9.  if failed to rotate buffer, then clear this buffer and reuse segment if the segment has been rotated.
   *
   * 10. if succeeded to rotate buffer,
   *   10-1. wait previous writes in entry, and write records count, and write to disk.
   *   10-2. if current segment has not been rotated, then continue.
   *   10-3. if current segment has been rotated, wait until pin is empty.
   *   10-4. take segment raw pointer in buffer, and then trigger checkpoint.
   */
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

      let Some(mut token) = buffer.pin_segment() else {
        backoff.snooze();
        continue;
      };

      let (offset, order) = buffer.reserve_entry(len);
      if offset + len < WAL_BLOCK_SIZE {
        let log_id = self.last_log_id.fetch_add(1, Ordering::Release);
        buffer.write_at(&record.init(log_id), offset);
        if !flush {
          buffer.commit_entry();
          return Ok(());
        }

        while order > buffer.load_commit() {
          backoff.snooze();
        }
        buffer.apply_record_count(order + 1);
        buffer.commit_entry();

        if let Err(err) = buffer.write_to_disk() {
          return Err(self.failover(err.kind()));
        };
        while !buffer.is_ready_to_flush() {
          backoff.snooze();
        }

        let f = buffer.flush();
        drop(token);

        if let Err(err) = self.sync_queue.wait_until(buffer.get_generation())? {
          return Err(self.failover(err.kind()));
        }

        return f.wait().unwrap().map_err(|err| self.failover(err.kind()));
      }

      if offset >= WAL_BLOCK_SIZE {
        drop(token);
        backoff.snooze();
        continue;
      }

      let replacement = if buffer.get_pointer() + 1 >= self.max_len {
        let new = match self.preloader.load() {
          Ok(v) => v,
          Err(Error::IO(err)) => return Err(self.failover(err.kind())),
          Err(err) => return Err(err),
        };
        LogBuffer::init_new(self.page_pool.acquire(), new, buffer.get_generation() + 1)
      } else {
        buffer.init_next(self.page_pool.acquire())
      };

      if let Err(failed) = self.buffer.compare_exchange(
        buffer_ptr,
        Owned::init(replacement),
        Ordering::Release,
        Ordering::Acquire,
        &guard,
      ) {
        if failed.new.get_pointer() > 0 {
          drop(token);
          backoff.snooze();
          continue;
        }

        let segment = failed.new.take_segment();
        self.preloader.reuse(segment);
        continue;
      }

      unsafe { guard.defer_destroy(buffer_ptr) };

      while order > buffer.load_commit() {
        backoff.snooze();
      }

      buffer.apply_record_count(order);
      let write_r = buffer.write_to_disk();
      buffer.increase_written_count();
      if let Err(err) = write_r {
        return Err(self.failover(err.kind()));
      };

      if buffer.get_pointer() + 1 < self.max_len {
        drop(token);
        backoff.snooze();
        continue;
      }

      loop {
        match token.try_upgrade() {
          Ok(t) => break forget(t),
          Err(t) => token = t,
        };
        backoff.snooze();
      }

      let segment = buffer.take_segment();
      self.sync_queue.push(segment.fsync());
      self.event_bus.publish(WALSegmentRotated(segment));
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
    if !self.state.load().is_available() {
      return;
    }

    let backoff = Backoff::new();
    loop {
      let guard = epoch::pin();
      let ptr = self.buffer.load(Ordering::Acquire, &guard);
      let buffer = ptr.as_raw().borrow_unsafe();
      if buffer.load_offset() >= WAL_BLOCK_SIZE {
        backoff.snooze();
        continue;
      }

      if buffer.pin_segment_exclusive().map(forget).is_none() {
        backoff.snooze();
        continue;
      }

      let taken = unsafe { ptr.into_owned() };
      let _ = taken.take_segment();
      self.preloader.close();
      return;
    }
  }
}
unsafe impl Send for WAL {}
unsafe impl Sync for WAL {}
