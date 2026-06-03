use std::{
  mem::forget,
  panic::RefUnwindSafe,
  path::PathBuf,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock, Weak,
  },
};

use crossbeam::{
  epoch::{self, Atomic, Owned},
  queue::SegQueue,
  utils::Backoff,
};

use crate::{
  disk::{IOPool, PagePool, Pointer},
  error::Result,
  info,
  table::TableId,
  utils::{ToBox, UnsafeBorrow},
};

use super::{
  replay, AtomicLogId, Checkpoint, FsyncResult, LogBuffer, LogId, LogRecord,
  ReplayResult, SegmentPreload, TxId, WALSegment, WAL_BLOCK_SIZE,
};

pub struct WALConfig {
  pub base_dir: PathBuf,
  pub max_file_size: usize,
  pub max_buffer_size: usize,
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
   *  preload wal segment
   *  reuse synced + checkpoint complete segment
   */
  preloader: Box<SegmentPreload>,
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
  /**
   * wal segment max size
   */
  max_len: Pointer,
  /**
   * preloaded data block.
   */
  page_pool: PagePool<WAL_BLOCK_SIZE>,

  checkpoint: OnceLock<Weak<Checkpoint>>,
  /**
   * fsync results for rotated segments, pushed asynchronously at rotation time.
   * commit_and_flush drains this queue to ensure all prior segments are durable.
   * Without this, a commit written to segment N could be fsynced while segment N-1
   * (containing the corresponding insert) has not — losing data on crash.
   */
  fsync_queue: SegQueue<FsyncResult>,
  /**
   * Number of segments whose fsync has completed. Used by commit_and_flush to
   * verify that all segments up to the current generation have been persisted.
   */
  synced_count: AtomicU64,
}
impl WAL {
  pub fn replay(
    config: &WALConfig,
    io_pool: Arc<IOPool>,
  ) -> Result<(Self, ReplayResult)> {
    let max_len = config.max_file_size / WAL_BLOCK_SIZE;
    let page_pool = PagePool::new(config.max_buffer_size / WAL_BLOCK_SIZE);
    let max_len = max_len as Pointer;
    info!("start to replay wal segments");

    let replay_result = replay(&config.base_dir, &page_pool, &io_pool)?;

    info!(
      "wal replay result: last_log_id {} last_tx_id {} aborted {} redo {} segments {}",
      replay_result.last_log_id,
      replay_result.last_tx_id,
      replay_result.aborted.len(),
      replay_result.redo.len(),
      replay_result.segments.len(),
    );

    let preloader = SegmentPreload::new(
      config.base_dir.clone(),
      replay_result.generation,
      max_len,
      io_pool,
    )
    .to_box();
    let buffer = LogBuffer::init_new(page_pool.acquire(), preloader.load()?, 0);

    Ok((
      Self {
        last_log_id: AtomicLogId::new(replay_result.last_log_id),
        preloader,
        buffer: Atomic::new(buffer),
        page_pool,
        max_len,
        checkpoint: OnceLock::new(),
        fsync_queue: SegQueue::new(),
        synced_count: AtomicU64::new(0),
      },
      replay_result,
    ))
  }

  pub fn initialize(&self, checkpoint: Weak<Checkpoint>) {
    debug_assert!(self.checkpoint.get().is_none());
    let _ = self.checkpoint.set(checkpoint);
  }

  /**
   * ## lock freely append wal record.
   *
   * 1.  create record by closure.
   *
   * 2.  load current buffer.
   *
   * 3.  pinning current segment in buffer.
   *
   * 4.  obtain offset and record count from buffer.
   *
   * 5.  is able to write in entry
   *   5-1. write and commit entry + unpin segment.
   *
   * 6.  if fsync required and able to write in entry
   *   6-1. wait commit for previous writes in entry.
   *   6-2. apply records count to entry and commit entry.
   *   6-3. wait previous writes in disk and fsync call and then unpin segment.
   *   6-4. wait previous fsync and current fsync, then return.
   *
   * 7.  if obtained offset exceed the threshold(eg. WAL_BLOCK_SIZE), yield and move to 2 and retry.
   *
   * 8.  if obtained offset exceed the threshold at first, then start to rotate current buffer.
   *   8-1. if current buffer segment pointer has been exceed the threshold(eg. max len),
   *          then trying to rotate buffer with rotated segment.
   *
   * 9.  if failed to rotate buffer, then clear this buffer and reuse segment if the segment has been rotated.
   *
   * 10. if succeeded to rotate buffer,
   *   10-1. wait previous writes in entry, and write records count, and write to disk.
   *   10-2. if current segment has not been rotated, then unpin segment and continue.
   *   10-3. if current segment has been rotated, wait until pin is empty.
   *   10-4. take segment raw pointer in buffer, and then trigger checkpoint.
   */
  fn append<F>(&self, create_record: F, flush: bool) -> Result
  where
    F: FnOnce(LogId) -> LogRecord,
  {
    let log_id = self.last_log_id.fetch_add(1, Ordering::Release);
    let record = create_record(log_id).to_bytes_with_len();
    let len = record.len();
    let backoff = Backoff::new();

    loop {
      let guard = epoch::pin();
      let buffer_ptr = self.buffer.load(Ordering::Acquire, &guard);
      let buffer = buffer_ptr.as_raw().borrow_unsafe();

      let token = match buffer.pin_segment() {
        Some(v) => v,
        None => {
          backoff.snooze();
          continue;
        }
      };
      let (offset, ready) = buffer.pin_entry(len);
      if offset + len < WAL_BLOCK_SIZE {
        buffer.write_at(&record, offset);
        if !flush {
          buffer.commit_entry();
          return Ok(());
        }

        while ready > buffer.load_commit() {
          backoff.snooze();
        }
        buffer.apply_record_count(ready + 1);
        buffer.commit_entry();

        buffer.write_to_disk()?;
        while !buffer.is_ready_to_flush() {
          backoff.snooze();
        }

        let f = buffer.flush();
        drop(token);

        while buffer.get_generation() > self.synced_count.load(Ordering::Acquire) {
          match self.fsync_queue.pop() {
            Some(f) => {
              f.wait()?;
              self.synced_count.fetch_add(1, Ordering::Release);
            }
            None => backoff.snooze(),
          }
        }
        return f.wait();
      }

      if offset >= WAL_BLOCK_SIZE {
        drop(token);
        backoff.snooze();
        continue;
      }

      let replacement = if buffer.get_pointer() + 1 >= self.max_len {
        LogBuffer::init_new(
          self.page_pool.acquire(),
          self.preloader.load()?,
          buffer.get_generation() + 1,
        )
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

      let buffer = buffer_ptr.as_raw().borrow_unsafe();
      while ready > buffer.load_commit() {
        backoff.snooze();
      }

      buffer.apply_record_count(ready);
      buffer.write_to_disk()?;
      buffer.increase_written_count();

      if buffer.get_pointer() + 1 < self.max_len {
        drop(token);
        backoff.snooze();
        continue;
      }

      forget(token.upgrade());

      let segment = buffer.take_segment();
      let sync = segment.fsync();
      if let Some(checkpoint) = self.checkpoint.wait().upgrade() {
        self.fsync_queue.push(sync);
        checkpoint.dispatch(segment);
        continue;
      }

      sync.wait()?;
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
    data: Vec<u8>,
  ) -> Result {
    self.append(
      |log_id| LogRecord::new_insert(log_id, tx_id, table_id, ptr, data),
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
      |log_id| LogRecord::new_checkpoint(log_id, last_log_id, current_version, path),
      true,
    )
  }
  pub fn append_start(&self, tx_id: TxId) -> Result {
    self.append(|log_id| LogRecord::new_start(log_id, tx_id), false)
  }
  pub fn commit_and_flush(&self, tx_id: TxId) -> Result {
    self.append(|log_id| LogRecord::new_commit(log_id, tx_id), true)
  }
  pub fn append_abort(&self, tx_id: TxId) -> Result {
    self.append(|log_id| LogRecord::new_abort(log_id, tx_id), false)
  }

  pub fn reuse(&self, segment: WALSegment) {
    self.preloader.reuse(segment);
  }

  pub fn close(&self) {
    while let Some(f) = self.fsync_queue.pop() {
      let _ = f.wait();
      self.synced_count.fetch_add(1, Ordering::Release);
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
      let _ = self.preloader.close();
      return;
    }
  }
}
unsafe impl Send for WAL {}
unsafe impl Sync for WAL {}
impl RefUnwindSafe for WAL {}
