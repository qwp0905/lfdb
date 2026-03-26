use std::{
  path::PathBuf,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  time::Duration,
};

use crossbeam::{
  epoch::{self, Atomic, Owned},
  queue::SegQueue,
  utils::Backoff,
};

use crate::{
  disk::PagePool,
  error::Result,
  thread::{BackgroundThread, WorkBuilder, WorkInput},
  utils::{LogFilter, ToArc, ToBox, UnsafeBorrow},
};

use super::{
  replay, FsyncResult, LogBuffer, LogRecord, ReplayResult, SegmentPreload, WALSegment,
  WAL_BLOCK_SIZE,
};

pub struct WALConfig {
  pub base_dir: PathBuf,
  pub prefix: PathBuf,
  pub checkpoint_interval: Duration,
  pub segment_flush_delay: Duration,
  pub segment_flush_count: usize,
  pub group_commit_count: usize,
  pub max_file_size: usize,
}

/**
 * Write ahead log
 */
pub struct WAL {
  /**
   *  preload wal segment
   *  reuse synced + checkpoint complete segment
   */
  preloader: Arc<SegmentPreload>,
  /**
   * last log id (LSN)
   */
  last_log_id: AtomicUsize,
  /**
   * wal log buffer.
   */
  buffer: Atomic<LogBuffer>,
  /**
   * wal segment max size
   */
  max_index: usize,
  /**
   * preloaded data block.
   */
  page_pool: PagePool<WAL_BLOCK_SIZE>,
  /**
   * buffering rotated segment and trigger checkpoint.
   */
  wait_checkpoint: Box<dyn BackgroundThread<WALSegment>>,
  /**
   * queue for segment whick checkpoint failed.
   * it will be clear and move to preloader to reuse segment when checkpoint complete.
   */
  checkpoint_failed: Arc<SegQueue<WALSegment>>,
  /**
   * fsync operation result queue.
   * asynchronously called in segment rotation .
   */
  fsync_queue: SegQueue<FsyncResult>,
  /**
   * fsync has been completed segments count
   */
  syned_count: AtomicUsize,
}
impl WAL {
  pub fn replay(
    config: WALConfig,
    checkpoint: WorkInput<(), Result>,
    logger: LogFilter,
  ) -> Result<(Self, ReplayResult)> {
    let max_index = config.max_file_size / WAL_BLOCK_SIZE;
    let page_pool = PagePool::new(max_index);
    logger.info(|| "start to replay wal segments");

    let replay_result = replay(
      config.base_dir.to_string_lossy().as_ref(),
      config.prefix.to_string_lossy().as_ref(),
      config.group_commit_count,
      &page_pool,
    )?;

    logger.info(|| {
      format!(
        "wal replay result: last_log_id {} last_tx_id {} aborted {} redo {} segments {}",
        replay_result.last_log_id,
        replay_result.last_tx_id,
        replay_result.aborted.len(),
        replay_result.redo.len(),
        replay_result.segments.len()
      )
    });

    let prefix = PathBuf::from(config.base_dir).join(config.prefix);

    let preloader = SegmentPreload::new(
      prefix,
      replay_result.generation,
      config.group_commit_count,
      max_index,
    )
    .to_arc();

    let not_flushed = SegQueue::new().to_arc();
    let wait_checkpoint = WorkBuilder::new()
      .name("wal checkpoint buffering")
      .stack_size(2 << 20)
      .single()
      .lazy_buffering(
        config.segment_flush_delay,
        config.segment_flush_count,
        waiting_checkpoint(checkpoint, preloader.clone(), not_flushed.clone()),
      )
      .to_box();

    let buffer = LogBuffer::init_new(page_pool.acquire(), preloader.load()?, 0);

    Ok((
      Self {
        last_log_id: AtomicUsize::new(replay_result.last_log_id),
        preloader,
        buffer: Atomic::new(buffer),
        page_pool,
        max_index,
        wait_checkpoint,
        checkpoint_failed: not_flushed,
        fsync_queue: SegQueue::new(),
        syned_count: AtomicUsize::new(0),
      },
      replay_result,
    ))
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
   * 8.  if obtained offset exceed the thredhold at first, then start to rotate current buffer.
   *   8-1. if current buffer segment index has been exceed the threshold(eg. max len),
   *          then trying to rotate buffer with rotated segment.
   *
   * 9.  if failed to rotate buffer, then clear this buffer and reuse segment if the segment has been rotated.
   *
   * 10. if succeeded to rotate buffer,
   *   10-1. wait previous writes in entry, and write records count, and write to disk.
   *   10-2. if current segment has not been rotated, then unpin segment and continue.
   *   10-3. if current segment has been rotated, wait until pin is emtpy.
   *   10-4. take segment raw pointer in buffer, and then trigger checkpoint.
   */
  fn append<F>(&self, create_record: F, flush: bool) -> Result
  where
    F: FnOnce(usize) -> LogRecord,
  {
    let log_id = self.last_log_id.fetch_add(1, Ordering::Release);
    let record = create_record(log_id).to_bytes_with_len();
    let len = record.len();
    let guard = &epoch::pin();
    let backoff = Backoff::new();

    loop {
      let buffer_ptr = self.buffer.load(Ordering::Acquire, guard);
      let buffer = buffer_ptr.as_raw().borrow_unsafe();

      buffer.pin_segment();
      let (offset, ready) = buffer.pin_entry(len);
      if offset + len < WAL_BLOCK_SIZE {
        buffer.write_at(&record, offset);
        if !flush {
          buffer.commit_entry();
          buffer.unpin_segment();
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
        buffer.unpin_segment();

        while buffer.get_generation() > self.syned_count.load(Ordering::Acquire) {
          if let Some(f) = self.fsync_queue.pop() {
            f.wait()?;
            self.syned_count.fetch_add(1, Ordering::Release);
          }
          backoff.snooze()
        }
        return f.wait();
      }

      if offset >= WAL_BLOCK_SIZE {
        buffer.unpin_segment();
        backoff.snooze();
        continue;
      }

      let replacement = if buffer.get_index() + 1 >= self.max_index {
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
        guard,
      ) {
        if failed.new.get_index() > 0 {
          failed.current.as_raw().borrow_unsafe().unpin_segment();
          backoff.snooze();
          continue;
        }

        let segment = failed.new.take_segment();
        self.syned_count.fetch_add(1, Ordering::Release);
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

      if buffer.get_index() + 1 < self.max_index {
        buffer.unpin_segment();
        backoff.snooze();
        continue;
      }
      while buffer.load_segment_pinned() > 1 {
        backoff.snooze();
      }

      let segment = buffer.take_segment();
      self.fsync_queue.push(segment.fsync());
      self.wait_checkpoint.send(segment);
    }
  }

  pub fn current_log_id(&self) -> usize {
    self.last_log_id.load(Ordering::Acquire)
  }

  pub fn append_insert(&self, tx_id: usize, index: usize, data: Vec<u8>) -> Result {
    self.append(
      |log_id| LogRecord::new_insert(log_id, tx_id, index, data),
      false,
    )
  }

  pub fn append_multi(
    &self,
    tx_id: usize,
    index1: usize,
    data1: Vec<u8>,
    index2: usize,
    data2: Vec<u8>,
  ) -> Result {
    self.append(
      |log_id| LogRecord::new_multi(log_id, tx_id, index1, data1, index2, data2),
      false,
    )
  }

  pub fn checkpoint_and_flush(&self, last_log_id: usize, min_active: usize) -> Result {
    self.append(
      |log_id| LogRecord::new_checkpoint(log_id, last_log_id, min_active),
      true,
    )
  }
  pub fn append_start(&self, tx_id: usize) -> Result {
    self.append(|log_id| LogRecord::new_start(log_id, tx_id), false)
  }
  pub fn commit_and_flush(&self, tx_id: usize) -> Result {
    self.append(|log_id| LogRecord::new_commit(log_id, tx_id), true)
  }
  pub fn append_abort(&self, tx_id: usize) -> Result {
    self.append(|log_id| LogRecord::new_abort(log_id, tx_id), false)
  }

  pub fn twostep_close<'a>(&'a self) -> impl Fn() + 'a {
    self.wait_checkpoint.close();

    while let Some(f) = self.fsync_queue.pop() {
      let _ = f.wait();
      self.syned_count.fetch_add(1, Ordering::Release);
    }
    while let Some(seg) = self.checkpoint_failed.pop() {
      self.preloader.reuse(seg);
    }

    || {
      let guard = &epoch::pin();
      let backoff = Backoff::new();
      loop {
        let ptr = self.buffer.load(Ordering::Acquire, guard);
        let buffer = ptr.as_raw().borrow_unsafe();
        if buffer.load_offset() >= WAL_BLOCK_SIZE {
          backoff.snooze();
          continue;
        }
        if buffer.load_segment_pinned() > 0 {
          backoff.snooze();
          continue;
        }

        let taken = unsafe { ptr.into_owned() };
        let segment = taken.take_segment();
        let _ = self.preloader.close();
        return segment.close();
      }
    }
  }
}
unsafe impl Send for WAL {}
unsafe impl Sync for WAL {}

fn waiting_checkpoint(
  checkpoint: WorkInput<(), Result>,
  preloader: Arc<SegmentPreload>,
  failed: Arc<SegQueue<WALSegment>>,
) -> impl Fn(Vec<WALSegment>) {
  move |segments| match checkpoint.send(()).wait_flatten() {
    Ok(_) => {
      while let Some(buffered) = failed.pop() {
        preloader.reuse(buffered);
      }
      segments.into_iter().for_each(|s| preloader.reuse(s));
    }
    Err(_) => segments.into_iter().for_each(|s| failed.push(s)),
  }
}
