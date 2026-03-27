use std::{
  collections::{HashSet, VecDeque},
  mem::replace,
  sync::Arc,
};

use super::{DataEntry, Pointer, RecordData, VersionRecord};
use crate::{
  buffer_pool::BufferPool,
  constant::RESERVED_TX,
  error::Result,
  thread::{BackgroundThread, BatchWorkResult, WorkBuilder, WorkResult},
  transaction::{FreeList, PageRecorder, VersionVisibility},
  utils::{DoubleBuffer, LogFilter, ToArc},
};

pub struct GarbageCollectionConfig {
  pub thread_count: usize,
}

/**
 * Trim: walk the version chain and discard expired or aborted versions.
 * Release: free an orphaned DataEntry header page. This happens when the tree
 * manager removes a key (e.g. after a tombstone) and the leaf entry is gone,
 * leaving the DataEntry page unreachable. Release is always deferred until all
 * Trim work is complete — a concurrent Trim on the same page would dereference
 * a dangling pointer if Release ran first.
 */
enum GcPointer {
  Trim(Pointer),
  Release(Pointer),
}

/**
 * Runs at each checkpoint to clean expired and aborted versions from DataEntry
 * pages, then confirms min_active in the checkpoint log. DataEntry pages are
 * marked for GC (Trim) when a cursor performs an update or remove, so the same
 * pointer can appear multiple times if the same key is written repeatedly
 * between two GC runs. check and entry are separate workers: check answers
 * emptiness queries, entry does version reclamation.
 */
pub struct GarbageCollector {
  check: Arc<dyn BackgroundThread<Pointer, Result<bool>>>,
  entry: Arc<dyn BackgroundThread<Pointer, Result>>,
  free_list: Arc<FreeList>,
  queue: Arc<DoubleBuffer<GcPointer>>,
  logger: LogFilter,
}
impl GarbageCollector {
  pub fn run(&self) -> Result {
    let queue = self.queue.switch();
    self
      .logger
      .debug(|| format!("{} data will check version in this scope.", queue.len()));
    let mut waiting = Vec::new();
    let mut release = Vec::new();
    let mut dedup = HashSet::new();
    while let Some(ptr) = queue.pop() {
      match ptr {
        GcPointer::Trim(ptr) => {
          if !dedup.insert(ptr) {
            continue;
          }
          waiting.push(self.entry.send(ptr));
        }
        GcPointer::Release(ptr) => release.push(ptr),
      }
    }
    self.logger.debug(|| "all entry cleaning triggered.");

    waiting
      .into_iter()
      .map(|v| v.wait_flatten())
      .collect::<Result>()?;
    self.logger.debug(|| "unreachable versions all collected.");

    // must release after triming because of trim type can contain release type.
    // it could occur dangling pointer reference.
    release.into_iter().for_each(|i| self.free_list.dealloc(i));
    Ok(())
  }

  pub fn mark(&self, pointer: Pointer) {
    self.queue.push(GcPointer::Trim(pointer))
  }
  pub fn lazy_release(&self, pointer: Pointer) {
    self.queue.push(GcPointer::Release(pointer))
  }

  pub fn batch_check_empty(
    &self,
    pointers: Vec<Pointer>,
  ) -> BatchWorkResult<Result<bool>> {
    self.check.send_batch(pointers)
  }
  pub fn check_empty(&self, pointer: Pointer) -> WorkResult<Result<bool>> {
    self.check.send(pointer)
  }

  pub fn start(
    buffer_pool: Arc<BufferPool>,
    version_visibility: Arc<VersionVisibility>,
    free_list: Arc<FreeList>,
    recorder: Arc<PageRecorder>,
    logger: LogFilter,
    config: GarbageCollectionConfig,
  ) -> Self {
    let queue = DoubleBuffer::new().to_arc();

    let entry = WorkBuilder::new()
      .name("gc found entry")
      .stack_size(2 << 20)
      .multi(config.thread_count)
      .shared(run_entry(
        buffer_pool.clone(),
        version_visibility,
        recorder.clone(),
        free_list.clone(),
      ))
      .to_arc();
    let check = WorkBuilder::new()
      .name("gc check top entry")
      .stack_size(2 << 20)
      .multi(config.thread_count)
      .shared(run_check(buffer_pool.clone()))
      .to_arc();

    Self {
      check,
      entry,
      free_list,
      queue,
      logger,
    }
  }

  pub fn close(&self) {
    self.check.close();
    self.entry.close();
  }
}

fn run_entry(
  buffer_pool: Arc<BufferPool>,
  version_visibility: Arc<VersionVisibility>,
  recorder: Arc<PageRecorder>,
  free_list: Arc<FreeList>,
) -> impl Fn(Pointer) -> Result {
  move |ptr: Pointer| {
    let mut index = Some(ptr);
    let mut max_found = false;

    let release = |record: VersionRecord| {
      if let RecordData::Chunked(pointers) = record.data {
        pointers.into_iter().for_each(|p| free_list.dealloc(p));
      }
    };

    while let Some(i) = index.take() {
      let mut slot = buffer_pool.peek(i)?.for_write();
      let mut entry: DataEntry = slot.as_ref().deserialize()?;

      let prev_len = entry.len();
      let mut expired_max: Option<VersionRecord> = None;
      let min_version = version_visibility.min_version();
      let mut new_versions = VecDeque::new();
      for record in entry.take_versions() {
        if version_visibility.is_aborted(&record.owner) {
          release(record);
          continue;
        }
        if record.version > min_version {
          new_versions.push_back(record);
          continue;
        }
        if max_found {
          release(record);
          continue;
        }

        // Keep only the newest version at or below min_version. All active
        // transactions started after min_version, so older versions can never
        // be reached again.
        match expired_max.as_mut() {
          Some(max) if max.version < record.version => release(replace(max, record)),
          None => expired_max = Some(record),
          _ => release(record),
        };
      }

      if !max_found {
        if let Some(record) = expired_max.take() {
          match &record.data {
            RecordData::Data(_) => new_versions.push_back(record),
            RecordData::Chunked(_) => new_versions.push_back(record),
            RecordData::Tombstone => {}
          }

          max_found = true;
        }
      }

      if new_versions.len() == prev_len {
        index = entry.get_next();
        continue;
      }

      if new_versions.len() > 0 {
        entry.set_versions(new_versions);
        recorder.serialize_and_log(RESERVED_TX, &mut slot, &entry)?;
        index = entry.get_next();
        continue;
      }

      let next = match entry.get_next() {
        Some(i) => i,
        None => return recorder.serialize_and_log(RESERVED_TX, &mut slot, &entry),
      };

      let next_entry: DataEntry =
        buffer_pool.peek(next)?.for_read().as_ref().deserialize()?;
      recorder.serialize_and_log(RESERVED_TX, &mut slot, &next_entry)?;
      index = Some(i);

      free_list.dealloc(next);
    }
    Ok(())
  }
}

fn run_check(buffer_pool: Arc<BufferPool>) -> impl Fn(Pointer) -> Result<bool> {
  move |pointer: Pointer| {
    Ok(
      buffer_pool
        .peek(pointer)?
        .for_read()
        .as_ref()
        .deserialize::<DataEntry>()?
        .is_empty(),
    )
  }
}
