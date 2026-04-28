use std::{
  collections::{BTreeMap, HashSet, VecDeque},
  mem::replace,
  sync::Arc,
  time::Duration,
};

use super::{DataEntry, RecordData, VersionRecord};
use crate::{
  cache::BlockCache,
  debug,
  disk::Pointer,
  error::Result,
  table::{TableHandle, TableMapper},
  thread::{BackgroundThread, BatchTaskHandle, TaskHandle, WorkBuilder},
  transaction::{PageRecorder, VersionVisibility},
  utils::{DoubleBuffer, ToArc, ToBox},
  wal::{TxId, RESERVED_TX},
};
use crossbeam::epoch::pin;

pub struct GarbageCollectionConfig {
  pub thread_count: usize,
}

const RELEASE_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/**
 * Trim: walk the version chain and discard expired or aborted versions.
 * Release: free an orphaned DataEntry header page. This happens when the tree
 * manager removes a key (e.g. after a tombstone) and the leaf entry is gone,
 * leaving the DataEntry page unreachable. Release is always deferred until all
 * Trim work is complete — a concurrent Trim on the same page would dereference
 * a dangling pointer if Release ran first.
 */
enum GcPointer {
  Trim(Arc<TableHandle>, Pointer),
  Release(Arc<TableHandle>, Pointer),
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
  version_visibility: Arc<VersionVisibility>,
  check: Arc<dyn BackgroundThread<(Arc<TableHandle>, Pointer), Result<bool>>>,
  entry: Arc<dyn BackgroundThread<(Arc<TableHandle>, Pointer), Result>>,
  queue: Arc<DoubleBuffer<GcPointer>>,
  table: Box<dyn BackgroundThread<(Arc<TableHandle>, TxId, TxId)>>,
}
impl GarbageCollector {
  pub fn run(&self) -> Result {
    let min_version = self.version_visibility.min_version();
    let queue = self.queue.switch();

    debug!("{} data will check version in this scope.", queue.len());
    let mut waiting = Vec::new();
    let mut release = BTreeMap::new();
    let mut dedup = HashSet::new();
    while let Some(ptr) = queue.pop() {
      match ptr {
        GcPointer::Trim(table, ptr) => {
          if !dedup.insert((table.metadata().get_id(), ptr)) {
            continue;
          }
          waiting.push(self.entry.execute((table, ptr)));
        }
        GcPointer::Release(table, ptr) => {
          release.insert((table.metadata().get_id(), ptr), table);
        }
      }
    }
    debug!("all entry cleaning triggered.");

    waiting
      .into_iter()
      .map(|v| v.wait().flatten())
      .collect::<Result>()?;
    debug!("unreachable versions all collected.");

    self.version_visibility.remove_aborted(&min_version);

    // must release after triming because of trim type can contain release type.
    // it could occur dangling pointer reference.
    release
      .into_iter()
      .filter(|(_, table)| !table.is_closed())
      .for_each(|((_, ptr), table)| table.free().dealloc(ptr));
    Ok(())
  }

  pub fn mark(&self, table: Arc<TableHandle>, pointer: Pointer) {
    self.queue.push(GcPointer::Trim(table, pointer))
  }
  pub fn lazy_release(&self, table: Arc<TableHandle>, pointer: Pointer) {
    self.queue.push(GcPointer::Release(table, pointer))
  }
  pub fn release_table(&self, table: Arc<TableHandle>, tx_id: TxId, version: TxId) {
    self.table.dispatch((table, tx_id, version));
  }

  pub fn batch_check_empty(
    &self,
    pointers: Vec<(Arc<TableHandle>, Pointer)>,
  ) -> BatchTaskHandle<Result<bool>> {
    self.check.execute_batch(pointers)
  }
  pub fn check_empty(
    &self,
    table: Arc<TableHandle>,
    pointer: Pointer,
  ) -> TaskHandle<Result<bool>> {
    self.check.execute((table, pointer))
  }

  pub fn start(
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    recorder: Arc<PageRecorder>,
    mapper: Arc<TableMapper>,
    config: GarbageCollectionConfig,
  ) -> Self {
    let queue = DoubleBuffer::new().to_arc();

    let entry = WorkBuilder::new()
      .name("gc found entry")
      .multi(config.thread_count)
      .shared(run_entry(
        block_cache.clone(),
        version_visibility.clone(),
        recorder.clone(),
        queue.clone(),
      ))
      .to_arc();
    let check = WorkBuilder::new()
      .name("gc check top entry")
      .multi(config.thread_count)
      .shared(run_check(block_cache.clone()))
      .to_arc();

    let table = WorkBuilder::new()
      .name("gc release tables")
      .single()
      .interval(
        RELEASE_CHECK_INTERVAL,
        run_release_table(mapper, version_visibility.clone()),
      )
      .to_box();

    Self {
      check,
      entry,
      queue,
      table,
      version_visibility,
    }
  }

  pub fn close(&self) {
    self.table.close();
    self.check.close();
    self.entry.close();
  }
}

const fn run_entry(
  block_cache: Arc<BlockCache>,
  version_visibility: Arc<VersionVisibility>,
  recorder: Arc<PageRecorder>,
  queue: Arc<DoubleBuffer<GcPointer>>,
) -> impl Fn((Arc<TableHandle>, Pointer)) -> Result {
  move |(table, pointer)| {
    if table.is_closed() {
      return Ok(());
    }
    let table = match table.try_pin() {
      Some(table) => table,
      None => {
        queue.push(GcPointer::Trim(table.clone(), pointer));
        return Ok(());
      }
    };

    let table_id = table.metadata().get_id();
    let mut ptr = Some(pointer);
    let mut max_found = false;

    let release = |record: VersionRecord| {
      if let RecordData::Chunked(pointers) = record.data {
        pointers.into_iter().for_each(|p| table.free().dealloc(p));
      }
    };

    while let Some(i) = ptr.take() {
      let mut slot = block_cache.peek(i, table.handle())?.for_write();
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
          new_versions.push_back(record);
          max_found = true;
        }
      }

      if new_versions.len() == prev_len {
        ptr = entry.get_next();
        continue;
      }

      if new_versions.len() > 0 {
        entry.set_versions(new_versions);
        recorder.serialize_and_log(RESERVED_TX, table_id, &mut slot, &entry)?;
        ptr = entry.get_next();
        continue;
      }

      let next = match entry.get_next() {
        Some(i) => i,
        None => {
          return recorder.serialize_and_log(RESERVED_TX, table_id, &mut slot, &entry)
        }
      };

      let next_entry: DataEntry = block_cache
        .peek(next, table.handle())?
        .for_read(&pin())
        .as_ref()
        .deserialize()?;
      recorder.serialize_and_log(RESERVED_TX, table_id, &mut slot, &next_entry)?;
      ptr = Some(i);

      table.free().dealloc(next);
    }
    Ok(())
  }
}

const fn run_check(
  block_cache: Arc<BlockCache>,
) -> impl Fn((Arc<TableHandle>, Pointer)) -> Result<bool> {
  move |(table, pointer)| {
    Ok(
      block_cache
        .peek(pointer, table)?
        .for_read(&pin())
        .as_ref()
        .deserialize::<DataEntry>()?
        .is_empty(),
    )
  }
}

const fn run_release_table(
  mapper: Arc<TableMapper>,
  version_visibility: Arc<VersionVisibility>,
) -> impl FnMut(Option<(Arc<TableHandle>, TxId, TxId)>) {
  let mut tables = Vec::new();
  let mut unpinned = Vec::new();
  let mut unreachable = Vec::new();
  move |recv| {
    if let Some((table, tx_id, version)) = recv {
      tables.push((table, tx_id, version));
    }

    let min_version = version_visibility.min_version();
    for (table, _, _) in tables.extract_if(.., |(_, tx_id, version)| {
      version_visibility.is_aborted(tx_id) || min_version >= *version
    }) {
      unpinned.push(table)
    }

    for table in unpinned.extract_if(.., |table| table.try_close()) {
      unreachable.push(table);
    }

    for table in unreachable.extract_if(.., |table| table.truncate().is_ok()) {
      mapper.remove(table.metadata().get_id());
    }
  }
}
