use std::{
  collections::{BTreeMap, VecDeque},
  mem::{replace, take},
  sync::Arc,
  time::Duration,
};

use crossbeam::{epoch, queue::SegQueue};

use super::{DataEntry, DataEntryView, RecordData, VersionRecord};
use crate::{
  cache::{BlockCache, RefedSlot},
  debug,
  disk::Pointer,
  error::Result,
  table::{PinnedHandle, TableHandleRef, TableId, TableMapper},
  thread::{BackgroundThread, WorkBuilder},
  transaction::{PageRecorder, VersionVisibility},
  utils::{ToArc, ToBox},
  wal::{TxId, RESERVED_TX},
};

pub struct GarbageCollectionConfig {
  pub thread_count: usize,
}

const RELEASE_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const GC_CHECK_INTERVAL: Duration = Duration::from_secs(3);

pub type GCQueue = Arc<SegQueue<GCMark>>;
pub struct GarbageCollector {
  queue: GCQueue,
  main: Box<dyn BackgroundThread<(), Result>>,
  entry: Arc<dyn BackgroundThread<(PinnedHandle, Pointer), Result>>,
  table: Box<dyn BackgroundThread<(TableHandleRef, TxId, TxId)>>,
}
impl GarbageCollector {
  pub fn mark(&self, mark: GCMark) {
    self.queue.push(mark);
  }
  pub fn release_table(&self, table: TableHandleRef, tx_id: TxId, version: TxId) {
    self.table.dispatch((table, tx_id, version));
  }

  pub fn from_queue(
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    recorder: Arc<PageRecorder>,
    mapper: Arc<TableMapper>,
    queue: Arc<SegQueue<GCMark>>,
    config: GarbageCollectionConfig,
  ) -> Self {
    let entry = WorkBuilder::new()
      .name("gc found entry")
      .multi(config.thread_count)
      .shared(run_entry(
        block_cache.clone(),
        version_visibility.clone(),
        recorder.clone(),
      ))
      .to_arc();

    let table = WorkBuilder::new()
      .name("gc release tables")
      .single()
      .interval(
        RELEASE_CHECK_INTERVAL,
        run_release_table(mapper.clone(), version_visibility.clone()),
      )
      .to_box();

    let main = WorkBuilder::new()
      .name("gc main")
      .single()
      .interval(
        GC_CHECK_INTERVAL,
        wait_gc(queue.clone(), version_visibility.clone(), entry.clone()),
      )
      .to_box();

    Self {
      queue,
      main,
      entry,
      table,
    }
  }

  pub fn new(
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    recorder: Arc<PageRecorder>,
    mapper: Arc<TableMapper>,
    config: GarbageCollectionConfig,
  ) -> Self {
    Self::from_queue(
      block_cache,
      version_visibility,
      recorder,
      mapper,
      SegQueue::new().to_arc(),
      config,
    )
  }

  pub fn close(&self) {
    self.main.close();
    self.table.close();
    self.entry.close();
  }
}

fn release_entry(
  block_cache: &BlockCache,
  version_visibility: &VersionVisibility,
  recorder: &PageRecorder,
  table: &PinnedHandle,
  pointer: Pointer,
) -> Result {
  let table_id = table.metadata().get_id();
  let mut next = Some(pointer);
  let mut max_found = false;
  let min_version = version_visibility.min_version();

  let release = |record: VersionRecord| {
    let pointers = match record.data {
      RecordData::Chunked(pointers) => pointers,
      _ => return,
    };

    let handle = table.handle();
    defer(move || pointers.into_iter().for_each(|p| handle.free().dealloc(p)));
  };

  let serialize_and_log = |slot: &mut RefedSlot, entry: &DataEntry| {
    recorder.serialize_and_log(RESERVED_TX, table_id, slot, entry)
  };

  let mut is_empty = false;
  let mut needs_inc = false;

  while let Some(ptr) = next.take() {
    if max_found {
      let mut entry = block_cache
        .read(ptr, table.handle())?
        .for_read()
        .as_ref()
        .deserialize::<DataEntry>()?;
      entry.take_versions().for_each(release);
      next = entry.get_next();
      let handle = table.handle();
      defer(move || handle.free().dealloc(ptr));
      continue;
    }

    {
      let mut found = false;
      let mut need_trim = false;
      let entry = block_cache
        .read(ptr, table.handle())?
        .for_read()
        .as_ref()
        .deserialize::<DataEntryView>()?;
      next = entry.get_next();

      if ptr == pointer {
        is_empty = entry.is_empty();
      }

      for record in entry.get_versions() {
        if version_visibility.is_aborted(&record.owner) {
          need_trim = true;
          break;
        }
        if record.version >= min_version {
          continue;
        }
        if !found {
          found = true;
          continue;
        }
        need_trim = true;
        break;
      }
      if !need_trim {
        continue;
      }
    }

    block_cache
      .read(ptr, table.handle())?
      .for_batch()
      .mutate(|slot| {
        let mut entry: DataEntry = slot.as_ref().deserialize()?;

        let prev_len = entry.len();
        let mut expired_max: Option<VersionRecord> = None;
        let mut new_versions = VecDeque::new();

        for record in entry.take_versions() {
          if version_visibility.is_aborted(&record.owner) {
            release(record);
            continue;
          }
          if record.version >= min_version {
            new_versions.push_back(record);
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

        if let Some(record) = expired_max.take() {
          new_versions.push_back(record);
          max_found = true;
        }

        if new_versions.len() == prev_len {
          return Ok(());
        }

        entry.set_versions(new_versions);
        if pointer == ptr {
          needs_inc = entry.is_empty();
        }
        if entry.len() > 0 {
          serialize_and_log(slot, &entry)?;
          return Ok(());
        }

        let next_ptr = match entry.get_next() {
          Some(ptr) => ptr,
          None => return serialize_and_log(slot, &entry),
        };

        let next_entry = block_cache
          .read(next_ptr, table.handle())?
          .for_read()
          .as_ref()
          .deserialize::<DataEntry>()?;
        serialize_and_log(slot, &next_entry)?;
        next = Some(ptr);

        let handle = table.handle();
        defer(move || handle.free().dealloc(next_ptr));
        Ok(())
      })?;
  }

  if !is_empty && needs_inc {
    table.inc_dead();
  }
  Ok(())
}

const fn run_entry(
  block_cache: Arc<BlockCache>,
  version_visibility: Arc<VersionVisibility>,
  recorder: Arc<PageRecorder>,
) -> impl Fn((PinnedHandle, Pointer)) -> Result {
  move |(table, pointer)| {
    release_entry(
      &block_cache,
      &version_visibility,
      &recorder,
      &table,
      pointer,
    )
  }
}

const fn run_release_table(
  mapper: Arc<TableMapper>,
  version_visibility: Arc<VersionVisibility>,
) -> impl FnMut(Option<(TableHandleRef, TxId, TxId)>) {
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

fn defer<F, R>(f: F)
where
  F: FnOnce() -> R + Send + 'static,
{
  epoch::pin().defer(f)
}

pub struct GCMark {
  pointer: Pointer,
  table: TableHandleRef,
  owner: TxId,
}
impl GCMark {
  pub const fn new(pointer: Pointer, table: TableHandleRef, owner: TxId) -> Self {
    Self {
      pointer,
      table,
      owner,
    }
  }
}

const fn wait_gc(
  queue: GCQueue,
  version_visibility: Arc<VersionVisibility>,
  entry: Arc<dyn BackgroundThread<(PinnedHandle, Pointer), Result>>,
) -> impl FnMut(Option<()>) -> Result {
  let mut not_committed = BTreeMap::<TxId, Vec<_>>::new();
  let mut triggered = Vec::new();
  let mut gc_ready = BTreeMap::<(TableId, Pointer), GCMark>::new();
  move |_| {
    while let Some(mark) = queue.pop() {
      not_committed.entry(mark.owner).or_default().push(mark);
    }

    let min_version = version_visibility.min_version();
    let mut available_version = min_version;
    let current = version_visibility.current_version();
    let splitted = not_committed.split_off(&min_version);
    for mark in replace(&mut not_committed, splitted)
      .into_values()
      .flatten()
    {
      triggered.push((mark, current));
    }

    for (mark, _) in triggered.extract_if(.., |(mark, version)| {
      if min_version >= *version || version_visibility.is_aborted(&mark.owner) {
        return true;
      }

      // keep owner which remaining in triggered
      available_version = available_version.min(mark.owner);
      false
    }) {
      let key = (mark.table.metadata().get_id(), mark.pointer);
      match gc_ready.get_mut(&key) {
        Some(m) if m.owner > mark.owner => *m = mark,
        None => drop(gc_ready.insert(key, mark)),
        _ => continue,
      }
    }

    let mut waiting = Vec::new();
    for mark in take(&mut gc_ready).into_values() {
      if mark.table.is_closed() {
        continue;
      }
      if let Some(table) = mark.table.try_pin() {
        waiting.push(entry.execute((table, mark.pointer)));
        continue;
      }

      // keep owner which remaining in gc_ready
      available_version = available_version.min(mark.owner);
      gc_ready.insert((mark.table.metadata().get_id(), mark.pointer), mark);
    }

    debug!("gc {} entries enqueued.", waiting.len());
    waiting
      .into_iter()
      .map(|done| done.wait().flatten())
      .collect::<Result>()?;
    version_visibility.remove_aborted(&available_version);
    Ok(())
  }
}
