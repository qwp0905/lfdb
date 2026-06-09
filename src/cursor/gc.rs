use std::{
  collections::{LinkedList, VecDeque},
  mem::replace,
  sync::Arc,
  time::Duration,
};

use crossbeam::epoch;

use super::{
  BTreeNodeView, CompactionTriggered, DataEntry, DataEntryView, RecordData, TreeHeader,
  VersionRecord, HEADER_POINTER,
};
use crate::{
  background::{BackgroundThread, EventBus, TaskHandle, WorkBuilder},
  cache::{BlockCache, RefedSlot},
  disk::Pointer,
  error::Result,
  table::{TableHandleRef, TableMapper, META_TABLE_ID},
  transaction::{PageRecorder, VersionVisibility},
  utils::{ToArc, ToBox},
  wal::{TxId, RESERVED_TX},
};

pub struct GarbageCollectionConfig {
  pub interval: Duration,
  pub batch_size: usize,
  pub thread_count: usize,
  pub compact_threshold: f64,
  pub compact_min_size: Pointer,
}

const RELEASE_CHECK_INTERVAL: Duration = Duration::from_secs(1);

pub struct GarbageCollector {
  main: Box<dyn BackgroundThread<(), Result>>,
  entry: Arc<dyn BackgroundThread<(TableHandleRef, Pointer), Result<Pointer>>>,
  table: Arc<dyn BackgroundThread<DropTableCommitted>>,
}
impl GarbageCollector {
  pub fn new(
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    recorder: Arc<PageRecorder>,
    mapper: Arc<TableMapper>,
    event_bus: Arc<EventBus>,
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

    let table: Arc<dyn BackgroundThread<DropTableCommitted>> = WorkBuilder::new()
      .name("gc release tables")
      .single()
      .interval(
        RELEASE_CHECK_INTERVAL,
        run_release_table(mapper.clone(), version_visibility.clone()),
      )
      .to_arc();

    let main = WorkBuilder::new()
      .name("gc main")
      .single()
      .interval(
        config.interval,
        gc_main_loop(
          block_cache,
          mapper,
          version_visibility,
          entry.clone(),
          event_bus.clone(),
          config.batch_size,
          config.compact_threshold,
          config.compact_min_size,
        ),
      )
      .to_box();

    event_bus.register(&table);
    Self { main, entry, table }
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
  table: &TableHandleRef,
  pointer: Pointer,
) -> Result<TxId> {
  let table_id = table.get_id();
  let mut next = Some(pointer);
  let mut max_found = None;
  let min_version = version_visibility.min_version();

  let release = |record: VersionRecord| {
    let pointers = match record.data {
      RecordData::Chunked(pointers) => pointers,
      _ => return,
    };

    let handle = table.clone();
    defer(move || pointers.into_iter().for_each(|p| handle.free().dealloc(p)));
  };

  let serialize_and_log = |slot: &mut RefedSlot, entry: &DataEntry| {
    recorder.serialize_and_log(RESERVED_TX, table_id, slot, entry)
  };

  while let Some(ptr) = next.take() {
    if max_found.is_some() {
      let mut entry = block_cache
        .read(ptr, table)?
        .for_read()
        .as_ref()
        .deserialize::<DataEntry>()?;
      entry.take_versions().for_each(release);
      next = entry.get_next();
      let handle = table.clone();
      defer(move || handle.free().dealloc(ptr));
      continue;
    }

    {
      let mut found = false;
      let mut need_trim = false;
      let slot = block_cache.read(ptr, table)?.for_read();
      let entry = slot.as_ref().view::<DataEntryView>()?;
      next = entry.get_next();

      let mut iter = entry.get_versions();
      while let Some(record) = iter.try_next()? {
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

    block_cache.read(ptr, table)?.for_batch().mutate(|slot| {
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
        max_found = Some(record.version);
        new_versions.push_back(record);
      }

      if new_versions.len() == prev_len {
        return Ok(());
      }

      if new_versions.len() > 0 {
        entry.set_versions(new_versions);
        serialize_and_log(slot, &entry)?;
        return Ok(());
      }

      let next_ptr = match entry.get_next() {
        Some(ptr) => ptr,
        None => return serialize_and_log(slot, &entry),
      };

      let next_entry = block_cache
        .read(next_ptr, table)?
        .for_read()
        .as_ref()
        .deserialize::<DataEntry>()?;
      serialize_and_log(slot, &next_entry)?;
      next = Some(ptr);

      let handle = table.clone();
      defer(move || handle.free().dealloc(next_ptr));
      Ok(())
    })?;
  }

  Ok(max_found.unwrap_or(min_version))
}

const fn run_entry(
  block_cache: Arc<BlockCache>,
  version_visibility: Arc<VersionVisibility>,
  recorder: Arc<PageRecorder>,
) -> impl Fn((TableHandleRef, Pointer)) -> Result<Pointer> {
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

pub struct DropTableCommitted {
  handle: TableHandleRef,
  owner: TxId,
  commit_version: TxId,
}
impl DropTableCommitted {
  pub const fn new(handle: TableHandleRef, owner: TxId, commit_version: TxId) -> Self {
    Self {
      handle,
      owner,
      commit_version,
    }
  }
}

const fn run_release_table(
  mapper: Arc<TableMapper>,
  version_visibility: Arc<VersionVisibility>,
) -> impl FnMut(Option<DropTableCommitted>) {
  let mut tables = LinkedList::new();
  let mut unpinned = LinkedList::new();
  let mut unreachable = LinkedList::new();
  move |recv| {
    if let Some(committed) = recv {
      tables.push_back((committed.handle, committed.owner, committed.commit_version));
    }

    let min_version = version_visibility.min_version();
    for (table, _, _) in tables.extract_if(|(_, tx_id, version)| {
      version_visibility.is_aborted(tx_id) || min_version >= *version
    }) {
      unpinned.push_back(table)
    }

    for table in unpinned.extract_if(|table| table.try_close()) {
      unreachable.push_back(table);
    }

    for table in unreachable.extract_if(|table| table.truncate().is_ok()) {
      mapper.remove(table.get_id());
    }
  }
}

fn defer<F, R>(f: F)
where
  F: FnOnce() -> R + Send + 'static,
{
  epoch::pin().defer(f)
}

struct GCState {
  tasks: LinkedList<GCTask>,
  min_version: TxId,
}
impl GCState {
  const fn new() -> Self {
    Self {
      tasks: LinkedList::new(),
      min_version: 0,
    }
  }
}
struct GCTask {
  table: TableHandleRef,
  total: usize,
  dead: usize,
  leaf_ptr: Option<Pointer>,
}
impl GCTask {
  const fn new(table: TableHandleRef) -> Self {
    Self {
      table,
      total: 0,
      dead: 0,
      leaf_ptr: None,
    }
  }
}

fn gc_main_loop(
  block_cache: Arc<BlockCache>,
  tables: Arc<TableMapper>,
  version_visibility: Arc<VersionVisibility>,
  entry_worker: Arc<dyn BackgroundThread<(TableHandleRef, Pointer), Result<TxId>>>,
  event_bus: Arc<EventBus>,
  key_count: usize,
  compaction_threshold: f64,
  compaction_min_size: Pointer,
) -> impl FnMut(Option<()>) -> Result {
  let mut state = GCState::new();
  let mut buffered = VecDeque::new();
  let flush = |buffered: &mut VecDeque<TaskHandle<Result<TxId>>>| -> Result<TxId> {
    let mut min = TxId::MAX;
    while let Some(handle) = buffered.pop_front() {
      min = min.min(handle.wait().flatten()?);
    }
    Ok(min)
  };

  move |_| {
    for _ in 0..key_count {
      let mut task = match state.tasks.pop_front() {
        Some(task) => task,
        None => {
          version_visibility
            .remove_aborted(&state.min_version.min(flush(&mut buffered)?));
          state.min_version = version_visibility.min_version();
          for task in tables.get_all().into_iter().map(GCTask::new) {
            state.tasks.push_back(task);
          }
          return Ok(());
        }
      };

      let table = match task.table.try_pin() {
        Some(table) => table,
        None => continue,
      };

      let ptr = match task.leaf_ptr.take() {
        Some(ptr) => ptr,
        None => {
          let mut ptr = block_cache
            .read(HEADER_POINTER, table.handle())?
            .for_read()
            .as_ref()
            .deserialize::<TreeHeader>()?
            .get_root();

          loop {
            let slot = block_cache.read(ptr, table.handle())?.for_read();
            match slot.as_ref().view::<BTreeNodeView>()? {
              BTreeNodeView::Internal(node) => ptr = node.first_child()?,
              BTreeNodeView::Leaf(_) => break,
            }
          }

          task.leaf_ptr = Some(ptr);
          state.tasks.push_back(task);
          continue;
        }
      };

      let slot = block_cache.read(ptr, table.handle())?.for_read();
      let node = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;
      let mut iter = node.get_entries();
      while let Some((_, _, record, p)) = iter.try_next()? {
        state.min_version = state.min_version.min(record.version);
        buffered.push_back(entry_worker.execute((table.handle().clone(), p)));
        task.total += 1;
        if record.data.is_tombstone() || version_visibility.is_aborted(&record.owner) {
          task.dead += 1;
        }
      }

      if let Some(i) = node.get_next() {
        task.leaf_ptr = Some(i);
        state.tasks.push_back(task);
        continue;
      }

      if table.get_id() == META_TABLE_ID {
        continue;
      }
      if table.free().file_len() <= compaction_min_size {
        continue;
      }
      if task.dead as f64 / task.total as f64 <= compaction_threshold {
        continue;
      }

      event_bus.publish(CompactionTriggered::new(table.into_inner()));
    }

    state.min_version = state.min_version.min(flush(&mut buffered)?);
    Ok(())
  }
}
