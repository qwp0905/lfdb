use std::{collections::VecDeque, mem::replace, sync::Arc, time::Duration};

use crossbeam::epoch;

use super::{
  BTreeNodeView, Compactor, DataEntry, DataEntryView, RecordData, TreeHeader,
  VersionRecord, HEADER_POINTER,
};
use crate::{
  cache::{BlockCache, WritableSlot},
  debug,
  disk::Pointer,
  error::Result,
  table::{MutationHandle, TableHandle, TableMapper, META_TABLE_ID},
  thread::{BackgroundThread, WorkBuilder},
  transaction::{PageRecorder, VersionVisibility},
  utils::{ToArc, ToBox},
  wal::{TxId, RESERVED_TX, WAL},
};

pub struct GarbageCollectionConfig {
  pub thread_count: usize,
  pub interval: Duration,
  pub compaction_threshold: f64,
  pub compaction_min_size: Pointer,
}

const RELEASE_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/**
 * Runs at each checkpoint to clean expired and aborted versions from DataEntry
 * pages, then confirms min_active in the checkpoint log. DataEntry pages are
 * marked for GC (Trim) when a cursor performs an update or remove, so the same
 * pointer can appear multiple times if the same key is written repeatedly
 * between two GC runs. check and entry are separate workers: check answers
 * emptiness queries, entry does version reclamation.
 */
pub struct GarbageCollector {
  main: Box<dyn BackgroundThread<(), Result>>,
  check: Arc<dyn BackgroundThread<(Arc<TableHandle>, Pointer), Result<bool>>>,
  entry: Arc<dyn BackgroundThread<(Arc<TableHandle>, Pointer), Result>>,
  compactor: Arc<Compactor>,
  table: Arc<dyn BackgroundThread<(Arc<TableHandle>, TxId, TxId)>>,
}
impl GarbageCollector {
  pub fn release_table(&self, table: Arc<TableHandle>, tx_id: TxId, version: TxId) {
    self.table.dispatch((table, tx_id, version));
  }

  pub fn compact(&self, old: Arc<TableHandle>, new: MutationHandle, version: TxId) {
    self.compactor.register_wait(old, new, version);
  }
  pub fn resume_compact(&self, old: MutationHandle, new: MutationHandle) {
    self.compactor.resume(old, new);
  }
  pub fn start(
    block_cache: Arc<BlockCache>,
    version_visibility: Arc<VersionVisibility>,
    recorder: Arc<PageRecorder>,
    mapper: Arc<TableMapper>,
    wal: Arc<WAL>,
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
        run_release_table(mapper.clone(), version_visibility.clone()),
      )
      .to_arc();
    let compactor = Compactor::new(
      block_cache.clone(),
      mapper.clone(),
      recorder.clone(),
      version_visibility.clone(),
      wal.clone(),
      table.clone(),
    )
    .to_arc();

    let main = WorkBuilder::new()
      .name("gc main")
      .single()
      .interval(
        config.interval,
        run_main(
          version_visibility,
          block_cache,
          mapper,
          entry.clone(),
          check.clone(),
          config.compaction_threshold,
          config.compaction_min_size,
          compactor.clone(),
        ),
      )
      .to_box();

    Self {
      main,
      check,
      entry,
      table,
      compactor,
    }
  }

  pub fn close(&self) {
    self.main.close();
    self.compactor.close();
    self.table.close();
    self.check.close();
    self.entry.close();
  }
}

const fn run_entry(
  block_cache: Arc<BlockCache>,
  version_visibility: Arc<VersionVisibility>,
  recorder: Arc<PageRecorder>,
) -> impl Fn((Arc<TableHandle>, Pointer)) -> Result {
  move |(table, pointer)| {
    let table_id = table.metadata().get_id();
    let mut next = Some(pointer);
    let mut max_found = false;

    let release = |record: VersionRecord| {
      let pointers = match record.data {
        RecordData::Chunked(pointers) => pointers,
        _ => return,
      };

      let handle = table.clone();
      defer(move || pointers.into_iter().for_each(|p| handle.free().dealloc(p)));
    };

    let serialize_and_log = |slot: &mut WritableSlot, entry: &DataEntry| {
      recorder.serialize_and_log(RESERVED_TX, table_id, slot, entry)
    };

    while let Some(ptr) = next.take() {
      if max_found {
        let mut entry = block_cache
          .read(ptr, table.clone())?
          .for_read()
          .as_ref()
          .deserialize::<DataEntry>()?;
        entry.take_versions().for_each(release);
        next = entry.get_next();
        let handle = table.clone();
        defer(move || handle.free().dealloc(ptr));
        continue;
      }

      let min_version = version_visibility.min_version();
      {
        let mut found = false;
        let mut need_trim = false;
        for record in block_cache
          .read(ptr, table.clone())?
          .for_read()
          .as_ref()
          .deserialize::<DataEntryView>()?
          .get_versions()
        {
          if version_visibility.is_aborted(&record.owner) {
            need_trim = true;
            break;
          }
          if record.version > min_version {
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

      let mut slot = block_cache.read(ptr, table.clone())?.for_lazy_write();
      let mut entry: DataEntry = slot.as_ref().deserialize()?;

      let prev_len = entry.len();
      let mut expired_max: Option<VersionRecord> = None;
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
        next = entry.get_next();
        continue;
      }

      if new_versions.len() > 0 {
        entry.set_versions(new_versions);
        serialize_and_log(&mut slot, &entry)?;
        next = entry.get_next();
        continue;
      }

      let next_ptr = match entry.get_next() {
        Some(ptr) => ptr,
        None => return serialize_and_log(&mut slot, &entry),
      };

      let next_entry = block_cache
        .read(next_ptr, table.clone())?
        .for_read()
        .as_ref()
        .deserialize::<DataEntry>()?;
      serialize_and_log(&mut slot, &next_entry)?;
      next = Some(ptr);

      let handle = table.clone();
      defer(move || handle.free().dealloc(next_ptr))
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
        .read(pointer, table)?
        .for_read()
        .as_ref()
        .deserialize::<DataEntryView>()?
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

fn defer<F, R>(f: F)
where
  F: FnOnce() -> R + Send + 'static,
{
  epoch::pin().defer(f)
}

const fn run_main(
  version_visibility: Arc<VersionVisibility>,
  block_cache: Arc<BlockCache>,
  tables: Arc<TableMapper>,
  entry: Arc<dyn BackgroundThread<(Arc<TableHandle>, Pointer), Result>>,
  check: Arc<dyn BackgroundThread<(Arc<TableHandle>, Pointer), Result<bool>>>,
  compaction_threshold: f64,
  compaction_min_size: Pointer,
  compactor: Arc<Compactor>,
) -> impl Fn(Option<()>) -> Result {
  move |_| {
    let min_version = version_visibility.min_version();
    for table in tables.get_all() {
      let table = match table.try_pin() {
        Some(table) => table,
        None => continue,
      };

      let name = table.metadata().get_name();
      debug!("gc table {name} start.");

      let mut total = 0;
      let mut dead = 0;

      let mut ptr = block_cache
        .read(HEADER_POINTER, table.handle())?
        .for_read()
        .as_ref()
        .deserialize::<TreeHeader>()?
        .get_root();

      while let BTreeNodeView::Internal(node) = block_cache
        .read(ptr, table.handle())?
        .for_read()
        .as_ref()
        .view::<BTreeNodeView>()?
      {
        ptr = node.first_child()
      }

      let mut next_ptr = Some(ptr);
      while let Some(ptr) = next_ptr.take() {
        let slot = block_cache.read(ptr, table.handle())?.for_read();
        let leaf = slot.as_ref().view::<BTreeNodeView>()?.as_leaf()?;
        next_ptr = leaf.get_next();
        total += leaf.len();

        let mut waiting = Vec::with_capacity(leaf.len());
        for i in leaf.get_entry_pointers() {
          waiting.push(entry.execute((table.handle(), i)))
        }

        waiting
          .into_iter()
          .map(|v| v.wait().flatten())
          .collect::<Result>()?;

        let mut waiting = Vec::with_capacity(leaf.len());
        for i in leaf.get_entry_pointers() {
          waiting.push(check.execute((table.handle(), i)));
        }

        for v in waiting {
          if v.wait().flatten()? {
            dead += 1;
          }
        }
      }
      debug!("gc table {name} unreachable versions all collected.");
      if table.free().file_len() < compaction_min_size {
        continue;
      }
      let ratio = dead as f64 / total as f64;
      if ratio <= compaction_threshold {
        continue;
      }
      if table.metadata().get_id() == META_TABLE_ID {
        continue;
      }
      debug!("clean leaf table {name} collect end. dead ratio {ratio}");
      compactor.register_new(table.handle());
    }

    version_visibility.remove_aborted(&min_version);
    Ok(())
  }
}
