use std::{collections::VecDeque, mem::replace, sync::Arc, time::Duration};

use super::{
  CursorNode, CursorNodeView, DataChunk, DataEntry, GarbageCollector, InternalNode, Key,
  KeyRef, NodeFindResult, RecordData, TreeHeader, VersionRecord, CHUNK_SIZE,
  HEADER_POINTER, LARGE_VALUE,
};
use crate::{
  buffer_pool::{BufferPool, ReadonlySlot, WritableSlot},
  disk::Pointer,
  serialize::Serializable,
  table::{MutationHandle, TableHandle, TableMapper, TableMetadata},
  transaction::{PageRecorder, TxSnapshot, TxState, VersionVisibility},
  utils::LogFilter,
  wal::{TxId, RESERVED_TX, WAL},
  Error, Result,
};

struct KVSnapshot {
  key: Key,
  value: Vec<u8>,
  owner: TxId,
  version: TxId,
}

struct Compactor<'a> {
  old: Arc<TableHandle>,
  new: Arc<TableHandle>,
  buffer_pool: &'a BufferPool,
  recorder: &'a PageRecorder,
  versions: &'a VersionVisibility,
  remaining_keys: VecDeque<(Key, Pointer)>,
  next_leaf: Option<Pointer>,
  moved_count: usize,
}
impl<'a> Compactor<'a> {
  fn boot(
    old: Arc<TableHandle>,
    new: Arc<TableHandle>,
    buffer_pool: &'a BufferPool,
    recorder: &'a PageRecorder,
    versions: &'a VersionVisibility,
  ) -> Result<Self> {
    let mut ptr = buffer_pool
      .peek(HEADER_POINTER, old.clone())?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    loop {
      let slot = buffer_pool.peek(ptr, old.clone())?.for_read();
      match slot.as_ref().view::<CursorNodeView>()? {
        CursorNodeView::Internal(node) => ptr = node.first_child(),
        CursorNodeView::Leaf(node) => {
          return Ok(Self {
            old,
            new,
            buffer_pool,
            recorder,
            versions,
            remaining_keys: node.get_entries().map(|(k, p)| (k.to_vec(), p)).collect(),
            next_leaf: node.get_next(),
            moved_count: 0,
          });
        }
      }
    }
  }

  pub fn next_kv(&mut self) -> Result<Option<KVSnapshot>> {
    loop {
      while let Some((key, ptr)) = self.remaining_keys.pop_front() {
        if let Some((value, owner, version)) = self.find_old_value(ptr)? {
          return Ok(Some(KVSnapshot {
            key,
            value,
            owner,
            version,
          }));
        }
      }

      let ptr = match self.next_leaf.take() {
        Some(i) => i,
        None => return Ok(None),
      };

      let slot = self.buffer_pool.peek(ptr, self.old.clone())?.for_read();
      let node = slot.as_ref().view::<CursorNodeView>()?.as_leaf()?;

      for (key, ptr) in node.get_entries() {
        self.remaining_keys.push_back((key.to_vec(), ptr));
      }
      self.next_leaf = node.get_next()
    }
  }

  fn find_old_value(&self, ptr: Pointer) -> Result<Option<(Vec<u8>, TxId, TxId)>> {
    let mut ptr = Some(ptr);
    while let Some(i) = ptr.take() {
      let slot = self.buffer_pool.peek(i, self.old.clone())?.for_read();
      let entry = slot.as_ref().deserialize::<DataEntry>()?;

      for record in entry.get_versions() {
        debug_assert!(self.versions.get_active_state(record.owner).is_none());

        if self.versions.is_aborted(&record.owner) {
          continue;
        }

        return match record.data.read_data(|i| {
          self
            .buffer_pool
            .peek(i, self.old.clone())?
            .for_read()
            .as_ref()
            .deserialize()
        })? {
          Some(v) => Ok(Some((v, record.owner, record.version))),
          None => Ok(None),
        };
      }

      ptr = entry.get_next();
    }

    Ok(None)
  }

  #[inline]
  fn alloc_and_log<T: Serializable>(&self, data: &T) -> Result<Pointer> {
    let ptr = self.new.free().alloc();
    let mut slot = self.buffer_pool.peek(ptr, self.new.clone())?.for_write();
    self.serialize_and_log(&mut slot, data)?;
    Ok(ptr)
  }
  #[inline]
  fn serialize_and_log<T: Serializable>(
    &self,
    slot: &mut WritableSlot<'_>,
    data: &T,
  ) -> Result {
    self.recorder.serialize_and_log(
      RESERVED_TX,
      self.new.metadata().get_id(),
      slot,
      data,
    )?;
    Ok(())
  }

  fn create_record(&self, data: Vec<u8>) -> Result<RecordData> {
    create_record(&self.new, &self.buffer_pool, &self.recorder, data)
  }

  fn apply_snapshot(&mut self, snapshot: KVSnapshot) -> Result {
    self.moved_count += 1;
    let record = VersionRecord::new(
      snapshot.owner,
      snapshot.version,
      self.create_record(snapshot.value)?,
    );

    let (mut ptr, mut old_height) = {
      let header = self
        .buffer_pool
        .peek(HEADER_POINTER, self.new.clone())?
        .for_read()
        .as_ref()
        .deserialize::<TreeHeader>()?;
      (header.get_root(), header.get_height())
    };
    let mut stack = vec![];

    while let CursorNodeView::Internal(node) = self
      .buffer_pool
      .peek(ptr, self.new.clone())?
      .for_read()
      .as_ref()
      .view::<CursorNodeView>()?
    {
      match node.find(&snapshot.key) {
        Ok(i) => stack.push(replace(&mut ptr, i)),
        Err(i) => ptr = i,
      }
    }

    let (mid_key, right_ptr) = loop {
      let mut slot = self.buffer_pool.peek(ptr, self.new.clone())?.for_write();

      let node = slot.as_ref().view::<CursorNodeView>()?.as_leaf()?;
      match node.find(&snapshot.key) {
        NodeFindResult::Found(_, i) => return self.insert_at(i, record, slot),
        NodeFindResult::Move(i) => ptr = i,
        NodeFindResult::NotFound(i) => {
          let mut node = node.writable();
          let entry_ptr = self.alloc_and_log(&DataEntry::init(record))?;

          let split = match node.insert_and_split(i, snapshot.key.clone(), entry_ptr) {
            Some(split) => split,
            None => return self.serialize_and_log(&mut slot, &node.to_node()),
          };

          let mid_key = split.top().clone();
          let split_ptr = self.alloc_and_log(&split.to_node())?;

          node.set_next(split_ptr);
          self.serialize_and_log(&mut slot, &node.to_node())?;

          break (mid_key, split_ptr);
        }
      }
    };

    let mut split_key = mid_key;
    let mut split_pointer = right_ptr;
    while let Some(index) = stack.pop() {
      match self.apply_split(split_key, split_pointer, index)? {
        Some((k, p)) => {
          split_key = k;
          split_pointer = p;
        }
        None => return Ok(()),
      };
    }

    loop {
      let mut header_slot = self
        .buffer_pool
        .peek(HEADER_POINTER, self.new.clone())?
        .for_write();
      let mut header: TreeHeader = header_slot.as_ref().deserialize()?;
      let current_height = header.get_height();
      let mut ptr = header.get_root();
      if old_height == current_height {
        let new_root = InternalNode::initialize(split_key, ptr, split_pointer);
        let new_root_index = self.alloc_and_log(&new_root.to_node())?;

        header.set_root(new_root_index);
        header.increase_height();
        return self.serialize_and_log(&mut header_slot, &header);
      }

      let diff = (current_height - old_height) as usize;
      old_height = current_height;
      drop(header_slot);
      let mut stack = vec![];

      while stack.len() < diff {
        let slot = self.buffer_pool.peek(ptr, self.new.clone())?.for_read();
        let node = slot.as_ref().view::<CursorNodeView>()?.as_internal()?;
        match node.find(&split_key) {
          Ok(i) => stack.push(replace(&mut ptr, i)),
          Err(i) => ptr = i,
        }
      }

      while let Some(index) = stack.pop() {
        match self.apply_split(split_key, split_pointer, index)? {
          Some((k, p)) => {
            split_key = k;
            split_pointer = p;
          }
          None => return Ok(()),
        };
      }
    }
  }

  fn insert_at<T>(
    &self,
    entry_ptr: Pointer,
    record: VersionRecord,
    coupling: T,
  ) -> Result {
    let mut slot = self
      .buffer_pool
      .peek(entry_ptr, self.new.clone())?
      .for_write();
    drop(coupling);

    let mut entry: DataEntry = slot.as_ref().deserialize()?;

    let mut new_versions = entry.take_versions().chain([record]).collect::<Vec<_>>();
    new_versions.sort_by(|a, b| b.version.cmp(&a.version));

    loop {
      while let Some(r) = new_versions.pop_if(|r| entry.is_available(r)) {
        entry.append(r);
      }

      if new_versions.is_empty() {
        self.serialize_and_log(&mut slot, &entry)?;
        return Ok(());
      }

      let new_ptr = self.alloc_and_log(&entry)?;
      entry = DataEntry::empty();
      entry.set_next(new_ptr);
    }
  }

  fn apply_split(
    &self,
    evicted_key: Key,
    evicted_ptr: Pointer,
    current: Pointer,
  ) -> Result<Option<(Key, Pointer)>> {
    let mut ptr = current;

    let (mut slot, mut internal) = loop {
      let slot = self.buffer_pool.peek(ptr, self.new.clone())?.for_write();
      let mut internal = slot.as_ref().deserialize::<CursorNode>()?.as_internal()?;
      match internal.insert_or_next(&evicted_key, evicted_ptr) {
        Ok(_) => break (slot, internal),
        Err(i) => ptr = i,
      }
    };

    let (split_node, split_key) = match internal.split_if_needed() {
      Some(split) => split,
      None => {
        return self
          .serialize_and_log(&mut slot, &internal.to_node())
          .map(|_| None)
      }
    };

    let split_index = self.alloc_and_log(&split_node.to_node())?;

    internal.set_right(&split_key, split_index);
    self.serialize_and_log(&mut slot, &internal.to_node())?;

    Ok(Some((split_key, split_index)))
  }
}

fn get_entry<'a>(
  buffer_pool: &'a BufferPool,
  table: &Arc<TableHandle>,
  key: KeyRef,
) -> Result<Option<(ReadonlySlot<'a>, Pointer)>> {
  let mut ptr = buffer_pool
    .peek(HEADER_POINTER, table.clone())?
    .for_read()
    .as_ref()
    .deserialize::<TreeHeader>()?
    .get_root();

  loop {
    let mut slot = buffer_pool.peek(ptr, table.clone())?.for_read();
    match slot.as_ref().view::<CursorNodeView>()? {
      CursorNodeView::Internal(node) => ptr = node.find(key).unwrap_or_else(|i| i),
      CursorNodeView::Leaf(mut node) => loop {
        match node.find(key) {
          NodeFindResult::Found(_, i) => return Ok(Some((slot, i))),
          NodeFindResult::Move(i) => {
            drop(slot);
            slot = buffer_pool.peek(i, table.clone())?.for_read();
            node = slot.as_ref().view::<CursorNodeView>()?.as_leaf()?;
          }
          NodeFindResult::NotFound(_) => return Ok(None),
        }
      },
    }
  }
}

fn get_data(
  buffer_pool: &BufferPool,
  table: &Arc<TableHandle>,
  state: &TxState,
  snapshot: &TxSnapshot,
  key: KeyRef,
) -> Result<Option<Vec<u8>>> {
  let (mut slot, ptr) = match get_entry(buffer_pool, table, key)? {
    Some(v) => v,
    None => return Ok(None),
  };

  let _ = replace(&mut slot, buffer_pool.peek(ptr, table.clone())?.for_read());

  loop {
    let entry: DataEntry = slot.as_ref().deserialize()?;
    if let Some(record) = entry.find_record(state.get_id(), |i| snapshot.is_visible(i)) {
      return record.read_data(|i| {
        buffer_pool
          .peek(i, table.clone())?
          .for_read()
          .as_ref()
          .deserialize()
      });
    }

    match entry.get_next() {
      Some(i) => drop(replace(
        &mut slot,
        buffer_pool.peek(i, table.clone())?.for_read(),
      )),
      None => return Ok(None),
    }
  }
}

fn alloc_and_log<T: Serializable>(
  table: &Arc<TableHandle>,
  buffer_pool: &BufferPool,
  recorder: &PageRecorder,
  data: &T,
) -> Result<Pointer> {
  let ptr = table.free().alloc();
  let mut slot = buffer_pool.peek(ptr, table.clone())?.for_write();
  recorder.serialize_and_log(RESERVED_TX, table.metadata().get_id(), &mut slot, data)?;
  Ok(ptr)
}

fn create_record(
  table: &Arc<TableHandle>,
  buffer_pool: &BufferPool,
  recorder: &PageRecorder,
  mut data: Vec<u8>,
) -> Result<RecordData> {
  if data.len() < LARGE_VALUE {
    return Ok(RecordData::Data(data));
  }

  let mut pointers = Vec::with_capacity(data.len().div_ceil(CHUNK_SIZE));
  while data.len() > CHUNK_SIZE {
    let remain = data.split_off(CHUNK_SIZE);
    let chunk = DataChunk::new(data);
    pointers.push(alloc_and_log(table, buffer_pool, recorder, &chunk)?);
    data = remain;
  }
  pointers.push(alloc_and_log(
    table,
    buffer_pool,
    recorder,
    &DataChunk::new(data),
  )?);

  Ok(RecordData::Chunked(pointers))
}

fn update_data(
  buffer_pool: &BufferPool,
  table: &Arc<TableHandle>,
  state: &TxState,
  snapshot: &TxSnapshot,
  recorder: &PageRecorder,
  versions: &VersionVisibility,
  key: KeyRef,
  value: Vec<u8>,
) -> Result {
  let tx_id = state.get_id();
  let data = create_record(table, buffer_pool, recorder, value)?;

  let (leaf, ptr) = match get_entry(buffer_pool, table, key)? {
    Some(v) => v,
    None => return Ok(()),
  };

  let mut slot = buffer_pool.peek(ptr, table.clone())?.for_write();
  drop(leaf);

  let mut entry: DataEntry = slot.as_ref().deserialize()?;
  if let Some(owner) = entry.get_last_owner() {
    if owner != tx_id && snapshot.is_active(&owner) {
      return Err(Error::WriteConflict);
    }
  }

  let record = VersionRecord::new(tx_id, versions.current_version(), data);
  if entry.is_available(&record) {
    entry.append(record);
    return recorder.serialize_and_log(
      tx_id,
      table.metadata().get_id(),
      &mut slot,
      &entry,
    );
  }

  let new_entry_ptr = alloc_and_log(table, buffer_pool, recorder, &entry)?;

  let mut new_entry = DataEntry::init(record);
  new_entry.set_next(new_entry_ptr);

  recorder.serialize_and_log(tx_id, table.metadata().get_id(), &mut slot, &new_entry)?;

  Ok(())
}

fn initialize(
  table: &Arc<TableHandle>,
  buffer_pool: &BufferPool,
  recorder: &PageRecorder,
) -> Result {
  let root = alloc_and_log(table, buffer_pool, recorder, &CursorNode::initial_state())?;

  let mut header = buffer_pool.peek(HEADER_POINTER, table.clone())?.for_write();
  recorder.serialize_and_log(
    RESERVED_TX,
    table.metadata().get_id(),
    &mut header,
    &TreeHeader::new(root),
  )?;
  Ok(())
}

pub enum CompactTask {
  Resume(Arc<TableHandle>, MutationHandle),
  New(Arc<TableHandle>),
}
impl CompactTask {
  fn name(&self) -> String {
    match self {
      CompactTask::Resume(table, _) => table,
      CompactTask::New(table) => table,
    }
    .metadata()
    .get_name()
    .to_string()
  }
}

struct MiniTx<'a> {
  state: TxState<'a>,
  snapshot: TxSnapshot<'a>,
  version_visibility: &'a VersionVisibility,
  wal: &'a WAL,
  committed: bool,
}
impl<'a> MiniTx<'a> {
  fn new(version_visibility: &'a VersionVisibility, wal: &'a WAL) -> Self {
    let (state, snapshot) = version_visibility.new_transaction();
    Self {
      state,
      snapshot,
      version_visibility,
      wal,
      committed: false,
    }
  }

  fn abort(&mut self) -> Result {
    if self.committed {
      return Ok(());
    }
    self.version_visibility.set_abort(self.state.get_id());
    self.wal.append_abort(self.state.get_id())?;
    self.state.deactive();
    Ok(())
  }

  fn commit(&mut self) -> Result {
    if self.committed {
      return Ok(());
    }
    self.wal.commit_and_flush(self.state.get_id())?;
    self.committed = true;
    self.state.deactive();
    Ok(())
  }
}
impl<'a> Drop for MiniTx<'a> {
  fn drop(&mut self) {
    let _ = self.abort();
  }
}

pub fn handle_compaction(
  tables: Arc<TableMapper>,
  buffer_pool: Arc<BufferPool>,
  versions: Arc<VersionVisibility>,
  wal: Arc<WAL>,
  recorder: Arc<PageRecorder>,
  gc: Arc<GarbageCollector>,
  logger: LogFilter,
) -> impl Fn(CompactTask) -> Result {
  move |task| {
    let table_name = task.name();
    logger.info(|| format!("table {table_name} compacting triggered."));
    let (old_table, new) = match task {
      CompactTask::Resume(old, new) => (old, new),
      CompactTask::New(old) => {
        let (new_table, wait_until) = {
          let mut tx = MiniTx::new(&versions, &wal);
          wal.append_start(tx.state.get_id())?;

          let meta_table = tables.meta_table();

          let mut metadata = match get_data(
            &buffer_pool,
            &meta_table,
            &tx.state,
            &tx.snapshot,
            table_name.as_bytes(),
          )? {
            Some(bytes) => TableMetadata::from_bytes(&bytes)?,
            None => return Err(Error::TableNotFound(table_name)),
          };

          if old.metadata().get_id() != metadata.get_id() {
            return Err(Error::TableNotFound(table_name));
          }
          if metadata.get_compaction_id().is_some() {
            return Ok(());
          }

          let table_meta = tables.create_metadata(&table_name);
          metadata.set_compaction(&table_meta);

          update_data(
            &buffer_pool,
            &meta_table,
            &tx.state,
            &tx.snapshot,
            &recorder,
            &versions,
            table_name.as_bytes(),
            metadata.to_vec(),
          )?;

          let new_table = tables.create_handle(&table_meta)?.try_mutation().unwrap();

          tables.insert(new_table.handle().clone());

          initialize(new_table.handle(), &buffer_pool, &recorder)?;

          tx.commit()?;
          (new_table, tx.state.get_id())
        };

        logger
          .info(|| format!("table {table_name} compacting wait until another tx close."));

        while versions.min_version() < wait_until {
          std::thread::sleep(Duration::from_secs(1));
        }

        (old, new_table)
      }
    };

    let old_table = loop {
      match old_table.try_mutation() {
        Some(handle) => break handle,
        None => std::thread::sleep(Duration::from_secs(1)),
      }
    };

    logger.info(|| format!("table {table_name} compacting begin."));

    let mut compactor = Compactor::boot(
      old_table.handle().clone(),
      new.handle().clone(),
      &buffer_pool,
      &recorder,
      &versions,
    )?;

    while let Some(snap) = compactor.next_kv()? {
      compactor.apply_snapshot(snap)?;
    }
    logger.info(|| {
      format!(
        "table {table_name} compacting copied {} count record complete.",
        compactor.moved_count,
      )
    });

    let tx_id = {
      let mut tx = MiniTx::new(&versions, &wal);
      wal.append_start(tx.state.get_id())?;

      let meta_table = tables.meta_table();

      if get_data(
        &buffer_pool,
        &meta_table,
        &tx.state,
        &tx.snapshot,
        table_name.as_bytes(),
      )?
      .is_none()
      {
        logger.warn(|| format!("table {table_name} already dropped."));
        return Ok(());
      }

      update_data(
        &buffer_pool,
        &meta_table,
        &tx.state,
        &tx.snapshot,
        &recorder,
        &versions,
        table_name.as_bytes(),
        compactor.new.metadata().to_vec(),
      )?;
      tx.commit()?;
      tx.state.get_id()
    };

    gc.release_table(compactor.old, tx_id);
    logger.info(|| format!("table {table_name} compacting totally complete."));

    Ok(())
  }
}
