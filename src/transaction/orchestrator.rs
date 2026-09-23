use std::sync::Arc;

use super::{Checkpoint, PageRecorder};

use crate::{
  blob::{BlobAppendGuard, BlobHandle, BlobId, BlobStorage},
  btree::ResolvedConflict,
  cache::{BlockCache, CachedSlot, RefedSlot},
  disk::{IOPool, Pointer},
  error::Result,
  maintenance::{Compactor, GarbageCollector},
  metrics::{measure, MetricsRegistry},
  mvcc::{TxState, VersionController},
  objects::Serializable,
  table::{TableHandleRef, TableId, TableMapper, TableMetadata, TableNameRef},
  utils::info,
  wal::{DurabilityGuard, TxId, WriteAheadLog},
};

/**
 * Composes WAL, block cache, GC, version visibility into a
 * unified interface for the cursor layer. Does not contain business logic —
 * it wires subsystems together and exposes transaction lifecycle operations.
 */
pub struct TxOrchestrator {
  wal: Arc<WriteAheadLog>,
  tables: Arc<TableMapper>,
  block_cache: Arc<BlockCache>,
  checkpoint: Arc<Checkpoint>,
  version_controller: Arc<VersionController>,
  gc: Arc<GarbageCollector>,
  recorder: Arc<PageRecorder>,
  compactor: Arc<Compactor>,
  io_pool: Arc<IOPool>,
  blob: Arc<BlobStorage>,
  metrics: Arc<MetricsRegistry>,
}
impl TxOrchestrator {
  pub const fn new(
    wal: Arc<WriteAheadLog>,
    block_cache: Arc<BlockCache>,
    tables: Arc<TableMapper>,
    version_controller: Arc<VersionController>,
    gc: Arc<GarbageCollector>,
    recorder: Arc<PageRecorder>,
    compactor: Arc<Compactor>,
    io_pool: Arc<IOPool>,
    blob: Arc<BlobStorage>,
    checkpoint: Arc<Checkpoint>,
    metrics: Arc<MetricsRegistry>,
  ) -> Self {
    Self {
      wal,
      tables,
      block_cache,
      checkpoint,
      version_controller,
      gc,
      recorder,
      compactor,
      io_pool,
      blob,
      metrics,
    }
  }

  #[inline]
  pub fn fetch(
    &self,
    pointer: Pointer,
    handle: &TableHandleRef,
  ) -> Result<CachedSlot<'_>> {
    self.block_cache.read(pointer, handle)
  }
  #[inline]
  pub fn alloc(
    &self,
    pointer: Pointer,
    handle: &TableHandleRef,
  ) -> Result<CachedSlot<'_>> {
    self.block_cache.alloc(pointer, handle)
  }

  #[inline]
  pub fn serialize_and_log<T>(
    &self,
    tx_id: TxId,
    table_id: TableId,
    current_version: TxId,
    slot: &mut RefedSlot,
    data: &T,
  ) -> Result
  where
    T: Serializable,
  {
    self
      .recorder
      .serialize_and_log(tx_id, table_id, current_version, slot, data)
  }

  #[inline]
  pub fn start_tx(&self) -> Option<TxState<'_>> {
    self.version_controller.new_transaction()
  }

  #[inline]
  pub fn commit_tx(&self, tx_id: TxId) -> Result<DurabilityGuard<'_>> {
    let metrics = &self.metrics.transaction_commit;
    measure!(metrics, self.wal.commit_and_flush(tx_id))
  }

  #[inline]
  pub fn abort_tx(&self, tx_id: TxId) {
    self.version_controller.set_abort(tx_id);
    self.metrics.transaction_abort_count.inc();
  }

  #[inline]
  pub fn get_table(&self, table_id: TableId) -> Option<TableHandleRef> {
    self.tables.get(table_id)
  }
  #[inline]
  pub fn register_table(&self, table: TableHandleRef) {
    self.tables.insert(table);
  }
  #[inline]
  pub fn open_table(&self, table_meta: &TableMetadata) -> Result<TableHandleRef> {
    self.tables.create_handle(table_meta)
  }
  #[inline]
  pub fn create_table_metadata(&self, name: TableNameRef) -> TableMetadata {
    self.tables.create_metadata(name)
  }

  #[inline]
  pub fn get_metadata_table(&self) -> TableHandleRef {
    self.tables.meta_table()
  }

  pub fn resolve_conflict(&self, owner: TxId, current: TxId) -> ResolvedConflict {
    self.version_controller.resolve_conflict(owner, current)
  }

  pub fn get_blob_handle(&self, blob_id: BlobId) -> Option<Arc<BlobHandle>> {
    self.blob.get(blob_id)
  }
  pub fn write_blob(&self, data: Vec<u8>) -> Result<BlobAppendGuard<'_>> {
    self.blob.append(data)
  }

  /**
   * Close subsystems in logical dependency order.
   *
   * Background components are stopped first so they stop producing cache/WAL/IO
   * work. Lower-level storage components are then closed after their users have
   * finished.
   */
  pub fn close(&self) -> Result {
    self.compactor.close()?;
    self.gc.close();
    info!("gc closed.");
    self.checkpoint.close()?;

    self.block_cache.close();
    info!("block cache closed.");
    self.wal.close();
    info!("wal closed.");
    self.io_pool.close();
    info!("io pool closed.");
    Ok(())
  }
}
