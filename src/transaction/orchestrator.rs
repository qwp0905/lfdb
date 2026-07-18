use std::{sync::Arc, time::Duration};

use super::{
  Checkpoint, PageRecorder, TimeoutThread, TxSnapshot, TxState, VersionVisibility,
};

use crate::{
  cache::{BlockCache, CachedSlot, RefedSlot},
  cursor::{
    BlobAppendGuard, BlobHandle, BlobId, BlobStorage, Compactor, GarbageCollector,
  },
  disk::{IOPool, Pointer},
  error::Result,
  info, measure,
  metrics::MetricsRegistry,
  serialize::Serializable,
  table::{TableHandleRef, TableId, TableMapper, TableMetadata, TableName},
  utils::SBox,
  wal::{TxId, WAL},
};

pub struct TransactionConfig {
  pub timeout: Duration,
  pub checkpoint_flush_factor: f64,
}

/**
 * Composes WAL, block cache, GC, version visibility into a
 * unified interface for the cursor layer. Does not contain business logic —
 * it wires subsystems together and exposes transaction lifecycle operations.
 */
pub struct TxOrchestrator {
  wal: Arc<WAL>,
  tables: Arc<TableMapper>,
  block_cache: Arc<BlockCache>,
  checkpoint: Arc<Checkpoint>,
  version_visibility: Arc<VersionVisibility>,
  gc: Arc<GarbageCollector>,
  recorder: Arc<PageRecorder>,
  compactor: Arc<Compactor>,
  io_pool: Arc<IOPool>,
  blob: Arc<BlobStorage>,
  timeout_thread: TimeoutThread,
  tx_timeout: Duration,
  metrics: Arc<MetricsRegistry>,
}
impl TxOrchestrator {
  pub fn new(
    config: TransactionConfig,
    wal: Arc<WAL>,
    block_cache: Arc<BlockCache>,
    tables: Arc<TableMapper>,
    version_visibility: Arc<VersionVisibility>,
    gc: Arc<GarbageCollector>,
    recorder: Arc<PageRecorder>,
    compactor: Arc<Compactor>,
    io_pool: Arc<IOPool>,
    blob: Arc<BlobStorage>,
    checkpoint: Arc<Checkpoint>,
    metrics: Arc<MetricsRegistry>,
  ) -> Self {
    let timeout_thread = TimeoutThread::new(version_visibility.clone());
    Self {
      wal,
      tables,
      block_cache,
      checkpoint,
      version_visibility,
      gc,
      recorder,
      compactor,
      timeout_thread,
      io_pool,
      blob,
      tx_timeout: config.timeout,
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
      .serialize_and_log(table_id, current_version, slot, data)
  }

  #[inline]
  pub fn start_tx(
    &self,
    timeout: Option<Duration>,
  ) -> Option<(TxState<'_>, TxSnapshot<'_>)> {
    let (snapshot, state) = self.version_visibility.new_transaction()?;
    self
      .timeout_thread
      .register(state.get_id(), timeout.unwrap_or(self.tx_timeout));
    Some((state, snapshot))
  }

  #[inline]
  pub fn commit_tx(&self, tx_id: TxId) -> Result {
    let metrics = &self.metrics.transaction_commit;
    measure!(metrics, self.wal.commit_and_flush(tx_id))?;
    Ok(())
  }

  #[inline]
  pub fn abort_tx(&self, tx_id: TxId) {
    self.version_visibility.set_abort(tx_id);
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
  pub fn create_table_metadata(&self, name: &TableName) -> TableMetadata {
    self.tables.create_metadata(name)
  }

  #[inline]
  pub fn get_metadata_table(&self) -> TableHandleRef {
    self.tables.meta_table()
  }

  pub fn wait_commit(&self, owner: TxId) {
    self.version_visibility.wait_commit(owner);
  }

  pub fn get_blob_handle(&self, blob_id: BlobId) -> Option<SBox<BlobHandle>> {
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
    self.timeout_thread.close();
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
