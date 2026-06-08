use std::{sync::Arc, time::Duration};

use super::{
  Checkpoint, PageRecorder, TimeoutThread, TxSnapshot, TxState, VersionVisibility,
};

use crate::{
  background::EventBus,
  cache::{BlockCache, CachedSlot, RefedSlot},
  cursor::{Compactor, GarbageCollector},
  disk::{IOHandle, IOPool, Pointer},
  error::Result,
  info,
  metrics::MetricsRegistry,
  serialize::Serializable,
  table::{TableHandleRef, TableId, TableMapper, TableMetadata, TableName},
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
    event_bus: Arc<EventBus>,
    metrics: Arc<MetricsRegistry>,
  ) -> Self {
    let checkpoint = Checkpoint::new(
      wal.clone(),
      block_cache.clone(),
      version_visibility.clone(),
      io_pool.clone(),
      event_bus.clone(),
      config.checkpoint_flush_factor,
    );
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
      tx_timeout: config.timeout,
      metrics,
    }
  }

  pub fn initial_checkpoint(
    config: TransactionConfig,
    wal: Arc<WAL>,
    block_cache: Arc<BlockCache>,
    tables: Arc<TableMapper>,
    version_visibility: Arc<VersionVisibility>,
    gc: Arc<GarbageCollector>,
    recorder: Arc<PageRecorder>,
    compactor: Arc<Compactor>,
    io_pool: Arc<IOPool>,
    event_bus: Arc<EventBus>,
    metrics: Arc<MetricsRegistry>,
    segments: Vec<IOHandle>,
  ) -> Result<Self> {
    Checkpoint::run_hard(&wal, &block_cache, &version_visibility, &io_pool)?;
    segments
      .into_iter()
      .map(|seg| seg.truncate())
      .collect::<Result>()?;

    Ok(Self::new(
      config,
      wal,
      block_cache,
      tables,
      version_visibility,
      gc,
      recorder,
      compactor,
      io_pool,
      event_bus,
      metrics,
    ))
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
    slot: &mut RefedSlot,
    data: &T,
  ) -> Result
  where
    T: Serializable,
  {
    self.recorder.serialize_and_log(tx_id, table_id, slot, data)
  }

  #[inline]
  pub fn start_tx(
    &self,
    timeout: Option<Duration>,
  ) -> Result<(TxState<'_>, TxSnapshot<'_>)> {
    let (state, snapshot) = self.version_visibility.new_transaction();
    let tx_id = state.get_id();
    self.wal.append_start(tx_id)?;
    self
      .timeout_thread
      .register(tx_id, timeout.unwrap_or(self.tx_timeout));
    Ok((state, snapshot))
  }

  #[inline]
  pub fn commit_tx(&self, tx_id: TxId) -> Result {
    self
      .metrics
      .transaction_commit
      .measure(|| self.wal.commit_and_flush(tx_id))?;
    Ok(())
  }

  #[inline]
  pub fn abort_tx(&self, tx_id: TxId) -> Result {
    self.version_visibility.set_abort(tx_id);
    self.wal.append_abort(tx_id)?;
    self.metrics.transaction_abort_count.inc();
    Ok(())
  }

  #[inline]
  pub fn current_version(&self) -> TxId {
    self.version_visibility.current_version()
  }

  #[inline]
  pub fn get_table(&self, table_id: TableId) -> Option<TableHandleRef> {
    self.tables.get(table_id)
  }
  #[inline]
  pub fn commit_table(&self, table: TableHandleRef) {
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

  /**
   * Closes components in dependency order — higher-level components first.
   * wal.half_close() step 1 stops new checkpoint triggers; checkpoint.close()
   * performs the final checkpoint; step 2 wal.close() finalizes the WAL.
   */
  pub fn close(&self) -> Result {
    self.compactor.close();
    self.gc.close();
    info!("gc closed.");
    self.timeout_thread.close();
    self.checkpoint.close();
    info!("last checkpoint completed.");

    self.block_cache.close();
    info!("block cache closed.");
    self.wal.close();
    info!("wal closed.");
    self.io_pool.close();
    info!("io pool closed.");
    Ok(())
  }
}
