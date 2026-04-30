use std::{sync::Arc, time::Duration};

use super::{PageRecorder, TimeoutThread, TxSnapshot, TxState, VersionVisibility};

use crate::{
  cache::{BlockCache, CacheSlot, WritableSlot},
  cursor::{GarbageCollector, TreeManager},
  debug,
  disk::Pointer,
  error::Result,
  info,
  metrics::MetricsRegistry,
  serialize::Serializable,
  table::{MutationHandle, TableHandle, TableId, TableMapper, TableMetadata},
  thread::{BackgroundThread, WorkBuilder},
  utils::ToArc,
  wal::{TxId, WALConfig, WALSegment, WAL},
};

pub struct TransactionConfig {
  pub timeout: Duration,
  pub checkpoint_interval: Duration,
}

/**
 * Composes WAL, block cache, GC, version visibility, and tree manager into a
 * unified interface for the cursor layer. Does not contain business logic —
 * it wires subsystems together and exposes transaction lifecycle operations.
 */
pub struct TxOrchestrator {
  wal: Arc<WAL>,
  tables: Arc<TableMapper>,
  block_cache: Arc<BlockCache>,
  checkpoint: Arc<dyn BackgroundThread<(), Result>>,
  version_visibility: Arc<VersionVisibility>,
  gc: Arc<GarbageCollector>,
  recorder: Arc<PageRecorder>,
  timeout_thread: TimeoutThread,
  tx_timeout: Duration,
  tree_manager: TreeManager,
  metrics: Arc<MetricsRegistry>,
}
impl TxOrchestrator {
  pub fn new(
    config: TransactionConfig,
    wal_config: &WALConfig,
    wal: Arc<WAL>,
    block_cache: Arc<BlockCache>,
    tables: Arc<TableMapper>,
    version_visibility: Arc<VersionVisibility>,
    gc: Arc<GarbageCollector>,
    recorder: Arc<PageRecorder>,
    tree_manager: TreeManager,
    metrics: Arc<MetricsRegistry>,
  ) -> Self {
    let checkpoint = WorkBuilder::new()
      .name("checkpoint")
      .single()
      .interval(
        config.checkpoint_interval,
        handle_checkpoint(wal.clone(), block_cache.clone(), version_visibility.clone()),
      )
      .to_arc();
    wal.initialize(wal_config, Arc::downgrade(&checkpoint));
    let timeout_thread = TimeoutThread::new(version_visibility.clone());

    Self {
      wal,
      tables,
      block_cache,
      checkpoint,
      version_visibility,
      gc,
      recorder,
      timeout_thread,
      tx_timeout: config.timeout,
      tree_manager,
      metrics,
    }
  }

  pub fn initial_checkpoint(
    config: TransactionConfig,
    wal_config: &WALConfig,
    wal: Arc<WAL>,
    block_cache: Arc<BlockCache>,
    tables: Arc<TableMapper>,
    version_visibility: Arc<VersionVisibility>,
    gc: Arc<GarbageCollector>,
    recorder: Arc<PageRecorder>,
    tree_manager: TreeManager,
    metrics: Arc<MetricsRegistry>,
    segments: Vec<WALSegment>,
  ) -> Result<Self> {
    run_checkpoint(&wal, &block_cache, &version_visibility)?;
    segments
      .into_iter()
      .map(|seg| seg.truncate())
      .collect::<Result>()?;

    Ok(Self::new(
      config,
      wal_config,
      wal,
      block_cache,
      tables,
      version_visibility,
      gc,
      recorder,
      tree_manager,
      metrics,
    ))
  }

  #[inline]
  pub fn fetch(
    &self,
    pointer: Pointer,
    handle: Arc<TableHandle>,
  ) -> Result<CacheSlot<'_>> {
    self.block_cache.read(pointer, handle)
  }

  #[inline]
  pub fn serialize_and_log<T>(
    &self,
    tx_id: TxId,
    table_id: TableId,
    slot: &mut WritableSlot<'_>,
    data: &T,
  ) -> Result
  where
    T: Serializable,
  {
    self.recorder.serialize_and_log(tx_id, table_id, slot, data)
  }

  #[inline]
  pub fn mark_gc(&self, handle: Arc<TableHandle>, pointer: Pointer) {
    self.gc.mark(handle, pointer);
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
  pub fn get_table(&self, table_id: TableId) -> Option<Arc<TableHandle>> {
    self.tables.get(table_id)
  }
  #[inline]
  pub fn commit_table(&self, table: Arc<TableHandle>) {
    self.tables.insert(table);
  }
  #[inline]
  pub fn open_table(&self, table_meta: &TableMetadata) -> Result<Arc<TableHandle>> {
    self.tables.create_handle(table_meta)
  }
  #[inline]
  pub fn create_table_metadata(&self, name: &str) -> TableMetadata {
    self.tables.create_metadata(name)
  }

  #[inline]
  pub fn drop_table(&self, table: Arc<TableHandle>, tx_id: TxId, version: TxId) {
    self.gc.release_table(table, tx_id, version);
  }
  #[inline]
  pub fn get_metadata_table(&self) -> Arc<TableHandle> {
    self.tables.meta_table()
  }
  #[inline]
  pub fn compact_table(&self, old: Arc<TableHandle>, new: MutationHandle, version: TxId) {
    self.tree_manager.compact(old, new, version);
  }

  /**
   * Closes components in dependency order — higher-level components first.
   * wal.half_close() step 1 stops new checkpoint triggers; checkpoint.close()
   * performs the final checkpoint; step 2 wal.close() finalizes the WAL.
   */
  pub fn close(&self) -> Result {
    self.tree_manager.close();
    self.timeout_thread.close();
    self.wal.half_close();
    self.checkpoint.close();
    info!("last checkpoint completed.");

    self.block_cache.close();
    info!("block cache closed.");
    self.gc.close();
    self.tables.close();
    info!("tables closed.");
    self.wal.close();
    info!("wal closed.");
    Ok(())
  }
}

const fn handle_checkpoint(
  wal: Arc<WAL>,
  block_cache: Arc<BlockCache>,
  version: Arc<VersionVisibility>,
) -> impl Fn(Option<()>) -> Result {
  move |_| run_checkpoint(&wal, &block_cache, &version)
}

fn run_checkpoint(
  wal: &WAL,
  block_cache: &BlockCache,
  version: &VersionVisibility,
) -> Result {
  let log_id = wal.current_log_id();
  let current_version = version.current_version();
  info!("checkpoint trigger id {log_id} version {current_version}");

  block_cache.flush()?;
  let path = version.persist_snapshot(current_version)?;
  debug!("checkpoint snapshot persisted.");

  wal.checkpoint_and_flush(log_id, path.clone())?;
  info!("checkpoint complete id {log_id}");

  version.clear(&path)?;
  Ok(())
}
