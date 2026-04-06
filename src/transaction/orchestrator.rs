use std::{sync::Arc, time::Duration};

use super::{PageRecorder, TimeoutThread, TxSnapshot, TxState, VersionVisibility};

use crate::{
  buffer_pool::{BufferPool, Slot, WritableSlot},
  cursor::{GarbageCollector, TreeManager},
  disk::Pointer,
  error::Result,
  metrics::MetricsRegistry,
  serialize::Serializable,
  table::{ReserveResult, TableHandle, TableId, TableMapper, TableMetadata},
  thread::{BackgroundThread, WorkBuilder, WorkInput},
  utils::{LogFilter, ToBox},
  wal::{TxId, WALSegment, WAL},
};

pub struct TransactionConfig {
  pub timeout: Duration,
  pub checkpoint_interval: Duration,
}

/**
 * Composes WAL, buffer pool, GC, version visibility, and tree manager into a
 * unified interface for the cursor layer. Does not contain business logic —
 * it wires subsystems together and exposes transaction lifecycle operations.
 */
pub struct TxOrchestrator {
  wal: Arc<WAL>,
  tables: Arc<TableMapper>,
  buffer_pool: Arc<BufferPool>,
  checkpoint: Box<dyn BackgroundThread<(), Result>>,
  version_visibility: Arc<VersionVisibility>,
  gc: Arc<GarbageCollector>,
  recorder: Arc<PageRecorder>,
  logger: LogFilter,
  timeout_thread: TimeoutThread,
  tx_timeout: Duration,
  tree_manager: TreeManager,
  metrics: Arc<MetricsRegistry>,
}
impl TxOrchestrator {
  pub fn new(
    config: TransactionConfig,
    wal: Arc<WAL>,
    buffer_pool: Arc<BufferPool>,
    tables: Arc<TableMapper>,
    version_visibility: Arc<VersionVisibility>,
    gc: Arc<GarbageCollector>,
    recorder: Arc<PageRecorder>,
    logger: LogFilter,
    tree_manager: TreeManager,
    metrics: Arc<MetricsRegistry>,
    checkpoint_ch: WorkInput<(), Result>,
  ) -> Result<Self> {
    let checkpoint = WorkBuilder::new()
      .name("checkpoint")
      .stack_size(2 << 20)
      .single()
      .from_channel(checkpoint_ch)
      .interval(
        config.checkpoint_interval,
        handle_checkpoint(
          wal.clone(),
          buffer_pool.clone(),
          gc.clone(),
          version_visibility.clone(),
          logger.clone(),
        ),
      )?
      .to_box();
    let timeout_thread = TimeoutThread::new(version_visibility.clone(), logger.clone());
    Ok(Self {
      wal,
      tables,
      buffer_pool,
      checkpoint,
      version_visibility,
      gc,
      recorder,
      logger,
      timeout_thread,
      tx_timeout: config.timeout,
      tree_manager,
      metrics,
    })
  }

  pub fn initial_checkpoint(
    config: TransactionConfig,
    wal: Arc<WAL>,
    buffer_pool: Arc<BufferPool>,
    tables: Arc<TableMapper>,
    version_visibility: Arc<VersionVisibility>,
    gc: Arc<GarbageCollector>,
    recorder: Arc<PageRecorder>,
    logger: LogFilter,
    tree_manager: TreeManager,
    metrics: Arc<MetricsRegistry>,
    checkpoint_ch: WorkInput<(), Result>,
    segments: Vec<WALSegment>,
  ) -> Result<Self> {
    run_checkpoint(&wal, &buffer_pool, &gc, &version_visibility, &logger)?;
    segments
      .into_iter()
      .map(|seg| seg.truncate())
      .collect::<Result>()?;

    Self::new(
      config,
      wal,
      buffer_pool,
      tables,
      version_visibility,
      gc,
      recorder,
      logger,
      tree_manager,
      metrics,
      checkpoint_ch,
    )
  }

  #[inline]
  pub fn fetch(&self, pointer: Pointer, handle: Arc<TableHandle>) -> Result<Slot<'_>> {
    self.buffer_pool.read(pointer, handle)
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
  pub fn alloc(&self, handle: Arc<TableHandle>) -> Result<WritableSlot<'_>> {
    let free = handle.free().alloc();
    Ok(self.buffer_pool.read(free, handle)?.for_write())
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
    self.wal.append_start(state.get_id())?;
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
  pub fn reserve_table(&self, name: &str) -> Result<ReserveResult> {
    self.tables.get_or_reserve(name)
  }
  #[inline]
  pub fn get_table(&self, name: &str) -> Option<Arc<TableHandle>> {
    self.tables.get(name)
  }
  #[inline]
  pub fn open_table(&self, name: String, table: Arc<TableHandle>) {
    self.tables.insert(name, table);
  }
  pub fn create_table(&self, table_meta: TableMetadata) -> Result<Arc<TableHandle>> {
    self.tables.create_handle(table_meta)
  }
  pub fn create_table_metadata(&self, name: &str) -> TableMetadata {
    self.tables.create_metadata(name)
  }
  #[inline]
  pub fn drop_table(&self, name: &str, table: Arc<TableHandle>) -> Result {
    self.tables.remove(name);
    table.truncate()?;
    Ok(())
  }
  pub fn get_metadata_table(&self) -> Arc<TableHandle> {
    self.tables.meta_table()
  }

  /**
   * Closes components in dependency order — higher-level components first.
   * wal.twostep_close() step 1 stops new checkpoint triggers; checkpoint.close()
   * performs the final checkpoint; step 2 (wal_close) finalizes the WAL.
   */
  pub fn close(&self) -> Result {
    self.tree_manager.close();
    self.timeout_thread.close();
    let wal_close = self.wal.twostep_close();
    self.checkpoint.close();
    self.logger.info(|| "last checkpoint completed.");

    self.gc.close();
    self.tables.close();
    self.logger.info(|| "buffer pool closed.");
    wal_close();
    self.logger.info(|| "wal closed.");
    Ok(())
  }
}

fn handle_checkpoint(
  wal: Arc<WAL>,
  buffer_pool: Arc<BufferPool>,
  gc: Arc<GarbageCollector>,
  version: Arc<VersionVisibility>,
  logger: LogFilter,
) -> impl Fn(Option<()>) -> Result {
  move |_| run_checkpoint(&wal, &buffer_pool, &gc, &version, &logger)
}

fn run_checkpoint(
  wal: &WAL,
  buffer_pool: &BufferPool,
  gc: &GarbageCollector,
  version: &VersionVisibility,
  logger: &LogFilter,
) -> Result {
  let log_id = wal.current_log_id();
  let min_version = version.min_version();
  logger.debug(|| format!("checkpoint trigger id {log_id} version {min_version}"));

  gc.run()?;
  version.remove_aborted(&min_version);
  logger.debug(|| format!("checkpoint garbage collected id {log_id}"));

  buffer_pool.flush()?;
  wal.checkpoint_and_flush(log_id, min_version)?;
  logger.debug(|| format!("checkpoint complete id {log_id}"));
  Ok(())
}
