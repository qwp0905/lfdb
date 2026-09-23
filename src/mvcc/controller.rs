use std::{
  collections::BTreeSet,
  sync::{
    atomic::{AtomicBool, AtomicU8, Ordering},
    Arc, Mutex, OnceLock,
  },
};

use super::{AbortedSet, ActiveSet, ReadonlyState, WritableState};

use crate::{
  background::{binding_events, EventBus, SharedSubscription},
  btree::ResolvedConflict,
  cache::ShrinkMap,
  utils::{error, warn, SBox, ShortenedMutex},
  wal::{TxId, WALFailed, RESERVED_TX},
};

const STATUS_AVAILABLE: u8 = 0;
const STATUS_ON_COMMIT: u8 = 1; // Exclusive state during commit attempt — prevents timeout thread from aborting while WAL write is in progress
const STATUS_ABORTED: u8 = 2;

pub struct TxState<'a> {
  status: AtomicU8,
  readonly: ReadonlyState,
  writable: OnceLock<SBox<WritableState>>,
  controller: &'a VersionController,
}
impl<'a> TxState<'a> {
  const fn new(readonly: ReadonlyState, controller: &'a VersionController) -> Self {
    Self {
      status: AtomicU8::new(STATUS_AVAILABLE),
      readonly,
      writable: OnceLock::new(),
      controller,
    }
  }

  pub fn is_available(&self) -> bool {
    !self.controller.is_closed()
      && self.status.load(Ordering::Relaxed) == STATUS_AVAILABLE
  }

  pub fn try_abort(&self) -> bool {
    self
      .status
      .compare_exchange(
        STATUS_AVAILABLE,
        STATUS_ABORTED,
        Ordering::Relaxed,
        Ordering::Relaxed,
      )
      .is_ok()
  }

  #[inline]
  pub fn try_commit(&self) -> bool {
    !self.controller.is_closed()
      && self
        .status
        .compare_exchange(
          STATUS_AVAILABLE,
          STATUS_ON_COMMIT,
          Ordering::Relaxed,
          Ordering::Relaxed,
        )
        .is_ok()
  }

  #[inline]
  pub fn make_available(&self) {
    self.status.store(STATUS_AVAILABLE, Ordering::Relaxed)
  }

  #[inline]
  pub fn is_active(&self, &tx_id: &TxId) -> bool {
    self.readonly.contains_in_snapshot(tx_id)
  }
  pub fn is_aborted(&self, tx_id: TxId) -> bool {
    self.controller.aborted.contains(tx_id)
  }

  pub fn deactive(&self) {
    let Some(writable) = self.writable.get() else {
      return;
    };
    self.controller.remove_and_wake(writable.get_id());
  }

  pub const fn get_upper_bound(&self) -> TxId {
    self.readonly.get_upper_bound()
  }

  pub fn get_writable(&self) -> Option<&WritableState> {
    self.writable.get().map(|v| &**v)
  }
  pub fn ensure_writable(&self) -> &WritableState {
    self
      .writable
      .get_or_init(|| self.controller.active.new_writable())
  }

  pub fn current_version(&self) -> TxId {
    self.controller.active.current_version()
  }
}
impl<'a> Drop for TxState<'a> {
  fn drop(&mut self) {
    self
      .controller
      .active
      .remove_readonly(self.readonly.get_upper_bound());
  }
}

struct WaitGraph(Mutex<ShrinkMap<TxId, TxId>>);
impl WaitGraph {
  fn new() -> Self {
    Self(Default::default())
  }

  fn get_or_insert(&self, waiter: TxId, target: TxId) -> bool {
    let mut this = self.0.l();
    let mut id = target;
    while let Some(&next) = this.get(&id) {
      if next == waiter {
        return false;
      }
      id = next;
    }
    this.insert(waiter, target);
    true
  }

  fn remove(&self, waiter: TxId) {
    self.0.l().remove(&waiter);
  }
}

/**
 * Tracks MVCC visibility for transactions.
 *
 * Visibility is determined by exclusion: a transaction's writes are visible
 * if it is neither aborted nor still active. Committed transactions are not
 * tracked explicitly — committing simply removes the tx from active.
 */
pub struct VersionController {
  aborted: AbortedSet,
  active: ActiveSet,
  wait_graph: WaitGraph,
  closed: AtomicBool,
}
impl VersionController {
  /**
   * Rebuild visibility after replay.
   *
   * Transactions that were active in the persisted snapshot or started in the WAL
   * window are treated as aborted unless a close record is also present. After a
   * restart there are no still-active user transactions; committed transactions
   * are represented implicitly as ids that are neither active-at-crash nor aborted.
   */
  pub fn replay(
    last_tx_id: TxId,
    started: BTreeSet<TxId>,
    closed: BTreeSet<TxId>,
    active_versions: Vec<TxId>,
    aborted_versions: Vec<TxId>,
    event_bus: &EventBus,
  ) -> Arc<Self> {
    let this = Arc::new(Self {
      aborted: AbortedSet::from_exists(
        active_versions
          .into_iter()
          .chain(started)
          .chain(aborted_versions)
          .filter(|c| !closed.contains(c))
          .collect(),
      ),
      active: ActiveSet::new(last_tx_id),
      wait_graph: WaitGraph::new(),
      closed: AtomicBool::new(false),
    });
    event_bus.register(&this);
    this
  }
  pub fn init(event_bus: &EventBus) -> Arc<Self> {
    let this = Arc::new(Self {
      aborted: AbortedSet::empty(),
      active: ActiveSet::new(RESERVED_TX + 1),
      wait_graph: WaitGraph::new(),
      closed: AtomicBool::new(false),
    });
    event_bus.register(&this);
    this
  }

  /**
   * Advance the retained abort marker boundary.
   *
   * Abort markers below `version` are removed. Safety of that boundary is supplied
   * by the caller.
   */
  pub fn remove_aborted(&self, version: TxId) {
    self.aborted.remove_until(version);
  }

  #[inline]
  pub fn is_aborted(&self, tx_id: TxId) -> bool {
    self.aborted.contains(tx_id)
  }

  pub fn resolve_conflict(&self, owner: TxId, current: TxId) -> ResolvedConflict {
    let Some(state) = self.active.get_writable(&owner) else {
      return ResolvedConflict::Closed;
    };
    if !self.wait_graph.get_or_insert(current, owner) {
      warn!("dead lock detected at tx {}.", current);
      return ResolvedConflict::DeadLock;
    }
    state.park();
    self.wait_graph.remove(current);
    ResolvedConflict::Closed
  }

  /**
   * Returns the oldest active tx_id, or the current version if no transaction is active.
   * Called before GC to determine the safe cleanup boundary — versions older than this
   * are not visible to any active reader and can be collected.
   */
  pub fn min_version(&self) -> TxId {
    self.active.min_version()
  }
  #[inline]
  pub fn set_abort(&self, tx_id: TxId) {
    self.aborted.insert(tx_id);
  }
  pub fn new_transaction(&self) -> Option<TxState<'_>> {
    if self.is_closed() {
      return None;
    }
    let state = self.active.new_readonly();
    Some(TxState::new(state, self))
  }

  fn is_closed(&self) -> bool {
    self.closed.load(Ordering::Relaxed)
  }

  fn remove_and_wake(&self, tx_id: TxId) {
    let Some(state) = self.active.remove_writable(tx_id) else {
      return;
    };
    state.wake_all();
  }

  /**
   * Return the current visibility state and return its covered transaction
   * boundary.
   */
  pub fn snapshot(&self) -> (TxId, Vec<TxId>, Vec<TxId>) {
    let (tx_id, active) = self.active.snapshot_until();
    (tx_id, active, self.aborted.snapshot_until(tx_id))
  }
}
impl SharedSubscription<WALFailed> for VersionController {
  /**
   * End all currently active transactions as aborted after WAL failure.
   *
   * No active transaction can commit once WAL durability is unavailable, so every
   * abortable active state is moved to the aborted set and removed from active.
   */
  fn handle(&self, _: Arc<WALFailed>) {
    if self.closed.fetch_or(true, Ordering::Relaxed) {
      return;
    }
    error!("version controller transit to closed since wal failure detected.");
  }
}
binding_events!(VersionController {
  shared: [WALFailed]
});
