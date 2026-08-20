use std::{
  collections::BTreeSet,
  ops::Deref,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
  },
};

use crossbeam_skiplist::SkipSet;

use super::{ActiveSet, ActiveState};

use crate::{
  background::{EventBus, SharedSubscription},
  binding_events,
  cache::ShrinkMap,
  cursor::ResolvedConflict,
  error,
  utils::{OffsetBitmap, SBox, ShortenedMutex},
  wal::{TxId, WALFailed, RESERVED_TX},
  warn, Result,
};

fn remove_and_wake(active: &ActiveSet, tx_id: &TxId) {
  let Some(state) = active.remove(tx_id) else {
    return;
  };
  state.wake_all();
}

pub struct TxState<'a> {
  state: SBox<ActiveState>,
  set: &'a ActiveSet,
}
impl<'a> TxState<'a> {
  const fn new(state: SBox<ActiveState>, set: &'a ActiveSet) -> Self {
    Self { state, set }
  }
  pub fn deactive(&self) {
    remove_and_wake(self.set, &self.state.get_id());
  }
  pub fn current_version(&self) -> TxId {
    self.set.current_version()
  }
}
impl<'a> Deref for TxState<'a> {
  type Target = ActiveState;

  fn deref(&self) -> &Self::Target {
    &self.state
  }
}

/**
 * Transaction snapshot used for visibility checks.
 *
 * The active set is captured at transaction start to provide snapshot isolation.
 * The aborted set is referenced live instead of copied: transactions added there
 * were not readable to this snapshot anyway, so observing later abort markings
 * does not expand visibility.
 */
pub struct TxSnapshot<'a> {
  active: OffsetBitmap,
  aborted: &'a SkipSet<TxId>,
}
impl<'a> TxSnapshot<'a> {
  fn new(active: OffsetBitmap, aborted: &'a SkipSet<TxId>) -> Self {
    Self { active, aborted }
  }

  #[inline]
  pub fn is_active(&self, &tx_id: &TxId) -> bool {
    self.active.contains(tx_id)
  }
  pub fn is_aborted(&self, tx_id: &TxId) -> bool {
    self.aborted.contains(tx_id)
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
pub struct VersionVisibility {
  aborted: SkipSet<TxId>,
  active: ActiveSet,
  wait_graph: WaitGraph,
  closed: AtomicBool,
}
impl VersionVisibility {
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
  ) -> Result<Arc<Self>> {
    let this = Arc::new(Self {
      aborted: active_versions
        .into_iter()
        .chain(started)
        .chain(aborted_versions)
        .filter(|c| !closed.contains(c))
        .collect(),
      active: ActiveSet::new(last_tx_id),
      wait_graph: WaitGraph::new(),
      closed: AtomicBool::new(false),
    });
    event_bus.register(&this);
    Ok(this)
  }
  pub fn init(event_bus: &EventBus) -> Arc<Self> {
    let this = Arc::new(Self {
      aborted: Default::default(),
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
  pub fn remove_aborted(&self, version: &TxId) {
    while let Some(v) = self.aborted.front() {
      if v.value() >= version {
        return;
      }
      v.remove();
    }
  }

  #[inline]
  pub fn is_aborted(&self, tx_id: &TxId) -> bool {
    self.aborted.contains(tx_id)
  }

  pub fn resolve_conflict(&self, owner: TxId, current: TxId) -> ResolvedConflict {
    let Some(state) = self.active.get(&owner) else {
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
    self
      .active
      .min_version()
      .unwrap_or_else(|| self.active.current_version())
  }
  #[inline]
  pub fn set_abort(&self, tx_id: TxId) {
    self.aborted.insert(tx_id);
  }
  pub fn new_transaction(&self) -> Option<(TxSnapshot<'_>, TxState<'_>)> {
    if self.closed.load(Ordering::Acquire) {
      return None;
    }
    let state = self.active.new_state();
    Some((
      TxSnapshot::new(self.active.snapshot_until(state.get_id()), &self.aborted),
      TxState::new(state, &self.active),
    ))
  }
  #[inline]
  pub fn get_active_state(&self, tx_id: TxId) -> Option<TxState<'_>> {
    self
      .active
      .get(&tx_id)
      .map(|state| TxState::new(state, &self.active))
  }

  /**
   * Return the current visibility state and return its covered transaction
   * boundary.
   */
  pub fn snapshot(&self) -> (TxId, Vec<TxId>, Vec<TxId>) {
    let tx_id = self.active.current_version();
    (
      tx_id,
      self.active.until(tx_id),
      self.aborted.range(..tx_id).map(|e| *e.value()).collect(),
    )
  }
}
impl SharedSubscription<WALFailed> for VersionVisibility {
  /**
   * End all currently active transactions as aborted after WAL failure.
   *
   * No active transaction can commit once WAL durability is unavailable, so every
   * abortable active state is moved to the aborted set and removed from active.
   */
  fn handle(&self, _: Arc<WALFailed>) {
    if self.closed.fetch_or(true, Ordering::Release) {
      return;
    }
    for state in self.active.get_all().into_iter().filter(|v| v.try_abort()) {
      self.aborted.insert(state.get_id());
      remove_and_wake(&self.active, &state.get_id());
    }
    error!("all versions transit to abort since wal failure detected.");
  }
}
binding_events!(VersionVisibility {
  shared: [WALFailed]
});
