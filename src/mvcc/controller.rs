use std::{
  collections::BTreeSet,
  ops::Deref,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
  },
};

use super::{AbortedSet, ActiveSet, ActiveState};

use crate::{
  background::{binding_events, EventBus, OnceParker, SharedSubscription},
  btree::ResolvedConflict,
  cache::ShrinkMap,
  utils::{error, warn, OffsetBitmap, SBox, ShortenedMutex},
  wal::{TxId, WALFailed, RESERVED_TX},
};

fn remove_and_wake(active: &ActiveSet, conflicted: &ConflictSet, tx_id: &TxId) {
  active.remove(tx_id);
  if let Some(parker) = conflicted.l().remove(tx_id) {
    parker.wake_all();
  }
}

pub struct TxState<'a> {
  state: ActiveState,
  set: &'a ActiveSet,
  conflicted: &'a ConflictSet,
  closed: &'a AtomicBool,
}
impl<'a> TxState<'a> {
  const fn new(
    state: ActiveState,
    set: &'a ActiveSet,
    conflicted: &'a ConflictSet,
    closed: &'a AtomicBool,
  ) -> Self {
    Self {
      state,
      set,
      conflicted,
      closed,
    }
  }
  pub fn deactive(&self) {
    remove_and_wake(self.set, self.conflicted, &self.state.get_id());
  }
  pub fn current_version(&self) -> TxId {
    self.set.current_version()
  }
  pub fn is_available(&self) -> bool {
    !self.closed.load(Ordering::Relaxed) && self.state.is_available()
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
  aborted: &'a AbortedSet,
}
impl<'a> TxSnapshot<'a> {
  const fn new(active: OffsetBitmap, aborted: &'a AbortedSet) -> Self {
    Self { active, aborted }
  }

  #[inline]
  pub fn is_active(&self, &tx_id: &TxId) -> bool {
    self.active.contains(tx_id)
  }
  pub fn is_aborted(&self, tx_id: TxId) -> bool {
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

type ConflictSet = Mutex<ShrinkMap<TxId, SBox<OnceParker>>>;

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
  conflicted: ConflictSet,
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
    let aborted = active_versions
      .into_iter()
      .chain(started)
      .chain(aborted_versions)
      .filter(|c| !closed.contains(c))
      .collect();
    let this = Arc::new(Self {
      aborted: AbortedSet::from_exists(aborted),
      active: ActiveSet::new(last_tx_id),
      wait_graph: WaitGraph::new(),
      conflicted: Mutex::new(ShrinkMap::new()),
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
      conflicted: Mutex::new(ShrinkMap::new()),
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
    if !self.active.is_active(&owner) {
      return ResolvedConflict::Closed;
    };
    if !self.wait_graph.get_or_insert(current, owner) {
      warn!("dead lock detected at tx {}.", current);
      return ResolvedConflict::DeadLock;
    };
    let parker = self
      .conflicted
      .l()
      .get_or_insert_with(owner, Default::default)
      .clone();
    if self.active.is_active(&owner) {
      parker.park();
    } else if let Some(parker) = self.conflicted.l().remove(&owner) {
      parker.wake_all();
    };
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
    if self.closed.load(Ordering::Relaxed) {
      return None;
    }
    let state = self.active.new_state();
    Some((
      TxSnapshot::new(self.active.snapshot(), &self.aborted),
      TxState::new(state, &self.active, &self.conflicted, &self.closed),
    ))
  }

  /**
   * Return the current visibility state and return its covered transaction
   * boundary.
   */
  pub fn snapshot(&self) -> (TxId, Vec<TxId>, Vec<TxId>) {
    let tx_id = self.active.current_version();
    let active = self
      .active
      .snapshot()
      .iter()
      .take_while(|&id| id < tx_id)
      .collect();
    let aborted = self.aborted.snapshot_until(tx_id);
    (tx_id, active, aborted)
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
    error!("all versions transit to abort since wal failure detected.");
  }
}
binding_events!(VersionController {
  shared: [WALFailed]
});
