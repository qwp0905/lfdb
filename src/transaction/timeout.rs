use std::{
  num::NonZero,
  sync::Arc,
  thread::{park, park_timeout, Builder, Thread},
  time::{Duration, Instant},
};

use crossbeam::utils::Backoff;

use crate::{
  background::{ThreadSlot, UnwindSpawner},
  mvcc::VersionController,
  utils::{debug, warn, MpscQueue},
  wal::TxId,
};

const TICK_SIZE: Duration = Duration::from_millis(1);

const LAYER_PER_BUCKET_BIT: u64 = 6;
const LAYER_PER_BUCKET: usize = 1 << LAYER_PER_BUCKET_BIT as usize;
const LAYER_PER_BUCKET_MASK: u64 = LAYER_PER_BUCKET as u64 - 1;
const MAX_LAYER_PER_BUCKET: usize =
  (usize::MAX.ilog2() as usize).div_ceil(LAYER_PER_BUCKET_BIT as usize);

struct Task<T> {
  execute_at: u64,
  data: T,
}
impl<T> Task<T> {
  const fn new(data: T, execute_at: u64) -> Self {
    Self { execute_at, data }
  }
  #[inline]
  const fn get_bucket_index(&self, layer_index: u64) -> u64 {
    (self.execute_at >> (layer_index * LAYER_PER_BUCKET_BIT)) & LAYER_PER_BUCKET_MASK
  }
  #[inline]
  const fn layer_size(&self) -> usize {
    (64 - self.execute_at.leading_zeros() as u64).div_ceil(LAYER_PER_BUCKET_BIT) as usize
  }
  fn take(self) -> T {
    self.data
  }
}

type Bucket<T> = Vec<Task<T>>;

struct BucketLayer<T> {
  buckets: [Option<Bucket<T>>; LAYER_PER_BUCKET],
  layer_index: u64,
  size: usize,
}
impl<T> BucketLayer<T> {
  #[inline]
  const fn new(layer_index: u64) -> Self {
    Self {
      buckets: [const { None }; LAYER_PER_BUCKET],
      layer_index,
      size: 0,
    }
  }

  #[inline]
  fn insert(&mut self, task: Task<T>) {
    let bucket = task.get_bucket_index(self.layer_index) as usize;
    self.buckets[bucket].get_or_insert_default().push(task);
    self.size += 1;
  }

  #[inline]
  const fn is_empty(&self) -> bool {
    self.size == 0
  }

  #[inline]
  fn dropdown(&mut self, bucket: usize) -> Option<Bucket<T>> {
    let tasks = self.buckets[bucket].take()?;
    self.size -= tasks.len();
    Some(tasks)
  }
}

/**
 * Hierarchical timing wheel for scheduling timeouts.
 * execute_at is stored as milliseconds elapsed since the wheel's last reset.
 * The number of layers grows with the magnitude of execute_at (6 bits per layer),
 * so the timer is reset whenever the wheel becomes empty — keeping execute_at
 * values small and the layer count minimal.
 */
struct TimingWheel<T> {
  layers: Vec<BucketLayer<T>>,
  tasks: usize,
}
impl<T> TimingWheel<T> {
  fn new() -> Self {
    Self {
      layers: Vec::with_capacity(MAX_LAYER_PER_BUCKET),
      tasks: 0,
    }
  }

  fn register(&mut self, data: T, execute_at: NonZero<u64>) {
    let task = Task::new(data, execute_at.get());
    let layer_size = task.layer_size();
    for len in self.layers.len()..layer_size {
      self.layers.push(BucketLayer::new(len as u64));
    }

    self.layers[layer_size - 1].insert(task);
    self.tasks += 1;
  }

  fn tick(&mut self, current: u64) -> Option<impl Iterator<Item = T> + '_> {
    let mut dropdown: Option<Bucket<T>> = None;
    for (i, layer) in self.layers.iter_mut().enumerate().rev() {
      match (layer.is_empty(), dropdown.take()) {
        (true, None) => continue,
        (_, Some(tasks)) => tasks.into_iter().for_each(|task| layer.insert(task)),
        _ => {}
      }

      let index = (current >> (i as u64 * LAYER_PER_BUCKET_BIT)) & LAYER_PER_BUCKET_MASK;
      dropdown = layer.dropdown(index as usize);
    }

    while let Some(true) = self.layers.last().map(|l| l.is_empty()) {
      self.layers.pop();
    }

    let tasks = dropdown?;
    self.tasks -= tasks.len();
    if self.tasks == 0 {
      self.layers.clear();
    }
    Some(tasks.into_iter().map(|t| t.take()))
  }

  #[inline]
  const fn is_empty(&self) -> bool {
    self.tasks == 0
  }
}

enum Context {
  Register(TxId, Duration),
  Term,
}

const fn handle_timeout(version_controller: Arc<VersionController>) -> impl Fn(TxId) {
  move |tx_id: TxId| {
    let Some(state) = version_controller.get_active_state(tx_id) else {
      return;
    };
    if !state.try_timeout() {
      return;
    }
    warn!("tx {} timeout reached", state.get_id());

    version_controller.set_abort(state.get_id());
    state.deactive();
  }
}

const fn handle_thread(
  version_controller: Arc<VersionController>,
  queue: Arc<MpscQueue<Context>>,
) -> impl FnOnce() {
  move || {
    let mut wheel = TimingWheel::new();
    let handle = handle_timeout(version_controller);

    let backoff = Backoff::new();
    let mut peeked = None;
    let mut next_tick = Instant::now() + TICK_SIZE;
    let mut standard = Instant::now();
    loop {
      for ctx in (0u8..32).map_while(|_| peeked.take().or_else(|| unsafe { queue.pop() }))
      {
        backoff.reset();
        let (id, timeout) = match ctx {
          Context::Register(id, timeout) => (id, timeout),
          Context::Term => return,
        };
        if wheel.is_empty() {
          standard = Instant::now();
        }

        let Some(execute_at) =
          NonZero::new((Instant::now() + timeout - standard).as_millis() as u64)
        else {
          handle(id);
          continue;
        };
        wheel.register(id, execute_at);
      }

      let now = Instant::now();
      while !wheel.is_empty() && next_tick <= now {
        let current = (next_tick - standard).as_millis() as u64;
        for id in wheel.tick(current).into_iter().flatten() {
          handle(id);
        }
        next_tick += TICK_SIZE;
      }

      if let Some(ctx) = unsafe { queue.pop() } {
        peeked = Some(ctx);
        continue;
      }

      if !backoff.is_completed() {
        backoff.snooze();
        continue;
      }

      if wheel.is_empty() {
        debug!("timeout thread switches to idle.");
        park();
        debug!("timeout thread wake up.");
        next_tick = Instant::now();
        continue;
      }

      if let Some(dur) = next_tick.checked_duration_since(Instant::now()) {
        park_timeout(dur);
      }
    }
  }
}

/**
 * Aborts transactions that exceed their timeout.
 * Uses a timing wheel internally to schedule abort callbacks efficiently.
 * The thread idles when no transactions are registered, waking on the first registration.
 */
pub struct TimeoutThread {
  queue: Arc<MpscQueue<Context>>,
  waker: Thread,
  slot: ThreadSlot,
}
impl TimeoutThread {
  pub fn new(version_controller: Arc<VersionController>) -> Self {
    let queue = Arc::new(MpscQueue::new());
    let handle = Builder::new()
      .name("timeout".to_string())
      .stack_size(2 << 20)
      .spawn_unwind(handle_thread(version_controller, queue.clone()));
    let waker = handle.thread().clone();
    Self {
      queue,
      waker,
      slot: ThreadSlot::new(handle),
    }
  }

  pub fn register(&self, id: TxId, timeout: Duration) {
    self.queue.push(Context::Register(id, timeout));
    self.waker.unpark();
  }

  pub fn close(&self) {
    let Some(handle) = self.slot.close() else {
      return;
    };
    self.queue.push(Context::Term);
    handle.thread().unpark();
    handle.join().unwrap();
  }
}
