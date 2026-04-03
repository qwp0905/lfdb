use std::{
  cell::UnsafeCell,
  mem::replace,
  sync::Arc,
  thread::{Builder, JoinHandle},
  time::{Duration, Instant},
};

use crossbeam::{
  channel::{tick, unbounded, Sender},
  select,
};

use crate::{
  utils::{LogFilter, UnsafeBorrowMut},
  wal::TxId,
};

use super::VersionVisibility;

const TICK_SIZE: Duration = Duration::from_millis(1);

const LAYER_PER_BUCKET_BIT: u64 = 6;
const LAYER_PER_BUCKET: usize = 1 << LAYER_PER_BUCKET_BIT as usize;
const LAYER_PER_BUCKET_MASK: u64 = LAYER_PER_BUCKET as u64 - 1;
const MAX_LAYER_PER_BUCKET: usize =
  (usize::MAX.ilog2() as usize).div_ceil(LAYER_PER_BUCKET_BIT as usize);

struct SystemTimer(Instant);
impl SystemTimer {
  #[inline]
  pub fn new() -> Self {
    Self(Instant::now())
  }

  #[inline]
  fn now(&self) -> u64 {
    self.0.elapsed().as_millis() as u64
  }

  #[inline]
  fn reset(&mut self) {
    self.0 = Instant::now();
  }
}

struct Task<T> {
  execute_at: u64,
  data: T,
}
impl<T> Task<T> {
  fn new(data: T, execute_at: u64) -> Self {
    Self { execute_at, data }
  }
  #[inline]
  fn get_bucket_index(&self, layer_index: u64) -> u64 {
    (self.execute_at >> (layer_index * LAYER_PER_BUCKET_BIT)) & LAYER_PER_BUCKET_MASK
  }
  #[inline]
  fn layer_size(&self) -> usize {
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
  fn new(layer_index: u64) -> Self {
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
  fn is_empty(&self) -> bool {
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
struct TimingWheel<T, F> {
  layers: Vec<BucketLayer<T>>,
  current: u64,
  timer: SystemTimer,
  tasks: usize,
  handle: F,
}
impl<T, F> TimingWheel<T, F>
where
  F: Fn(T),
{
  fn new(handle: F) -> Self {
    Self {
      layers: Vec::with_capacity(MAX_LAYER_PER_BUCKET),
      current: 0,
      timer: SystemTimer::new(),
      tasks: 0,
      handle,
    }
  }
  #[inline]
  fn reset(&mut self) {
    self.timer.reset();
    self.current = 0;
  }
  fn register(&mut self, data: T, delay: Duration) {
    if self.tasks == 0 {
      self.reset();
    }

    let execute_at = self.timer.now() + delay.as_millis() as u64;
    if execute_at == 0 {
      return (self.handle)(data);
    }

    let task = Task::new(data, execute_at);
    let layer_size = task.layer_size();
    for len in self.layers.len()..layer_size {
      self.layers.push(BucketLayer::new(len as u64));
    }

    self.layers[layer_size - 1].insert(task);
    self.tasks += 1;
  }

  fn tick(&mut self) {
    let now = self.timer.now();
    let mut dropdown: Option<Bucket<T>> = None;

    for current in replace(&mut self.current, now)..now {
      for (i, layer) in self.layers.iter_mut().enumerate().rev() {
        match (layer.is_empty(), dropdown.take()) {
          (true, None) => continue,
          (_, Some(tasks)) => tasks.into_iter().for_each(|task| layer.insert(task)),
          _ => {}
        }

        let index =
          (current >> (i as u64 * LAYER_PER_BUCKET_BIT)) & LAYER_PER_BUCKET_MASK;
        dropdown = layer.dropdown(index as usize);
      }

      while let Some(true) = self.layers.last().map(|l| l.is_empty()) {
        self.layers.pop();
      }

      match dropdown.take() {
        Some(tasks) => self.handle_tasks(tasks),
        None => continue,
      }

      if self.tasks == 0 {
        self.layers.clear();
        break;
      }
    }
  }

  fn handle_tasks(&mut self, tasks: Bucket<T>) {
    self.tasks -= tasks.len();
    tasks.into_iter().map(|t| t.take()).for_each(&self.handle);
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.tasks == 0
  }
}

enum Msg {
  Register(TxId, Duration),
  Term,
}

/**
 * Aborts transactions that exceed their timeout.
 * Uses a timing wheel internally to schedule abort callbacks efficiently.
 * The thread idles when no transactions are registered, waking on the first registration.
 */
pub struct TimeoutThread {
  handle: UnsafeCell<Option<JoinHandle<()>>>,
  channel: Sender<Msg>,
}
impl TimeoutThread {
  pub fn new(version_visibility: Arc<VersionVisibility>, logger: LogFilter) -> Self {
    let (tx, rx) = unbounded();
    let th = Builder::new()
      .name("timeout".to_string())
      .stack_size(2 << 20)
      .spawn(move || {
        let logger_c = logger.clone();
        let mut wheel = TimingWheel::new(move |tx_id: TxId| {
          let state = match version_visibility.get_active_state(tx_id) {
            Some(s) => s,
            None => return,
          };
          if !state.try_timeout() {
            return;
          }
          logger_c.trace(|| format!("tx {} timeout reached", state.get_id()));

          version_visibility.set_abort(state.get_id());
          state.deactive();
        });
        let ticker = tick(TICK_SIZE);

        while let Ok(ctx) = rx.recv() {
          match ctx {
            Msg::Register(id, timeout) => wheel.register(id, timeout),
            Msg::Term => return,
          }
          logger.debug(|| "timeout thread wake up.");

          while !wheel.is_empty() {
            select! {
              recv(ticker) -> _ => wheel.tick(),
              recv(rx) -> msg => match msg {
                Ok(Msg::Register(id, timeout)) => wheel.register(id, timeout),
                Err(_) | Ok(Msg::Term) => return,
              }
            }
          }
          logger.debug(|| "timeout thread switches to idle.");
        }
      })
      .unwrap();

    Self {
      handle: UnsafeCell::new(Some(th)),
      channel: tx,
    }
  }

  pub fn register(&self, id: TxId, timeout: Duration) {
    self.channel.send(Msg::Register(id, timeout)).unwrap();
  }

  pub fn close(&self) {
    if let Some(th) = self.handle.get().borrow_mut_unsafe().take() {
      let _ = self.channel.send(Msg::Term);
      let _ = th.join();
    }
  }
}
unsafe impl Send for TimeoutThread {}
unsafe impl Sync for TimeoutThread {}
