use std::{
  cell::UnsafeCell,
  ops::Index,
  sync::{Arc, Weak},
  thread::{Builder, JoinHandle},
  time::{Duration, Instant},
};

use crossbeam::{
  channel::{tick, unbounded, Sender},
  select,
};

use crate::utils::{LogFilter, UnsafeBorrowMut};

use super::{TxState, VersionVisibility};

const TICK_SIZE: Duration = Duration::from_millis(1);

const LAYER_PER_BUCKET_BIT: usize = 6;
const LAYER_PER_BUCKET: usize = 1 << LAYER_PER_BUCKET_BIT;
const LAYER_PER_BUCKET_MASK: usize = LAYER_PER_BUCKET - 1;
const MAX_LAYER_PER_BUCKET: usize =
  (usize::MAX.checked_ilog2().unwrap() as usize).div_ceil(LAYER_PER_BUCKET_BIT);

struct SystemTimer(Instant);
impl SystemTimer {
  #[inline]
  pub fn new() -> Self {
    Self(Instant::now())
  }

  #[inline]
  fn now(&self) -> Duration {
    self.0.elapsed()
  }

  #[inline]
  fn reset(&mut self) {
    self.0 = Instant::now();
  }
}

#[inline]
fn init_hands(hands: &mut [u8; MAX_LAYER_PER_BUCKET], timestamp: usize) -> usize {
  let mut current = timestamp;
  for len in 0..MAX_LAYER_PER_BUCKET {
    if current == 0 {
      return len;
    }
    hands[len] = (current & LAYER_PER_BUCKET_MASK) as u8;
    current >>= LAYER_PER_BUCKET_BIT;
  }
  MAX_LAYER_PER_BUCKET
}

struct ClockHands {
  hands: [u8; MAX_LAYER_PER_BUCKET],
  len: usize,
  timestamp: Duration,
}
impl ClockHands {
  #[inline]
  pub fn new(timestamp: Duration) -> Self {
    let mut hands = [0; MAX_LAYER_PER_BUCKET];
    let len = init_hands(&mut hands, timestamp.as_millis() as usize);
    Self {
      hands,
      len,
      timestamp,
    }
  }

  #[inline]
  fn reset(&mut self) {
    self.len = 0;
    self.timestamp = Duration::ZERO;
  }

  #[inline]
  fn len(&self) -> usize {
    self.len
  }

  #[inline]
  fn advance_until(&mut self, timestamp: Duration) -> bool {
    if self.timestamp >= timestamp {
      return false;
    }

    self.timestamp += TICK_SIZE;
    for i in self.hands.iter_mut().take(self.len) {
      if *i < LAYER_PER_BUCKET_MASK as u8 {
        *i += 1;
        return true;
      }

      *i = 0;
    }

    self.hands[self.len] = 1;
    self.len += 1;
    true
  }

  #[inline]
  fn get(&self, index: usize) -> Option<usize> {
    if index >= self.len {
      return None;
    }

    Some(self.hands[index] as usize)
  }
}

impl Index<usize> for ClockHands {
  type Output = u8;

  #[inline]
  fn index(&self, index: usize) -> &Self::Output {
    &self.hands[index]
  }
}

impl Default for ClockHands {
  #[inline]
  fn default() -> Self {
    Self::new(Duration::ZERO)
  }
}

struct Task<T> {
  clock_hands: ClockHands,
  data: T,
}
impl<T> Task<T> {
  fn new(data: T, scheduled_at: Duration, delay: Duration) -> Self {
    Self {
      clock_hands: ClockHands::new(scheduled_at + delay),
      data,
    }
  }
  #[inline]
  fn get_bucket_index(&self, layer_index: usize) -> usize {
    self.clock_hands[layer_index] as usize
  }
  #[inline]
  fn layer_size(&self) -> usize {
    self.clock_hands.len()
  }
  fn take(self) -> T {
    self.data
  }
}

type Bucket<T> = Vec<Task<T>>;

struct BucketLayer<T> {
  buckets: [Option<Bucket<T>>; LAYER_PER_BUCKET],
  layer_index: usize,
  size: usize,
}
impl<T> BucketLayer<T> {
  #[inline]
  fn new(layer_index: usize) -> Self {
    Self {
      buckets: [const { None }; LAYER_PER_BUCKET],
      layer_index,
      size: 0,
    }
  }

  #[inline]
  fn insert(&mut self, task: Task<T>) {
    let bucket = task.get_bucket_index(self.layer_index);
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

struct TimingWheel<T, F> {
  layers: Vec<BucketLayer<T>>,
  clock_hands: ClockHands,
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
      clock_hands: Default::default(),
      timer: SystemTimer::new(),
      tasks: 0,
      handle,
    }
  }
  #[inline]
  fn reset(&mut self) {
    self.timer.reset();
    self.clock_hands.reset();
  }
  fn register(&mut self, data: T, delay: Duration) {
    if self.tasks == 0 {
      self.reset();
    }

    let task = Task::new(data, self.timer.now(), delay);

    let layer_size = task.layer_size();
    for len in self.layers.len()..layer_size {
      self.layers.push(BucketLayer::new(len));
    }

    self.layers[layer_size - 1].insert(task);
    self.tasks += 1;
  }

  fn tick(&mut self) {
    let now = self.timer.now();

    while self.clock_hands.advance_until(now) {
      match self.dropdown() {
        Some(tasks) => self.handle_tasks(tasks),
        None => continue,
      }

      if self.tasks == 0 {
        self.layers.clear();
        break;
      }
    }
  }

  #[inline]
  fn dropdown(&mut self) -> Option<Bucket<T>> {
    let mut dropdown: Option<Bucket<T>> = None;

    for (i, layer) in self.layers.iter_mut().enumerate().rev() {
      match (layer.is_empty(), dropdown.take()) {
        (true, None) => continue,
        (_, Some(tasks)) => tasks.into_iter().for_each(|task| layer.insert(task)),
        _ => {}
      }

      dropdown = self.clock_hands.get(i).and_then(|i| layer.dropdown(i));
    }

    while let Some(true) = self.layers.last().map(|l| l.is_empty()) {
      self.layers.pop();
    }
    dropdown
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
  Register(Weak<TxState>, Duration),
  Term,
}

/**
 * Timeout Threads
 * Manage the timeout for each transaction
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
        let mut wheel = TimingWheel::new(move |weak: Weak<TxState>| {
          let state = match weak.upgrade() {
            Some(s) => s,
            None => return,
          };
          if !state.try_abort() {
            return;
          }
          logger_c.debug(format!("tx {} timeout reached", state.get_id()));
          version_visibility.move_to_abort(state.get_id())
        });
        let ticker = tick(TICK_SIZE);

        while let Ok(ctx) = rx.recv() {
          match ctx {
            Msg::Register(state, timeout) => wheel.register(state, timeout),
            Msg::Term => return,
          }
          logger.debug("timeout thread wake up.");

          while !wheel.is_empty() {
            select! {
              recv(ticker) -> _ => wheel.tick(),
              recv(rx) -> msg => match msg {
                Ok(Msg::Register(state,timeout)) => wheel.register(state, timeout),
                Err(_) | Ok(Msg::Term) => return,
              }
            }
          }
          logger.debug("timeout thread switches to idle.");
        }
      })
      .unwrap();

    Self {
      handle: UnsafeCell::new(Some(th)),
      channel: tx,
    }
  }

  pub fn register(&self, state: Weak<TxState>, timeout: Duration) {
    self.channel.send(Msg::Register(state, timeout)).unwrap();
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
