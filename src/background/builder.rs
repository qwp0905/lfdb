use super::ThreadPool;
#[cfg(not(target_os = "linux"))]
use super::{SharedFn, SharedWorkThread};

const DEFAULT_STACK_SIZE: usize = 64 << 10;

/**
 * Builder for the engine's multi-threaded background runtimes.
 *
 * `ThreadBuilder` stores common thread settings such as name, stack size and
 * worker count, then creates either a `ThreadPool` or a `SharedWorkThread`.
 */
pub struct ThreadBuilder {
  name: String,
  stack_size: usize,
  count: usize,
}
impl ThreadBuilder {
  pub const fn new() -> Self {
    ThreadBuilder {
      name: String::new(),
      stack_size: DEFAULT_STACK_SIZE,
      count: 1,
    }
  }
  pub fn name<S: ToString>(mut self, name: S) -> Self {
    self.name = name.to_string();
    self
  }
  #[allow(unused)]
  pub const fn stack_size(mut self, size: usize) -> Self {
    self.stack_size = size;
    self
  }
  pub const fn count(mut self, count: usize) -> Self {
    self.count = count;
    self
  }

  pub fn pool(self) -> ThreadPool {
    ThreadPool::new(self.name, self.stack_size, self.count)
  }

  #[cfg(not(target_os = "linux"))]
  pub fn shared<T, F>(self, f: F) -> SharedWorkThread<T>
  where
    T: Send + 'static,
    F: Fn(T) + Send + Sync + 'static,
  {
    SharedWorkThread::new(self.name, self.stack_size, self.count, SharedFn::new(f))
  }
}
