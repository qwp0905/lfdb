use super::{TryWaitError, WaitDisconnectedError};
use crate::utils::loom::{thread::park, Backoff};

pub trait Selectable: Sized {
  type Output;
  fn try_wait(self) -> Result<Self::Output, TryWaitError<Self>>;
  fn try_park(&self) -> bool;
}

pub struct Select<S> {
  tasks: Vec<S>,
}
impl<S> Select<S> {
  #[allow(dead_code)]
  pub const fn new() -> Self {
    Self { tasks: Vec::new() }
  }
  pub fn with_capacity(capacity: usize) -> Self {
    Self {
      tasks: Vec::with_capacity(capacity),
    }
  }
  pub fn push(&mut self, selectable: S) {
    self.tasks.push(selectable);
  }
  pub const fn len(&self) -> usize {
    self.tasks.len()
  }
  pub const fn is_empty(&self) -> bool {
    self.tasks.is_empty()
  }
}
impl<S: Selectable> Select<S> {
  fn try_select(&mut self) -> Option<Result<S::Output, WaitDisconnectedError>> {
    let len = self.tasks.len();
    for i in 0..len {
      let task = self.tasks.swap_remove(i);
      let task = match task.try_wait() {
        Ok(v) => return Some(Ok(v)),
        Err(TryWaitError::Disconnected) => return Some(Err(WaitDisconnectedError)),
        Err(TryWaitError::Empty(err)) => err,
      };
      self.tasks.push(task);
      self.tasks.swap(i, len - 1);
    }
    None
  }

  pub fn select(&mut self) -> Result<S::Output, WaitDisconnectedError> {
    if self.is_empty() {
      return Err(WaitDisconnectedError);
    }

    let backoff = Backoff::new();
    loop {
      while !backoff.is_completed() {
        if let Some(result) = self.try_select() {
          return result;
        }
        backoff.snooze();
      }

      if let Some(i) = self.tasks.iter().position(|task| !task.try_park()) {
        return match self.tasks.swap_remove(i).try_wait() {
          Ok(v) => Ok(v),
          Err(TryWaitError::Disconnected) => Err(WaitDisconnectedError),
          Err(TryWaitError::Empty(_)) => unreachable!(),
        };
      }

      park();
      backoff.snooze();
    }
  }
}

#[cfg(all(test, not(loom)))]
#[path = "tests/select.rs"]
mod tests;

#[cfg(all(test, loom))]
#[path = "tests/select_loom.rs"]
mod tests;
