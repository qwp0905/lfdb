use std::{marker::PhantomData, panic::UnwindSafe};

use crossbeam::channel::{unbounded, Sender};

use crate::{utils::UnwrappedSender, Result};

use super::{oneshot, OneshotFulfill, SharedFn, Spawner, TaskHandle};

pub struct SubPool<'a, T, R> {
  handles: Vec<TaskHandle>,
  queue: Sender<(T, OneshotFulfill<Result<R>>)>,
  _marker: PhantomData<&'a Spawner>,
}
impl<'a, T, R> SubPool<'a, T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn new(spawner: &'a Spawner, task: SharedFn<'static, T, R>, count: usize) -> Self {
    let (tx, rx) = unbounded::<(T, OneshotFulfill<Result<R>>)>();
    let handles = (0..count)
      .map(|_| {
        let task = task.clone();
        let rx = rx.clone();
        spawner.spawn(move || {
          while let Ok((v, done)) = rx.recv() {
            done.fulfill(task.call(v))
          }
        })
      })
      .collect::<Vec<_>>();
    Self {
      handles,
      queue: tx,
      _marker: PhantomData,
    }
  }

  pub fn push(&self, v: T) -> TaskHandle<R> {
    let (o, f) = oneshot();
    self.queue.must_send((v, f));
    TaskHandle::new(o)
  }

  pub fn join(self) -> Result {
    drop(self.queue);
    self
      .handles
      .into_iter()
      .map(|handle| handle.wait())
      .collect::<Result>()?;
    Ok(())
  }
}

#[cfg(test)]
#[path = "tests/pool.rs"]
mod tests;
