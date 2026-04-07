// use std::{
//   cell::UnsafeCell,
//   panic::{catch_unwind, UnwindSafe},
//   sync::Arc,
//   thread::{Builder, JoinHandle},
// };

// use crossbeam::{
//   channel::{unbounded, Receiver, Sender, TryRecvError},
//   utils::Backoff,
// };

// use super::{oneshot, Oneshot};
// use crate::{
//   utils::{ToArc, ToBox, UnsafeBorrowMut, UnwrappedSender},
//   Error, Result,
// };

// enum Task {
//   Done,
//   Work(Box<dyn FnOnce() + Send>),
// }
// impl Task {
//   #[inline]
//   fn work<F: FnOnce() + Send + 'static>(f: F) -> Self {
//     Task::Work(f.to_box())
//   }
// }

// pub struct TaskHandle<T>(Oneshot<Result<T>>);
// impl<T> TaskHandle<T> {
//   #[inline]
//   pub fn wait(self) -> Result<T> {
//     self.0.wait()?
//   }
// }

// fn worker_loop(receiver: Receiver<Task>) -> impl Fn() {
//   move || {
//     let backoff = Backoff::new();
//     while let Ok(Task::Work(task)) = receiver.recv() {
//       task();
//       backoff.reset();

//       while !backoff.is_completed() {
//         match receiver.try_recv() {
//           Ok(Task::Work(task)) => {
//             task();
//             backoff.reset();
//           }
//           Ok(Task::Done) | Err(TryRecvError::Disconnected) => return,
//           Err(TryRecvError::Empty) => backoff.snooze(),
//         }
//       }
//     }
//   }
// }

// /**
//  * Multiple worker threads sharing a single channel for task distribution.
//  * Suitable for tasks that require burst throughput but have long idle periods.
//  */
// pub struct RuntimeInner {
//   threads: UnsafeCell<Vec<JoinHandle<()>>>,
//   queue: Sender<Task>,
// }
// impl RuntimeInner {
//   pub fn new(size: usize, count: usize) -> Self {
//     let (tx, rx) = unbounded();
//     let mut threads = Vec::with_capacity(count);
//     for i in 0..count {
//       let thread = Builder::new()
//         .name(format!("thread-runtime-{i}"))
//         .stack_size(size)
//         .spawn(worker_loop(rx.clone()))
//         .unwrap();

//       threads.push(thread);
//     }

//     Self {
//       queue: tx,
//       threads: UnsafeCell::new(threads),
//     }
//   }

//   pub fn submit<T, F>(&self, f: F) -> TaskHandle<T>
//   where
//     T: Send + 'static,
//     F: FnOnce() -> T + Send + UnwindSafe + 'static,
//   {
//     let (oneshot, fulfill) = oneshot();
//     let work = || catch_unwind(f).map_err(Arc::from).map_err(Error::panic);
//     let task = Task::work(|| fulfill.fulfill(work()));
//     self.queue.must_send(task);
//     TaskHandle(oneshot)
//   }

//   pub fn close(&self) {
//     let threads = self.threads.get().borrow_mut_unsafe();
//     for _ in 0..threads.len() {
//       self.queue.must_send(Task::Done);
//     }
//     for th in threads.drain(..) {
//       let _ = th.join();
//     }
//   }
// }

// unsafe impl Send for RuntimeInner {}
// unsafe impl Sync for RuntimeInner {}

// pub struct Runtime(Arc<RuntimeInner>);
// impl Runtime {
//   #[inline]
//   pub fn new(size: usize, count: usize) -> Self {
//     Self(RuntimeInner::new(size, count).to_arc())
//   }

//   #[inline]
//   pub fn submit<T, F>(&self, task: F) -> TaskHandle<T>
//   where
//     T: Send + 'static,
//     F: FnOnce() -> T + Send + UnwindSafe + 'static,
//   {
//     self.0.submit(task)
//   }

//   #[inline]
//   pub fn close(&self) {
//     self.0.close();
//   }
// }
