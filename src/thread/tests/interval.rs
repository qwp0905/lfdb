use super::*;
use crate::utils::ToArc;
use crate::Error;
use crossbeam::channel::{bounded, Sender};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_STACK_SIZE: usize = 64 << 10;

fn make() -> (Arc<Spawner>, Arc<TimerThread>) {
  let spawner = Spawner::new(DEFAULT_STACK_SIZE, 4).to_arc();
  let timer = TimerThread::new(spawner.clone()).to_arc();
  (spawner, timer)
}

fn shutdown<T, R>(
  thread: IntervalWorkThread<T, R>,
  spawner: Arc<Spawner>,
  timer: Arc<TimerThread>,
) where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  thread.close();
  timer.close();
  spawner.close();
}

#[test]
fn test_send_delivers_value_to_work() {
  let (spawner, timer) = make();

  let work = SharedFn::new(std::sync::Arc::new(|x: Option<usize>| match x {
    Some(v) => v * 3,
    None => 0,
  }));

  // Long timeout so timer-path doesn't interleave during this test.
  let thread =
    IntervalWorkThread::new(timer.clone(), spawner.clone(), Duration::from_secs(60), work);

  assert_eq!(thread.send(5usize).wait().unwrap(), 15);
  assert_eq!(thread.send(7usize).wait().unwrap(), 21);

  shutdown(thread, spawner, timer);
}

#[test]
fn test_timer_fires_work_with_none() {
  // Deterministic: signal on first None call via a bounded(1) channel,
  // wait for the signal. No sleep, no time assertion.
  let (spawner, timer) = make();

  let (tx, rx) = bounded::<()>(1);
  let tx: Arc<Sender<()>> = Arc::new(tx);
  let work = {
    let tx = tx.clone();
    SharedFn::new(std::sync::Arc::new(move |x: Option<()>| {
      if x.is_none() {
        // best-effort send; ignore if already filled.
        let _ = tx.try_send(());
      }
    }))
  };

  let thread = IntervalWorkThread::new(
    timer.clone(),
    spawner.clone(),
    Duration::from_millis(20),
    work,
  );

  // wait until the timer-path has invoked work(None) at least once.
  rx.recv().expect("timer never fired work(None)");

  shutdown(thread, spawner, timer);
}

#[test]
fn test_panic_in_work_returns_err() {
  let (spawner, timer) = make();

  let work = SharedFn::new(std::sync::Arc::new(|x: Option<i32>| -> i32 {
    match x {
      Some(v) if v < 0 => panic!("negative not allowed"),
      Some(v) => v * 2,
      None => 0,
    }
  }));

  let thread = IntervalWorkThread::new(
    timer.clone(),
    spawner.clone(),
    Duration::from_secs(60),
    work,
  );

  // normal path
  assert_eq!(thread.send(10).wait().unwrap(), 20);

  // suppress panic-hook output
  let prev = std::panic::take_hook();
  std::panic::set_hook(Box::new(|_| {}));

  let result = thread.send(-5).wait();
  assert!(matches!(result, Err(Error::Panic(_))));

  std::panic::set_hook(prev);

  // worker still alive after panic
  assert_eq!(thread.send(3).wait().unwrap(), 6);

  shutdown(thread, spawner, timer);
}

#[test]
fn test_send_results_in_order() {
  // Each send is serialized via the generation chain (prev.wait), so the
  // send-side ordering is preserved across handles.
  let (spawner, timer) = make();

  let counter = AtomicUsize::new(0).to_arc();
  let work = {
    let counter = counter.clone();
    SharedFn::new(std::sync::Arc::new(move |x: Option<usize>| match x {
      Some(_) => counter.fetch_add(1, Ordering::AcqRel),
      None => usize::MAX,
    }))
  };

  let thread = IntervalWorkThread::new(
    timer.clone(),
    spawner.clone(),
    Duration::from_secs(60),
    work,
  );

  let handles: Vec<_> = (0..10).map(|_| thread.send(0usize)).collect();
  let results: Vec<usize> = handles.into_iter().map(|h| h.wait().unwrap()).collect();

  // serialized: each call sees a strictly increasing counter value.
  assert_eq!(results, (0..10).collect::<Vec<_>>());

  shutdown(thread, spawner, timer);
}

#[test]
fn test_multiple_close() {
  let (spawner, timer) = make();

  let work = SharedFn::new(std::sync::Arc::new(|_: Option<()>| ()));
  let thread = IntervalWorkThread::new(
    timer.clone(),
    spawner.clone(),
    Duration::from_secs(60),
    work,
  );

  thread.close();
  thread.close();
  thread.close();
  timer.close();
  spawner.close();
}
