// use super::*;
// use std::sync::atomic::{AtomicUsize, Ordering};
// use std::sync::Mutex;
// use std::thread;
// use std::time::Duration;

// const DEFAULT_STACK_SIZE: usize = 64 << 10;

// #[test]
// fn test_no_timeout() {
//   let m: Arc<Mutex<usize>> = Default::default();
//   let mc = m.clone();
//   let c = AtomicUsize::new(0);
//   let counter = Arc::new(AtomicUsize::new(0));
//   let counter_clone = counter.clone();

//   let work = move |x: usize| {
//     let cc = c.fetch_add(1, Ordering::Release);
//     let mut m = m.lock().unwrap();
//     *m = m.max(cc + 1);
//     drop(m);
//     thread::sleep(Duration::from_millis(100));
//     counter_clone.fetch_add(x, Ordering::Release);
//     c.fetch_sub(1, Ordering::Release);
//     x * 2
//   };

//   let thread_count = 4;
//   let thread =
//     SharedWorkThread::new("test-no-timeout", DEFAULT_STACK_SIZE, thread_count, work);

//   // Send multiple tasks
//   let receivers: Vec<_> = (1..=thread_count).map(|i| thread.send(i)).collect();
//   let results = receivers
//     .into_iter()
//     .map(|receiver| receiver.wait().expect("closed"))
//     .collect::<Vec<usize>>();

//   assert_eq!(results, vec![2, 4, 6, 8]);
//   assert_eq!(counter.load(Ordering::Acquire), 10); // 1+2+3+4 = 10
//   assert_eq!(*mc.lock().unwrap(), thread_count);

//   thread.close();
// }

// #[test]
// fn test_multiple_threads() {
//   let max = Arc::new(Mutex::new(0));
//   let count = AtomicUsize::new(0);
//   let max_c = max.clone();

//   let work = move |_| {
//     let c = count.fetch_add(1, Ordering::Release);
//     let mut m = max.lock().unwrap();
//     *m = m.max(c + 1);
//     drop(m);
//     thread::sleep(Duration::from_millis(10));
//     count.fetch_sub(1, Ordering::Release);
//   };

//   let thread_count = 4;
//   let thread =
//     SharedWorkThread::new("test-multi", DEFAULT_STACK_SIZE, thread_count, work);

//   let receivers: Vec<_> = (0..(thread_count << 1)).map(|i| thread.send(i)).collect();

//   // Collect all results
//   for receiver in receivers.into_iter() {
//     let _ = receiver.wait().unwrap();
//   }

//   assert_eq!(*max_c.lock().unwrap(), thread_count);

//   thread.close();
// }

// #[test]
// fn test_multiple_close() {
//   let thread_count = 4;
//   let work = |_: ()| {};
//   let thread =
//     SharedWorkThread::new("test-multi-close", DEFAULT_STACK_SIZE, thread_count, work);

//   thread.close();
//   thread.close();
//   thread.close();
// }
