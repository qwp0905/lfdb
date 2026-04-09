// use std::time::{Duration, Instant};

// pub struct Timer {
//   duration: Duration,
//   point: Instant,
//   remain: Duration,
// }
// impl Timer {
//   pub fn new(duration: Duration) -> Self {
//     Self {
//       duration,
//       point: Instant::now(),
//       remain: duration,
//     }
//   }

//   pub fn check(&mut self) {
//     let now = Instant::now();
//     self.remain -= now.duration_since(self.point).min(self.remain);
//     self.point = now;
//   }

//   pub fn reset(&mut self) {
//     self.remain = self.duration;
//     self.point = Instant::now();
//   }

//   pub fn get_remain(&self) -> Duration {
//     self.remain
//   }
// }

// pub trait AsTimer {
//   fn as_timer(&self) -> Timer;
// }
// impl AsTimer for Duration {
//   fn as_timer(&self) -> Timer {
//     Timer::new(*self)
//   }
// }
