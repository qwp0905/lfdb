use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

const SPIN_LIMIT: usize = 7;

fn spin(count: usize) {
  for _ in 0..(1 << count) {
    std::hint::spin_loop();
  }
}

pub trait ShortenedMutex<T: ?Sized> {
  fn l(&self) -> MutexGuard<'_, T>;
}
impl<T: ?Sized> ShortenedMutex<T> for Mutex<T> {
  #[inline(always)]
  fn l(&self) -> MutexGuard<'_, T> {
    self.lock().unwrap()
  }
}

pub trait SpinningWait<T: ?Sized> {
  fn spin_lock(&self) -> MutexGuard<'_, T>;
}
impl<T: ?Sized> SpinningWait<T> for Mutex<T> {
  #[inline]
  fn spin_lock(&self) -> MutexGuard<'_, T> {
    let mut backoff = 0;

    while backoff < SPIN_LIMIT {
      if let Ok(guard) = self.try_lock() {
        return guard;
      }
      spin(backoff);
      backoff += 1;
    }

    self.lock().unwrap()
  }
}

pub trait SpinningRwWait<T: ?Sized> {
  fn spin_rl(&self) -> RwLockReadGuard<'_, T>;
  fn spin_wl(&self) -> RwLockWriteGuard<'_, T>;
}
impl<T: ?Sized> SpinningRwWait<T> for RwLock<T> {
  #[inline]
  fn spin_rl(&self) -> RwLockReadGuard<'_, T> {
    let mut backoff = 0;

    while backoff < SPIN_LIMIT {
      if let Ok(guard) = self.try_read() {
        return guard;
      }
      spin(backoff);
      backoff += 1;
    }

    self.read().unwrap()
  }

  #[inline]
  fn spin_wl(&self) -> RwLockWriteGuard<'_, T> {
    let mut backoff = 0;

    while backoff < SPIN_LIMIT {
      if let Ok(guard) = self.try_write() {
        return guard;
      }
      spin(backoff);
      backoff += 1;
    }

    self.write().unwrap()
  }
}
