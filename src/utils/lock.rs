use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub trait ShortenedMutex<T: ?Sized> {
  fn l(&self) -> MutexGuard<'_, T>;
}
impl<T: ?Sized> ShortenedMutex<T> for Mutex<T> {
  #[inline(always)]
  fn l(&self) -> MutexGuard<'_, T> {
    self.lock().unwrap()
  }
}

pub trait ShortenedRwLock<T: ?Sized> {
  fn rl(&self) -> RwLockReadGuard<'_, T>;
  fn wl(&self) -> RwLockWriteGuard<'_, T>;
}
impl<T: ?Sized> ShortenedRwLock<T> for RwLock<T> {
  #[inline(always)]
  fn rl(&self) -> RwLockReadGuard<'_, T> {
    self.read().unwrap()
  }
  #[inline(always)]
  fn wl(&self) -> RwLockWriteGuard<'_, T> {
    self.write().unwrap()
  }
}
