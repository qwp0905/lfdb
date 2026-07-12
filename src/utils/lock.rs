/**
 * Short aliases for lock acquisition inside engine code.
 *
 * These helpers intentionally unwrap poisoned locks. A poisoned lock means a
 * panic occurred while holding the lock, which is treated as a programming
 * error rather than a recoverable engine state. Code using these helpers should
 * be written so poisoning never occurs during normal operation.
 */
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/**
 * Extension trait for `Mutex::lock().unwrap()`.
 */
pub trait ShortenedMutex<T: ?Sized> {
  fn l(&self) -> MutexGuard<'_, T>;
}
impl<T: ?Sized> ShortenedMutex<T> for Mutex<T> {
  #[inline(always)]
  fn l(&self) -> MutexGuard<'_, T> {
    self.lock().unwrap()
  }
}

/**
 * Extension trait for `RwLock::{read, write}().unwrap()`.
 */
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
