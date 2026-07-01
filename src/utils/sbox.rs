use std::{
  mem::MaybeUninit,
  ops::Deref,
  ptr::NonNull,
  sync::atomic::{fence, AtomicUsize, Ordering},
};

#[repr(C, align(2))]
struct Inner<T: ?Sized> {
  count: AtomicUsize,
  data: T,
}
impl<T> Inner<T> {
  const fn new(data: T) -> Self {
    Self {
      count: AtomicUsize::new(1),
      data,
    }
  }
}

/**
 * Strong-only variant of `Arc`.
 *
 * `SBox` is intentionally modeled after `std::sync::Arc`: it uses the same
 * strong reference-counting flow, the same clone/drop ordering pattern, and the
 * same `get_mut` semantics. The main difference is that `SBox` has no weak
 * reference count, so the allocation is reclaimed as soon as the last strong
 * owner is dropped.
 *
 * Use `Arc` as the reference model for the safety and memory-ordering rules;
 * this type exists only to remove weak-reference overhead in engine-internal
 * paths that do not need `Weak`.
 */
pub struct SBox<T: ?Sized> {
  inner: NonNull<Inner<T>>,
}
impl<T> SBox<T> {
  pub fn new(data: T) -> Self {
    Self {
      inner: NonNull::from_mut(Box::leak(Box::new(Inner::new(data)))),
    }
  }

  pub fn get_mut(this: &mut SBox<T>) -> Option<&mut T> {
    let inner = unsafe { this.inner.as_mut() };
    if inner.count.load(Ordering::Acquire) > 1 {
      return None;
    }
    Some(&mut inner.data)
  }
}
impl<T: ?Sized> Clone for SBox<T> {
  fn clone(&self) -> Self {
    if unsafe { self.inner.as_ref() }
      .count
      .fetch_add(1, Ordering::Relaxed)
      == usize::MAX
    {
      std::process::abort();
    };
    Self { inner: self.inner }
  }
}

impl<T: ?Sized> Drop for SBox<T> {
  fn drop(&mut self) {
    if unsafe { self.inner.as_ref() }
      .count
      .fetch_sub(1, Ordering::Release)
      > 1
    {
      return;
    }

    fence(Ordering::Acquire);
    let _ = unsafe { Box::from_raw(self.inner.as_ptr()) };
  }
}

unsafe impl<T: Send + Sync + ?Sized> Send for SBox<T> {}
unsafe impl<T: Send + Sync + ?Sized> Sync for SBox<T> {}

impl<T: ?Sized> Deref for SBox<T> {
  type Target = T;

  fn deref(&self) -> &T {
    unsafe { &self.inner.as_ref().data }
  }
}
impl<T> SBox<T> {
  pub fn new_uninit() -> SBox<MaybeUninit<T>> {
    SBox::new(MaybeUninit::uninit())
  }
}
impl<T> SBox<MaybeUninit<T>> {
  pub unsafe fn assume_init(self) -> SBox<T> {
    let inner = self.inner;
    std::mem::forget(self);
    SBox {
      inner: inner.cast(),
    }
  }
}

// impl<T: ?Sized> std::borrow::Borrow<T> for SArc<T> {
//   fn borrow(&self) -> &T {
//     &**self
//   }
// }
// impl<T: ?Sized> AsRef<T> for SBox<T> {
//   fn as_ref(&self) -> &T {
//     &**self
//   }
// }
// impl<T: ?Sized + std::fmt::Debug> std::fmt::Debug for SBox<T> {
//   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//     self.deref().fmt(f)
//   }
// }
// impl<T: ?Sized + std::fmt::Display> std::fmt::Display for SBox<T> {
//   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//     self.deref().fmt(f)
//   }
// }
// impl<T: Default> Default for SBox<T> {
//   fn default() -> Self {
//     Self::new(Default::default())
//   }
// }
// impl<T: core::error::Error + ?Sized> core::error::Error for SBox<T> {
//   #[allow(deprecated)]
//   fn cause(&self) -> Option<&dyn core::error::Error> {
//     core::error::Error::cause(self.deref())
//   }

//   fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
//     core::error::Error::source(self.deref())
//   }
// }

#[cfg(test)]
#[path = "tests/sbox.rs"]
mod tests;
