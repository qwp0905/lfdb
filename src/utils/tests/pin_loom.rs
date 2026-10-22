use loom::{cell::UnsafeCell, sync::Arc, thread};

use super::*;

struct Guarded {
  pin: ExclusivePin,
  value: UnsafeCell<usize>,
}
impl Guarded {
  fn new() -> Arc<Self> {
    Arc::new(Self {
      pin: ExclusivePin::new(),
      value: UnsafeCell::new(0),
    })
  }

  fn read(&self) -> Option<usize> {
    let _token = self.pin.try_shared()?;
    Some(self.value.with(|v| unsafe { *v }))
  }

  fn increment(&self) -> bool {
    let Some(_token) = self.pin.try_exclusive() else {
      return false;
    };
    self.value.with_mut(|v| unsafe { *v += 1 });
    true
  }

  fn is_released(&self) -> bool {
    self.pin.try_exclusive().is_some()
  }
}
unsafe impl Sync for Guarded {}

#[test]
fn shared_and_exclusive_exclude_each_other() {
  loom::model(|| {
    let guarded = Guarded::new();
    let reader = {
      let guarded = guarded.clone();
      thread::spawn(move || guarded.read())
    };

    guarded.increment();
    reader.join().unwrap();

    assert!(guarded.is_released());
  });
}

/*
 * The exclusive acquirer reads the count written by the last shared release,
 * so the earlier shared release must reach it through the release sequence of
 * the `fetch_sub` chain.
 */
#[test]
fn every_shared_release_is_visible_to_exclusive() {
  loom::model(|| {
    let guarded = Guarded::new();
    let readers: Vec<_> = (0..2)
      .map(|_| {
        let guarded = guarded.clone();
        thread::spawn(move || guarded.read())
      })
      .collect();

    guarded.increment();
    for reader in readers {
      reader.join().unwrap();
    }

    assert!(guarded.is_released());
  });
}

#[test]
fn exclusive_holders_observe_previous_writes() {
  loom::model(|| {
    let guarded = Guarded::new();
    let writer = {
      let guarded = guarded.clone();
      thread::spawn(move || guarded.increment())
    };

    let succeeded = guarded.increment() as usize + writer.join().unwrap() as usize;

    assert_eq!(guarded.read(), Some(succeeded));
    assert!(guarded.is_released());
  });
}

#[test]
fn downgrade_publishes_writes_and_blocks_exclusive() {
  loom::model(|| {
    let guarded = Guarded::new();
    let exclusive = guarded.pin.try_exclusive().unwrap();

    let reader = {
      let guarded = guarded.clone();
      thread::spawn(move || guarded.read())
    };
    let writer = {
      let guarded = guarded.clone();
      thread::spawn(move || guarded.increment())
    };

    guarded.value.with_mut(|v| unsafe { *v = 1 });
    let shared = exclusive.downgrade();
    assert_eq!(guarded.value.with(|v| unsafe { *v }), 1);
    drop(shared);

    if let Some(value) = reader.join().unwrap() {
      assert!(value >= 1);
    }
    writer.join().unwrap();

    assert!(guarded.is_released());
  });
}

#[test]
fn only_last_shared_release_reports_last() {
  loom::lazy_static! {
    static ref PIN: ExclusivePin = ExclusivePin::new();
  }

  loom::model(|| {
    let first = PIN.try_shared().unwrap();
    let second = PIN.try_shared().unwrap();

    let other = thread::spawn(move || first.drop_in_place());
    let last = second.drop_in_place() as usize + other.join().unwrap() as usize;

    assert_eq!(last, 1);
    assert!(PIN.try_exclusive().is_some());
  });
}

#[test]
fn drop_in_place_releases_to_exclusive() {
  loom::model(|| {
    let guarded = Guarded::new();
    let reader = {
      let guarded = guarded.clone();
      thread::spawn(move || {
        if let Some(token) = guarded.pin.try_shared() {
          guarded.value.with(|v| unsafe { *v });
          token.drop_in_place();
        }
      })
    };

    guarded.increment();
    reader.join().unwrap();

    assert!(guarded.is_released());
  });
}

#[test]
fn shared_never_fails_without_exclusive() {
  loom::model(|| {
    let guarded = Guarded::new();
    let reader = {
      let guarded = guarded.clone();
      thread::spawn(move || guarded.read())
    };

    assert!(guarded.read().is_some());
    assert!(reader.join().unwrap().is_some());
    assert!(guarded.is_released());
  });
}
