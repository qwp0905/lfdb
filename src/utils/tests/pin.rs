use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use super::*;

// ----- Basic single-holder behavior -----

#[test]
fn fresh_pin_accepts_shared() {
  let pin = ExclusivePin::new();
  assert!(pin.try_shared().is_some());
}

#[test]
fn fresh_pin_accepts_exclusive() {
  let pin = ExclusivePin::new();
  assert!(pin.try_exclusive().is_some());
}

#[test]
fn fresh_pin_is_not_exclusive() {
  let pin = ExclusivePin::new();
  assert!(!pin.is_exclusive());
}

#[test]
fn exclusive_reports_is_exclusive() {
  let pin = ExclusivePin::new();
  let _e = pin.try_exclusive().unwrap();
  assert!(pin.is_exclusive());
}

// ----- Mutual exclusion -----

#[test]
fn shared_blocks_exclusive() {
  let pin = ExclusivePin::new();
  let _s = pin.try_shared().unwrap();
  assert!(pin.try_exclusive().is_none());
}

#[test]
fn exclusive_blocks_shared() {
  let pin = ExclusivePin::new();
  let _e = pin.try_exclusive().unwrap();
  assert!(pin.try_shared().is_none());
}

#[test]
fn exclusive_blocks_another_exclusive() {
  let pin = ExclusivePin::new();
  let _e = pin.try_exclusive().unwrap();
  assert!(pin.try_exclusive().is_none());
}

// ----- Shared can coexist -----

#[test]
fn many_shared_coexist() {
  let pin = ExclusivePin::new();
  let s1 = pin.try_shared().unwrap();
  let s2 = pin.try_shared().unwrap();
  let s3 = pin.try_shared().unwrap();
  // all three alive simultaneously
  drop(s1);
  drop(s2);
  drop(s3);
}

// ----- Drop releases -----

#[test]
fn sole_shared_drop_allows_exclusive() {
  let pin = ExclusivePin::new();
  {
    let _s = pin.try_shared().unwrap();
    assert!(pin.try_exclusive().is_none());
  }
  assert!(pin.try_exclusive().is_some());
}

#[test]
fn partial_shared_drop_still_blocks_exclusive() {
  let pin = ExclusivePin::new();
  let s1 = pin.try_shared().unwrap();
  let _s2 = pin.try_shared().unwrap();
  drop(s1);
  // s2 still alive — exclusive must still be blocked
  assert!(pin.try_exclusive().is_none());
}

#[test]
fn exclusive_drop_allows_shared() {
  let pin = ExclusivePin::new();
  {
    let _e = pin.try_exclusive().unwrap();
    assert!(pin.try_shared().is_none());
  }
  assert!(pin.try_shared().is_some());
  assert!(!pin.is_exclusive());
}

#[test]
fn exclusive_drop_allows_another_exclusive() {
  let pin = ExclusivePin::new();
  {
    let _e = pin.try_exclusive().unwrap();
  }
  assert!(pin.try_exclusive().is_some());
}

// ----- Upgrade / Downgrade -----

#[test]
fn upgrade_from_sole_shared() {
  let pin = ExclusivePin::new();
  let s = pin.try_shared().unwrap();
  let _e = s.upgrade();
  assert!(pin.is_exclusive());
  assert!(pin.try_shared().is_none());
}

#[test]
fn upgraded_drop_clears_exclusive() {
  let pin = ExclusivePin::new();
  {
    let s = pin.try_shared().unwrap();
    let _e = s.upgrade();
  }
  assert!(!pin.is_exclusive());
  assert!(pin.try_shared().is_some());
}

#[test]
fn upgrade_blocks_until_other_shared_drops() {
  let pin = ExclusivePin::new();
  let s1 = pin.try_shared().unwrap();
  let s2 = pin.try_shared().unwrap();

  let done = AtomicBool::new(false);

  thread::scope(|scope| {
    let handle = scope.spawn(|| {
      let _e = s1.upgrade();
      done.store(true, Ordering::Release);
    });

    // give the thread time to enter spin loop
    thread::sleep(Duration::from_millis(30));
    assert!(
      !done.load(Ordering::Acquire),
      "upgrade must wait while other shared holder exists"
    );

    drop(s2);

    handle.join().unwrap();
    assert!(done.load(Ordering::Acquire));
  });
}

#[test]
fn downgrade_to_shared() {
  let pin = ExclusivePin::new();
  let e = pin.try_exclusive().unwrap();
  let _s = e.downgrade();
  assert!(!pin.is_exclusive());
  // another shared can now join
  assert!(pin.try_shared().is_some());
  // but exclusive still blocked by the downgraded shared
  assert!(pin.try_exclusive().is_none());
}

#[test]
fn downgraded_drop_releases_fully() {
  let pin = ExclusivePin::new();
  {
    let e = pin.try_exclusive().unwrap();
    let _s = e.downgrade();
  }
  assert!(pin.try_exclusive().is_some());
}

// ----- Alternation and sequential usage -----

#[test]
fn alternating_shared_and_exclusive() {
  let pin = ExclusivePin::new();
  for _ in 0..10 {
    let s = pin.try_shared().unwrap();
    drop(s);
    let e = pin.try_exclusive().unwrap();
    drop(e);
  }
}

// ----- Concurrency -----

#[test]
fn concurrent_shared_all_succeed() {
  let pin = ExclusivePin::new();
  let success = AtomicUsize::new(0);

  thread::scope(|scope| {
    for _ in 0..8 {
      scope.spawn(|| {
        for _ in 0..200 {
          if pin.try_shared().is_some() {
            success.fetch_add(1, Ordering::Relaxed);
          }
        }
      });
    }
  });

  // all try_shared calls should succeed (no contention from exclusive)
  assert_eq!(success.load(Ordering::Relaxed), 8 * 200);
  assert!(pin.try_exclusive().is_some());
}

#[test]
fn concurrent_exclusive_is_serialized() {
  let pin = ExclusivePin::new();
  let acquired = AtomicUsize::new(0);

  thread::scope(|scope| {
    for _ in 0..4 {
      scope.spawn(|| {
        for _ in 0..50 {
          loop {
            if let Some(_e) = pin.try_exclusive() {
              acquired.fetch_add(1, Ordering::Relaxed);
              break;
            }
            thread::yield_now();
          }
        }
      });
    }
  });

  assert_eq!(acquired.load(Ordering::Relaxed), 4 * 50);
  assert!(!pin.is_exclusive());
}

#[test]
fn concurrent_shared_and_exclusive_mix() {
  let pin = ExclusivePin::new();

  thread::scope(|scope| {
    // readers
    for _ in 0..4 {
      scope.spawn(|| {
        for _ in 0..200 {
          loop {
            if let Some(_s) = pin.try_shared() {
              thread::yield_now();
              break;
            }
            thread::yield_now();
          }
        }
      });
    }
    // writers
    for _ in 0..2 {
      scope.spawn(|| {
        for _ in 0..50 {
          loop {
            if let Some(_e) = pin.try_exclusive() {
              thread::yield_now();
              break;
            }
            thread::yield_now();
          }
        }
      });
    }
  });

  // final state: no holders
  assert!(!pin.is_exclusive());
  assert!(pin.try_exclusive().is_some());
}

// ----- Memory ordering (acquire / release semantics) -----

/**
 * Canonical lost-update test. Multiple threads acquire exclusive, perform a
 * non-atomic read-modify-write on a shared counter using Relaxed ordering,
 * then release. If try_exclusive's Acquire and ExclusiveToken drop's Release
 * synchronize correctly, every increment is observed by the next acquirer
 * and the final count matches the total. Broken ordering surfaces as lost
 * updates (final count < expected).
 */
#[test]
fn exclusive_serializes_relaxed_writes() {
  let pin = ExclusivePin::new();
  let counter = AtomicUsize::new(0);
  const THREADS: usize = 8;
  const ITERS: usize = 500;

  thread::scope(|scope| {
    for _ in 0..THREADS {
      scope.spawn(|| {
        for _ in 0..ITERS {
          let _token = loop {
            if let Some(t) = pin.try_exclusive() {
              break t;
            }
            thread::yield_now();
          };
          // Relaxed RMW — only safe because exclusive provides ordering.
          let cur = counter.load(Ordering::Relaxed);
          counter.store(cur + 1, Ordering::Relaxed);
        }
      });
    }
  });

  assert_eq!(counter.load(Ordering::Relaxed), THREADS * ITERS);
}

/**
 * Writes performed under exclusive must be visible to a subsequent shared
 * acquirer. Tests that ExclusiveToken::drop (Release) synchronizes with
 * try_shared (Acquire).
 */
#[test]
fn exclusive_release_visible_to_shared_acquire() {
  let pin = ExclusivePin::new();
  let payload = AtomicUsize::new(0);
  let ready = AtomicBool::new(false);

  thread::scope(|scope| {
    scope.spawn(|| {
      let _e = pin.try_exclusive().unwrap();
      payload.store(0xDEAD_BEEF, Ordering::Relaxed);
      ready.store(true, Ordering::Relaxed);
      // drop releases exclusive (Release)
    });

    scope.spawn(|| {
      // Wait for the writer to have held exclusive at least once.
      while !ready.load(Ordering::Relaxed) {
        thread::yield_now();
      }
      let _s = loop {
        if let Some(s) = pin.try_shared() {
          break s;
        }
        thread::yield_now();
      };
      // After Acquire on try_shared, relaxed load must observe the write.
      assert_eq!(payload.load(Ordering::Relaxed), 0xDEAD_BEEF);
    });
  });
}

/**
 * Consecutive exclusive holders must observe each other's writes. Chains
 * a relaxed write through a handoff between exclusive acquirers.
 */
#[test]
fn consecutive_exclusive_propagates_writes() {
  let pin = ExclusivePin::new();
  let value = AtomicUsize::new(0);
  const STAGES: usize = 64;

  let pin_ref = &pin;
  let value_ref = &value;

  thread::scope(|scope| {
    for stage in 1..=STAGES {
      scope.spawn(move || {
        let _e = loop {
          if let Some(t) = pin_ref.try_exclusive() {
            // Only proceed when the previous stage has been observed.
            if value_ref.load(Ordering::Relaxed) == stage - 1 {
              break t;
            }
            // wrong stage — drop token and retry
          }
          thread::yield_now();
        };
        value_ref.store(stage, Ordering::Relaxed);
      });
    }
  });

  assert_eq!(value.load(Ordering::Relaxed), STAGES);
}

/**
 * Writes published by the last shared holder (via SharedToken::drop Release)
 * must be visible to a subsequent exclusive acquirer. Tests SharedToken drop's
 * Release paired with try_exclusive's Acquire.
 */
#[test]
fn shared_drop_release_visible_to_exclusive_acquire() {
  let pin = ExclusivePin::new();
  let payload = AtomicUsize::new(0);

  thread::scope(|scope| {
    scope.spawn(|| {
      let _s = pin.try_shared().unwrap();
      payload.store(0xCAFE_BABE, Ordering::Relaxed);
      // drop releases shared (fetch_sub with Release)
    });
  });

  // After the reader thread finished (joined via scope), take exclusive.
  let _e = pin.try_exclusive().unwrap();
  assert_eq!(payload.load(Ordering::Relaxed), 0xCAFE_BABE);
}

/**
 * Writes performed under shared(1) and then upgraded to exclusive stay
 * consistent across the upgrade boundary. Tests that upgrade's CAS Acquire
 * establishes happens-before with prior Release operations.
 */
#[test]
fn upgrade_preserves_happens_before() {
  let pin = ExclusivePin::new();
  let payload = AtomicUsize::new(0);
  let ready = AtomicBool::new(false);

  thread::scope(|scope| {
    // Writer: takes exclusive, writes, drops. Provides a Release.
    scope
      .spawn(|| {
        let _e = pin.try_exclusive().unwrap();
        payload.store(123, Ordering::Relaxed);
        ready.store(true, Ordering::Relaxed);
      })
      .join()
      .unwrap();

    // Reader: takes shared (Acquire), then upgrades (Acquire on CAS).
    scope.spawn(|| {
      assert!(ready.load(Ordering::Relaxed));
      let s = pin.try_shared().unwrap();
      assert_eq!(payload.load(Ordering::Relaxed), 123);
      let _e = s.upgrade();
      // After upgrade (CAS Acquire), still see the write.
      assert_eq!(payload.load(Ordering::Relaxed), 123);
    });
  });
}
