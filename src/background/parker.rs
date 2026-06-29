use std::sync::{Once, OnceState};

/**
 * A tiny one-shot wake-all primitive.
 *
 * `park` waits until the parker is opened, and `wake_all` opens it
 * permanently. Once opened, all current waiters are released and future calls
 * to `park` return immediately.
 *
 * This is a small utility around `Once` used as a wake-all latch. It is not
 * tied to a specific background runtime and could live in the general utility
 * module. Poisoning is intentionally ignored: a panic while opening the latch is
 * treated as a severe programming error, not as a recoverable state.
 */
pub struct OnceParker(Once);
impl OnceParker {
  pub const fn new() -> Self {
    Self(Once::new())
  }

  pub fn park(&self) {
    self.0.wait_force();
  }

  pub fn wake_all(&self) {
    self.0.call_once_force(empty_fn);
  }
}

const fn empty_fn(_: &OnceState) {}
