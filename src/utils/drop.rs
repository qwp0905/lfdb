pub const fn just_drop<T: Send, R>(needs_drop: T) -> impl FnOnce(R) -> R {
  move |value| {
    drop(needs_drop);
    value
  }
}
