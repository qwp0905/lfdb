#[cfg(any(
  target_os = "dragonfly",
  target_os = "freebsd",
  target_os = "netbsd",
  target_os = "openbsd",
  target_vendor = "apple",
  target_os = "cygwin",
))]
pub const fn max_iov() -> usize {
  libc::IOV_MAX as usize
}
#[cfg(any(
  target_os = "android",
  target_os = "emscripten",
  target_os = "linux",
  target_os = "nto",
))]
pub const fn max_iov() -> usize {
  libc::UIO_MAXIOV as usize
}
#[cfg(not(any(
  target_os = "android",
  target_os = "dragonfly",
  target_os = "emscripten",
  target_os = "espidf",
  target_os = "freebsd",
  target_os = "linux",
  target_os = "netbsd",
  target_os = "nuttx",
  target_os = "nto",
  target_os = "openbsd",
  target_os = "horizon",
  target_os = "vita",
  target_vendor = "apple",
  target_os = "cygwin",
)))]
pub const fn max_iov() -> usize {
  16 // The minimum value required by POSIX.
}
