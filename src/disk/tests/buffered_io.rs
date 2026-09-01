use std::sync::Arc;

use crate::{disk::IOPool, metrics::MetricsRegistry, DefaultIOBackend};

use super::*;
use tempfile::TempDir;

#[test]
fn test_scan_io() {
  let dir = TempDir::new_in(".").expect("dir failed.");
  let metrics = Arc::new(MetricsRegistry::new());
  let io_pool = IOPool::with_backend(DefaultIOBackend, 1, dir.path(), metrics).unwrap();
  let filename = PathBuf::from("temp");

  let count = 10u8;
  const SIZE: usize = 384;
  let mut vectors = Vec::with_capacity(count as usize);
  for i in 0..count {
    let buf = [i; SIZE];
    vectors.push(buf);
  }

  {
    let mut handle = io_pool.open_append_io(filename.clone()).unwrap();
    for buf in vectors.iter() {
      handle.append(buf).unwrap();
    }
    handle.flush_all().unwrap();
  }

  {
    let mut handle = io_pool.open_scan_io(filename).unwrap();
    let mut offset = 0;
    for buf in vectors.iter() {
      assert_eq!(handle.get_offset(), offset);
      offset += buf.len() as u64;
      assert_eq!(*buf, handle.read_array::<SIZE>().unwrap());
    }
  }

  io_pool.close();
}
