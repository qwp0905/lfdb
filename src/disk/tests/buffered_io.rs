use std::sync::Arc;

use crate::{disk::IOPool, metrics::MetricsRegistry, DefaultDiskBackend};

use super::*;
use tempfile::TempDir;

#[test]
fn test_scan_io() {
  let dir = TempDir::new_in(".").expect("dir failed.");
  let metrics = Arc::new(MetricsRegistry::new());
  let io_pool =
    IOPool::with_backend(DefaultDiskBackend::new().unwrap(), dir.path(), metrics)
      .unwrap();
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
}

const PATTERN_LEN: usize = ALIGN * 4;

fn pattern(offset: usize, len: usize) -> Vec<u8> {
  (offset..offset + len).map(|i| (i % 251) as u8).collect()
}

fn open_pattern() -> (TempDir, ScanIOHandle) {
  let dir = TempDir::new_in(".").expect("dir failed.");
  let metrics = Arc::new(MetricsRegistry::new());
  let io_pool =
    IOPool::with_backend(DefaultDiskBackend::new().unwrap(), dir.path(), metrics)
      .unwrap();
  let filename = PathBuf::from("temp");

  let mut handle = io_pool.open_append_io(filename.clone()).unwrap();
  handle.append(&pattern(0, PATTERN_LEN)).unwrap();
  handle.flush_all().unwrap();

  let handle = io_pool.open_scan_io(filename).unwrap();
  (dir, handle)
}

#[test]
fn test_scan_io_seek_aligned() {
  let (_dir, mut handle) = open_pattern();
  let offset = ALIGN * 2;

  handle.seek(offset as u64).unwrap();
  assert_eq!(handle.read_to_vec(64).unwrap(), pattern(offset, 64));
}

#[test]
fn test_scan_io_seek_across_block() {
  let (_dir, mut handle) = open_pattern();
  let offset = ALIGN * 2 - 16;

  handle.seek(offset as u64).unwrap();
  assert_eq!(handle.read_to_vec(64).unwrap(), pattern(offset, 64));
}

#[test]
fn test_scan_io_seek_offset() {
  let (_dir, mut handle) = open_pattern();
  let offset = ALIGN + 100;

  handle.seek(offset as u64).unwrap();
  assert_eq!(handle.get_offset(), offset as u64);

  handle.read_to_vec(ALIGN).unwrap();
  assert_eq!(handle.get_offset(), (offset + ALIGN) as u64);
}
