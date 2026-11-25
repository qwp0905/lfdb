use std::path::PathBuf;

use super::*;
use crate::disk::OpenFlags;
use tempfile::tempdir_in;

fn disk() -> Arc<dyn DiskBackend> {
  Arc::new(DefaultDiskBackend::new().unwrap())
}

fn open(disk: &Arc<dyn DiskBackend>, path: PathBuf) -> Box<dyn IOBackend> {
  disk
    .open(OpenFlags::buffered(), path)
    .wait()
    .unwrap()
    .unwrap()
}

#[test]
fn test_pread() -> Result<()> {
  let dir = tempdir_in(".")?;
  let disk = disk();
  let file = open(&disk, dir.path().join("test_file.txt"));
  let content = b"Hello, World!";

  // Create a test file with content
  let done = file.pwrite(content, 0);
  done.wait().unwrap()?;

  let done = file.fsync();
  done.wait().unwrap()?;

  // Test 1: Normal read
  let mut buf = vec![0; 5];
  let bytes_read = file.pread(&mut buf, 0)?;
  assert_eq!(bytes_read, 5);
  assert_eq!(&buf, b"Hello");

  // Test 2: Read from middle
  let mut buf = vec![0; 5];
  let bytes_read = file.pread(&mut buf, 7)?;
  assert_eq!(bytes_read, 5);
  assert_eq!(&buf, b"World");

  // Test 3: Read beyond file size
  let mut buf = vec![0; 5];
  let bytes_read = file.pread(&mut buf, 20)?;
  assert_eq!(bytes_read, 0);

  // Test 4: Read with empty buffer
  let mut buf = vec![];
  let bytes_read = file.pread(&mut buf, 0)?;
  assert_eq!(bytes_read, 0);

  Ok(())
}

#[test]
fn test_pwrite() {
  let dir = tempdir_in(".").unwrap();
  let disk = disk();
  let file = open(&disk, dir.path().join("test_pwrite.txt"));

  // Test 1: Write at the beginning
  let content = b"Hello";
  let done = file.pwrite(content, 0);
  let bytes_written = done.wait().unwrap().unwrap();
  assert_eq!(bytes_written, 5);

  // Test 2: Write at specific offset
  let content = b"World";
  let done = file.pwrite(content, 6);
  let bytes_written = done.wait().unwrap().unwrap();
  assert_eq!(bytes_written, 5);

  // Verify written content
  let mut content = vec![0; 11];
  file.pread(&mut content, 0).unwrap();
  assert_eq!(
    unsafe { str::from_utf8_unchecked(&content) },
    "Hello\0World"
  );

  // Test 3: Write with empty buffer
  let empty_buf: &[u8] = &[];
  let done = file.pwrite(empty_buf, 0);
  let bytes_written = done.wait().unwrap().unwrap();
  assert_eq!(bytes_written, 0);
}

#[test]
fn test_allocate() -> Result<()> {
  let dir = tempdir_in(".")?;
  let disk = disk();
  let file = open(&disk, dir.path().join("fallocate.txt"));

  let done = file.fallocate(0, 0);
  assert!(done.wait().unwrap().is_err());

  let done = file.fallocate(0, 100);
  done.wait().unwrap()?;
  assert_eq!(file.metadata()?.len(), 100);

  let done = file.fallocate(100, 200);
  done.wait().unwrap()?;
  assert_eq!(file.metadata()?.len(), 300);

  let done = file.fallocate(500, 10);
  done.wait().unwrap()?;
  assert_eq!(file.metadata()?.len(), 510);

  let done = file.fallocate(510, 0);
  assert!(done.wait().unwrap().is_err());

  Ok(())
}
