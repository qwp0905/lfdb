use super::super::NO_CALLBACK;

use super::*;
use tempfile::tempdir_in;

#[test]
fn test_pread() -> Result<()> {
  let dir = tempdir_in(".")?;
  let disk = DefaultDiskBackend::new().unwrap();
  let file = disk
    .open(
      OpenOptions::new().read(true).write(true).create(true),
      &dir.path().join("test_file.txt"),
    )
    .unwrap();
  let content = b"Hello, World!";

  // Create a test file with content
  let (task, done) = IOTask::new_pwrite(content, 0, NO_CALLBACK);
  file.submit(task);
  done.wait().unwrap();

  let (task, done) = IOTask::new_fsync(NO_CALLBACK);
  file.submit(task);
  done.wait().unwrap();

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
  let file_path = dir.path().join("test_pwrite.txt");
  let disk = DefaultDiskBackend::new().unwrap();
  let file = disk
    .open(
      OpenOptions::new().read(true).write(true).create(true),
      &file_path,
    )
    .unwrap();

  // Test 1: Write at the beginning
  let content = b"Hello";
  let (task, done) = IOTask::new_pwrite(content, 0, NO_CALLBACK);
  file.submit(task);
  let bytes_written = done.wait().unwrap();
  assert_eq!(bytes_written, 5);

  // Test 2: Write at specific offset
  let content = b"World";
  let (task, done) = IOTask::new_pwrite(content, 6, NO_CALLBACK);
  file.submit(task);
  let bytes_written = done.wait().unwrap();
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
  let (task, done) = IOTask::new_pwrite(empty_buf, 0, NO_CALLBACK);
  file.submit(task);
  let bytes_written = done.wait().unwrap();
  assert_eq!(bytes_written, 0);
}

#[test]
fn test_allocate() -> Result<()> {
  let dir = tempdir_in(".")?;
  let path = dir.path().join("fallocate.txt");
  let disk = DefaultDiskBackend::new().unwrap();
  let file = disk
    .open(
      OpenOptions::new().read(true).write(true).create(true),
      &path,
    )
    .unwrap();

  let (task, done) = IOTask::new_fallocate(0, 0, NO_CALLBACK);
  file.submit(task);
  assert!(done.wait().is_err());

  let (task, done) = IOTask::new_fallocate(0, 100, NO_CALLBACK);
  file.submit(task);
  done.wait()?;
  assert_eq!(file.metadata()?.len(), 100);

  let (task, done) = IOTask::new_fallocate(100, 200, NO_CALLBACK);
  file.submit(task);
  done.wait()?;
  assert_eq!(file.metadata()?.len(), 300);

  let (task, done) = IOTask::new_fallocate(500, 10, NO_CALLBACK);
  file.submit(task);
  done.wait()?;
  assert_eq!(file.metadata()?.len(), 510);

  let (task, done) = IOTask::new_fallocate(510, 0, NO_CALLBACK);
  file.submit(task);
  assert!(done.wait().is_err());

  Ok(())
}
