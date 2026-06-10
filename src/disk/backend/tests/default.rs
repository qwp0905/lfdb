use super::*;
use std::fs::File;
use std::io::{Read, Write};
use tempfile::tempdir_in;

#[test]
fn test_pread() -> Result<()> {
  let dir = tempdir_in(".")?;
  let file_path = dir.path().join("test_file.txt");
  let content = b"Hello, World!";

  // Create a test file with content
  let mut file = File::create(&file_path)?;
  file.write_all(content)?;
  file.sync_all()?;

  // Open file for reading
  let file = File::open(&file_path)?;

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
fn test_pwrite() -> Result<()> {
  let dir = tempdir_in(".")?;
  let file_path = dir.path().join("test_pwrite.txt");

  // Create an empty file
  let file = File::create(&file_path)?;

  // Test 1: Write at the beginning
  let content1 = b"Hello";
  let bytes_written = file.pwrite(content1, 0)?;
  assert_eq!(bytes_written, 5);

  // Test 2: Write at specific offset
  let content2 = b"World";
  let bytes_written = file.pwrite(content2, 6)?;
  assert_eq!(bytes_written, 5);

  // Verify written content
  let mut content = String::new();
  File::open(&file_path)?.read_to_string(&mut content)?;
  assert_eq!(content, "Hello\0World");

  // Test 3: Write with empty buffer
  let empty_buf: &[u8] = &[];
  let bytes_written = file.pwrite(empty_buf, 0)?;
  assert_eq!(bytes_written, 0);

  Ok(())
}

#[test]
fn test_allocate() -> Result<()> {
  let dir = tempdir_in(".")?;
  let file = File::create(dir.path().join("fallocate.txt"))?;

  file.fallocate(0, 100)?;
  assert_eq!(file.metadata()?.len(), 100);
  file.fallocate(100, 200)?;
  assert_eq!(file.metadata()?.len(), 300);

  file.fallocate(500, 10)?;
  assert_eq!(file.metadata()?.len(), 510);

  Ok(())
}
