use crate::disk::{Page, PAGE_SIZE};

#[test]
fn test_writer() {
  let mut page = Page::<PAGE_SIZE>::new();
  let mut wt = page.writer();
  wt.write(&[1, 2, 3, 5, 6]).unwrap();

  assert_eq!(wt.finalize(), 5);

  let mut scanner = page.scanner();

  assert_eq!(scanner.read().unwrap(), 1);
  assert_eq!(scanner.read().unwrap(), 2);
  assert_eq!(scanner.read().unwrap(), 3);
  assert_eq!(scanner.read().unwrap(), 5);
  assert_eq!(scanner.read().unwrap(), 6);
  assert_eq!(scanner.read().unwrap(), 0);
  assert_eq!(scanner.read().unwrap(), 0);
}

#[test]
fn test_read_write() {
  let mut page = Page::<4>::new();
  let test_data = [1, 2, 3, 4];

  // Write test
  let mut writer = page.writer();
  writer.write(&test_data).unwrap();
  assert_eq!(writer.finalize(), 4);

  // Read test
  let mut scanner = page.scanner();
  for &expected in test_data.iter() {
    assert_eq!(scanner.read().unwrap(), expected);
  }
}

#[test]
fn test_read_n() {
  let mut page = Page::<PAGE_SIZE>::new();
  let test_data = [1, 2, 3, 4, 5];

  // Write test
  let mut writer = page.writer();
  writer.write(&test_data).unwrap();

  // Read test using read_n
  let mut scanner = page.scanner();
  let read_data = scanner.read_n(test_data.len()).unwrap();
  assert_eq!(read_data, &test_data);
}

#[test]
fn test_write_overflow() {
  const SMALL_SIZE: usize = 4;
  let mut page = Page::<SMALL_SIZE>::new();
  let test_data = [1, 2, 3, 4, 5, 6]; // Data larger than SMALL_SIZE

  let mut writer = page.writer();
  assert!(writer.write(&test_data).is_err()); // Expect EOF error
}

#[test]
fn test_read_n_overflow() {
  const SMALL_SIZE: usize = 4;
  let page = Page::<SMALL_SIZE>::new();
  let mut scanner = page.scanner();

  // Request size larger than page size
  assert!(scanner.read_n(SMALL_SIZE + 1).is_err());
}

#[test]
fn test_sequential_operations() {
  let mut page = Page::<PAGE_SIZE>::new();

  // First write operation (starts from offset 0)
  {
    let mut writer = page.writer();
    writer.write(&[1, 2, 3]).unwrap();
  }

  // Second write operation overwrites from the beginning
  {
    let mut writer = page.writer();
    writer.write(&[4, 5]).unwrap();
    writer.write(&[6]).unwrap();
  }

  // Scanner always reads from offset 0
  let mut scanner = page.scanner();
  assert_eq!(scanner.read().unwrap(), 4);
  assert_eq!(scanner.read().unwrap(), 5);
  assert_eq!(scanner.read().unwrap(), 6);
  assert_eq!(scanner.read().unwrap(), 0); // Rest remains as initial value
}

#[test]
fn test_writer_fresh_start() {
  let mut page = Page::<PAGE_SIZE>::new();

  // First write operation
  {
    let mut writer = page.writer();
    writer.write(&[1, 2, 3]).unwrap();
  }

  // New writer only resets offset to 0
  {
    let mut writer = page.writer();
    writer.write(&[7, 8]).unwrap();
  }

  let mut scanner = page.scanner();
  assert_eq!(scanner.read().unwrap(), 7);
  assert_eq!(scanner.read().unwrap(), 8);
  assert_eq!(scanner.read().unwrap(), 3); // Previous data remains in unwritten portion
}

#[test]
fn test_scanner_fresh_start() {
  let mut page = Page::<PAGE_SIZE>::new();

  // Write data
  {
    let mut writer = page.writer();
    writer.write(&[1, 2, 3]).unwrap();
  }

  // First scanner
  {
    let mut scanner = page.scanner();
    assert_eq!(scanner.read().unwrap(), 1);
    assert_eq!(scanner.read().unwrap(), 2);
  }

  // New scanner starts reading from the beginning
  let mut scanner = page.scanner();
  assert_eq!(scanner.read().unwrap(), 1);
  assert_eq!(scanner.read().unwrap(), 2);
  assert_eq!(scanner.read().unwrap(), 3);
}

#[test]
fn test_interleaved_operations() {
  let mut page = Page::<PAGE_SIZE>::new();
  let test_data = [1, 2, 3];

  // First write
  {
    let mut writer = page.writer();
    writer.write(&test_data).unwrap();
  }

  // First read
  {
    let mut scanner = page.scanner();
    for &expected in test_data.iter() {
      assert_eq!(scanner.read().unwrap(), expected);
    }
  }

  // Second write (only resets offset to 0)
  {
    let mut writer = page.writer();
    writer.write(&[4, 5]).unwrap();
  }

  // Final verification (overwritten portion changes, rest remains as previous data)
  let mut scanner = page.scanner();
  assert_eq!(scanner.read().unwrap(), 4);
  assert_eq!(scanner.read().unwrap(), 5);
  assert_eq!(scanner.read().unwrap(), 3); // Third byte remains unchanged
}

#[test]
fn test_page_copy() {
  let mut page = Page::<PAGE_SIZE>::new();
  let test_data = [1, 2, 3, 4, 5];

  // Write data to original page
  let mut writer = page.writer();
  writer.write(&test_data).unwrap();

  // Create copy and verify data
  let copied = page.copy_n(5);
  for (i, e) in copied.iter().enumerate() {
    assert_eq!(e, &test_data[i]);
  }

  // Modify original, verify copy remains unchanged
  let mut writer = page.writer();
  writer.write(&[9, 9]).unwrap();

  for (i, e) in copied.iter().enumerate() {
    assert_eq!(e, &test_data[i]);
  }
}

#[test]
fn test_from_slice() {
  const SIZE: usize = 4;
  let data = [1, 2, 3, 4];
  let page = Page::<SIZE>::from(&data[..]);

  let mut scanner = page.scanner();
  for &expected in data.iter() {
    assert_eq!(scanner.read().unwrap(), expected);
  }
}

#[test]
fn test_read_u64() {
  let mut page = Page::<16>::new();
  let test_value = 42u64.to_le_bytes();

  // Write value
  let mut writer = page.writer();
  writer.write(&test_value).unwrap();

  // Read and verify usize value
  let mut scanner = page.scanner();
  let read_value = scanner.read_const_n::<8>().unwrap();
  assert_eq!(read_value, test_value);
}

#[test]
fn test_as_ref() {
  const SIZE: usize = 4;
  let mut page = Page::<SIZE>::new();
  let test_data = [1, 2, 3, 4];

  let mut writer = page.writer();
  writer.write(&test_data).unwrap();

  // Verify AsRef implementation
  let slice: &[u8] = page.as_ref();
  assert_eq!(slice, &test_data);
}

#[test]
fn test_as_mut() {
  const SIZE: usize = 4;
  let mut page = Page::<SIZE>::new();
  let test_data = [1, 2, 3, 4];

  // Modify through AsMut
  let slice: &mut [u8] = page.as_mut();
  slice.copy_from_slice(&test_data);

  // Verify changes
  let mut scanner = page.scanner();
  for &expected in test_data.iter() {
    assert_eq!(scanner.read().unwrap(), expected);
  }
}
