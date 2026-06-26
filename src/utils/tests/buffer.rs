use std::mem::size_of;

use super::{OffsetReader, OffsetWriter};

#[test]
fn write_and_read_bytes() {
  let expected = b"lfdb buffer";
  let mut buf = [0; 32];

  let written = {
    let mut writer = OffsetWriter::new(&mut buf);
    assert!(writer.write(expected));
    writer.written_bytes()
  };

  let mut reader = OffsetReader::new(&buf[..written]);
  assert_eq!(reader.read(expected.len()), Some(expected.as_slice()));
  assert!(reader.is_eof());
  assert_eq!(reader.read_byte(), None);
}

#[test]
fn write_and_read_values() {
  let expected_u8 = 0xab;
  let expected_u16 = 0x1234;
  let expected_u32 = 0x56789abc;
  let expected_u64 = 0xdef0123456789abc;
  let mut buf = [0; 32];

  let written = {
    let mut writer = OffsetWriter::new(&mut buf);
    assert!(writer.write_u8(expected_u8));
    assert!(writer.write_u16(expected_u16));
    assert!(writer.write_u32(expected_u32));
    assert!(writer.write_u64(expected_u64));
    writer.written_bytes()
  };

  let mut reader = OffsetReader::new(&buf[..written]);
  assert_eq!(reader.read_byte(), Some(expected_u8));
  assert_eq!(reader.read_u16(), Some(expected_u16));
  assert_eq!(reader.read_u32(), Some(expected_u32));
  assert_eq!(reader.read_u64(), Some(expected_u64));
  assert!(reader.is_eof());
}

#[test]
fn write_and_read_mixed_values_in_order() {
  let first = b"first";
  let middle = 42_u64;
  let last = b"last";
  let mut buf = [0; 32];

  let written = {
    let mut writer = OffsetWriter::new(&mut buf);
    assert!(writer.write(first));
    assert!(writer.write_u64(middle));
    assert!(writer.write(last));
    writer.written_bytes()
  };

  let mut reader = OffsetReader::new(&buf[..written]);
  assert_eq!(reader.read(first.len()), Some(first.as_slice()));
  assert_eq!(reader.read_u64(), Some(middle));
  assert_eq!(reader.read(last.len()), Some(last.as_slice()));
  assert!(reader.is_eof());
}

#[test]
fn writer_and_reader_support_exact_capacity() {
  let expected = u64::MAX;
  let mut buf = [0; size_of::<u64>()];

  {
    let mut writer = OffsetWriter::new(&mut buf);
    assert!(writer.write_u64(expected));
    assert_eq!(writer.written_bytes(), size_of::<u64>());
  }

  let mut reader = OffsetReader::new(&buf);
  assert_eq!(reader.read_u64(), Some(expected));
  assert!(reader.is_eof());
}

#[test]
fn writer_rejects_values_after_reaching_capacity() {
  let expected = u64::MAX;
  let mut buf = [0; size_of::<u64>()];

  let written = {
    let mut writer = OffsetWriter::new(&mut buf);
    assert!(writer.write_u64(expected));
    assert!(!writer.write_u8(1));
    writer.written_bytes()
  };

  let mut reader = OffsetReader::new(&buf[..written]);
  assert_eq!(reader.read_u64(), Some(expected));
  assert!(reader.is_eof());
}

#[test]
fn reader_returns_no_values_after_reaching_eof() {
  let expected = u64::MAX;
  let mut buf = [0; size_of::<u64>()];

  {
    let mut writer = OffsetWriter::new(&mut buf);
    assert!(writer.write_u64(expected));
  }

  let mut reader = OffsetReader::new(&buf);
  assert_eq!(reader.read_u64(), Some(expected));
  assert!(reader.is_eof());
  assert_eq!(reader.read_byte(), None);
  assert_eq!(reader.read_u16(), None);
  assert_eq!(reader.advance(1), None);
  assert!(reader.is_eof());
}

#[test]
fn failed_write_does_not_discard_previous_values() {
  let first = 10_u16;
  let second = 20_u16;
  let mut buf = [0; size_of::<u16>() * 2];

  let written = {
    let mut writer = OffsetWriter::new(&mut buf);
    assert!(writer.write_u16(first));
    assert!(!writer.write_u64(u64::MAX));
    assert!(writer.write_u16(second));
    writer.written_bytes()
  };

  let mut reader = OffsetReader::new(&buf[..written]);
  assert_eq!(reader.read_u16(), Some(first));
  assert_eq!(reader.read_u16(), Some(second));
  assert!(reader.is_eof());
}

#[test]
fn failed_read_does_not_discard_remaining_values() {
  let expected = 10_u16;
  let mut buf = [0; size_of::<u16>()];

  {
    let mut writer = OffsetWriter::new(&mut buf);
    assert!(writer.write_u16(expected));
  }

  let mut reader = OffsetReader::new(&buf);
  assert_eq!(reader.read_u64(), None);
  assert_eq!(reader.read_u16(), Some(expected));
  assert!(reader.is_eof());
}

#[test]
fn advance_skips_written_values() {
  let skipped = b"skip";
  let expected = 99_u32;
  let mut buf = [0; 16];

  let written = {
    let mut writer = OffsetWriter::new(&mut buf);
    assert!(writer.write(skipped));
    assert!(writer.write_u32(expected));
    writer.written_bytes()
  };

  let mut reader = OffsetReader::new(&buf[..written]);
  assert_eq!(reader.advance(skipped.len()), Some(0));
  assert_eq!(reader.read_u32(), Some(expected));
  assert!(reader.is_eof());
}

#[test]
fn read_all_returns_all_remaining_written_values() {
  let first = b"first";
  let remaining = b"remaining";
  let mut buf = [0; 32];

  let written = {
    let mut writer = OffsetWriter::new(&mut buf);
    assert!(writer.write(first));
    assert!(writer.write(remaining));
    writer.written_bytes()
  };

  let mut reader = OffsetReader::new(&buf[..written]);
  assert_eq!(reader.read(first.len()), Some(first.as_slice()));
  assert_eq!(reader.read_all(), remaining.as_slice());
  assert!(reader.is_eof());
}

#[test]
fn each_reader_reads_written_values_from_the_beginning() {
  let expected = b"reusable memory";
  let mut buf = [0; 32];

  let written = {
    let mut writer = OffsetWriter::new(&mut buf);
    assert!(writer.write(expected));
    writer.written_bytes()
  };

  let mut first = OffsetReader::new(&buf[..written]);
  assert_eq!(first.read_all(), expected.as_slice());

  let mut second = OffsetReader::new(&buf[..written]);
  assert_eq!(second.read_all(), expected.as_slice());
}
