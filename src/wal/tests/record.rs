use super::*;

fn assert_roundtrip(record: &LogRecord) -> LogRecord {
  let bytes: &[u8] = &record.to_bytes();
  let parsed: LogRecord = bytes.try_into().expect("deserialize failed");
  assert_eq!(parsed.log_id, record.log_id);
  assert_eq!(parsed.tx_id, record.tx_id);
  parsed
}

#[test]
fn test_start_roundtrip() {
  let r = LogRecord::new_start(1, 42);
  let parsed = assert_roundtrip(&r);
  assert!(matches!(parsed.operation, Operation::Start));
}

#[test]
fn test_commit_roundtrip() {
  let r = LogRecord::new_commit(2, 42);
  let parsed = assert_roundtrip(&r);
  assert!(matches!(parsed.operation, Operation::Commit));
}

#[test]
fn test_abort_roundtrip() {
  let r = LogRecord::new_abort(3, 42);
  let parsed = assert_roundtrip(&r);
  assert!(matches!(parsed.operation, Operation::Abort));
}

#[test]
fn test_insert_roundtrip() {
  let mut page = vec![0; 100];
  page[0] = 0xAB;
  page[99] = 0xCD;

  let r = LogRecord::new_insert(4, 42, 1, 99, page);
  let parsed = assert_roundtrip(&r);
  match parsed.operation {
    Operation::Insert(table_id, ptr, data) => {
      assert_eq!(table_id, 1);
      assert_eq!(ptr, 99);
      assert_eq!(data[0], 0xAB);
      assert_eq!(data[99], 0xCD);
    }
    _ => panic!("expected Insert"),
  }
}

#[test]
fn test_checkpoint_roundtrip() {
  let log_id = 5;
  let last_log_id = 200;
  let min_active = 123;
  let path: PathBuf = format!("sdfsdf").into();
  let r = LogRecord::new_checkpoint(log_id, last_log_id, min_active, path.clone());
  let parsed = assert_roundtrip(&r);
  match parsed.operation {
    Operation::Checkpoint(id, mc, p) => {
      assert_eq!(last_log_id, id);
      assert_eq!(min_active, mc);
      assert_eq!(path, p);
    }
    _ => panic!("expected Checkpoint"),
  }
}

#[test]
fn test_entry_roundtrip() {
  let mut page = Page::new();
  let mut writer = page.writer();

  let _ = writer.write(&(3 as u16).to_le_bytes());
  let r1 = LogRecord::new_start(1, 1);
  let r2 = LogRecord::new_insert(2, 1, 0, 10, vec![]);
  let r3 = LogRecord::new_commit(3, 1);
  let _ = writer.write(&r1.to_bytes_with_len());
  let _ = writer.write(&r2.to_bytes_with_len());
  let _ = writer.write(&r3.to_bytes_with_len());

  let (d, complete) = (&page).into();
  assert_eq!(complete, false);

  assert_eq!(d.len(), 3);
  assert_eq!(d[0].log_id, 1);
  assert_eq!(d[0].tx_id, 1);
  assert!(matches!(d[0].operation, Operation::Start));
  assert_eq!(d[1].log_id, 2);
  assert_eq!(d[1].tx_id, 1);
  assert!(matches!(d[1].operation, Operation::Insert(0, 10, _)));
  assert_eq!(d[2].log_id, 3);
  assert_eq!(d[2].tx_id, 1);
  assert!(matches!(d[2].operation, Operation::Commit));
}

#[test]
fn test_invalid_format() {
  let short: Vec<u8> = vec![0; 10];
  assert!(LogRecord::try_from(short.as_ref()).is_err());

  let mut bad_op = vec![0u8; 17];
  bad_op[16] = 255; // invalid operation type
  assert!(LogRecord::try_from(bad_op.as_ref()).is_err());
}
