use super::*;

fn assert_roundtrip(record: LogRecordUninit, log_id: LogId) -> LogRecord {
  let tx_id = record.tx_id;
  let bytes = record.init(log_id);
  assert_eq!(
    bytes.len(),
    u16::from_le_bytes(unsafe { (bytes[..2].as_ptr() as *const [u8; 2]).read() })
      as usize
      + 2
  );
  let parsed: LogRecord = LogRecord::read_from(&bytes[2..]).unwrap();
  assert_eq!(parsed.log_id, log_id);
  assert_eq!(parsed.tx_id, tx_id);
  parsed
}

#[test]
fn test_start_roundtrip() {
  let r = LogRecordUninit::new_start(42);
  let parsed = assert_roundtrip(r, 1);
  assert!(matches!(parsed.operation, Operation::Start));
}

#[test]
fn test_commit_roundtrip() {
  let r = LogRecordUninit::new_commit(42);
  let parsed = assert_roundtrip(r, 2);
  assert!(matches!(parsed.operation, Operation::Commit));
}

#[test]
fn test_abort_roundtrip() {
  let r = LogRecordUninit::new_abort(42);
  let parsed = assert_roundtrip(r, 3);
  assert!(matches!(parsed.operation, Operation::Abort));
}

#[test]
fn test_insert_roundtrip() {
  let mut page = vec![0; 100];
  page[0] = 0xAB;
  page[99] = 0xCD;

  let r = LogRecordUninit::new_insert(42, 1, 99, page);
  let parsed = assert_roundtrip(r, 4);
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
  let last_log_id = 200;
  let current_version = 10;
  let path: PathBuf = format!("sdfsdf").into();
  let r = LogRecordUninit::new_checkpoint(last_log_id, current_version, path.clone());
  let parsed = assert_roundtrip(r, 5);
  match parsed.operation {
    Operation::Checkpoint(id, v, p) => {
      assert_eq!(last_log_id, id);
      assert_eq!(current_version, v);
      assert_eq!(path, p);
    }
    _ => panic!("expected Checkpoint"),
  }
}

#[test]
fn test_entry_roundtrip() {
  let mut page = Page::new();
  let mut writer = page.writer();

  writer.write(&(4 as u16).to_le_bytes()).unwrap();
  let r1 = LogRecordUninit::new_start(1);
  let r2 = LogRecordUninit::new_insert(1, 0, 10, vec![1]);
  let r4 = LogRecordUninit::new_checkpoint(456, 123, format!("sdlfkj").into());
  let r3 = LogRecordUninit::new_commit(1);

  writer.write(&r1.init(1)).unwrap();
  writer.write(&r2.init(2)).unwrap();
  writer.write(&r4.init(3)).unwrap();
  writer.write(&r3.init(4)).unwrap();

  let (d, complete) = read_page(&page);
  assert_eq!(complete, false);

  assert_eq!(d.len(), 4);

  assert_eq!(d[0].log_id, 1);
  assert_eq!(d[0].tx_id, 1);
  assert!(matches!(d[0].operation, Operation::Start));

  assert_eq!(d[1].log_id, 2);
  assert_eq!(d[1].tx_id, 1);
  assert!(matches!(d[1].operation, Operation::Insert(0, 10, _)));

  assert_eq!(d[2].log_id, 3);
  assert!(matches!(
    &d[2].operation,
    Operation::Checkpoint(456, 123, s) if
    *s == PathBuf::from("sdlfkj"),
  ));

  assert_eq!(d[3].log_id, 4);
  assert_eq!(d[3].tx_id, 1);
  assert!(matches!(d[3].operation, Operation::Commit));
}

#[test]
fn test_invalid_format() {
  let short: Vec<u8> = vec![0; 10];
  assert!(LogRecord::read_from(short.as_ref()).is_none());

  let mut bad_op = vec![0u8; 17];
  bad_op[16] = 255; // invalid operation type
  assert!(LogRecord::read_from(bad_op.as_ref()).is_none());
}
