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
fn test_commit_roundtrip() {
  let r = LogRecordUninit::new_commit(42);
  let parsed = assert_roundtrip(r, 2);
  assert!(matches!(parsed.operation, Operation::Commit));
}

#[test]
fn test_insert_roundtrip() {
  let mut page = vec![0; 100];
  page[0] = 0xAB;
  page[99] = 0xCD;

  let r = LogRecordUninit::new_insert(42, 1, 99, 11, page);
  let parsed = assert_roundtrip(r, 4);
  match parsed.operation {
    Operation::Insert {
      table_id,
      pointer,
      data,
      current_version,
    } => {
      assert_eq!(table_id, 1);
      assert_eq!(pointer, 99);
      assert_eq!(data[0], 0xAB);
      assert_eq!(data[99], 0xCD);
      assert_eq!(current_version, 11);
    }
    _ => panic!("expected Insert"),
  }
}

#[test]
fn test_checkpoint_roundtrip() {
  let i = 200;
  let cv = 10;
  let path = PathBuf::from("sdfsdf");
  let r = LogRecordUninit::new_checkpoint(i, cv, path.clone());
  let parsed = assert_roundtrip(r, 5);
  match parsed.operation {
    Operation::Checkpoint {
      last_log_id,
      current_version,
      snapshot,
    } => {
      assert_eq!(last_log_id, i);
      assert_eq!(current_version, cv);
      assert_eq!(path, snapshot);
    }
    _ => panic!("expected Checkpoint"),
  }
}

#[test]
fn test_entry_roundtrip() {
  let mut page = Page::new();
  let mut writer = page.writer();

  writer.write(&(3u16).to_le_bytes()).unwrap();
  let r2 = LogRecordUninit::new_insert(1, 0, 10, 12, vec![1]);
  let r4 = LogRecordUninit::new_checkpoint(456, 123, "sdlfkj".into());
  let r3 = LogRecordUninit::new_commit(1);

  writer.write(&r2.init(2)).unwrap();
  writer.write(&r4.init(3)).unwrap();
  writer.write(&r3.init(4)).unwrap();

  let (d, complete) = read_page(&page);
  assert!(!complete);

  assert_eq!(d.len(), 3);

  assert_eq!(d[0].log_id, 2);
  assert_eq!(d[0].tx_id, 1);
  assert!(matches!(
    &d[0].operation,
    Operation::Insert {
      table_id: 0,
      pointer: 10,
      current_version: 12,
      ..
    }
  ));

  assert_eq!(d[1].log_id, 3);
  assert!(matches!(
    &d[1].operation,
    Operation::Checkpoint{ last_log_id:456, current_version:123, snapshot } if
    snapshot == PathBuf::from("sdlfkj").as_path(),
  ));

  assert_eq!(d[2].log_id, 4);
  assert_eq!(d[2].tx_id, 1);
  assert!(matches!(d[2].operation, Operation::Commit));
}

#[test]
fn test_invalid_format() {
  let short: Vec<u8> = vec![0; 10];
  assert!(LogRecord::read_from(short.as_ref()).is_none());

  let mut bad_op = vec![0u8; 17];
  bad_op[16] = 255; // invalid operation type
  assert!(LogRecord::read_from(bad_op.as_ref()).is_none());
}
