use super::*;
use crate::cursor::objects::*;
use crate::{disk::Page, serialize::SerializeFrom};

#[test]
fn test_entry_with_data_roundtrip() {
  let mut page = Page::new();
  let entry = DataEntry::init(
    VersionRecord::new(1, 100, RecordData::Data(vec![10, 20, 30]), 1),
    None,
    56,
  );
  page.serialize_from(&entry).expect("serialize error");

  let decoded: DataEntryView = page.view().expect("deserialize error");

  let mut records = Vec::new();
  let mut iter = decoded.get_versions();
  while let Some(record) = iter.try_next().unwrap() {
    records.push(record);
  }
  assert_eq!(records.len(), 1);
  assert_eq!(records[0].owner, 1);
  assert_eq!(records[0].version, 100);
  match &records[0].data {
    RecordDataView::Data(s, e) => assert_eq!(page.range(*s..*e), &vec![10, 20, 30]),
    RecordDataView::Tombstone => panic!("expected Data"),
    RecordDataView::Blob(_, _, _) => panic!("expected Data"),
  }
}

#[test]
fn test_entry_with_tombstone_roundtrip() {
  let mut page = Page::new();
  let entry = DataEntry::init(
    VersionRecord::new(2, 200, RecordData::Tombstone, 10),
    None,
    84,
  );
  page.serialize_from(&entry).expect("serialize error");

  let decoded: DataEntryView = page.view().expect("deserialize error");

  let mut records = Vec::new();
  let mut iter = decoded.get_versions();
  while let Some(record) = iter.try_next().unwrap() {
    records.push(record);
  }
  assert_eq!(records.len(), 1);
  assert_eq!(records[0].owner, 2);
  match &records[0].data {
    RecordDataView::Data(_, _) => panic!("expected Tombstone"),
    RecordDataView::Tombstone => {}
    RecordDataView::Blob(_, _, _) => panic!("expected Tombstone"),
  }
}
#[test]
fn test_entry_with_chunked_roundtrip() {
  let mut page = Page::new();
  let owner = 2;
  let entry = DataEntry::init(
    VersionRecord::new(2, 200, RecordData::Blob(1, 2, 3), 1),
    None,
    4,
  );
  page.serialize_from(&entry).expect("serialize error");

  let decoded: DataEntryView = page.view().expect("deserialize error");
  assert_eq!(decoded.len(), 1);

  let mut records = Vec::new();
  let mut iter = decoded.get_versions();
  while let Some(record) = iter.try_next().unwrap() {
    records.push(record);
  }
  assert_eq!(records.len(), 1);
  assert_eq!(records[0].owner, owner);
  match &records[0].data {
    RecordDataView::Data(_, _) => panic!("expected Chunked"),
    RecordDataView::Tombstone => panic!("expected Chunked"),
    RecordDataView::Blob(id, offset, len) => {
      assert_eq!(*id, 1);
      assert_eq!(*offset, 2);
      assert_eq!(*len, 3);
    }
  };
}

#[test]
fn test_entry_with_next_roundtrip() {
  let mut page = Page::new();
  let entry = DataEntry::init(
    VersionRecord::new(1, 10, RecordData::Data(vec![1]), 4),
    Some(42),
    69,
  );
  page.serialize_from(&entry).expect("serialize error");

  let decoded: DataEntry = page.deserialize().expect("deserialize error");
  assert_eq!(decoded.get_next(), Some(42));
}

#[test]
fn test_entry_multiple_versions_roundtrip() {
  let mut page = Page::new();
  let mut entry = DataEntry::init(
    VersionRecord::new(3, 300, RecordData::Data(vec![3]), 32),
    None,
    68,
  );
  entry.attach_front(VersionRecord::new(2, 200, RecordData::Tombstone, 8), 29);
  entry.attach_front(
    VersionRecord::new(1, 100, RecordData::Data(vec![1, 2]), 89),
    48757,
  );
  page.serialize_from(&entry).expect("serialize error");

  let decoded: DataEntry = page.deserialize().expect("deserialize error");
  assert_eq!(decoded.get_next(), None);
}
