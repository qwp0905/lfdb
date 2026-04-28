use crate::{disk::Page, serialize::SerializeFrom};

use super::*;

#[test]
fn test_entry_with_data_roundtrip() {
  let mut page = Page::new();
  let entry = DataEntry::init(VersionRecord::new(
    1,
    100,
    RecordData::Data(vec![10, 20, 30]),
  ));
  page.serialize_from(&entry).expect("serialize error");

  let decoded: DataEntryView = page.view().expect("deserialize error");
  assert!(!decoded.is_empty());
  assert_eq!(decoded.get_last_owner(), Some(1));

  let records: Vec<_> = decoded.get_versions().collect();
  assert_eq!(records.len(), 1);
  assert_eq!(records[0].owner, 1);
  assert_eq!(records[0].version, 100);
  match &records[0].data {
    RecordDataRef::Data(d) => assert_eq!(d, &vec![10, 20, 30]),
    RecordDataRef::Tombstone => panic!("expected Data"),
    RecordDataRef::Chunked(_) => panic!("expected Data"),
  }
}

#[test]
fn test_entry_with_tombstone_roundtrip() {
  let mut page = Page::new();
  let entry = DataEntry::init(VersionRecord::new(2, 200, RecordData::Tombstone));
  page.serialize_from(&entry).expect("serialize error");

  let decoded: DataEntryView = page.view().expect("deserialize error");
  assert!(decoded.is_empty());
  assert_eq!(decoded.get_last_owner(), Some(2));

  let records: Vec<_> = decoded.get_versions().collect();
  assert_eq!(records.len(), 1);
  assert_eq!(records[0].owner, 2);
  match &records[0].data {
    RecordDataRef::Data(_) => panic!("expected Tombstone"),
    RecordDataRef::Tombstone => {}
    RecordDataRef::Chunked(_) => panic!("expected Tombstone"),
  }
}
#[test]
fn test_entry_with_chunked_roundtrip() {
  let mut page = Page::new();
  let pointers = vec![10, 20, 30, 500];
  let owner = 2;
  let entry = DataEntry::init(VersionRecord::new(
    2,
    200,
    RecordData::Chunked(pointers.clone()),
  ));
  page.serialize_from(&entry).expect("serialize error");

  let decoded: DataEntryView = page.view().expect("deserialize error");
  assert_eq!(decoded.get_last_owner(), Some(owner));

  let records: Vec<_> = decoded.get_versions().collect();
  assert_eq!(records.len(), 1);
  assert_eq!(records[0].owner, owner);
  match &records[0].data {
    RecordDataRef::Data(_) => panic!("expected Chunked"),
    RecordDataRef::Tombstone => panic!("expected Chunked"),
    RecordDataRef::Chunked(p) => assert_eq!(p, &pointers),
  }
}

#[test]
fn test_entry_with_next_roundtrip() {
  let mut page = Page::new();
  let mut entry = DataEntry::init(VersionRecord::new(1, 10, RecordData::Data(vec![1])));
  entry.set_next(42);
  page.serialize_from(&entry).expect("serialize error");

  let decoded: DataEntry = page.deserialize().expect("deserialize error");
  assert_eq!(decoded.get_next(), Some(42));
}

#[test]
fn test_entry_multiple_versions_roundtrip() {
  let mut page = Page::new();
  let mut entry = DataEntry::init(VersionRecord::new(3, 300, RecordData::Data(vec![3])));
  entry.append(VersionRecord::new(2, 200, RecordData::Tombstone));
  entry.append(VersionRecord::new(1, 100, RecordData::Data(vec![1, 2])));
  page.serialize_from(&entry).expect("serialize error");

  let decoded: DataEntryView = page.view().expect("deserialize error");
  assert!(!decoded.is_empty());
  assert_eq!(decoded.get_last_owner(), Some(1));
  assert_eq!(decoded.get_next(), None);
}
