use super::*;
use crate::disk::Page;
use crate::objects::TypedObject;

#[test]
fn test_entry_with_data_roundtrip() {
  let mut page = Page::new();
  let mut obj: TypedObject = DataEntry::empty(None).into();
  let entry = obj.as_data_entry_mut().unwrap();
  entry.append(VersionRecord::new(
    1,
    100,
    RecordData::Data(vec![10, 20, 30]),
  ));
  page.serialize_from(&obj).expect("serialize error");

  let obj = page.view().expect("deserialize error");
  let decoded = obj.as_data_entry().unwrap();

  let records: Vec<_> = decoded.get_versions().collect();
  assert_eq!(records.len(), 1);
  assert_eq!(records[0].owner, 1);
  assert_eq!(records[0].version, 100);
  match &records[0].data {
    RecordDataView::Data(s, e) => assert_eq!(page.range(*s..*e), &vec![10, 20, 30]),
    RecordDataView::Tombstone => panic!("expected Data"),
    RecordDataView::Chunked(_) => panic!("expected Data"),
  }
}

#[test]
fn test_entry_with_tombstone_roundtrip() {
  let mut page = Page::new();
  let mut obj: TypedObject = DataEntry::empty(None).into();
  let entry = obj.as_data_entry_mut().unwrap();
  entry.append(VersionRecord::new(2, 200, RecordData::Tombstone));
  page.serialize_from(&obj).expect("serialize error");

  let obj = page.view().expect("deserialize error");
  let decoded = obj.as_data_entry().unwrap();

  let records: Vec<_> = decoded.get_versions().collect();
  assert_eq!(records.len(), 1);
  assert_eq!(records[0].owner, 2);
  match &records[0].data {
    RecordDataView::Data(_, _) => panic!("expected Tombstone"),
    RecordDataView::Tombstone => {}
    RecordDataView::Chunked(_) => panic!("expected Tombstone"),
  }
}
#[test]
fn test_entry_with_chunked_roundtrip() {
  let mut page = Page::new();
  let pointers = vec![10, 20, 30, 500];
  let owner = 2;

  let mut obj: TypedObject = DataEntry::empty(None).into();
  let entry = obj.as_data_entry_mut().unwrap();
  entry.append(VersionRecord::new(
    2,
    200,
    RecordData::Chunked(pointers.clone()),
  ));
  page.serialize_from(&obj).expect("serialize error");

  let obj = page.view().expect("deserialize error");
  let decoded = obj.as_data_entry().unwrap();
  assert_eq!(decoded.len(), 1);

  let records: Vec<_> = decoded.get_versions().collect();
  assert_eq!(records.len(), 1);
  assert_eq!(records[0].owner, owner);
  match &records[0].data {
    RecordDataView::Data(_, _) => panic!("expected Chunked"),
    RecordDataView::Tombstone => panic!("expected Chunked"),
    RecordDataView::Chunked(p) => assert_eq!(p, &pointers),
  }
}

#[test]
fn test_entry_with_next_roundtrip() {
  let mut page = Page::new();
  let mut obj: TypedObject = DataEntry::empty(Some(42)).into();
  let entry = obj.as_data_entry_mut().unwrap();
  entry.append(VersionRecord::new(1, 10, RecordData::Data(vec![1])));
  page.serialize_from(&obj).expect("serialize error");

  let obj = page.view().expect("deserialize error");
  let decoded = obj.as_data_entry().unwrap();
  assert_eq!(decoded.get_next(), Some(42));
}

#[test]
fn test_entry_multiple_versions_roundtrip() {
  let mut page = Page::new();
  let mut obj: TypedObject = DataEntry::empty(None).into();
  let entry = obj.as_data_entry_mut().unwrap();
  entry.append(VersionRecord::new(3, 300, RecordData::Data(vec![3])));
  entry.append(VersionRecord::new(2, 200, RecordData::Tombstone));
  entry.append(VersionRecord::new(1, 100, RecordData::Data(vec![1, 2])));
  page.serialize_from(&obj).expect("serialize error");

  let obj = page.deserialize().expect("deserialize error");
  let decoded = obj.as_data_entry().unwrap();
  assert_eq!(decoded.get_next(), None);
}
