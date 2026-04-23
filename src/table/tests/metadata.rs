use super::*;

#[test]
fn test_simple() {
  let id = 13;
  let name = format!("test");
  let path = PathBuf::from(format!("path/to/test"));
  let metadata = TableMetadata::new(id, name.clone(), path.clone());

  let bytes = metadata.to_vec();

  let d = TableMetadata::from_bytes(&bytes).unwrap();
  assert_eq!(d.get_id(), id);
  assert_eq!(d.get_name(), name);
  assert_eq!(d.get_path(), path);
}

#[test]
fn test_compaction() {
  let id = 13;
  let name = format!("test");
  let path = PathBuf::from(format!("path/to/test"));
  let cid = 123123;
  let cpath = PathBuf::from(format!("path/to/compaction"));
  let mut metadata = TableMetadata::new(id, name.clone(), path.clone());
  let cmeta = TableMetadata::new(cid, name.clone(), cpath.clone());
  metadata.set_compaction(&cmeta);

  let bytes = metadata.to_vec();

  let d = TableMetadata::from_bytes(&bytes).unwrap();
  assert_eq!(d.get_id(), id);
  assert_eq!(d.get_name(), name);
  assert_eq!(d.get_path(), path);
  let m = d.get_compaction_metadata().unwrap();
  assert_eq!(m.get_id(), cid);
  assert_eq!(m.get_path(), cpath.as_path());
}
