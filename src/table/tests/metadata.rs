use super::*;

#[test]
fn test_simple() {
  let id = 13;
  let name = unsafe { TableName::from_str_unchecked("test") };
  let path = PathBuf::from(&*name);
  let metadata = TableMetadata::new(id, name.clone(), path.clone());

  let bytes = metadata.to_vec();

  let d = TableMetadata::from_bytes(&bytes).unwrap();
  assert_eq!(d.get_id(), id);
  assert_eq!(d.get_name(), &name);
  assert_eq!(d.get_version(), TableFormatVersion::CURRENT);
  assert_eq!(d.get_filename(), path);
}

#[test]
fn test_compaction() {
  let id = 13;
  let name = unsafe { TableName::from_str_unchecked("test") };
  let path = PathBuf::from(&*name);
  let cid = 123123;
  let cpath = PathBuf::from("compaction");
  let mut metadata = TableMetadata::new(id, name.clone(), path.clone());
  let cmeta = TableMetadata::new(cid, name.clone(), cpath.clone());
  metadata.set_compaction(&cmeta);

  let bytes = metadata.to_vec();

  let d = TableMetadata::from_bytes(&bytes).unwrap();
  assert_eq!(d.get_id(), id);
  assert_eq!(d.get_name(), &name);
  assert_eq!(d.get_version(), TableFormatVersion::CURRENT);
  assert_eq!(d.get_filename(), path);
  let m = d.get_compaction_metadata().unwrap();
  assert_eq!(m.get_id(), cid);
  assert_eq!(m.get_version(), TableFormatVersion::CURRENT);
  assert_eq!(m.get_filename(), cpath.as_path());
}
