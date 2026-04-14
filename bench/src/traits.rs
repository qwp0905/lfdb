pub trait BenchmarkDB {
  fn ensure_table(&self, table: &str);
  fn drop_table(&self, table: &str);
  fn bulk(&self, table: &str, kvs: Vec<(Vec<u8>, Vec<u8>)>);
  fn get(&self, table: &str, key: &[u8]);
  fn insert(&self, table: &str, key: Vec<u8>, value: Vec<u8>);
  fn scan(&self, table: &str, start: &[u8], end: &[u8]);
  fn read_modify_write(&self, table: &str, key: Vec<u8>, value: Vec<u8>);
}
