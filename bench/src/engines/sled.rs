use std::path::Path;

use sled::{Batch, Config, Db, transaction::ConflictableTransactionResult};

use crate::BenchmarkDB;

pub fn new(cache_size: usize, dir: &Path) -> Db {
  Config::new()
    .cache_capacity(cache_size as u64)
    .path(dir)
    .open()
    .unwrap()
}
impl BenchmarkDB for Db {
  fn ensure_table(&self, table: &str) {
    self.open_tree(table).unwrap();
  }

  fn drop_table(&self, table: &str) {
    self.drop_tree(table).unwrap();
  }

  fn bulk(&self, table: &str, kvs: Vec<(Vec<u8>, Vec<u8>)>) {
    let t = self.open_tree(table).unwrap();
    let mut b = Batch::default();
    for (k, v) in kvs {
      b.insert(k, v);
    }
    t.apply_batch(b).unwrap();
    t.flush().unwrap();
  }

  fn get(&self, table: &str, key: &[u8]) {
    let t = self.open_tree(table).unwrap();
    t.transaction(|tx| {
      tx.get(key)?;
      Ok(()) as ConflictableTransactionResult<()>
    })
    .unwrap();
  }

  fn insert(&self, table: &str, key: Vec<u8>, value: Vec<u8>) {
    let t = self.open_tree(table).unwrap();
    t.transaction(|tx| {
      tx.insert(key.clone(), value.clone())?;
      tx.flush();
      Ok(()) as ConflictableTransactionResult<()>
    })
    .unwrap();
  }

  fn scan(&self, table: &str, start: &[u8], end: &[u8]) {
    let t = self.open_tree(table).unwrap();
    for _ in t.range(start..end) {}
  }

  fn read_modify_write(&self, table: &str, key: Vec<u8>, value: Vec<u8>) {
    let t = self.open_tree(table).unwrap();

    t.transaction(|tx| {
      tx.get(&key)?;
      tx.insert(key.clone(), value.clone())?;
      tx.flush();
      Ok(()) as ConflictableTransactionResult<()>
    })
    .unwrap();
  }
}
