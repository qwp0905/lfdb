use std::path::Path;

use redb::{Database, ReadableDatabase, TableDefinition};

use crate::BenchmarkDB;

pub fn new(cache_size: usize, dir: &Path) -> impl BenchmarkDB + 'static {
  Database::builder()
    .set_cache_size(cache_size)
    .create(dir.join("redb.redb"))
    .unwrap()
}

impl BenchmarkDB for Database {
  fn ensure_table(&self, table: &str) {
    let tx = self.begin_write().unwrap();
    tx.open_table(TableDefinition::<&[u8], Vec<u8>>::new(table))
      .unwrap();
    tx.commit().unwrap();
  }
  fn drop_table(&self, table: &str) {
    let tx = self.begin_write().unwrap();
    tx.delete_table(TableDefinition::<&[u8], Vec<u8>>::new(table))
      .unwrap();
    tx.commit().unwrap();
  }
  fn get(&self, table: &str, key: &[u8]) {
    let tx = self.begin_read().unwrap();
    {
      let table = tx
        .open_table(TableDefinition::<&[u8], Vec<u8>>::new(table))
        .unwrap();
      table.get(key).unwrap();
    }
    tx.close().unwrap();
  }
  fn insert(&self, table: &str, key: Vec<u8>, value: Vec<u8>) {
    let tx = self.begin_write().unwrap();
    {
      let mut table = tx
        .open_table(TableDefinition::<&[u8], Vec<u8>>::new(table))
        .unwrap();
      table.insert(key.as_slice(), value).unwrap();
    }

    tx.commit().unwrap();
  }
  fn scan(&self, table: &str, start: &[u8], end: &[u8]) {
    let tx = self.begin_read().unwrap();
    {
      let table = tx
        .open_table(TableDefinition::<&[u8], Vec<u8>>::new(table))
        .unwrap();
      let mut iter = table.range(start..end).unwrap();
      while let Some(_) = iter.next() {}
    }
    tx.close().unwrap();
  }
  fn read_modify_write(&self, table: &str, key: Vec<u8>, value: Vec<u8>) {
    let tx = self.begin_write().unwrap();
    {
      let mut table = tx
        .open_table(TableDefinition::<&[u8], Vec<u8>>::new(table))
        .unwrap();
      if let Some(mut g) = table.get_mut(key.as_slice()).unwrap() {
        g.insert(value).unwrap();
      } else {
        table.insert(key.as_slice(), value).unwrap();
      };
    }
    tx.commit().unwrap();
  }
  fn bulk(&self, table: &str, kvs: Vec<(Vec<u8>, Vec<u8>)>) {
    let tx = self.begin_write().unwrap();
    {
      let mut table = tx
        .open_table(TableDefinition::<&[u8], Vec<u8>>::new(table))
        .unwrap();
      for (k, v) in kvs {
        table.insert(k.as_slice(), v).unwrap();
      }
    }
    tx.commit().unwrap();
  }
}
