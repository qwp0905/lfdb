use std::{path::Path, time::Duration};

use lfdb::{Engine, EngineBuilder};

use crate::BenchmarkDB;

pub fn new(cache_size: usize, dir: &Path) -> impl BenchmarkDB + 'static {
  EngineBuilder::new(dir)
    .buffer_pool_memory_capacity(cache_size)
    .build()
    .unwrap()
}

impl BenchmarkDB for Engine {
  fn get(&self, table: &str, key: &[u8]) {
    let tx = self.new_tx().unwrap();
    let table = tx.table(table).unwrap();
    table.get(&key).unwrap();
  }

  fn insert(&self, table: &str, key: Vec<u8>, value: Vec<u8>) {
    loop {
      let mut tx = self.new_tx().unwrap();
      let table = tx.table(table).unwrap();
      match table.insert(key.clone(), value.clone()) {
        Ok(_) => return tx.commit().unwrap(),
        Err(lfdb::Error::WriteConflict) => continue,
        Err(err) => panic!("{err}"),
      }
    }
  }

  fn scan(&self, table: &str, start: &[u8], end: &[u8]) {
    let tx = self.new_tx().unwrap();
    let table = tx.table(table).unwrap();
    {
      let mut iter = table.scan(start..end).unwrap();
      while let Some(_) = iter.try_next().unwrap() {}
    }
  }

  fn ensure_table(&self, table: &str) {
    let mut tx = self.new_tx().unwrap();
    tx.open_table(table).unwrap();
    tx.commit().unwrap();
  }

  fn drop_table(&self, table: &str) {
    let mut tx = self.new_tx().unwrap();
    tx.drop_table(table).unwrap();
    tx.commit().unwrap();
  }

  fn read_modify_write(&self, table: &str, key: Vec<u8>, value: Vec<u8>) {
    loop {
      let mut tx = self.new_tx().unwrap();
      let table = tx.table(table).unwrap();
      table.get(&key).unwrap();
      match table.insert(key.clone(), value.clone()) {
        Ok(_) => return tx.commit().unwrap(),
        Err(lfdb::Error::WriteConflict) => continue,
        Err(err) => panic!("{err}"),
      }
    }
  }

  fn bulk(&self, table: &str, kvs: Vec<(Vec<u8>, Vec<u8>)>) {
    let mut tx = self.new_tx_timeout(Duration::from_mins(10)).unwrap();
    let table = tx.table(table).unwrap();
    for (k, v) in kvs {
      table.insert(k, v).unwrap();
    }
    tx.commit().unwrap();
  }
}
