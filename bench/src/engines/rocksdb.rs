use std::path::Path;

use rocksdb::{
  BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, Direction, ErrorKind,
  IteratorMode, OptimisticTransactionDB, OptimisticTransactionOptions, Options,
  WriteOptions,
};

use crate::BenchmarkDB;

struct Engine {
  db: OptimisticTransactionDB,
  options: Options,
  write_options: WriteOptions,
  tx_options: OptimisticTransactionOptions,
  _cache: Cache,
}

pub fn new(cache_size: usize, dir: &Path) -> impl BenchmarkDB + 'static {
  let _cache = Cache::new_lru_cache(cache_size);
  let mut block_opts = BlockBasedOptions::default();
  block_opts.set_block_cache(&_cache);

  let mut options = Options::default();
  options.create_if_missing(true);
  options.set_use_direct_reads(true);
  options.set_use_direct_io_for_flush_and_compaction(true);
  options.set_block_based_table_factory(&block_opts);

  let cf_names =
    DB::list_cf(&options, dir).unwrap_or_else(|_| vec!["default".to_string()]);
  let descriptors: Vec<_> = cf_names
    .iter()
    .map(|name| ColumnFamilyDescriptor::new(name, options.clone()))
    .collect();
  let db =
    OptimisticTransactionDB::open_cf_descriptors(&options, dir, descriptors).unwrap();

  let mut write_options = WriteOptions::default();
  write_options.set_sync(true);
  let tx_options = OptimisticTransactionOptions::default();

  Engine {
    db,
    options,
    write_options,
    tx_options,
    _cache,
  }
}

impl BenchmarkDB for Engine {
  fn ensure_table(&self, table: &str) {
    let _ = self.db.create_cf(table, &self.options);
  }

  fn drop_table(&self, table: &str) {
    self.db.drop_cf(table).unwrap();
  }

  fn bulk(&self, table: &str, kvs: Vec<(Vec<u8>, Vec<u8>)>) {
    let cf = self.db.cf_handle(table).unwrap();
    let tx = self
      .db
      .transaction_opt(&self.write_options, &self.tx_options);
    for (k, v) in kvs {
      tx.put_cf(&cf, k, v).unwrap();
    }
    tx.commit().unwrap();
  }

  fn get(&self, table: &str, key: &[u8]) {
    let cf = self.db.cf_handle(table).unwrap();
    let tx = self
      .db
      .transaction_opt(&self.write_options, &self.tx_options);
    tx.get_cf(&cf, key).unwrap();
  }

  fn insert(&self, table: &str, key: Vec<u8>, value: Vec<u8>) {
    let cf = self.db.cf_handle(table).unwrap();
    loop {
      let tx = self
        .db
        .transaction_opt(&self.write_options, &self.tx_options);
      tx.put_cf(&cf, &key, &value).unwrap();
      match tx.commit() {
        Ok(_) => return,
        Err(e) if e.kind() == ErrorKind::Busy => continue,
        Err(e) => panic!("{e}"),
      }
    }
  }

  fn scan(&self, table: &str, start: &[u8], end: &[u8]) {
    let cf = self.db.cf_handle(table).unwrap();
    let tx = self
      .db
      .transaction_opt(&self.write_options, &self.tx_options);
    let iter = tx.iterator_cf(&cf, IteratorMode::From(start, Direction::Forward));
    for item in iter {
      let (k, _v) = item.unwrap();
      if k.as_ref() >= end {
        break;
      }
    }
  }

  fn read_modify_write(&self, table: &str, key: Vec<u8>, value: Vec<u8>) {
    let cf = self.db.cf_handle(table).unwrap();
    loop {
      let tx = self
        .db
        .transaction_opt(&self.write_options, &self.tx_options);
      tx.get_cf(&cf, &key).unwrap();
      tx.put_cf(&cf, &key, &value).unwrap();
      match tx.commit() {
        Ok(_) => return,
        Err(e) if e.kind() == ErrorKind::Busy => continue,
        Err(e) => panic!("{e}"),
      }
    }
  }
}
