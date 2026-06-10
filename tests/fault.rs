use std::{
  fs::{Metadata, OpenOptions, ReadDir},
  io::{Error as IoError, ErrorKind, IoSlice, Read, Result as IoResult, Write},
  panic::RefUnwindSafe,
  path::Path,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};

use lfdb::{DefaultIOBackend, DiskBackend, EngineBuilder, IOBackend};
use tempfile::tempdir_in;

#[derive(Clone)]
struct FaultController {
  fail_next_wal_fdatasync: Arc<AtomicBool>,
  fill_wal_on_next_write: Arc<AtomicBool>,
}
impl FaultController {
  fn new() -> Self {
    Self {
      fail_next_wal_fdatasync: Arc::new(AtomicBool::new(false)),
      fill_wal_on_next_write: Arc::new(AtomicBool::new(false)),
    }
  }

  fn fail_next_wal_fdatasync(&self) {
    self.fail_next_wal_fdatasync.store(true, Ordering::Release);
  }

  fn fill_wal_on_next_write(&self) {
    self.fill_wal_on_next_write.store(true, Ordering::Release);
  }
}

struct FaultIO {
  inner: Box<dyn IOBackend>,
  controller: FaultController,
  is_wal: bool,
}
impl RefUnwindSafe for FaultIO {}
impl Read for FaultIO {
  fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
    self.inner.read(buf)
  }
}
impl Write for FaultIO {
  fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
    self.inner.write(buf)
  }

  fn flush(&mut self) -> IoResult<()> {
    self.inner.flush()
  }
}
impl IOBackend for FaultIO {
  fn pread(&self, buf: &mut [u8], offset: u64) -> IoResult<usize> {
    self.inner.pread(buf, offset)
  }

  fn pwrite(&self, buf: &[u8], offset: u64) -> IoResult<usize> {
    if self.is_wal
      && self
        .controller
        .fill_wal_on_next_write
        .swap(false, Ordering::AcqRel)
    {
      return Err(IoError::from(ErrorKind::StorageFull));
    }
    self.inner.pwrite(buf, offset)
  }

  fn pwritev(&self, bufs: &[IoSlice], offset: u64) -> IoResult<usize> {
    if self.is_wal
      && self
        .controller
        .fill_wal_on_next_write
        .swap(false, Ordering::AcqRel)
    {
      return Err(IoError::from(ErrorKind::StorageFull));
    }
    self.inner.pwritev(bufs, offset)
  }

  fn fallocate(&self, offset: u64, len: u64) -> IoResult<()> {
    self.inner.fallocate(offset, len)
  }

  fn fsync(&self) -> IoResult<()> {
    self.inner.fsync()
  }

  fn fdatasync(&self) -> IoResult<()> {
    if self.is_wal
      && self
        .controller
        .fail_next_wal_fdatasync
        .swap(false, Ordering::AcqRel)
    {
      return Err(IoError::other("injected WAL fdatasync failure"));
    }
    self.inner.fdatasync()
  }

  fn metadata(&self) -> IoResult<Metadata> {
    self.inner.metadata()
  }
}

struct FaultBackend {
  inner: DefaultIOBackend,
  controller: FaultController,
}
impl FaultBackend {
  fn new(controller: FaultController) -> Self {
    Self {
      inner: DefaultIOBackend,
      controller,
    }
  }

  fn wrap(&self, path: &Path, inner: Box<dyn IOBackend>) -> Box<dyn IOBackend> {
    Box::new(FaultIO {
      inner,
      controller: self.controller.clone(),
      is_wal: path.extension().is_some_and(|ext| ext == "log"),
    })
  }
}
impl RefUnwindSafe for FaultBackend {}
impl DiskBackend for FaultBackend {
  fn open(&self, options: &mut OpenOptions, path: &Path) -> IoResult<Box<dyn IOBackend>> {
    self
      .inner
      .open(options, path)
      .map(|inner| self.wrap(path, inner))
  }

  fn open_direct_io(
    &self,
    options: &mut OpenOptions,
    path: &Path,
  ) -> IoResult<Box<dyn IOBackend>> {
    self
      .inner
      .open_direct_io(options, path)
      .map(|inner| self.wrap(path, inner))
  }

  fn read_dir(&self, path: &Path) -> IoResult<ReadDir> {
    self.inner.read_dir(path)
  }

  fn remove_file(&self, path: &Path) -> IoResult<()> {
    self.inner.remove_file(path)
  }

  fn exists(&self, path: &Path) -> IoResult<bool> {
    self.inner.exists(path)
  }

  fn rename(&self, from: &Path, to: &Path) -> IoResult<()> {
    self.inner.rename(from, to)
  }

  fn ensure_dir(&self, path: &Path) -> IoResult<()> {
    self.inner.ensure_dir(path)
  }
}

#[test]
fn wal_fdatasync_failure_is_returned_by_commit() {
  let dir = tempdir_in(".").unwrap();
  let controller = FaultController::new();
  let engine = EngineBuilder::new(dir.path())
    .with_backend(FaultBackend::new(controller.clone()))
    .unwrap();

  {
    let mut create = engine.new_tx().unwrap();
    create.open_table("test").unwrap();
    create.commit().unwrap();
  }

  {
    let mut failed = engine.new_tx().unwrap();
    failed
      .table("test")
      .unwrap()
      .insert(b"failed".to_vec(), b"value".to_vec())
      .unwrap();

    controller.fail_next_wal_fdatasync();
    assert!(failed.commit().is_err());
  }

  {
    let mut committed = engine.new_tx().unwrap();
    committed
      .table("test")
      .unwrap()
      .insert(b"committed".to_vec(), b"value".to_vec())
      .unwrap();
    committed.commit().unwrap();
  }

  {
    let read = engine.new_tx().unwrap();
    let table = read.table("test").unwrap();
    assert_eq!(table.get(b"failed").unwrap(), None);
    assert_eq!(
      table.get(b"committed").unwrap().as_deref(),
      Some(b"value".as_slice())
    );
  }
}

#[test]
#[ignore]
fn failed_commit_does_not_hide_previous_commits_or_publish_current_writes() {
  let dir = tempdir_in(".").unwrap();
  let controller = FaultController::new();
  {
    let engine = EngineBuilder::new(dir.path())
      .with_backend(FaultBackend::new(controller.clone()))
      .unwrap();

    {
      let mut create = engine.new_tx().unwrap();
      create.open_table("test").unwrap();
      create.commit().unwrap();
    }

    {
      let mut committed = engine.new_tx().unwrap();
      let table = committed.table("test").unwrap();
      for i in 0..8 {
        table
          .insert(
            format!("committed-{i}").into_bytes(),
            format!("value-{i}").into_bytes(),
          )
          .unwrap();
      }
      committed.commit().unwrap();
    }

    {
      let mut failed = engine.new_tx().unwrap();
      let table = failed.table("test").unwrap();
      for i in 0..8 {
        table
          .insert(
            format!("failed-{i}").into_bytes(),
            format!("value-{i}").into_bytes(),
          )
          .unwrap();
      }

      controller.fail_next_wal_fdatasync();
      assert!(failed.commit().is_err());
    }

    {
      let read = engine.new_tx().unwrap();
      let table = read.table("test").unwrap();
      for i in 0..8 {
        let committed_key = format!("committed-{i}").into_bytes();
        let failed_key = format!("failed-{i}").into_bytes();
        let value = format!("value-{i}").into_bytes();
        assert_eq!(
          table.get(&committed_key).unwrap().as_deref(),
          Some(value.as_slice())
        );
        assert_eq!(table.get(&failed_key).unwrap(), None);
      }
    }

    {
      let mut failed = engine.new_tx().unwrap();
      let table = failed.table("test").unwrap();
      for i in 0..8 {
        assert!(table
          .insert(
            format!("failed-{i}").into_bytes(),
            format!("value-{i}").into_bytes(),
          )
          .is_err());
      }

      assert!(failed.commit().is_err());
    }
  }

  let reopened = EngineBuilder::new(dir.path())
    .with_backend(FaultBackend::new(controller))
    .unwrap();
  let read = reopened.new_tx().unwrap();
  let table = read.table("test").unwrap();
  for i in 0..8 {
    let committed_key = format!("committed-{i}").into_bytes();
    let failed_key = format!("failed-{i}").into_bytes();
    let value = format!("value-{i}").into_bytes();
    assert_eq!(
      table.get(&committed_key).unwrap().as_deref(),
      Some(value.as_slice())
    );
    assert_eq!(table.get(&failed_key).unwrap(), None);
  }
}

#[test]
#[ignore]
fn wal_disk_full_is_returned_without_publishing_transaction() {
  let dir = tempdir_in(".").unwrap();
  let controller = FaultController::new();
  {
    let engine = EngineBuilder::new(dir.path())
      .with_backend(FaultBackend::new(controller.clone()))
      .unwrap();

    {
      let mut create = engine.new_tx().unwrap();
      create.open_table("test").unwrap();
      create.commit().unwrap();
    }

    {
      let mut baseline = engine.new_tx().unwrap();
      baseline
        .table("test")
        .unwrap()
        .insert(b"baseline".to_vec(), b"value".to_vec())
        .unwrap();
      baseline.commit().unwrap();
    }

    {
      let mut failed = engine.new_tx().unwrap();
      failed
        .table("test")
        .unwrap()
        .insert(b"disk-full".to_vec(), b"value".to_vec())
        .unwrap();

      controller.fill_wal_on_next_write();
      let err = failed.commit().unwrap_err();
      assert!(matches!(
        err,
        lfdb::Error::IO(ref err) if err.kind() == ErrorKind::StorageFull
      ));
    }

    {
      let read = engine.new_tx().unwrap();
      let table = read.table("test").unwrap();
      assert_eq!(
        table.get(b"baseline").unwrap().as_deref(),
        Some(b"value".as_slice())
      );
      assert_eq!(table.get(b"disk-full").unwrap(), None);
    }
  }

  let reopened = EngineBuilder::new(dir.path())
    .with_backend(FaultBackend::new(controller))
    .unwrap();
  let read = reopened.new_tx().unwrap();
  let table = read.table("test").unwrap();
  assert_eq!(
    table.get(b"baseline").unwrap().as_deref(),
    Some(b"value".as_slice())
  );
  assert_eq!(table.get(b"disk-full").unwrap(), None);
}
