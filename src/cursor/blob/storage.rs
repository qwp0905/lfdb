use crate::{
  debug,
  disk::IOPool,
  utils::{uuid_simple, SBox, ShortenedRwLock},
  Result,
};

use super::{BlobHandle, BlobId, BlobLen, BlobOffset, BlobReserved};
use std::{
  collections::HashMap,
  path::PathBuf,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
  },
};

const FILE_EXT: &str = "blob";
fn filename() -> PathBuf {
  PathBuf::from(uuid_simple()).with_extension(FILE_EXT)
}

pub struct BlobStorage {
  readonly: RwLock<HashMap<BlobId, SBox<BlobHandle>>>,
  writable: RwLock<HashMap<BlobId, SBox<BlobHandle>>>,
  last_id: AtomicU64,
  io_pool: Arc<IOPool>,
}
impl BlobStorage {
  pub fn replay(io_pool: Arc<IOPool>) -> Result<Self> {
    let mut readonly = HashMap::new();
    let mut last_id = 0;
    for entry in io_pool.read_dir()? {
      let filename = PathBuf::from(entry.file_name());
      if filename.extension().is_none_or(|ext| ext != FILE_EXT) {
        continue;
      }

      let handle = BlobHandle::replay(io_pool.open_buffered_io(filename)?)?;
      last_id = last_id.max(handle.get_id() + 1);
      readonly.insert(handle.get_id(), SBox::new(handle));
    }

    Ok(Self {
      readonly: RwLock::new(readonly),
      writable: RwLock::new(HashMap::new()),
      last_id: AtomicU64::new(last_id),
      io_pool,
    })
  }

  pub fn readonly_handles(&self) -> Vec<SBox<BlobHandle>> {
    self.readonly.rl().values().cloned().collect()
  }
  fn writable_handles(&self) -> Vec<SBox<BlobHandle>> {
    self.writable.rl().values().cloned().collect()
  }

  pub fn truncate(&self, blob_id: BlobId) -> Result {
    debug!("blob {blob_id} unreachable.");
    let Some(handle) = self.readonly.wl().remove(&blob_id) else {
      return Ok(());
    };
    handle.truncate()
  }

  pub fn get(&self, blob_id: BlobId) -> Option<SBox<BlobHandle>> {
    self
      .readonly
      .rl()
      .get(&blob_id)
      .cloned()
      .or_else(|| self.writable.rl().get(&blob_id).cloned())
  }

  pub fn append(&self, buf: Vec<u8>) -> Result<BlobAppendGuard<'_>> {
    let len = buf.len() as BlobOffset;
    loop {
      for handle in self.writable_handles() {
        match handle.reserve(len) {
          BlobReserved::Ok(offset) => {
            handle.write(&buf, offset)?;
            handle.sync().wait()?;
            return Ok(BlobAppendGuard::new(
              handle.get_id(),
              offset,
              len as BlobLen,
              None,
            ));
          }
          BlobReserved::Last(offset) => {
            handle.write(&buf, offset)?;
            handle.sync().wait()?;
            return Ok(BlobAppendGuard::new(
              handle.get_id(),
              offset,
              len as BlobLen,
              Some((handle, self)),
            ));
          }
          BlobReserved::Eof => continue,
        }
      }

      let last_id = self.last_id.fetch_add(1, Ordering::Relaxed);
      let new = BlobHandle::open(last_id, self.io_pool.open_buffered_io(filename())?)?;
      self.io_pool.sync_dir()?;
      self.writable.wl().insert(last_id, SBox::new(new));
    }
  }
}

pub struct BlobAppendGuard<'a> {
  id: BlobId,
  offset: BlobOffset,
  len: BlobLen,
  handle: Option<(SBox<BlobHandle>, &'a BlobStorage)>,
}
impl<'a> BlobAppendGuard<'a> {
  const fn new(
    id: BlobId,
    offset: BlobOffset,
    len: BlobLen,
    handle: Option<(SBox<BlobHandle>, &'a BlobStorage)>,
  ) -> Self {
    Self {
      id,
      offset,
      len,
      handle,
    }
  }
  pub const fn get_id(&self) -> BlobId {
    self.id
  }
  pub const fn get_offset(&self) -> BlobOffset {
    self.offset
  }
  pub const fn get_len(&self) -> BlobLen {
    self.len
  }
}
impl<'a> Drop for BlobAppendGuard<'a> {
  fn drop(&mut self) {
    let Some((handle, storage)) = self.handle.take() else {
      return;
    };
    storage
      .readonly
      .wl()
      .insert(handle.get_id(), handle.clone());
    storage.writable.wl().remove(&handle.get_id());
  }
}
