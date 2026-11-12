use std::{sync::Arc, time::Duration};

use tempfile::{tempdir_in, TempDir};

use super::*;
use crate::{
  background::EventBus,
  disk::{DefaultDiskBackend, IOPool},
  error::Error,
  page::Page,
  table::TableMapper,
  wal::{DurabilityGuard, WALConfig, RESERVED_TX},
};

const ORIGINAL: [u8; 8] = [1; 8];
const CHANGED: [u8; 8] = [2; 8];

fn append<'a>(
  wal: &'a WriteAheadLog,
  table: &TableHandleRef,
  pointer: Pointer,
  page: &Page<PAGE_SIZE>,
) -> Result<DurabilityGuard<'a>> {
  wal.append_insert(
    RESERVED_TX,
    table.get_id(),
    pointer,
    RESERVED_TX,
    page.range(0..ORIGINAL.len()),
  )
}

struct Fixture {
  _dir: TempDir,
  cache: BlockCache,
  wal: Arc<WriteAheadLog>,
  table: TableHandleRef,
  pointer: Pointer,
}
impl Fixture {
  fn new() -> Result<Self> {
    let dir = tempdir_in(".").map_err(Error::IO)?;
    let metrics = Arc::new(MetricsRegistry::new());
    let io_pool = Arc::new(IOPool::with_backend(
      DefaultDiskBackend::new().map_err(Error::IO)?,
      dir.path(),
      metrics.clone(),
    )?);
    let wal = Arc::new(WriteAheadLog::init(
      WALConfig {
        max_file_size: 1 << 20,
        max_buffer_size: WALConfig::MIN_BUFFER_SIZE,
        segment_expiration: Duration::from_secs(60),
        segment_min_count: 1,
      },
      Arc::new(EventBus::new()),
      io_pool.clone(),
      metrics.clone(),
    )?);
    let (mapper, _) = TableMapper::open_new(io_pool)?;
    let table = mapper.meta_table();
    let config = BlockCacheConfig {
      shard_count: 1,
      capacity: 4 * PAGE_SIZE,
      buffer_size: 4 * PAGE_SIZE,
    };
    let cache = BlockCache::open(config, wal.clone(), metrics);

    let pointer = table.free().alloc();
    cache.alloc(pointer, &table)?.commit_with(|page| {
      page.copy_from(&ORIGINAL, 0);
      append(&wal, &table, pointer, page)
    })?;
    cache.create_flusher().flush_hard()?;

    Ok(Self {
      _dir: dir,
      cache,
      wal,
      table,
      pointer,
    })
  }

  fn write(&self, data: &[u8], fail: bool) -> Result {
    let slot = self.cache.read(self.pointer, &self.table)?.for_write();
    slot.commit_with(|page| {
      page.copy_from(data, 0);
      if fail {
        return Err(Error::InvalidFormat("injected failure"));
      }
      append(&self.wal, &self.table, self.pointer, page)
    })
  }

  fn content(&self) -> Result<Vec<u8>> {
    let slot = self.cache.read(self.pointer, &self.table)?.for_read();
    Ok(slot.as_ref().range(0..ORIGINAL.len()).to_vec())
  }

  fn dirty_count(&self) -> usize {
    self.cache.create_flusher().remaining()
  }
}
impl Drop for Fixture {
  fn drop(&mut self) {
    self.cache.close();
    self.wal.close();
  }
}

#[test]
fn failed_commit_keeps_previous_page() -> Result {
  let fixture = Fixture::new()?;

  assert!(fixture.write(&CHANGED, true).is_err());
  assert_eq!(fixture.content()?, ORIGINAL);
  Ok(())
}

#[test]
fn failed_commit_releases_slot() -> Result {
  let fixture = Fixture::new()?;

  assert!(fixture.write(&CHANGED, true).is_err());
  fixture.write(&CHANGED, false)?;
  assert_eq!(fixture.content()?, CHANGED);
  Ok(())
}

#[test]
fn failed_commit_leaves_nothing_to_flush() -> Result {
  let fixture = Fixture::new()?;

  assert!(fixture.write(&CHANGED, true).is_err());
  assert_eq!(fixture.dirty_count(), 0);

  fixture.write(&CHANGED, false)?;
  assert_eq!(fixture.dirty_count(), 1);
  Ok(())
}
