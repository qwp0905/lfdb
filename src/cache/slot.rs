use std::ops::Range;

use super::{BlockCacheCore, BlockId, BlockLatch, MappingToken};
use crate::{
  disk::Pointer,
  page::{Page, PageMut, PageRef, VecRef, PAGE_SIZE},
  table::TableHandleRef,
  utils::{just_drop, SBox},
  wal::{DurabilityGuard, WriteSeq},
  Result,
};

/**
 * Access interface for one cached block.
 *
 * A `CachedSlot` is returned after the block cache has found and pinned a block.
 * The caller then chooses the access mode: read the current page, write through
 * a shadow page, or join a batched mutation pass. The slot hides the cached page
 * replacement, dirty marking, and page-pool details behind those modes.
 */
pub struct CachedSlot<'a> {
  block_id: BlockId,
  core: &'a BlockCacheCore,
  token: MappingToken<'a>,
}
impl<'a> CachedSlot<'a> {
  pub const fn new(
    block_id: BlockId,
    core: &'a BlockCacheCore,
    token: MappingToken<'a>,
  ) -> Self {
    Self {
      block_id,
      core,
      token,
    }
  }

  pub fn for_read(self) -> ReadonlySlot {
    ReadonlySlot {
      page: self.core.get_block(self.block_id).load_page(),
    }
  }
  pub fn for_write<'b>(self) -> WritableSlot<'b>
  where
    'a: 'b,
  {
    let block = self.core.get_block(self.block_id);
    let latch = block.latch();
    WritableSlot {
      pointer: block.get_pointer(),
      target: WriteTarget::Cached {
        page: block.load_page(),
        latch,
        _token: self.token,
      },
      core: self.core,
    }
  }
}

/**
 * Immutable snapshot of a cached page.
 *
 * The slot owns an `SBox` reference to the page version it loaded. Later writers
 * may replace the block's current page, but this reader continues to observe the
 * same page snapshot without batch mutation.
 */
pub struct ReadonlySlot {
  page: SBox<PageRef<PAGE_SIZE>>,
}
impl ReadonlySlot {
  pub fn create_ref(&self, range: Range<usize>) -> VecRef {
    VecRef::refed(self.page.clone(), range)
  }
  pub fn as_page(&self) -> Page<'_, PAGE_SIZE> {
    self.page.as_page()
  }
}

enum WriteTarget<'a> {
  Cached {
    page: SBox<PageRef<PAGE_SIZE>>,
    latch: BlockLatch<'a>,
    _token: MappingToken<'a>,
  },
  Temp {
    page: PageRef<PAGE_SIZE>,
    table: TableHandleRef,
  },
}

/**
 * A page is changed only inside `commit_with`, and the change is installed only
 * when the closure succeeds, so a page whose serialization or WAL append failed
 * never reaches the cache or the disk.
 */
pub struct WritableSlot<'a> {
  pointer: Pointer,
  target: WriteTarget<'a>,
  core: &'a BlockCacheCore,
}
impl<'a> WritableSlot<'a> {
  pub const fn new_temp(
    table: TableHandleRef,
    page: PageRef<PAGE_SIZE>,
    pointer: Pointer,
    core: &'a BlockCacheCore,
  ) -> Self {
    Self {
      pointer,
      target: WriteTarget::Temp { page, table },
      core,
    }
  }

  pub const fn get_pointer(&self) -> Pointer {
    self.pointer
  }

  pub unsafe fn commit_unchecked(self, data: &[u8], write_seq: WriteSeq) -> Result {
    match self.target {
      WriteTarget::Cached {
        mut latch, _token, ..
      } => {
        let mut shadow = self.core.allocate_page();
        shadow.as_page_mut().copy_from(data, 0);
        latch.apply(shadow, write_seq);
      }
      WriteTarget::Temp { mut page, table } => {
        page.as_page_mut().copy_from(data, 0);
        // SAFETY: the callback owns `page` until the write completes.
        let static_ref = unsafe { page.as_page().into_static() };
        table
          .disk()
          .write_async_with(self.pointer, static_ref, just_drop(page))
          .wait_flatten()?;
        self.core.mark_dirty_table(&table);
      }
    };
    Ok(())
  }

  pub fn commit_with<F>(self, f: F) -> Result
  where
    F: FnOnce(PageMut<PAGE_SIZE>) -> Result<DurabilityGuard<'a>>,
  {
    // The guard has to outlive the install. Once it drops, a checkpoint boundary can
    // pass this record, so a page that is not dirty by then would be left out of
    // that checkpoint's flush while its record falls outside replay.
    match self.target {
      WriteTarget::Cached {
        mut latch, _token, ..
      } => {
        let mut shadow = self.core.allocate_page();
        let guard = f(shadow.as_page_mut())?;
        latch.apply(shadow, guard.get_write_seq());
        Ok(())
      }
      WriteTarget::Temp { mut page, table } => {
        let guard = f(page.as_page_mut())?;
        self.core.ensure_durability(guard.get_write_seq())?;
        // SAFETY: the callback owns `page` until the write completes.
        let static_ref = unsafe { page.as_page().into_static() };
        table
          .disk()
          .write_async_with(self.pointer, static_ref, just_drop(page))
          .wait_flatten()?;
        self.core.mark_dirty_table(&table);
        Ok(())
      }
    }
  }

  pub fn as_page(&self) -> Page<'_, PAGE_SIZE> {
    match &self.target {
      WriteTarget::Cached { page, .. } => page.as_page(),
      WriteTarget::Temp { page, .. } => page.as_page(),
    }
  }
}

unsafe impl<'a> Send for WritableSlot<'a> {}
unsafe impl<'a> Sync for WritableSlot<'a> {}
