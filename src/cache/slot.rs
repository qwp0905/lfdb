use std::{
  mem::{transmute, ManuallyDrop},
  sync::Arc,
};

use crossbeam::queue::SegQueue;

use super::{
  BlockId, CachedBlock, DirtyTables, LatchGuard, TempBlock, TempBlockRef, TempGuard,
};
use crate::{
  disk::{Page, PagePool, PageRef, Pointer, PAGE_SIZE},
  utils::{AtomicBitmap, SharedToken},
};

/**
 * A handle to a block cache page, abstracting over LRU blocks and temp pages.
 *
 * Callers only need to call for_read() or for_write() — the distinction between
 * Page and Temp is an internal detail. Dirty tracking and disk writes are handled
 * by the block cache itself when the slot is dropped.
 */
pub struct CacheSlot<'a> {
  slot: SlotType<'a>,
  page_pool: &'a PagePool<PAGE_SIZE>,
}
impl<'a> CacheSlot<'a> {
  pub fn hit(
    block: &'a CachedBlock,
    dirty: &'a AtomicBitmap,
    block_id: BlockId,
    token: SharedToken<'a>,
    page_pool: &'a PagePool<PAGE_SIZE>,
  ) -> Self {
    Self {
      slot: SlotType::Hit(HitSlot {
        block,
        dirty,
        block_id,
        token,
      }),
      page_pool,
    }
  }

  pub fn temp(
    block: TempBlockRef<SharedToken<'a>>,
    w_queue: &'a SegQueue<Arc<TempBlock>>,
    dirty_tables: &'a DirtyTables,
    guard: Option<TempGuard<'a>>,
    page_pool: &'a PagePool<PAGE_SIZE>,
  ) -> Self {
    Self {
      slot: SlotType::Temp(TempSlot {
        block,
        w_queue,
        dirty_tables,
        guard,
      }),
      page_pool,
    }
  }

  pub fn for_read<'b>(self) -> ReadonlySlot<'b>
  where
    'a: 'b,
  {
    match self.slot {
      SlotType::Hit(slot) => ReadonlySlot {
        page: slot.block.load_page(),
        _temp: None,
      },
      SlotType::Temp(slot) => {
        let page = slot.block.load_page();
        let temp = slot.guard.map(|g| ReadonlyTempSlot {
          block: ManuallyDrop::new(slot.block),
          dirty_tables: slot.dirty_tables,
          _guard: g,
        });
        ReadonlySlot { page, _temp: temp }
      }
    }
  }
  pub fn for_write<'b>(self) -> WritableSlot<'b>
  where
    'a: 'b,
  {
    let mut shadow = self.page_pool.acquire();
    match self.slot {
      SlotType::Hit(slot) => {
        let _latch = slot.block.latch();
        shadow.copy_from(&**slot.block.load_page());

        WritableSlot {
          shadow: ManuallyDrop::new(shadow),
          slot: WritableType::Hit(WritableHitSlot {
            block: slot.block,
            dirty: slot.dirty,
            block_id: slot.block_id,
            _token: slot.token,
            _latch,
          }),
        }
      }
      SlotType::Temp(slot) => {
        let latch = unsafe { transmute(slot.block.latch()) };
        shadow.copy_from(&**slot.block.load_page());

        WritableSlot {
          shadow: ManuallyDrop::new(shadow),
          slot: WritableType::Temp(WritableTempSlot {
            block: ManuallyDrop::new(slot.block),
            w_queue: slot.w_queue,
            guard: slot.guard.map(|g| (g, slot.dirty_tables)),
            latch: ManuallyDrop::new(latch),
          }),
        }
      }
    }
  }
  pub fn for_lazy_write<'b>(self) -> WritableSlot<'b>
  where
    'a: 'b,
  {
    let mut shadow = self.page_pool.acquire();
    match self.slot {
      SlotType::Hit(slot) => {
        let _latch = slot.block.lazy_latch();
        shadow.copy_from(&**slot.block.load_page());

        WritableSlot {
          shadow: ManuallyDrop::new(shadow),
          slot: WritableType::Hit(WritableHitSlot {
            block: slot.block,
            dirty: slot.dirty,
            block_id: slot.block_id,
            _token: slot.token,
            _latch,
          }),
        }
      }
      SlotType::Temp(slot) => {
        let latch = unsafe { transmute(slot.block.lazy_latch()) };
        shadow.copy_from(&**slot.block.load_page());

        WritableSlot {
          shadow: ManuallyDrop::new(shadow),
          slot: WritableType::Temp(WritableTempSlot {
            block: ManuallyDrop::new(slot.block),
            w_queue: slot.w_queue,
            guard: slot.guard.map(|g| (g, slot.dirty_tables)),
            latch: ManuallyDrop::new(latch),
          }),
        }
      }
    }
  }
}

enum SlotType<'a> {
  Hit(HitSlot<'a>),
  Temp(TempSlot<'a>),
}

pub struct ReadonlySlot<'a> {
  page: Arc<PageRef<PAGE_SIZE>>,
  _temp: Option<ReadonlyTempSlot<'a>>,
}
impl<'a> ReadonlySlot<'a> {
  pub fn page(&self) -> Arc<PageRef<PAGE_SIZE>> {
    self.page.clone()
  }
}
impl<'a> AsRef<Page<PAGE_SIZE>> for ReadonlySlot<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    &*self.page
  }
}
pub struct WritableSlot<'a> {
  shadow: ManuallyDrop<PageRef<PAGE_SIZE>>,
  slot: WritableType<'a>,
}
impl<'a> WritableSlot<'a> {
  pub fn get_pointer(&self) -> Pointer {
    match &self.slot {
      WritableType::Hit(slot) => slot.block.get_pointer(),
      WritableType::Temp(slot) => slot.block.get_pointer(),
    }
  }
}
impl<'a> AsRef<Page<PAGE_SIZE>> for WritableSlot<'a> {
  fn as_ref(&self) -> &Page<PAGE_SIZE> {
    &*self.shadow
  }
}
impl<'a> AsMut<Page<PAGE_SIZE>> for WritableSlot<'a> {
  fn as_mut(&mut self) -> &mut Page<PAGE_SIZE> {
    &mut *self.shadow
  }
}
impl<'a> Drop for WritableSlot<'a> {
  fn drop(&mut self) {
    let shadow = unsafe { ManuallyDrop::take(&mut self.shadow) };
    match &mut self.slot {
      WritableType::Hit(slot) => slot.release(shadow),
      WritableType::Temp(slot) => slot.release(shadow),
    }
  }
}

enum WritableType<'a> {
  Hit(WritableHitSlot<'a>),
  Temp(WritableTempSlot<'a>),
}

struct HitSlot<'a> {
  block: &'a CachedBlock,
  dirty: &'a AtomicBitmap,
  block_id: BlockId,
  token: SharedToken<'a>,
}

struct WritableHitSlot<'a> {
  block: &'a CachedBlock,
  dirty: &'a AtomicBitmap,
  block_id: BlockId,
  _token: SharedToken<'a>,
  _latch: LatchGuard<'a>,
}
impl<'a> WritableHitSlot<'a> {
  fn release(&mut self, shadow: PageRef<PAGE_SIZE>) {
    // Obtaining a WritableSlot is treated as equivalent to modifying the page.
    // We cannot know whether the caller actually modified it without expensive
    // byte-level comparison. The cost of an occasional unnecessary flush is
    // far lower than tracking write intent.
    self.dirty.insert(self.block_id);
    self.block.store(shadow);
  }
}

struct TempSlot<'a> {
  block: TempBlockRef<SharedToken<'a>>,
  w_queue: &'a SegQueue<Arc<TempBlock>>,
  dirty_tables: &'a DirtyTables,
  guard: Option<TempGuard<'a>>,
}

struct ReadonlyTempSlot<'a> {
  block: ManuallyDrop<TempBlockRef<SharedToken<'a>>>,
  dirty_tables: &'a DirtyTables,
  _guard: TempGuard<'a>,
}
impl<'a> Drop for ReadonlyTempSlot<'a> {
  fn drop(&mut self) {
    let block = unsafe { ManuallyDrop::take(&mut self.block) }.upgrade();
    let wait = {
      let _latch = block.lazy_latch();
      if !block.is_dirty() {
        return;
      }

      let wait = block.flush_async();
      block.mark_dirty(false);
      wait
    };
    let _ = wait.wait();
    self.dirty_tables.mark(block.table())
  }
}

struct WritableTempSlot<'a> {
  block: ManuallyDrop<TempBlockRef<SharedToken<'a>>>,
  w_queue: &'a SegQueue<Arc<TempBlock>>,
  guard: Option<(TempGuard<'a>, &'a DirtyTables)>,
  latch: ManuallyDrop<LatchGuard<'a>>,
}
impl<'a> WritableTempSlot<'a> {
  pub fn release(&mut self, shadow: PageRef<PAGE_SIZE>) {
    self.block.store(shadow);

    if let Some((_guard, dirty_tables)) = self.guard.take() {
      self.block.mark_dirty(true);
      unsafe { ManuallyDrop::drop(&mut self.latch) };
      let block = unsafe { ManuallyDrop::take(&mut self.block) }.upgrade();
      let wait = {
        let _latch = block.lazy_latch();
        if !block.is_dirty() {
          return;
        }

        let wait = block.flush_async();
        block.mark_dirty(false);
        wait
      };
      let _ = wait.wait();
      dirty_tables.mark(block.table());
      return;
    }

    let block = unsafe { ManuallyDrop::take(&mut self.block) }.into_inner();
    if !block.is_dirty() {
      block.mark_dirty(true);
      self.w_queue.push(block);
    }
    unsafe { ManuallyDrop::drop(&mut self.latch) };
  }
}
