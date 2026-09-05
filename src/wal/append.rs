use std::sync::atomic::{AtomicU64, Ordering};

use super::WAL_BLOCK_SIZE;

pub type AppendOrder = u32;

const BITS: u32 = 40;
const MASK: u64 = (1 << BITS) - 1;

/**
 * append order (24bit) + offset (40bit), packed into one AtomicU64.
 * A single fetch_add atomically reserves a position in the block and increments
 * the in-flight writer count. The offset portion can grow up to ~4000 bytes per
 * record, so 40 bits is sufficient; 24 bits accommodates the concurrent writer count.
 */
pub struct OffsetBooking(AtomicU64);
impl OffsetBooking {
  pub const fn new(offset: usize, order: AppendOrder) -> Self {
    let high = (order as u64) << BITS;
    let low = offset as u64;
    Self(AtomicU64::new(high | low))
  }

  /**
   * Atomically reserves a write slot and returns ticket.
   * order is the number of writers that claimed a slot before this call.
   * Since each writer appends exactly one record, order also equals the number
   * of records already in the block — used by flush callers to write the correct
   * record count header and to wait for all prior writers to finish.
   */
  pub fn reserve(&self, len: usize) -> BookingResult {
    let high = 1 << BITS;
    let low = len as u64;
    let old = self.0.fetch_add(high | low, Ordering::Relaxed);

    let order = (old >> BITS) as AppendOrder;
    let offset = (old & MASK) as usize;

    debug_assert!((offset + len) as u64 <= MASK);
    debug_assert!(order < 1 << (u64::BITS - BITS));

    if offset > WAL_BLOCK_SIZE {
      return BookingResult::Overflow;
    }
    if offset + len <= WAL_BLOCK_SIZE {
      return BookingResult::Available(AppendTicket::new(offset, len, order));
    }

    let available_len = WAL_BLOCK_SIZE - offset;
    let available = AppendTicket::new(offset, available_len, order);
    let overflow = AppendTicket::new(0, len - available_len, 0);
    BookingResult::Splitted {
      available,
      overflow,
    }
  }
}

pub enum BookingResult {
  Overflow,
  Available(AppendTicket),
  Splitted {
    available: AppendTicket,
    overflow: AppendTicket,
  },
}

pub struct AppendTicket {
  offset: usize,
  len: usize,
  order: AppendOrder,
}
impl AppendTicket {
  const fn new(offset: usize, len: usize, order: AppendOrder) -> Self {
    Self { offset, len, order }
  }

  pub const fn get_len(&self) -> usize {
    self.len
  }
  pub const fn get_offset(&self) -> usize {
    self.offset
  }
  pub const fn get_order(&self) -> AppendOrder {
    self.order
  }
}
