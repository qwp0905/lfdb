use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam::utils::Backoff;

use super::WAL_BLOCK_SIZE;

pub type AppendOrder = u16;

const BITS: u32 = u64::BITS - AppendOrder::BITS;
const MASK: u64 = (1 << BITS) - 1;

/**
 * append order (16bit) + offset (48bit), packed into one AtomicU64.
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
    let backoff = Backoff::new();
    let desired = (1 << BITS) | (len as u64);
    let mut current = self.0.load(Ordering::Relaxed);
    loop {
      let offset = (current & MASK) as usize;
      if offset > WAL_BLOCK_SIZE {
        return BookingResult::Overflow;
      }

      if let Err(err) = self.0.compare_exchange_weak(
        current,
        current + desired,
        Ordering::Relaxed,
        Ordering::Relaxed,
      ) {
        current = err;
        backoff.spin();
        continue;
      }

      let order = (current >> BITS) as AppendOrder;
      if offset + len <= WAL_BLOCK_SIZE {
        return BookingResult::Available(AppendTicket::new(offset, len, order));
      }

      let available_len = WAL_BLOCK_SIZE - offset;
      let available = AppendTicket::new(offset, available_len, order);
      let overflow = AppendTicket::new(0, len - available_len, 0);
      return BookingResult::Splitted {
        available,
        overflow,
      };
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
