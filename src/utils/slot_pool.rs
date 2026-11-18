use std::mem::replace;

enum Slot<T> {
  Occupied(T),
  Vacant(Option<usize>),
}

/**
 * Fixed number of preallocated slots. An index stays bound to its value until it
 * is removed, and vacant slots are chained through themselves so they are reused
 * without allocating.
 */
pub struct SlotPool<T> {
  slots: Box<[Slot<T>]>,
  /**
   * First vacant slot.
   */
  head: Option<usize>,
  len: usize,
}
impl<T> SlotPool<T> {
  pub fn new(capacity: usize) -> Self {
    let slots = (1..=capacity)
      .map(|next| Slot::Vacant((next < capacity).then_some(next)))
      .collect();
    Self {
      slots,
      head: (capacity > 0).then_some(0),
      len: 0,
    }
  }

  pub fn insert(&mut self, value: T) -> Result<usize, T> {
    let Some(index) = self.head else {
      return Err(value);
    };
    let Slot::Vacant(next) = replace(&mut self.slots[index], Slot::Occupied(value))
    else {
      unreachable!("head points to an occupied slot");
    };
    self.head = next;
    self.len += 1;
    Ok(index)
  }

  pub fn get(&self, index: usize) -> Option<&T> {
    let Some(Slot::Occupied(value)) = self.slots.get(index) else {
      return None;
    };
    Some(value)
  }

  pub fn remove(&mut self, index: usize) -> Option<T> {
    let slot = self.slots.get_mut(index)?;
    let Slot::Occupied(_) = slot else {
      return None;
    };
    let Slot::Occupied(value) = replace(slot, Slot::Vacant(self.head)) else {
      unreachable!("slot was checked to be occupied");
    };
    self.head = Some(index);
    self.len -= 1;
    Some(value)
  }

  pub const fn len(&self) -> usize {
    self.len
  }
  pub const fn is_empty(&self) -> bool {
    self.len == 0
  }
  pub const fn is_full(&self) -> bool {
    self.len == self.slots.len()
  }
  pub const fn capacity(&self) -> usize {
    self.slots.len()
  }
}

#[cfg(all(test, not(loom)))]
#[path = "tests/slot_pool.rs"]
mod tests;
