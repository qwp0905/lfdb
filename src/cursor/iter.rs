use std::{collections::VecDeque, mem::replace, sync::Arc};

use super::{CursorNodeView, DataEntry, Key, KeyRef};
use crate::{
  disk::Pointer,
  error::{Error, Result},
  table::TableHandle,
  transaction::{TxOrchestrator, TxSnapshot, TxState},
};

/**
 * Range iterator over the leaf chain. Holds a deserialized copy of the current
 * leaf rather than a page latch. This is safe because: (1) a concurrent split
 * only moves entries to a new node whose next pointer still leads to the same
 * successor, and newly inserted data is invisible to this transaction anyway;
 * (2) as long as this transaction is alive, GC cannot collect DataEntry versions
 * visible to it, so leaf merge cannot occur on those leaves and their pages are
 * never freed.
 */
pub struct CursorIterator<'a> {
  table: Arc<TableHandle>,
  state: &'a TxState<'a>,
  snapshot: &'a TxSnapshot<'a>,
  orchestrator: &'a TxOrchestrator,
  keys: VecDeque<(Key, Pointer)>,
  next: Option<Pointer>,
  end: Option<Key>,
  closed: bool,
}
impl<'a> CursorIterator<'a> {
  pub fn new(
    table: Arc<TableHandle>,
    state: &'a TxState,
    snapshot: &'a TxSnapshot<'a>,
    orchestrator: &'a TxOrchestrator,
    keys: VecDeque<(Key, Pointer)>,
    next: Option<Pointer>,
    end: Option<Key>,
  ) -> Self {
    Self {
      table,
      state,
      snapshot,
      orchestrator,
      keys,
      next,
      end,
      closed: false,
    }
  }

  fn find_value(&self, ptr: Pointer) -> Result<Option<Vec<u8>>> {
    let mut slot = self.orchestrator.fetch(ptr, self.table.clone())?.for_read();
    loop {
      let entry = slot.as_ref().deserialize::<DataEntry>()?;
      if let Some(record) =
        entry.find_record(self.state.get_id(), |i| self.snapshot.is_visible(i))
      {
        return record.read_data(|i| {
          self
            .orchestrator
            .fetch(i, self.table.clone())?
            .for_read()
            .as_ref()
            .deserialize()
        });
      }

      match entry.get_next() {
        Some(i) => drop(replace(
          &mut slot,
          self.orchestrator.fetch(i, self.table.clone())?.for_read(),
        )),
        None => return Ok(None),
      }
    }
  }

  pub fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    if self.closed {
      return Ok(None);
    }

    'outer: loop {
      if !self.state.is_available() {
        return Err(Error::TransactionClosed);
      }

      while let Some((key, ptr)) = self.keys.pop_front() {
        if let Some(value) = self.find_value(ptr)? {
          return Ok(Some((key, value)));
        }
      }

      let ptr = match self.next.take() {
        Some(p) => p,
        None => return Ok(None),
      };

      let slot = self.orchestrator.fetch(ptr, self.table.clone())?.for_read();
      let node = slot.as_ref().view::<CursorNodeView>()?.as_leaf()?;

      for (k, p) in node.get_entries() {
        if let Some(end) = self.end.as_ref() {
          if end as KeyRef <= k {
            continue 'outer;
          }
        }

        self.keys.push_back((k.to_vec(), p));
      }
      self.next = node.get_next()
    }
  }
}
