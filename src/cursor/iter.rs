use std::mem::replace;

use super::{CursorNode, DataEntry, Key, LeafNode};
use crate::{
  disk::Pointer,
  error::{Error, Result},
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
  state: &'a TxState<'a>,
  snapshot: &'a TxSnapshot<'a>,
  orchestrator: &'a TxOrchestrator,
  leaf: LeafNode,
  pos: usize,
  end: Option<Key>,
  closed: bool,
}
impl<'a> CursorIterator<'a> {
  pub fn new(
    state: &'a TxState,
    snapshot: &'a TxSnapshot<'a>,
    orchestrator: &'a TxOrchestrator,
    leaf: LeafNode,
    pos: usize,
    end: Option<Key>,
  ) -> Self {
    Self {
      state,
      snapshot,
      orchestrator,
      leaf,
      pos,
      end,
      closed: false,
    }
  }

  fn find_value(&self, ptr: Pointer) -> Result<Option<Vec<u8>>> {
    let mut slot = self.orchestrator.fetch(ptr)?.for_read();
    loop {
      let entry = slot.as_ref().deserialize::<DataEntry>()?;
      if let Some(record) =
        entry.find_record(self.state.get_id(), |i| self.snapshot.is_visible(i))
      {
        return record.read_data(|i| {
          self
            .orchestrator
            .fetch(i)?
            .for_read()
            .as_ref()
            .deserialize()
        });
      }

      match entry.get_next() {
        Some(i) => drop(replace(&mut slot, self.orchestrator.fetch(i)?.for_read())),
        None => return Ok(None),
      }
    }
  }

  pub fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    if self.closed {
      return Ok(None);
    }

    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }

    loop {
      for i in self.pos..self.leaf.len() {
        let (key, ptr) = self.leaf.at(i);
        if self.end.as_ref().map(|e| key >= e).unwrap_or(false) {
          self.closed = true;
          return Ok(None);
        }

        self.pos += 1;
        if let Some(value) = self.find_value(*ptr)? {
          return Ok(Some((key.clone(), value)));
        }
      }

      let ptr = match self.leaf.get_next() {
        Some(i) => i,
        None => {
          self.closed = true;
          return Ok(None);
        }
      };

      self.leaf = self
        .orchestrator
        .fetch(ptr)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
        .as_leaf()?;
      self.pos = 0;
    }
  }
}
