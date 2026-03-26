use std::mem::replace;

use super::{CursorNode, DataEntry, Key, LeafNode, Pointer};
use crate::{
  error::{Error, Result},
  transaction::{TxOrchestrator, TxState},
};

pub struct CursorIterator<'a> {
  state: &'a TxState<'a>,
  orchestrator: &'a TxOrchestrator,
  leaf: LeafNode,
  pos: usize,
  end: Option<Key>,
  closed: bool,
}
impl<'a> CursorIterator<'a> {
  pub fn new(
    state: &'a TxState,
    orchestrator: &'a TxOrchestrator,
    leaf: LeafNode,
    pos: usize,
    end: Option<Key>,
  ) -> Self {
    Self {
      state,
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
        entry.find_record(self.state.get_id(), |i| self.orchestrator.is_visible(i))
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
        if self.end.as_ref().map(|e| key.ge(e)).unwrap_or(false) {
          self.closed = true;
          return Ok(None);
        }

        self.pos += 1;
        if let Some(value) = self.find_value(*ptr)? {
          return Ok(Some((key.clone(), value)));
        }
      }

      let index = match self.leaf.get_next() {
        Some(i) => i,
        None => {
          self.closed = true;
          return Ok(None);
        }
      };

      self.leaf = self
        .orchestrator
        .fetch(index)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
        .as_leaf()?;
      self.pos = 0;
    }
  }
}
