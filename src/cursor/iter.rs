use std::{collections::VecDeque, mem::replace, ops::Bound, sync::Arc};

use super::{
  CursorNodeView, DataEntry, Key, KeyRef, LeafNodeView, NodeFindResult, TreeHeader,
  HEADER_POINTER,
};
use crate::{
  disk::Pointer,
  error::{Error, Result},
  table::TableHandle,
  transaction::{TxOrchestrator, TxSnapshot, TxState},
};

pub struct CursorIterator<'a> {
  state: &'a TxState<'a>,
  table: TableIterator<'a>,
  compaction: Option<(TableIterator<'a>, Option<(Vec<u8>, Option<Vec<u8>>)>)>,
  buffered: Option<(Vec<u8>, Option<Vec<u8>>)>,
}
impl<'a> CursorIterator<'a> {
  pub fn new(
    table: &'a Arc<TableHandle>,
    compaction: Option<&'a Arc<TableHandle>>,
    state: &'a TxState,
    snapshot: &'a TxSnapshot<'a>,
    orchestrator: &'a TxOrchestrator,
    start: Bound<Key>,
    end: Bound<Key>,
  ) -> Result<Self> {
    let mut compaction_table = None;
    if let Some(table) = compaction {
      compaction_table = Some((
        TableIterator::open(
          table,
          state,
          snapshot,
          orchestrator,
          start.clone(),
          end.clone(),
        )?,
        None,
      ));
    }

    Ok(Self {
      state,
      table: TableIterator::open(
        table,
        state,
        snapshot,
        orchestrator,
        start.clone(),
        end.clone(),
      )?,
      compaction: compaction_table,
      buffered: None,
    })
  }
  pub fn try_next(&mut self) -> Result<Option<(Key, Vec<u8>)>> {
    if !self.state.is_available() {
      return Err(Error::TransactionClosed);
    }

    let (compaction, cb) = match &mut self.compaction {
      Some(v) => v,
      None => loop {
        match self.table.fetch_next()? {
          Some((_, None)) => continue,
          Some((k, Some(v))) => return Ok(Some((k, v))),
          None => return Ok(None),
        }
      },
    };

    loop {
      let kv_old = match self.buffered.take() {
        Some(kv) => Some(kv),
        None => self.table.fetch_next()?,
      };
      let kv_new = match cb.take() {
        Some(kv) => Some(kv),
        None => compaction.fetch_next()?,
      };

      let (k_old, v_old, k_new, v_new) = match (kv_old, kv_new) {
        (None, None) => return Ok(None),
        (None, Some((k, Some(v)))) => return Ok(Some((k, v))),
        (Some((k, Some(v))), None) => return Ok(Some((k, v))),
        (None, Some((_, None))) | (Some((_, None)), None) => continue,
        (Some((k1, v1)), Some((k2, v2))) => (k1, v1, k2, v2),
      };

      if k_old < k_new {
        *cb = Some((k_new, v_new));
        if let Some(v) = v_old {
          return Ok(Some((k_old, v)));
        }
        continue;
      }
      if k_new < k_old {
        self.buffered = Some((k_old, v_old));
        if let Some(v) = v_new {
          return Ok(Some((k_new, v)));
        }
        continue;
      }

      if let Some(v) = v_new {
        return Ok(Some((k_new, v)));
      }
    }
  }
}

/**
 * Range iterator over the leaf chain. Holds a deserialized copy of the current
 * leaf rather than a page latch. This is safe because: (1) a concurrent split
 * only moves entries to a new node whose next pointer still leads to the same
 * successor, and newly inserted data is invisible to this transaction anyway;
 * (2) as long as this transaction is alive, GC cannot collect DataEntry versions
 * visible to it, so leaf merge cannot occur on those leaves and their pages are
 * never freed.
 */
pub struct TableIterator<'a> {
  table: &'a Arc<TableHandle>,
  state: &'a TxState<'a>,
  snapshot: &'a TxSnapshot<'a>,
  orchestrator: &'a TxOrchestrator,
  keys: VecDeque<(Key, Pointer)>,
  next: Option<Pointer>,
  end: Bound<Key>,
  closed: bool,
}
impl<'a> TableIterator<'a> {
  fn new<'b>(
    table: &'a Arc<TableHandle>,
    state: &'a TxState,
    snapshot: &'a TxSnapshot<'a>,
    orchestrator: &'a TxOrchestrator,
    end: Bound<Key>,
    pos: usize,
    node: &LeafNodeView,
  ) -> Self {
    let (e, include): (KeyRef, bool) = match &end {
      Bound::Included(e) => (e, true),
      Bound::Excluded(e) => (e, false),
      Bound::Unbounded => {
        return Self {
          table,
          state,
          snapshot,
          orchestrator,
          keys: node
            .get_entries()
            .skip(pos)
            .map(|(k, p)| (k.to_vec(), p))
            .collect(),
          next: node.get_next(),
          end,
          closed: false,
        }
      }
    };

    let mut keys = VecDeque::new();
    for (k, p) in node.get_entries().skip(pos) {
      if !include && e <= k {
        return Self {
          table,
          state,
          snapshot,
          orchestrator,
          keys,
          next: None,
          end,
          closed: false,
        };
      }

      if include && e < k {
        return Self {
          table,
          state,
          snapshot,
          orchestrator,
          keys,
          next: None,
          end,
          closed: false,
        };
      }
      keys.push_back((k.to_vec(), p));
    }

    Self {
      table,
      state,
      snapshot,
      orchestrator,
      keys,
      next: node.get_next(),
      end,
      closed: false,
    }
  }

  fn open(
    table: &'a Arc<TableHandle>,
    state: &'a TxState,
    snapshot: &'a TxSnapshot<'a>,
    orchestrator: &'a TxOrchestrator,
    start: Bound<Key>,
    end: Bound<Key>,
  ) -> Result<Self> {
    let mut ptr = orchestrator
      .fetch(HEADER_POINTER, table.clone())?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    loop {
      let mut slot = orchestrator.fetch(ptr, table.clone())?.for_read();
      match slot.as_ref().view::<CursorNodeView>()? {
        CursorNodeView::Internal(node) => match &start {
          Bound::Included(k) => ptr = node.find(k).unwrap_or_else(|i| i),
          Bound::Excluded(k) => ptr = node.find(k).unwrap_or_else(|i| i),
          Bound::Unbounded => ptr = node.first_child(),
        },
        CursorNodeView::Leaf(mut node) => {
          let pos = match &start {
            Bound::Unbounded => 0,
            Bound::Included(k) => loop {
              match node.find(k) {
                NodeFindResult::Found(i, _) => break i,
                NodeFindResult::NotFound(i) => break i,
                NodeFindResult::Move(i) => {
                  drop(slot);
                  slot = orchestrator.fetch(i, table.clone())?.for_read();
                  node = slot.as_ref().view::<CursorNodeView>()?.as_leaf()?;
                }
              }
            },
            Bound::Excluded(k) => loop {
              match node.find(k) {
                NodeFindResult::Found(i, _) => break i + 1,
                NodeFindResult::NotFound(i) => break i,
                NodeFindResult::Move(i) => {
                  drop(slot);
                  slot = orchestrator.fetch(i, table.clone())?.for_read();
                  node = slot.as_ref().view::<CursorNodeView>()?.as_leaf()?;
                }
              }
            },
          };

          return Ok(Self::new(
            table,
            state,
            snapshot,
            orchestrator,
            end,
            pos,
            &node,
          ));
        }
      }
    }
  }

  fn find_value(&self, ptr: Pointer) -> Result<Option<Option<Vec<u8>>>> {
    let mut slot = self.orchestrator.fetch(ptr, self.table.clone())?.for_read();
    loop {
      let entry = slot.as_ref().deserialize::<DataEntry>()?;
      if let Some(record) =
        entry.find_record(self.state.get_id(), |i| self.snapshot.is_visible(i))
      {
        return Ok(Some(record.read_data(|i| {
          self
            .orchestrator
            .fetch(i, self.table.clone())?
            .for_read()
            .as_ref()
            .deserialize()
        })?));
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

  fn fetch_next(&mut self) -> Result<Option<(Key, Option<Vec<u8>>)>> {
    if self.closed {
      return Ok(None);
    }

    loop {
      while let Some((key, ptr)) = self.keys.pop_front() {
        if let Some(value) = self.find_value(ptr)? {
          return Ok(Some((key, value)));
        }
      }

      let ptr = match self.next.take() {
        Some(p) => p,
        None => {
          self.closed = true;
          return Ok(None);
        }
      };

      let slot = self.orchestrator.fetch(ptr, self.table.clone())?.for_read();
      let node = slot.as_ref().view::<CursorNodeView>()?.as_leaf()?;

      let (end, include) = match &self.end {
        Bound::Included(k) => (k, true),
        Bound::Excluded(k) => (k, false),
        Bound::Unbounded => {
          node
            .get_entries()
            .for_each(|(k, p)| self.keys.push_back((k.to_vec(), p)));
          self.next = node.get_next();
          continue;
        }
      };

      for (k, p) in node
        .get_entries()
        .take_while(|(k, _)| (!include && *k < end) || (include && *k <= end))
      {
        self.keys.push_back((k.to_vec(), p))
      }

      if node.len() == self.keys.len() {
        self.next = node.get_next();
      }
    }
  }
}
