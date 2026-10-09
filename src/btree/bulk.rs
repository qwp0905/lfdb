use std::mem::replace;

use crate::{
  disk::Pointer,
  objects::{BTreeNodeView, StaticKey, StaticKeyRef},
  table::TableHandleRef,
  Result,
};

use super::{
  append_or_reserve_at_leaf, copy_and_update, propagate_split, read_header,
  resolve_conflict, AppendOrReserve, CreatablePolicy, KeyPair, KeyPairList,
  MaybeWrittenKeyPair, ReadonlyPolicy, WriteOp,
};

pub struct BulkOp(std::collections::BTreeSet<KeyPair>);
impl BulkOp {
  pub const fn new() -> Self {
    Self(std::collections::BTreeSet::new())
  }

  pub fn append_insert(&mut self, key: StaticKey, data: Vec<u8>, create: bool) {
    self.0.replace(KeyPair(key, WriteOp::Insert(data), create));
  }
  pub fn append_remove(&mut self, key: StaticKey, create: bool) {
    self.0.replace(KeyPair(key, WriteOp::Remove, create));
  }

  pub fn drain_all(self) -> KeyPairList {
    KeyPairList::new(self.0)
  }
}

pub struct BulkResult {
  pub splitted: usize,
}
impl BulkResult {
  pub const fn new() -> Self {
    Self { splitted: 0 }
  }
}

fn drain_bulk_once<Policy: CreatablePolicy + Sync>(
  policy: &Policy,
  leaf_ptr: Pointer,
  table: &TableHandleRef,
  must_apply: KeyPair,
  bulk: &mut KeyPairList,
  stack: Vec<Pointer>,
) -> Result<BulkResult> {
  let mut copy_old = Vec::new();
  let mut splitted = Vec::new();
  let mut ptr = leaf_ptr;
  let mut must_apply = MaybeWrittenKeyPair::KeyPair(must_apply);
  loop {
    match append_or_reserve_at_leaf(policy, ptr, must_apply, table, Some(bulk))? {
      AppendOrReserve::Move(p, kp) => (ptr, must_apply) = (p, kp),
      AppendOrReserve::Conflict {
        owner,
        resume,
        copy_old: c,
        splitted: s,
        ..
      } => {
        copy_old.extend(c);
        splitted.extend(s);
        resolve_conflict(policy, owner)?;
        must_apply = resume;
      }
      AppendOrReserve::Done {
        copy_old: c,
        splitted: s,
        ..
      } => {
        copy_old.extend(c);
        splitted.extend(s);
        break;
      }
      AppendOrReserve::Stopped {
        resume,
        copy_old: c,
        splitted: s,
        ..
      } => {
        copy_old.extend(c);
        splitted.extend(s);
        must_apply = resume;
      }
    };
  }

  let mut result = BulkResult::new();
  result.splitted += copy_and_update(policy, leaf_ptr, table, stack.clone(), copy_old)?;
  for (k, p) in splitted {
    propagate_split(policy, k, p, stack.clone(), table, stack.len())?;
    result.splitted += 1;
  }

  Ok(result)
}

enum FindKey {
  ReachedLeaf,
  MoveDown(Option<StaticKey>, Pointer),
  MoveRight(Pointer),
}

fn find_key<Policy: ReadonlyPolicy>(
  policy: &Policy,
  key: StaticKeyRef,
  ptr: Pointer,
  table: &TableHandleRef,
) -> Result<FindKey> {
  loop {
    let Some(slot) = policy.fetch_slot(ptr, table)? else {
      continue;
    };
    let slot = slot.for_read();
    let BTreeNodeView::Internal(node) = slot.as_ref().view::<BTreeNodeView>()? else {
      return Ok(FindKey::ReachedLeaf);
    };
    match node.find(key)? {
      Ok(p) => {
        return Ok(FindKey::MoveDown(
          node.get_right_key().map(|k| k.to_vec()),
          p,
        ))
      }
      Err(p) => return Ok(FindKey::MoveRight(p)),
    };
  }
}

pub fn fill_stack_from<Policy: ReadonlyPolicy>(
  policy: &Policy,
  key: StaticKeyRef,
  table: &TableHandleRef,
  start: Pointer,
  stack: &mut Vec<(Option<StaticKey>, Pointer)>,
) -> Result<Pointer> {
  let mut ptr = start;

  loop {
    match find_key(policy, key, ptr, table)? {
      FindKey::ReachedLeaf => break,
      FindKey::MoveDown(k, p) => stack.push((k, replace(&mut ptr, p))),
      FindKey::MoveRight(p) => ptr = p,
    }
  }
  Ok(ptr)
}

pub struct BulkExecutor<'a, Policy> {
  policy: &'a Policy,
  bulk: KeyPairList,
  table: TableHandleRef,
  stack: Vec<(Option<StaticKey>, Pointer)>,
}
impl<'a, Policy> BulkExecutor<'a, Policy> {
  pub const fn new(policy: &'a Policy, bulk: KeyPairList, table: TableHandleRef) -> Self {
    Self {
      policy,
      bulk,
      table,
      stack: Vec::new(),
    }
  }

  fn clone_stack(&self) -> Vec<Pointer> {
    self.stack.iter().map(|(_, p)| *p).collect()
  }
}
impl<'a, Policy: CreatablePolicy + Sync> BulkExecutor<'a, Policy> {
  pub fn drain_once(&mut self) -> Result<Option<BulkResult>> {
    let Some(KeyPair(current, op, create)) = self.bulk.pop() else {
      return Ok(None);
    };
    while self
      .stack
      .pop_if(|(k, _)| k.as_deref().is_some_and(|k| k <= &current))
      .is_some()
    {}

    let start = match self.stack.pop() {
      Some((_, p)) => p,
      None => read_header(&self.policy, &self.table)?.get_root(),
    };

    let leaf_ptr =
      fill_stack_from(&self.policy, &current, &self.table, start, &mut self.stack)?;
    let stack = self.clone_stack();

    let result = drain_bulk_once(
      &self.policy,
      leaf_ptr,
      &self.table,
      KeyPair(current, op, create),
      &mut self.bulk,
      stack,
    )?;
    Ok(Some(result))
  }
}
