use std::collections::HashMap;

use crate::{disk::Pointer, utils::SpinRwLock, Error, Result};

enum Slot {
  Reserved,
  Occupied(Pointer),
}

pub struct TableMapper {
  mapping: SpinRwLock<HashMap<String, Slot>>,
}
impl TableMapper {
  pub fn new() -> Self {
    Self {
      mapping: Default::default(),
    }
  }

  pub fn get(&self, name: &str) -> Option<Pointer> {
    match self.mapping.read().get(name)? {
      Slot::Reserved => None,
      Slot::Occupied(i) => Some(*i),
    }
  }
  pub fn get_or_reserve(&self, name: &str) -> Result<Option<Pointer>> {
    let mut mapping = self.mapping.write();
    if let Some(slot) = mapping.get(name) {
      match slot {
        Slot::Reserved => return Err(Error::WriteConflict),
        Slot::Occupied(i) => return Ok(Some(*i)),
      }
    }

    mapping.insert(name.to_string(), Slot::Reserved);
    Ok(None)
  }

  pub fn insert(&self, name: String, header: Pointer) {
    self.mapping.write().insert(name, Slot::Occupied(header));
  }
  pub fn remove(&self, name: &str) {
    self.mapping.write().remove(name);
  }

  pub fn get_all(&self) -> Vec<(String, Pointer)> {
    self
      .mapping
      .read()
      .iter()
      .filter_map(|(k, v)| match v {
        Slot::Reserved => None,
        Slot::Occupied(i) => Some((k.clone(), *i)),
      })
      .collect()
  }
}
