use std::{
  fmt::{Debug, Display},
  ops::Deref,
};

use crate::{Error, Result};

const MAX_TABLE_NAME_LEN: usize = 256usize;
pub const META_TABLE: TableNameRef = TableNameRef("__meta__");

pub struct TableName(String);
impl TableName {
  pub const fn get_ref(&self) -> TableNameRef<'_> {
    unsafe { TableNameRef::from_str_unchecked(self.0.as_str()) }
  }
}

impl Deref for TableName {
  type Target = str;

  fn deref(&self) -> &Self::Target {
    self.0.as_str()
  }
}

impl Clone for TableName {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl Display for TableName {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    Display::fmt(self.deref(), f)
  }
}
impl Debug for TableName {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    Debug::fmt(self.deref(), f)
  }
}
impl PartialEq for TableName {
  fn eq(&self, other: &Self) -> bool {
    self.0 == other.0
  }
}
impl Eq for TableName {}

#[derive(Clone, Copy)]
pub struct TableNameRef<'a>(&'a str);
impl<'a> TableNameRef<'a> {
  pub fn from_str(name: &'a str) -> Result<Self> {
    if name.is_empty() {
      return Err(Error::TableNameEmpty);
    }
    if name.len() > MAX_TABLE_NAME_LEN {
      return Err(Error::TableNameExceeded(MAX_TABLE_NAME_LEN, name.len()));
    }

    if let Some(c) = name
      .chars()
      .find(|c| !c.is_alphanumeric() && !matches!(c, '-' | '_'))
    {
      return Err(Error::NotAllowedChar(c));
    }

    Ok(unsafe { Self::from_str_unchecked(name) })
  }

  /**
   * Construct a table name without validation.
   *
   * This is an unsafe-equivalent constructor: callers must guarantee the same
   * invariants enforced by `from_str`.
   */
  pub const unsafe fn from_str_unchecked(name: &'a str) -> Self {
    Self(name)
  }
  pub fn into_owned(self) -> TableName {
    TableName(self.0.to_string())
  }
}

impl<'a> Deref for TableNameRef<'a> {
  type Target = str;

  fn deref(&self) -> &Self::Target {
    self.0
  }
}

impl<'a> Display for TableNameRef<'a> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    Display::fmt(self.deref(), f)
  }
}
impl<'a> Debug for TableNameRef<'a> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    Debug::fmt(self.deref(), f)
  }
}
impl<'a> PartialEq for TableNameRef<'a> {
  fn eq(&self, other: &Self) -> bool {
    self.0 == other.0
  }
}
impl<'a> Eq for TableNameRef<'a> {}

#[cfg(test)]
#[path = "tests/table_name.rs"]
mod tests;
