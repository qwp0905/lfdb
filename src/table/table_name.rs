use std::{
  fmt::{Debug, Display},
  ops::Deref,
};

use crate::{Error, Result};

const MAX_TABLE_NAME_LEN: usize = 256 as usize;
pub const META_TABLE: &str = "__meta__";

pub struct TableName(String);
impl TableName {
  pub fn from_str(name: &str) -> Result<Self> {
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

    Ok(Self(name.to_string()))
  }

  pub fn from_str_unchecked(name: &str) -> Self {
    Self(name.to_string())
  }
}

impl Clone for TableName {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}
impl Deref for TableName {
  type Target = str;

  fn deref(&self) -> &Self::Target {
    self.0.as_str()
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
