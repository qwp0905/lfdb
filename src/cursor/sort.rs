use std::cmp::Ordering;

use super::VecRef;
use crate::Result;

pub trait MergeSortable {
  fn try_next(&mut self) -> Result<Option<(VecRef, Option<VecRef>)>>;

  fn get_next_pair(&mut self) -> Result<Option<(VecRef, VecRef)>> {
    loop {
      match self.try_next()? {
        Some((k, Some(v))) => return Ok(Some((k, v))),
        None => return Ok(None),
        Some((_, None)) => continue,
      }
    }
  }
}

pub struct MergeSorted<T, R = T> {
  default: T,
  default_buffered: Option<(VecRef, Option<VecRef>)>,
  optional: Option<(R, Option<(VecRef, Option<VecRef>)>)>,
}
impl<T, R> MergeSorted<T, R> {
  pub fn new(default: T, optional: Option<R>) -> Self {
    Self {
      default,
      default_buffered: None,
      optional: optional.map(|v| (v, None)),
    }
  }
}
impl<T, R> MergeSortable for MergeSorted<T, R>
where
  T: MergeSortable,
  R: MergeSortable,
{
  fn try_next(&mut self) -> Result<Option<(VecRef, Option<VecRef>)>> {
    let (optional, optional_buffered) = match self.optional.as_mut() {
      Some(v) => v,
      None => return self.default.try_next(),
    };

    let default_kv = match self.default_buffered.take() {
      Some(kv) => Some(kv),
      None => self.default.try_next()?,
    };
    let optional_kv = match optional_buffered.take() {
      Some(kv) => Some(kv),
      None => optional.try_next()?,
    };

    let (k_d, v_d, k_o, v_o) = match (default_kv, optional_kv) {
      (None, None) => return Ok(None),
      (Some(kv), None) | (None, Some(kv)) => return Ok(Some(kv)),
      (Some((k_d, v_d)), Some((k_o, v_o))) => (k_d, v_d, k_o, v_o),
    };

    match k_d.cmp(&k_o) {
      Ordering::Less => {
        *optional_buffered = Some((k_o, v_o));
        Ok(Some((k_d, v_d)))
      }
      Ordering::Greater => {
        self.default_buffered = Some((k_d, v_d));
        Ok(Some((k_o, v_o)))
      }
      Ordering::Equal => Ok(Some((k_o, v_o))),
    }
  }
}
