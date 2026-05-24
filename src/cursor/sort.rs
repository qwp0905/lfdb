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

pub enum MergeSorted<T, R = T> {
  Single(T),
  MergeSorted {
    primary: T,
    primary_buffered: Option<(VecRef, Option<VecRef>)>,
    secondary: R,
    secondary_buffered: Option<(VecRef, Option<VecRef>)>,
  },
}

impl<T, R> MergeSorted<T, R> {
  pub fn single(sorted: T) -> Self {
    Self::Single(sorted)
  }
  pub fn merge(primary: T, secondary: R) -> Self {
    Self::MergeSorted {
      primary,
      primary_buffered: None,
      secondary,
      secondary_buffered: None,
    }
  }
}
impl<T, R> MergeSortable for MergeSorted<T, R>
where
  T: MergeSortable,
  R: MergeSortable,
{
  fn try_next(&mut self) -> Result<Option<(VecRef, Option<VecRef>)>> {
    let (primary, primary_buffered, secondary, secondary_buffered) = match self {
      MergeSorted::Single(sorted) => return sorted.try_next(),
      MergeSorted::MergeSorted {
        primary,
        primary_buffered,
        secondary,
        secondary_buffered,
      } => (primary, primary_buffered, secondary, secondary_buffered),
    };

    let primary_kv = match primary_buffered.take() {
      Some(kv) => Some(kv),
      None => primary.try_next()?,
    };
    let secondary_kv = match secondary_buffered.take() {
      Some(kv) => Some(kv),
      None => secondary.try_next()?,
    };

    let (k_p, v_p, k_s, v_s) = match (primary_kv, secondary_kv) {
      (None, None) => return Ok(None),
      (Some(kv), None) | (None, Some(kv)) => return Ok(Some(kv)),
      (Some((k_p, v_p)), Some((k_s, v_s))) => (k_p, v_p, k_s, v_s),
    };

    match k_p.cmp(&k_s) {
      Ordering::Less => {
        *secondary_buffered = Some((k_s, v_s));
        Ok(Some((k_p, v_p)))
      }
      Ordering::Greater => {
        *primary_buffered = Some((k_p, v_p));
        Ok(Some((k_s, v_s)))
      }
      Ordering::Equal => Ok(Some((k_p, v_p))),
    }
  }
}
