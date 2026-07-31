use std::cmp::Ordering;

use super::VecRef;
use crate::Result;

pub enum ScannedItem {
  Present(VecRef),
  Deleted,
}

/**
 * Merge two sorted key streams for tree-level compaction/merge work.
 *
 * When both streams contain the same key, the primary stream wins. This lets a
 * newer or otherwise higher-priority tree shadow an older secondary tree while
 * preserving tombstones during the merge. `get_next_pair` is only a convenience
 * reader for callers that want to skip tombstones in the final visible output.
 */
pub trait MergeSortable {
  /**
   * A sorted stream of key/value records used by merge-style tree operations.
   *
   * The value side is optional because `None` is a tombstone. Merge code must keep
   * tombstones in the stream so they can shadow older values for the same key;
   * callers that need only live key/value pairs can use `get_next_pair`.
   */
  fn try_next(&mut self) -> Result<Option<(VecRef, ScannedItem)>>;

  fn get_next_pair(&mut self) -> Result<Option<(VecRef, VecRef)>> {
    loop {
      match self.try_next()? {
        Some((k, ScannedItem::Present(v))) => return Ok(Some((k, v))),
        None => return Ok(None),
        Some((_, ScannedItem::Deleted)) => continue,
      }
    }
  }
}

pub enum SortDirection {
  Ascending,
  Descending,
}
enum SortedSet<T, R> {
  Single(T),
  Merged {
    primary: T,
    primary_buffered: Option<(VecRef, ScannedItem)>,
    secondary: R,
    secondary_buffered: Option<(VecRef, ScannedItem)>,
  },
}

pub struct MergeSorted<T, R = T> {
  set: SortedSet<T, R>,
  direction: SortDirection,
}

impl<T, R> MergeSorted<T, R> {
  pub fn single(sorted: T, direction: SortDirection) -> Self {
    Self {
      set: SortedSet::Single(sorted),
      direction,
    }
  }
  pub fn merge(primary: T, secondary: R, direction: SortDirection) -> Self {
    Self {
      set: SortedSet::Merged {
        primary,
        primary_buffered: None,
        secondary,
        secondary_buffered: None,
      },
      direction,
    }
  }
}
impl<T, R> MergeSortable for MergeSorted<T, R>
where
  T: MergeSortable,
  R: MergeSortable,
{
  fn try_next(&mut self) -> Result<Option<(VecRef, ScannedItem)>> {
    let (primary, primary_buffered, secondary, secondary_buffered) = match &mut self.set {
      SortedSet::Single(sorted) => return sorted.try_next(),
      SortedSet::Merged {
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

    match (k_p.cmp(&k_s), &self.direction) {
      (Ordering::Less, SortDirection::Ascending)
      | (Ordering::Greater, SortDirection::Descending) => {
        *secondary_buffered = Some((k_s, v_s));
        Ok(Some((k_p, v_p)))
      }
      (Ordering::Less, SortDirection::Descending)
      | (Ordering::Greater, SortDirection::Ascending) => {
        *primary_buffered = Some((k_p, v_p));
        Ok(Some((k_s, v_s)))
      }
      (Ordering::Equal, _) => Ok(Some((k_p, v_p))),
    }
  }
}
