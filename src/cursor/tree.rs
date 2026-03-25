use std::{collections::HashSet, mem::replace, sync::Arc, time::Duration};

use super::{
  CursorNode, DataEntry, GarbageCollector, InternalNode, Key, Pointer, TreeHeader,
  HEADER_INDEX,
};
use crate::{
  buffer_pool::BufferPool,
  constant::RESERVED_TX,
  thread::{BackgroundThread, WorkBuilder},
  transaction::{FreeList, PageRecorder},
  utils::{LogFilter, ToBox},
  Result,
};

pub struct TreeManagerConfig {
  pub merge_interval: Duration,
}

pub struct SplitTask {
  stack: Vec<Pointer>,
  split_key: Key,
  split_pointer: Pointer,
  root_height: u32,
}
impl SplitTask {
  pub fn new(
    stack: Vec<Pointer>,
    split_key: Key,
    split_pointer: Pointer,
    root_height: u32,
  ) -> Self {
    Self {
      stack,
      split_key,
      split_pointer,
      root_height,
    }
  }
}

pub struct TreeManager {
  merge_leaf: Box<dyn BackgroundThread<(), Result>>,
  split_internal: Box<dyn BackgroundThread<SplitTask, Result>>,
}
impl TreeManager {
  fn new(
    free_list: Arc<FreeList>,
    buffer_pool: Arc<BufferPool>,
    recorder: Arc<PageRecorder>,
    gc: Arc<GarbageCollector>,
    logger: LogFilter,
    config: TreeManagerConfig,
  ) -> Self {
    let merge_leaf = WorkBuilder::new()
      .name("merge leaf nodes")
      .stack_size(2 << 20)
      .single()
      .interval(
        config.merge_interval,
        run_merge_leaf(
          buffer_pool.clone(),
          recorder.clone(),
          gc.clone(),
          logger.clone(),
        ),
      )
      .to_box();
    let split_internal = WorkBuilder::new()
      .name("split internal nodes")
      .stack_size(2 << 20)
      .multi(3)
      .shared(run_split_internal(buffer_pool, recorder, free_list))
      .to_box();

    Self {
      merge_leaf,
      split_internal,
    }
  }
  pub fn initial_state(
    free_list: Arc<FreeList>,
    buffer_pool: Arc<BufferPool>,
    recorder: Arc<PageRecorder>,
    gc: Arc<GarbageCollector>,
    logger: LogFilter,
    config: TreeManagerConfig,
  ) -> Result<Self> {
    {
      logger.info("btree initial state.");

      let node_index = free_list.alloc();
      let mut slot = buffer_pool.read(node_index)?.for_write();
      recorder.serialize_and_log(0, &mut slot, &CursorNode::initial_state())?;

      let root = TreeHeader::new(node_index);
      let mut root_slot = buffer_pool.read(HEADER_INDEX)?.for_write();
      recorder.serialize_and_log(0, &mut root_slot, &root)?;
    }
    Ok(Self::new(
      free_list,
      buffer_pool,
      recorder,
      gc,
      logger,
      config,
    ))
  }
  pub fn clean_and_start(
    free_list: Arc<FreeList>,
    buffer_pool: Arc<BufferPool>,
    recorder: Arc<PageRecorder>,
    gc: Arc<GarbageCollector>,
    logger: LogFilter,
    config: TreeManagerConfig,
    file_end: usize,
  ) -> Result<Self> {
    let mut visited: HashSet<usize> = HashSet::from_iter(vec![HEADER_INDEX]);

    let root = buffer_pool
      .read(HEADER_INDEX)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();
    let mut node_stack = vec![root];
    let mut entry_stack = vec![];

    while let Some(index) = node_stack.pop() {
      visited.insert(index);
      match buffer_pool
        .read(index)?
        .for_read()
        .as_ref()
        .deserialize::<CursorNode>()?
      {
        CursorNode::Internal(internal) => node_stack.extend(internal.get_all_child()),
        CursorNode::Leaf(leaf) => entry_stack.extend(leaf.get_entry_pointers()),
      };
    }

    // push to queue for initial checkpoint
    for &pointer in entry_stack.iter() {
      gc.mark(pointer);
    }
    logger.debug(format!(
      "{} entry queued for initial gc.",
      entry_stack.len()
    ));

    while let Some(index) = entry_stack.pop() {
      visited.insert(index);
      let entry: DataEntry =
        buffer_pool.read(index)?.for_read().as_ref().deserialize()?;
      entry.get_next().map(|i| entry_stack.push(i));
    }

    for index in 0..file_end {
      if visited.remove(&index) {
        continue;
      }
      gc.release(index);
    }

    logger.info("orphand block has released successfully.");
    Ok(Self::new(
      free_list,
      buffer_pool,
      recorder,
      gc,
      logger,
      config,
    ))
  }

  pub fn split(&self, task: SplitTask) {
    self.split_internal.send(task);
  }

  pub fn close(&self) {
    self.merge_leaf.close();
    self.split_internal.close();
  }
}

fn run_merge_leaf(
  buffer_pool: Arc<BufferPool>,
  recorder: Arc<PageRecorder>,
  gc: Arc<GarbageCollector>,
  logger: LogFilter,
) -> impl Fn(Option<()>) -> Result {
  move |_| {
    logger.debug("clean leaf collect start.");

    let mut index = buffer_pool
      .peek(HEADER_INDEX)?
      .for_read()
      .as_ref()
      .deserialize::<TreeHeader>()?
      .get_root();

    while let CursorNode::Internal(node) = buffer_pool
      .peek(index)?
      .for_read()
      .as_ref()
      .deserialize::<CursorNode>()?
    {
      index = node.first_child()
    }

    let mut index = Some(index);
    while let Some(i) = index.take() {
      {
        let leaf = buffer_pool
          .peek(i)?
          .for_read()
          .as_ref()
          .deserialize::<CursorNode>()?
          .as_leaf()?;
        if !gc
          .batch_check_empty(leaf.get_entry_pointers().collect())
          .wait()?
          .into_iter()
          .fold(Ok(false), |a, c| a.and_then(|a| c.map(|c| a || c)))?
        {
          index = leaf.get_next();
          continue;
        }
      }

      let mut slot = buffer_pool.peek(i)?.for_write();
      let mut leaf = slot.as_ref().deserialize::<CursorNode>()?.as_leaf()?;
      index = leaf.get_next();

      let prev_len = leaf.len();
      let mut new_entries = vec![];
      let mut orphand = vec![];

      let found = leaf
        .drain()
        .map(|(key, ptr)| (key, ptr, gc.check_empty(ptr)))
        .collect::<Vec<_>>();
      for (key, ptr, r) in found.into_iter() {
        if r.wait_flatten()? {
          orphand.push(ptr);
        } else {
          new_entries.push((key, ptr));
        }
      }

      let next = match (new_entries.len(), index) {
        (0, Some(next)) => next,
        (len, _) if len == prev_len => continue,
        _ => {
          leaf.set_entries(new_entries);
          recorder.serialize_and_log(0, &mut slot, &leaf.to_node())?;
          drop(slot);

          orphand.into_iter().for_each(|ptr| gc.release(ptr));
          continue;
        }
      };

      // merge start
      logger.debug(format!(
        "trying to start merge {} with {}",
        slot.get_index(),
        next
      ));

      let mut next_slot = buffer_pool.peek(next)?.for_write();
      let next_leaf = next_slot.as_ref().deserialize::<CursorNode>()?;
      leaf.set_next(slot.get_index());

      recorder.log_multi(
        RESERVED_TX,
        &mut slot,
        &next_leaf,
        &mut next_slot,
        &leaf.to_node(),
      )?;
      index = Some(slot.get_index());

      drop(slot);
      drop(next_slot);

      orphand.into_iter().for_each(|ptr| gc.release(ptr));
    }

    logger.debug("clean leaf collect end.");
    Ok(())
  }
}

fn run_split_internal(
  buffer_pool: Arc<BufferPool>,
  recorder: Arc<PageRecorder>,
  free_list: Arc<FreeList>,
) -> impl Fn(SplitTask) -> Result {
  move |task| {
    let SplitTask {
      mut stack,
      mut split_key,
      mut split_pointer,
      mut root_height,
    } = task;

    while let Some(index) = stack.pop() {
      match apply_split(
        &buffer_pool,
        &recorder,
        &free_list,
        split_key,
        split_pointer,
        index,
      )? {
        Some((k, p)) => {
          split_key = k;
          split_pointer = p;
        }
        None => return Ok(()),
      }
    }

    loop {
      let mut header_slot = buffer_pool.peek(HEADER_INDEX)?.for_write();
      let mut header: TreeHeader = header_slot.as_ref().deserialize()?;
      let current_height = header.get_height();
      let mut index = header.get_root();
      if root_height == current_height {
        let new_root = InternalNode::initialize(split_key, index, split_pointer);
        let new_root_index = {
          let i = free_list.alloc();
          let mut s = buffer_pool.peek(i)?.for_write();
          recorder.serialize_and_log(RESERVED_TX, &mut s, &new_root.to_node())?;
          i
        };

        header.set_root(new_root_index);
        header.increase_height();
        return recorder.serialize_and_log(RESERVED_TX, &mut header_slot, &header);
      }

      let diff = (current_height - root_height) as usize;
      root_height = current_height;
      drop(header_slot);
      let mut stack = vec![];

      while stack.len() < diff {
        let node = buffer_pool
          .peek(index)?
          .for_read()
          .as_ref()
          .deserialize::<CursorNode>()?
          .as_internal()?;
        match node.find(&split_key) {
          Ok(i) => stack.push(replace(&mut index, i)),
          Err(i) => index = i,
        }
      }

      while let Some(index) = stack.pop() {
        match apply_split(
          &buffer_pool,
          &recorder,
          &free_list,
          split_key,
          split_pointer,
          index,
        )? {
          Some((k, p)) => {
            split_key = k;
            split_pointer = p;
          }
          None => return Ok(()),
        };
      }
    }
  }
}

fn apply_split(
  buffer_pool: &BufferPool,
  recorder: &PageRecorder,
  free_list: &FreeList,
  key: Key,
  ptr: Pointer,
  current: Pointer,
) -> Result<Option<(Key, Pointer)>> {
  let mut index = current;

  let (mut slot, mut internal) = loop {
    let slot = buffer_pool.peek(index)?.for_write();
    let mut internal = slot.as_ref().deserialize::<CursorNode>()?.as_internal()?;
    match internal.insert_or_next(&key, ptr) {
      Ok(_) => break (slot, internal),
      Err(i) => index = i,
    }
  };

  let (split_node, split_key) = match internal.split_if_needed() {
    Some(split) => split,
    None => {
      return recorder
        .serialize_and_log(RESERVED_TX, &mut slot, &internal.to_node())
        .map(|_| None)
    }
  };

  let split_index = {
    let index = free_list.alloc();
    let mut s = buffer_pool.peek(index)?.for_write();
    recorder.serialize_and_log(RESERVED_TX, &mut s, &split_node.to_node())?;
    index
  };

  internal.set_right(&split_key, split_index);
  recorder.serialize_and_log(RESERVED_TX, &mut slot, &internal.to_node())?;

  Ok(Some((split_key, split_index)))
}
