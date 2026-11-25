use std::{
  fs::File,
  io::{Error, ErrorKind, Read, Result, Write},
  mem::{replace, ManuallyDrop},
  os::fd::{AsRawFd, FromRawFd, RawFd},
  ptr::null_mut,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  thread::{current, Builder},
};

use io_uring::{
  opcode, squeue, types, CompletionQueue, IoUring, SubmissionQueue, Submitter,
};

use crate::{
  background::{ThreadSlot, UnwindSpawner},
  utils::{warn, ChunkQueue, MpscQueue},
};

use super::BoundIOTask;

enum Slot<T> {
  Occupied(T),
  Vacant(Option<usize>),
}

#[repr(align(2))]
struct Node<T> {
  slot: Slot<T>,
  next: *mut Node<T>,
  prev: *mut Node<T>,
}
impl<T> Node<T> {
  const fn vacant(next: Option<usize>) -> Self {
    Self {
      slot: Slot::Vacant(next),
      next: null_mut(),
      prev: null_mut(),
    }
  }

  const fn occupied(value: T) -> Self {
    Self {
      slot: Slot::Occupied(value),
      next: null_mut(),
      prev: null_mut(),
    }
  }
}

pub struct PendingTasks<T> {
  slots: Box<[Node<T>]>,
  head: Option<usize>,
  len: usize,
  overflow: *mut Node<T>,
}
impl<T> PendingTasks<T> {
  fn with_capacity(capacity: usize) -> Self {
    debug_assert!(capacity > 0);
    let mut slots = Vec::with_capacity(capacity);
    for (i, slot) in slots
      .spare_capacity_mut()
      .iter_mut()
      .take(capacity - 1)
      .enumerate()
    {
      slot.write(Node::vacant(Some(i + 1)));
    }
    unsafe { slots.set_len(capacity - 1) };
    slots.push(Node::vacant(None));
    Self {
      slots: slots.into_boxed_slice(),
      head: (capacity > 0).then_some(0),
      len: 0,
      overflow: null_mut(),
    }
  }

  fn insert(&mut self, value: T) -> u64 {
    if let Some(index) = self.head {
      let Slot::Vacant(next) =
        replace(&mut self.slots[index].slot, Slot::Occupied(value))
      else {
        unreachable!("head points to an occupied slot");
      };
      self.head = next;
      self.len += 1;
      return (index << 1 | 1) as _;
    }

    let node = Box::into_raw(Box::new(Node::occupied(value)));
    if !self.overflow.is_null() {
      unsafe { (*self.overflow).next = node };
      unsafe { (*node).prev = self.overflow };
    };
    self.overflow = node;
    self.len += 1;
    node as _
  }

  fn get(&self, index: u64) -> Option<&T> {
    if index & 1 != 0 {
      let index = index as usize >> 1;
      let Slot::Occupied(value) = &self.slots[index].slot else {
        return None;
      };
      return Some(value);
    };
    unsafe {
      let Slot::Occupied(value) = &(*(index as *mut Node<T>)).slot else {
        unreachable!("heap allocated node must be occupied.")
      };
      Some(value)
    }
  }

  fn remove(&mut self, index: u64) -> Option<T> {
    if index & 1 != 0 {
      let index = index as usize >> 1;
      let Slot::Occupied(value) =
        replace(&mut self.slots[index].slot, Slot::Vacant(self.head))
      else {
        return None;
      };
      self.head = Some(index);
      self.len -= 1;
      return Some(value);
    }

    let node = unsafe { Box::from_raw(index as *mut Node<T>) };
    let prev = node.prev;
    let next = node.next;
    if !prev.is_null() {
      unsafe { (*prev).next = next };
    } else {
      self.overflow = next;
    };

    if !next.is_null() {
      unsafe { (*next).prev = prev };
    };

    self.len -= 1;
    let Slot::Occupied(value) = node.slot else {
      unreachable!("heap allocated node must be occupied.");
    };
    Some(value)
  }
}
impl<T> Drop for PendingTasks<T> {
  fn drop(&mut self) {
    let mut overflow = self.overflow;
    while !overflow.is_null() {
      overflow = unsafe { Box::from_raw(overflow) }.next;
    }
  }
}

struct EventFd(File);
impl EventFd {
  fn new() -> Result<Self> {
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
      return Err(Error::last_os_error());
    }
    Ok(unsafe { Self::from_raw_fd(fd) })
  }
  unsafe fn from_raw_fd(fd: RawFd) -> Self {
    Self(unsafe { File::from_raw_fd(fd) })
  }

  fn as_raw_fd(&self) -> RawFd {
    self.0.as_raw_fd()
  }

  fn consume(&self) {
    let mut buf = [0; 8];
    loop {
      let Err(ref err) = (&self.0).read_exact(&mut buf) else {
        return;
      };
      match err.kind() {
        ErrorKind::Interrupted => continue,
        ErrorKind::WouldBlock => return,
        _ => panic!("{err}"),
      }
    }
  }
  fn wake(&self) {
    loop {
      let Err(err) = (&self.0).write_all(&1u64.to_ne_bytes()) else {
        return;
      };
      match err.kind() {
        ErrorKind::Interrupted => continue,
        ErrorKind::WouldBlock => return,
        _ => panic!("{err}"),
      }
    }
  }
}

fn cvt(ret: i32) -> Result<usize> {
  if ret < 0 {
    return Err(Error::from_raw_os_error(-ret));
  }
  Ok(ret as usize)
}

fn to_entry(task: &BoundIOTask) -> squeue::Entry {
  match task {
    BoundIOTask::Pwrite {
      offset, buf, file, ..
    } => opcode::Write::new(types::Fd(file.as_raw_fd()), buf.as_ptr(), buf.len() as u32)
      .offset(*offset)
      .build(),
    BoundIOTask::Pwritev {
      offset, bufs, file, ..
    } => opcode::Writev::new(
      types::Fd(file.as_raw_fd()),
      bufs.as_ptr() as *const libc::iovec,
      bufs.len() as u32,
    )
    .offset(*offset)
    .build(),
    BoundIOTask::Fsync { file, .. } => {
      opcode::Fsync::new(types::Fd(file.as_raw_fd())).build()
    }
    BoundIOTask::Fdatasync { file, .. } => {
      opcode::Fsync::new(types::Fd(file.as_raw_fd()))
        .flags(types::FsyncFlags::DATASYNC)
        .build()
    }
    BoundIOTask::Fallocate {
      offset, len, file, ..
    } => opcode::Fallocate::new(types::Fd(file.as_raw_fd()), *len)
      .offset(*offset)
      .build(),
    BoundIOTask::Open { path, flags, .. } => {
      opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr())
        .mode(0o666)
        .flags(flags.as_raw())
        .build()
    }
  }
}

fn fulfill_task(task: BoundIOTask, result: Result<usize>) {
  match task {
    BoundIOTask::Pwrite { fulfiller, .. } | BoundIOTask::Pwritev { fulfiller, .. } => {
      fulfiller.fulfill(result)
    }
    BoundIOTask::Fallocate { fulfiller, .. }
    | BoundIOTask::Fdatasync { fulfiller, .. }
    | BoundIOTask::Fsync { fulfiller, .. } => fulfiller.fulfill(result.map(drop)),
    BoundIOTask::Open { fulfiller, .. } => fulfiller.fulfill(result),
  }
}

fn drain_completion(
  cq: &mut CompletionQueue,
  retry: &mut ChunkQueue<u64>,
  mut maybe_waker: Option<&EventFd>,
  pendings: &mut PendingTasks<BoundIOTask>,
) -> (usize, bool) {
  cq.sync();
  let mut count = 0;
  let mut found = false;
  for cqe in cq {
    count += 1;
    let result = cvt(cqe.result());
    let user_data = cqe.user_data();
    if user_data != POLL {
      if result
        .as_ref()
        .is_err_and(|err| err.kind() == ErrorKind::Interrupted)
      {
        retry.push(user_data);
        continue;
      }

      let task = pendings.remove(user_data).unwrap_or_else(|| unreachable!());
      fulfill_task(task, result);
      continue;
    }
    let Some(waker) = maybe_waker.as_mut() else {
      continue;
    };
    match result {
      Ok(_) => waker.consume(),
      Err(err) => warn!("consume fd has been skipped since: {err}"),
    };
    found = true;
  }
  (count, found)
}

fn drain_retry(
  submitter: &Submitter,
  sq: &mut SubmissionQueue,
  retry: &mut ChunkQueue<u64>,
  pendings: &mut PendingTasks<BoundIOTask>,
) -> usize {
  let mut count = 0;
  loop {
    if sq.is_full() && is_ebusy(submitter.submit()) {
      break;
    };
    sq.sync();
    let Some(user_data) = retry.pop() else {
      break;
    };
    let task = pendings.get(user_data).unwrap_or_else(|| unreachable!());
    let mut entry = to_entry(task);
    entry.set_user_data(user_data);
    unsafe { sq.push(&entry).unwrap() };
    count += 1;
  }
  count
}

fn drain_input(
  submitter: &Submitter,
  sq: &mut SubmissionQueue,
  input: &mut InputQueue,
  pendings: &mut PendingTasks<BoundIOTask>,
) -> (usize, bool) {
  let mut count = 0;
  while let Some(ctx) = input.peek() {
    if let Context::Term = ctx {
      return (count, true);
    }
    if sq.is_full() && is_ebusy(submitter.submit()) {
      break;
    }
    sq.sync();
    let Context::Task(task) = input.pop().unwrap_or_else(|| unreachable!()) else {
      unreachable!()
    };
    let mut entry = to_entry(&task);
    let user_data = pendings.insert(task);
    entry.set_user_data(user_data);
    unsafe { sq.push(&entry).unwrap() };
    count += 1;
  }
  (count, false)
}

fn shutdown_gracefully(
  submitter: Submitter,
  mut sq: SubmissionQueue,
  mut cq: CompletionQueue,
  mut retry: ChunkQueue<u64>,
  mut pendings: PendingTasks<BoundIOTask>,
  mut submitted: usize,
) {
  sq.sync();
  while submitted > 0 || !retry.is_empty() {
    cq.sync();
    ignore_ebusy(submitter.submit_and_wait(submitted.min(cq.capacity())));
    let (count, _) = drain_completion(&mut cq, &mut retry, None, &mut pendings);
    submitted -= count;
    submitted += drain_retry(&submitter, &mut sq, &mut retry, &mut pendings);
  }
}

fn register_waker(
  submitter: &Submitter,
  sq: &mut SubmissionQueue,
  cq: &mut CompletionQueue,
  wake: &squeue::Entry,
) -> bool {
  if sq.is_full() {
    cq.sync();
    if is_ebusy(submitter.submit()) {
      return false;
    }
  };
  sq.sync();
  unsafe { sq.push(wake).unwrap() };
  true
}

fn ignore_ebusy<T>(result: Result<T>) {
  is_ebusy(result);
}
fn is_ebusy<T>(result: Result<T>) -> bool {
  let Err(ref err) = result else {
    return false;
  };
  if err
    .raw_os_error()
    .is_some_and(|ret| matches!(ret, libc::EBUSY | libc::EINTR))
  {
    return true;
  };
  panic!("{err}")
}
fn submit_if_not_empty(submitter: &Submitter, sq: &SubmissionQueue) {
  if sq.is_empty() && !sq.taskrun() {
    return;
  }
  ignore_ebusy(submitter.submit());
}

struct InputQueue {
  queue: Arc<MpscQueue<Context<BoundIOTask>>>,
  peeked: Option<Context<BoundIOTask>>,
}
impl InputQueue {
  const fn new(queue: Arc<MpscQueue<Context<BoundIOTask>>>) -> Self {
    Self {
      queue,
      peeked: None,
    }
  }
  fn peek(&mut self) -> Option<&Context<BoundIOTask>> {
    if self.peeked.is_none() {
      self.peeked = unsafe { self.queue.pop() };
    }
    self.peeked.as_ref()
  }
  fn pop(&mut self) -> Option<Context<BoundIOTask>> {
    self.peeked.take().or_else(|| unsafe { self.queue.pop() })
  }
}

enum Context<T> {
  Task(T),
  Term,
}

const POLL: u64 = 0;

pub struct AsyncIO {
  queue: Arc<MpscQueue<Context<BoundIOTask>>>,
  parked: Arc<AtomicBool>,
  waker: EventFd,
  slot: ThreadSlot,
}
impl AsyncIO {
  const fn worker_loop(
    entries: u32,
    queue: Arc<MpscQueue<Context<BoundIOTask>>>,
    waker_fd: RawFd,
    parked: Arc<AtomicBool>,
  ) -> impl FnOnce() {
    move || {
      let mut input = InputQueue::new(queue);
      let mut submitted = 0;
      let mut ring = IoUring::builder()
        .setup_coop_taskrun()
        .setup_single_issuer()
        .setup_taskrun_flag()
        .build(entries)
        .unwrap();
      let (submitter, mut sq, mut cq) = ring.split();
      let wake = opcode::PollAdd::new(types::Fd(waker_fd), libc::POLLIN as u32)
        .build()
        .user_data(POLL);
      let waker = ManuallyDrop::new(unsafe { EventFd::from_raw_fd(waker_fd) });
      let mut pending = false;
      let mut retry = ChunkQueue::new();
      let mut pendings = PendingTasks::with_capacity((entries << 1) as _);

      loop {
        let (count, found) =
          drain_completion(&mut cq, &mut retry, Some(&waker), &mut pendings);
        submitted -= count;
        pending &= !found;
        submitted += drain_retry(&submitter, &mut sq, &mut retry, &mut pendings);
        let (count, terminated) =
          drain_input(&submitter, &mut sq, &mut input, &mut pendings);
        submitted += count;
        if terminated {
          return shutdown_gracefully(submitter, sq, cq, retry, pendings, submitted);
        }

        if !pending {
          if !register_waker(&submitter, &mut sq, &mut cq, &wake) {
            continue;
          }
          submitted += 1;
          pending = true;
        }

        sq.sync();
        cq.sync();
        if !cq.is_empty() || !retry.is_empty() || input.peek().is_some() {
          submit_if_not_empty(&submitter, &sq);
          continue;
        }

        parked.fetch_or(true, Ordering::AcqRel);
        cq.sync();
        if !cq.is_empty() || input.peek().is_some() {
          parked.fetch_and(false, Ordering::AcqRel);
          submit_if_not_empty(&submitter, &sq);
          continue;
        }

        ignore_ebusy(submitter.submit_and_wait(1));
        parked.fetch_and(false, Ordering::AcqRel);
      }
    }
  }
  pub fn new(entries: u32) -> Result<Self> {
    let queue = Arc::new(MpscQueue::new());
    let parked = Arc::new(AtomicBool::new(false));
    let waker = EventFd::new()?;
    let handle = Builder::new()
      .name("async io".to_string())
      .stack_size(64 << 10)
      .spawn_unwind(Self::worker_loop(
        entries,
        queue.clone(),
        waker.as_raw_fd(),
        parked.clone(),
      ));
    Ok(Self {
      queue,
      parked,
      waker,
      slot: ThreadSlot::new(handle),
    })
  }

  pub fn submit(&self, task: BoundIOTask) {
    self.queue.push(Context::Task(task));
    if self.parked.swap(false, Ordering::Release) {
      self.waker.wake();
    }
  }

  pub fn close(&self) {
    let Some(handle) = self.slot.close() else {
      return;
    };
    self.queue.push(Context::Term);
    self.waker.wake();

    debug_assert_ne!(
      handle.thread().id(),
      current().id(),
      "thread must not close its own handle."
    );
    handle.join().unwrap();
  }
}
impl Drop for AsyncIO {
  fn drop(&mut self) {
    self.close();
  }
}
