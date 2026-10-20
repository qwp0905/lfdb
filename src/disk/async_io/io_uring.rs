use std::{
  fs::File,
  io::{Error, ErrorKind, Read, Result, Write},
  mem::ManuallyDrop,
  os::fd::{AsRawFd, FromRawFd, RawFd},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  thread::Builder,
};

use io_uring::{
  opcode, squeue, types, CompletionQueue, IoUring, SubmissionQueue, Submitter,
};

use crate::{
  background::{ThreadSlot, UnwindSpawner},
  utils::{warn, ChunkQueue, MpscQueue},
};

use super::{AsyncIOTask, TaskType};

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

fn to_entry(task: AsyncIOTask<usize>) -> squeue::Entry {
  let fd = types::Fd(task.toward().as_raw_fd());
  let mut entry = match task.get_type() {
    TaskType::Pwrite { offset, buf } => {
      opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
        .offset(offset)
        .build()
    }
    TaskType::Pwritev { offset, bufs } => {
      opcode::Writev::new(fd, bufs.as_ptr() as *const libc::iovec, bufs.len() as u32)
        .offset(offset)
        .build()
    }
    TaskType::Fsync => opcode::Fsync::new(fd).build(),
    TaskType::Fdatasync => opcode::Fsync::new(fd)
      .flags(types::FsyncFlags::DATASYNC)
      .build(),
    TaskType::Fallocate { offset, len } => {
      opcode::Fallocate::new(fd, len).offset(offset).build()
    }
  };
  entry.set_user_data(AsyncIOTask::into_raw(task) as u64);
  entry
}

fn drain_completion(
  cq: &mut CompletionQueue,
  retry: &mut ChunkQueue<AsyncIOTask<usize>>,
  mut maybe_waker: Option<&EventFd>,
) -> (usize, bool) {
  cq.sync();
  let mut count = 0;
  let mut found = false;
  for cqe in cq {
    count += 1;
    let result = cvt(cqe.result());
    let user_data = cqe.user_data();
    if user_data != POLL {
      let task = unsafe { AsyncIOTask::from_raw((user_data as usize) as *mut ()) };
      if result
        .as_ref()
        .is_err_and(|err| err.kind() == ErrorKind::Interrupted)
      {
        retry.push(task);
        continue;
      }

      task.fulfill(result);
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
  retry: &mut ChunkQueue<AsyncIOTask<usize>>,
) -> usize {
  let mut count = 0;
  loop {
    if sq.is_full() && is_ebusy(submitter.submit()) {
      break;
    };
    sq.sync();
    let Some(task) = retry.pop() else {
      break;
    };
    let entry = to_entry(task);
    let _ = unsafe { sq.push(&entry) };
    count += 1;
  }
  count
}

fn drain_input(
  submitter: &Submitter,
  sq: &mut SubmissionQueue,
  input: &mut InputQueue,
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
    let entry = to_entry(task);
    let _ = unsafe { sq.push(&entry) };
    count += 1;
  }
  (count, false)
}

fn shutdown_gracefully(
  submitter: Submitter,
  mut sq: SubmissionQueue,
  mut cq: CompletionQueue,
  mut retry: ChunkQueue<AsyncIOTask<usize>>,
  mut submitted: usize,
) {
  sq.sync();
  while submitted > 0 || !retry.is_empty() {
    cq.sync();
    ignore_ebusy(submitter.submit_and_wait(submitted.min(cq.capacity())));
    let (count, _) = drain_completion(&mut cq, &mut retry, None);
    submitted -= count;
    submitted += drain_retry(&submitter, &mut sq, &mut retry);
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
  let _ = unsafe { sq.push(wake) };
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
  queue: Arc<MpscQueue<Context<AsyncIOTask<usize>>>>,
  peeked: Option<Context<AsyncIOTask<usize>>>,
}
impl InputQueue {
  const fn new(queue: Arc<MpscQueue<Context<AsyncIOTask<usize>>>>) -> Self {
    Self {
      queue,
      peeked: None,
    }
  }
  fn peek(&mut self) -> Option<&Context<AsyncIOTask<usize>>> {
    if self.peeked.is_none() {
      self.peeked = unsafe { self.queue.pop() };
    }
    self.peeked.as_ref()
  }
  fn pop(&mut self) -> Option<Context<AsyncIOTask<usize>>> {
    self.peeked.take().or_else(|| unsafe { self.queue.pop() })
  }
}

enum Context<T> {
  Task(T),
  Term,
}

const POLL: u64 = 0;

pub struct AsyncIO {
  queue: Arc<MpscQueue<Context<AsyncIOTask<usize>>>>,
  parked: Arc<AtomicBool>,
  waker: EventFd,
  slot: ThreadSlot,
}
impl AsyncIO {
  const fn worker_loop(
    entries: u32,
    queue: Arc<MpscQueue<Context<AsyncIOTask<usize>>>>,
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

      loop {
        let (count, found) = drain_completion(&mut cq, &mut retry, Some(&waker));
        submitted -= count;
        pending &= !found;
        submitted += drain_retry(&submitter, &mut sq, &mut retry);
        let (count, terminated) = drain_input(&submitter, &mut sq, &mut input);
        submitted += count;
        if terminated {
          return shutdown_gracefully(submitter, sq, cq, retry, submitted);
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

  pub fn submit(&self, task: AsyncIOTask<usize>) {
    self.queue.push(Context::Task(task));
    if self.parked.swap(false, Ordering::Relaxed) {
      self.waker.wake();
    }
  }

  pub fn close(&self) {
    let Some(handle) = self.slot.close() else {
      return;
    };
    self.queue.push(Context::Term);
    self.waker.wake();
    handle.join().unwrap();
  }
}
impl Drop for AsyncIO {
  fn drop(&mut self) {
    self.close();
  }
}
