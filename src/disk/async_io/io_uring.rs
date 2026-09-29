use std::{
  fs::File,
  io::{Error, Read, Result, Write},
  mem::ManuallyDrop,
  os::fd::{AsRawFd, FromRawFd, RawFd},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  thread::Builder,
};

use crossbeam::{queue::SegQueue, utils::Backoff};
use io_uring::{
  opcode, squeue, types, CompletionQueue, IoUring, SubmissionQueue, Submitter,
};

use crate::{
  background::{OneshotBehavior, OneshotFulfill, ThreadSlot, UnwindSpawner},
  utils::ChunkQueue,
};

use super::{Task, TaskType};

fn cvt(ret: i32) -> Result<usize> {
  if ret < 0 {
    return Err(Error::from_raw_os_error(-ret));
  }
  Ok(ret as usize)
}

fn to_entry(task: Task) -> squeue::Entry {
  let fd = types::Fd(task.toward.as_raw_fd());
  let mut entry = match task.task_type {
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
  entry.set_user_data(OneshotFulfill::into_raw(task.done) as u64);
  entry
}

fn drain_completion(
  cq: &mut CompletionQueue,
  mut maybe_waker: Option<&File>,
) -> (usize, bool) {
  cq.sync();
  let mut count = 0;
  let mut found = false;
  for cqe in cq {
    count += 1;
    let ret = cqe.result();
    let user_data = cqe.user_data();
    if user_data != POLL {
      let ptr = (user_data as usize) as *mut OneshotBehavior<Result<usize>>;
      let done = unsafe { OneshotFulfill::from_raw(ptr) };
      done.fulfill(cvt(ret));
      continue;
    }
    let Some(waker) = maybe_waker.as_mut() else {
      continue;
    };
    let mut buf = [0; 8];
    waker.read_exact(&mut buf).unwrap();
    found = true;
  }

  (count, found)
}

fn drain_backlog(
  submitter: &Submitter,
  sq: &mut SubmissionQueue,
  backlog: &mut ChunkQueue<squeue::Entry>,
) -> usize {
  let mut count = 0;
  loop {
    if sq.is_full() && is_ebusy(submitter.submit()) {
      break;
    };
    sq.sync();
    let Some(sqe) = backlog.pop() else {
      break;
    };
    let _ = unsafe { sq.push(&sqe) };
    count += 1;
  }
  count
}
fn drain_task(
  sq: &mut SubmissionQueue,
  input: &SegQueue<Context<Task>>,
  backlog: &mut ChunkQueue<squeue::Entry>,
) -> (usize, bool) {
  let mut count = 0;
  while let Some(ctx) = input.pop() {
    let entry = match ctx {
      Context::Task(task) => to_entry(task),
      Context::Term => return (count, true),
    };
    match unsafe { sq.push(&entry) } {
      Ok(_) => count += 1,
      Err(_) => backlog.push(entry),
    };
  }
  sq.sync();
  (count, false)
}

fn shutdown_gracefully(
  submitter: Submitter,
  mut sq: SubmissionQueue,
  mut cq: CompletionQueue,
  mut backlog: ChunkQueue<squeue::Entry>,
  mut submitted: usize,
) {
  while !backlog.is_empty() {
    submitted += drain_backlog(&submitter, &mut sq, &mut backlog);
    let (count, _) = drain_completion(&mut cq, None);
    submitted -= count;
  }

  while submitted > 0 {
    cq.sync();
    ignore_ebusy(submitter.submit_and_wait(submitted.min(cq.capacity())));
    let (count, _) = drain_completion(&mut cq, None);
    submitted -= count;
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
  match result {
    Ok(_) => false,
    Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => true,
    Err(err) => panic!("{err}"),
  }
}
fn submit_if_not_empty(submitter: &Submitter, sq: &SubmissionQueue) {
  if sq.is_empty() {
    return;
  }
  ignore_ebusy(submitter.submit());
}

enum Context<T> {
  Task(T),
  Term,
}

const POLL: u64 = 0;

pub struct AsyncIO {
  queue: Arc<SegQueue<Context<Task>>>,
  parked: Arc<AtomicBool>,
  waker: File,
  slot: ThreadSlot,
}
impl AsyncIO {
  const fn worker_loop(
    mut ring: IoUring,
    queue: Arc<SegQueue<Context<Task>>>,
    waker_fd: RawFd,
    parked: Arc<AtomicBool>,
  ) -> impl FnOnce() {
    move || {
      let backoff = Backoff::new();
      let mut backlog = ChunkQueue::new();
      let mut submitted = 0;
      let (submitter, mut sq, mut cq) = ring.split();
      let wake = opcode::PollAdd::new(types::Fd(waker_fd), libc::POLLIN as u32)
        .build()
        .user_data(POLL);
      let waker = ManuallyDrop::new(unsafe { File::from_raw_fd(waker_fd) });
      let mut pending = false;

      loop {
        let (completed_count, found) = drain_completion(&mut cq, Some(&waker));
        submitted -= completed_count;
        pending &= !found;
        let backlog_count = drain_backlog(&submitter, &mut sq, &mut backlog);
        submitted += backlog_count;
        let (task_count, terminated) = drain_task(&mut sq, &queue, &mut backlog);
        submitted += task_count;
        if terminated {
          return shutdown_gracefully(submitter, sq, cq, backlog, submitted);
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
        if completed_count > 0 || backlog_count + task_count > 0 || !backlog.is_empty() {
          submit_if_not_empty(&submitter, &sq);
          backoff.reset();
          continue;
        }
        if !backoff.is_completed() {
          submit_if_not_empty(&submitter, &sq);
          backoff.snooze();
          continue;
        }

        backoff.reset();
        cq.sync();
        if cq.is_empty() && queue.is_empty() && !parked.fetch_or(true, Ordering::Relaxed)
        {
          ignore_ebusy(submitter.submit_and_wait(1));
        } else {
          submit_if_not_empty(&submitter, &sq);
        }
      }
    }
  }
  pub fn new(entries: u32) -> Result<Self> {
    let ring = IoUring::new(entries)?;
    let queue = Arc::new(SegQueue::new());
    let parked = Arc::new(AtomicBool::new(false));
    let waker_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if waker_fd < 0 {
      return Err(Error::last_os_error());
    }
    let waker = unsafe { File::from_raw_fd(waker_fd) };
    let handle = Builder::new()
      .name("async io".to_string())
      .stack_size(64 << 10)
      .spawn_unwind(Self::worker_loop(
        ring,
        queue.clone(),
        waker_fd,
        parked.clone(),
      ));
    Ok(Self {
      queue,
      parked,
      waker,
      slot: ThreadSlot::new(handle),
    })
  }

  fn wake(&self) -> Result<()> {
    if self.parked.swap(false, Ordering::Relaxed) {
      (&self.waker).write_all(&1u64.to_ne_bytes())?;
    }
    Ok(())
  }

  pub fn submit(&self, task: Task) -> Result<()> {
    self.queue.push(Context::Task(task));
    self.wake()?;
    Ok(())
  }

  pub fn close(&self) {
    let Some(handle) = self.slot.close() else {
      return;
    };
    self.queue.push(Context::Term);
    self.wake().unwrap();
    handle.join().unwrap();
  }
}
