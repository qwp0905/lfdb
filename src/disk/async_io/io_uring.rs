use std::{
  fs::File,
  io::{Error, Read, Result, Write},
  os::fd::{AsRawFd, FromRawFd},
  sync::Arc,
  thread::{park, Builder, Thread},
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
  completion: &CompleteThread,
  mut maybe_waker: Option<&File>,
) -> (usize, bool) {
  cq.sync();
  let mut count = 0;
  let mut found = false;
  let input =
    cq.map(|cqe| (cqe.result(), cqe.user_data()))
      .filter_map(|(ret, user_data)| {
        count += 1;
        if user_data != POLL {
          let ptr = (user_data as usize) as *mut OneshotBehavior<Result<usize>>;
          return Some((cvt(ret), unsafe { OneshotFulfill::from_raw(ptr) }));
        }
        let waker = maybe_waker.as_mut()?;
        let mut buf = [0; 8];
        waker.read_exact(&mut buf).unwrap();
        found = true;
        None
      });
  completion.batch_dispatch(input);
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
  completion: &CompleteThread,
) {
  while !backlog.is_empty() {
    submitted += drain_backlog(&submitter, &mut sq, &mut backlog);
    let (count, _) = drain_completion(&mut cq, completion, None);
    submitted -= count;
  }

  while submitted > 0 {
    cq.sync();
    ignore_ebusy(submitter.submit_and_wait(submitted.min(cq.capacity())));
    let (count, _) = drain_completion(&mut cq, completion, None);
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

enum Context<T> {
  Task(T),
  Term,
}

const POLL: u64 = 0;

struct SubmitThread {
  queue: Arc<SegQueue<Context<Task>>>,
  waker: Arc<File>,
  slot: ThreadSlot,
}
impl SubmitThread {
  const fn worker_loop(
    mut ring: IoUring,
    queue: Arc<SegQueue<Context<Task>>>,
    completion: Arc<CompleteThread>,
    waker: Arc<File>,
  ) -> impl FnOnce() {
    move || {
      let backoff = Backoff::new();
      let mut backlog = ChunkQueue::new();
      let mut submitted = 0;
      let (submitter, mut sq, mut cq) = ring.split();
      let wake = opcode::PollAdd::new(types::Fd(waker.as_raw_fd()), libc::POLLIN as u32)
        .build()
        .user_data(POLL);
      let mut pending = false;

      loop {
        let (completed_count, found) =
          drain_completion(&mut cq, &completion, Some(&waker));
        submitted -= completed_count;
        pending &= !found;
        let backlog_count = drain_backlog(&submitter, &mut sq, &mut backlog);
        submitted += backlog_count;
        let (task_count, terminated) = drain_task(&mut sq, &queue, &mut backlog);
        submitted += task_count;
        if terminated {
          return shutdown_gracefully(submitter, sq, cq, backlog, submitted, &completion);
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
          if !sq.is_empty() {
            ignore_ebusy(submitter.submit());
          }
          backoff.reset();
          continue;
        }
        if !backoff.is_completed() {
          if !sq.is_empty() {
            ignore_ebusy(submitter.submit());
          }
          backoff.snooze();
          continue;
        }
        ignore_ebusy(submitter.submit_and_wait(1));
        backoff.reset();
      }
    }
  }

  fn new(entries: u32, completion: Arc<CompleteThread>) -> Result<Self> {
    let ring = IoUring::new(entries)?;
    let queue = Arc::new(SegQueue::new());
    let waker_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if waker_fd < 0 {
      return Err(Error::last_os_error());
    }
    let waker = Arc::new(unsafe { File::from_raw_fd(waker_fd) });
    let handle = Builder::new()
      .name("async io submit".to_string())
      .stack_size(64 << 10)
      .spawn_unwind(Self::worker_loop(
        ring,
        queue.clone(),
        completion,
        waker.clone(),
      ));

    Ok(Self {
      queue,
      waker,
      slot: ThreadSlot::new(handle),
    })
  }
  fn submit(&self, task: Task) -> Result<()> {
    self.queue.push(Context::Task(task));
    self.wake()
  }
  fn batch_submit(&self, mut tasks: impl Iterator<Item = Task>) -> Result<()> {
    let Some(task) = tasks.next() else {
      return Ok(());
    };
    self.queue.push(Context::Task(task));
    for task in tasks {
      self.queue.push(Context::Task(task));
    }
    self.wake()?;
    Ok(())
  }
  fn wake(&self) -> Result<()> {
    (&*self.waker).write_all(&1u64.to_ne_bytes())?;
    Ok(())
  }
  fn close(&self) {
    let Some(handle) = self.slot.close() else {
      return;
    };
    self.queue.push(Context::Term);
    self.wake().unwrap();
    handle.join().unwrap();
  }
}

type Completion = (Result<usize>, OneshotFulfill<Result<usize>>);
struct CompleteThread {
  queue: Arc<SegQueue<Context<Completion>>>,
  waker: Thread,
  slot: ThreadSlot,
}
impl CompleteThread {
  const fn worker_loop(queue: Arc<SegQueue<Context<Completion>>>) -> impl FnOnce() {
    move || {
      let backoff = Backoff::new();
      loop {
        let Some(ctx) = queue.pop() else {
          if !backoff.is_completed() {
            backoff.snooze();
            continue;
          }
          park();
          backoff.reset();
          continue;
        };
        match ctx {
          Context::Task((result, done)) => done.fulfill(result),
          Context::Term => break,
        };
        backoff.reset();
      }
    }
  }
  fn new() -> Self {
    let queue = Arc::new(SegQueue::new());
    let handle = Builder::new()
      .name("async io complete".to_string())
      .stack_size(64 << 10)
      .spawn_unwind(Self::worker_loop(queue.clone()));
    Self {
      queue,
      waker: handle.thread().clone(),
      slot: ThreadSlot::new(handle),
    }
  }

  fn batch_dispatch(
    &self,
    mut input: impl Iterator<Item = (Result<usize>, OneshotFulfill<Result<usize>>)>,
  ) {
    let Some(task) = input.next() else {
      return;
    };
    self.queue.push(Context::Task(task));
    for task in input {
      self.queue.push(Context::Task(task));
    }
    self.waker.unpark();
  }
  fn close(&self) {
    let Some(handle) = self.slot.close() else {
      return;
    };
    self.queue.push(Context::Term);
    self.waker.unpark();
    handle.join().unwrap();
  }
}

pub struct AsyncIO {
  submission: SubmitThread,
  completion: Arc<CompleteThread>,
}
impl AsyncIO {
  pub fn new(entries: u32) -> Result<Self> {
    let completion = Arc::new(CompleteThread::new());
    let submission = SubmitThread::new(entries, completion.clone())?;
    Ok(Self {
      submission,
      completion,
    })
  }

  pub fn submit(&self, task: Task) -> Result<()> {
    self.submission.submit(task)
  }

  pub fn batch_submit(&self, tasks: impl Iterator<Item = Task>) -> Result<()> {
    self.submission.batch_submit(tasks)
  }

  pub fn close(&self) {
    self.submission.close();
    self.completion.close();
  }
}
