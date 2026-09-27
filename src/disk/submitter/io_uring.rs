use std::{
  fs::File,
  io::{Error, Read, Result, Write},
  os::fd::{AsRawFd, FromRawFd},
  sync::Arc,
  thread::Builder,
};

use crossbeam::queue::SegQueue;
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

fn shutdown_gracefully(
  submitter: Submitter,
  mut sq: SubmissionQueue,
  mut cq: CompletionQueue,
  mut backlog: ChunkQueue<squeue::Entry>,
  mut submitted: usize,
) {
  while !sq.is_empty() {
    if sq.is_full() {
      match submitter.submit() {
        Ok(_) => {}
        Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => continue,
        Err(err) => panic!("{err}"),
      }
    };

    sq.sync();
    if let Some(entry) = backlog.pop() {
      let _ = unsafe { sq.push(&entry) };
      submitted += 1;
    }

    for cqe in &mut cq {
      submitted -= 1;
      let ret = cqe.result();
      let user_data = cqe.user_data();
      if user_data == 0 {
        continue;
      }
      let ptr = (user_data as usize) as *mut OneshotBehavior<Result<usize>>;
      let done = unsafe { OneshotFulfill::from_raw(ptr) };
      done.fulfill(cvt(ret));
    }
  }

  submitter.submit_and_wait(submitted).unwrap();
  for cqe in cq {
    let ret = cqe.result();
    let user_data = cqe.user_data();
    if user_data == 0 {
      continue;
    }
    let ptr = (user_data as usize) as *mut OneshotBehavior<Result<usize>>;
    let done = unsafe { OneshotFulfill::from_raw(ptr) };
    done.fulfill(cvt(ret));
  }
}

const fn worker_loop(
  mut ring: IoUring,
  queue: Arc<SegQueue<Context>>,
  waker: Arc<File>,
) -> impl FnOnce() {
  move || {
    let mut backlog = ChunkQueue::new();
    let mut submitted = 0;
    let (submitter, mut sq, mut cq) = ring.split();
    let wake = opcode::PollAdd::new(types::Fd(waker.as_raw_fd()), libc::POLLIN as u32)
      .build()
      .user_data(0);
    let mut pending = false;
    loop {
      if !pending {
        if sq.is_full() {
          match submitter.submit() {
            Ok(_) => {}
            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => continue,
            Err(err) => panic!("{err}"),
          }
        };
        sq.sync();
        let _ = unsafe { sq.push(&wake) };
        submitted += 1;
        pending = true;
      }

      match submitter.submit_and_wait(1) {
        Ok(_) => {}
        Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => {}
        Err(err) => panic!("{err}"),
      }
      cq.sync();

      for cqe in &mut cq {
        submitted -= 1;
        let ret = cqe.result();
        let user_data = cqe.user_data();
        if user_data == 0 {
          let mut buf = [0; 8];
          (&*waker).read_exact(&mut buf).unwrap();
          pending = false;
          continue;
        }
        let ptr = (user_data as usize) as *mut OneshotBehavior<Result<usize>>;
        let done = unsafe { OneshotFulfill::from_raw(ptr) };
        done.fulfill(cvt(ret));
      }

      loop {
        if sq.is_full() {
          match submitter.submit() {
            Ok(_) => {}
            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => break,
            Err(err) => panic!("{err}"),
          }
        };
        sq.sync();
        match backlog.pop() {
          Some(sqe) => unsafe {
            let _ = sq.push(&sqe);
            submitted += 1;
          },
          None => break,
        }
      }

      while let Some(ctx) = queue.pop() {
        let task = match ctx {
          Context::Task(task) => task,
          Context::Term => {
            return shutdown_gracefully(submitter, sq, cq, backlog, submitted)
          }
        };
        let fd = types::Fd(task.toward.as_raw_fd());
        let mut entry = match task.task_type {
          TaskType::Pwrite { offset, buf } => {
            opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
              .offset(offset)
              .build()
          }
          TaskType::Pwritev { offset, bufs } => opcode::Writev::new(
            fd,
            bufs.as_ptr() as *const libc::iovec,
            bufs.len() as u32,
          )
          .offset(offset)
          .build(),
          TaskType::Fsync => opcode::Fsync::new(fd).build(),
          TaskType::Fdatasync => opcode::Fsync::new(fd)
            .flags(types::FsyncFlags::DATASYNC)
            .build(),
          TaskType::Fallocate { offset, len } => {
            opcode::Fallocate::new(fd, len).offset(offset).build()
          }
        };
        entry.set_user_data(OneshotFulfill::into_raw(task.done) as u64);
        if unsafe { sq.push(&entry).is_err() } {
          backlog.push(entry);
          continue;
        };
        submitted += 1;
      }
      sq.sync();
    }
  }
}

enum Context {
  Task(Task),
  Term,
}

pub struct IoSubmitter {
  queue: Arc<SegQueue<Context>>,
  waker: Arc<File>,
  slot: ThreadSlot,
}
impl IoSubmitter {
  pub fn new(entries: u32) -> Result<Self> {
    let ring = IoUring::new(entries)?;
    let queue = Arc::new(SegQueue::new());
    let waker_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if waker_fd < 0 {
      return Err(Error::last_os_error());
    }
    let waker = Arc::new(unsafe { File::from_raw_fd(waker_fd) });

    let handle = Builder::new()
      .name("io submitter".to_string())
      .stack_size(64 << 10)
      .spawn_unwind(worker_loop(ring, queue.clone(), waker.clone()));

    Ok(Self {
      queue,
      waker,
      slot: ThreadSlot::new(handle),
    })
  }

  pub fn submit(&self, task: Task) -> Result<()> {
    self.queue.push(Context::Task(task));
    self.wake()
  }

  fn wake(&self) -> Result<()> {
    (&*self.waker).write_all(&1u64.to_ne_bytes())?;
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
