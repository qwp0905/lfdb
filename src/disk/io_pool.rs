use std::{
  fs::{
    create_dir_all, exists, read_dir, remove_file, rename, DirEntry, File, OpenOptions,
  },
  io::{BufReader, BufWriter, Error as IoError, ErrorKind, IoSlice, Read, Write},
  mem::forget,
  path::{Path, PathBuf},
  sync::{Arc, Mutex},
};

use crossbeam::utils::Backoff;

use super::{
  create_io_thread, DirectIO, HandleState, IOThread, Pread, TaskPublisher, WriteTask,
};
use crate::{
  disk::Fallocate,
  metrics::MetricsRegistry,
  thread::{TaskHandle, WorkBuilder},
  utils::{SBox, ShortenedMutex, ToArc},
  Error, Result,
};

pub struct IOPool {
  thread: Arc<IOThread>,
  metrics: Arc<MetricsRegistry>,
  base_dir: SBox<DirHandle>,
}
impl IOPool {
  pub fn new(
    thread_count: usize,
    base_path: &Path,
    metrics: Arc<MetricsRegistry>,
  ) -> Result<Self> {
    let thread = WorkBuilder::new()
      .name("io pool")
      .multi(thread_count)
      .shared(create_io_thread(metrics.clone()))
      .to_arc();
    let base_dir = SBox::new(DirHandle::ensure(base_path, thread.clone())?);
    Ok(Self {
      thread,
      metrics,
      base_dir,
    })
  }

  pub fn open_append_io(&self, filename: PathBuf) -> Result<AppendIOHandle> {
    let path = self.base_dir.get_path().join(&filename);
    let file = OpenOptions::new()
      .write(true)
      .append(true)
      .create(true)
      .open(path)
      .map_err(Error::IO)?;
    Ok(AppendIOHandle {
      file: BufWriter::new(file),
      filename,
    })
  }
  pub fn open_scan_io(&self, filename: PathBuf) -> Result<ScanIOHandle> {
    let path = self.base_dir.get_path().join(&filename);
    let file = OpenOptions::new()
      .read(true)
      .open(path)
      .map_err(Error::IO)?;
    Ok(ScanIOHandle {
      file: BufReader::new(file),
    })
  }
  pub fn open_direct_io(&self, filename: PathBuf) -> Result<IOHandle> {
    // Direct IO bypasses the OS page cache for predictable latency.
    // To compensate for the lack of OS write buffering, writes are
    // accumulated and sorted in the eager_buffering layer, then
    // flushed as a single pwritev call per contiguous block.
    let path = self.base_dir.get_path().join(&filename);
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .direct_io(&path)
      .map_err(Error::IO)?;
    Ok(IOHandle {
      file: SBox::new(file),
      write_handle: SBox::new(TaskPublisher::new()),
      sync_handle: SBox::new(TaskPublisher::new()),
      state: SBox::new(HandleState::new()),
      thread: self.thread.clone(),
      metrics: self.metrics.clone(),
      base_dir: self.base_dir.clone(),
      filename: Mutex::new(filename),
    })
  }

  pub fn sync_dir(&self) -> Result {
    self.base_dir.fdatasync()
  }
  pub fn read_dir(&self) -> Result<Vec<DirEntry>> {
    self.base_dir.read()
  }
  pub fn remove(&self, filename: &Path) -> Result {
    self.base_dir.remove(filename)
  }
  pub fn exists(&self, filename: &Path) -> Result<bool> {
    self.base_dir.exists(filename)
  }

  pub fn close(&self) {
    self.thread.close();
  }
}

pub struct IOHandle {
  file: SBox<File>,
  write_handle: SBox<TaskPublisher<WriteTask>>,
  sync_handle: SBox<TaskPublisher<()>>,
  thread: Arc<IOThread>,
  state: SBox<HandleState>,
  metrics: Arc<MetricsRegistry>,
  base_dir: SBox<DirHandle>,
  filename: Mutex<PathBuf>,
}
impl IOHandle {
  pub fn read(&self, buf: &mut [u8], offset: u64) -> Result {
    // SAFETY: Since the removed table cannot access this path, a pin guarantee is not required.
    // If a path for read access to the removed table is established, pin guarantees are required.
    self
      .metrics
      .disk_read
      .measure(|| self.file.pread_exact(buf, offset))
      .map_err(Error::IO)
  }

  pub fn read_unchecked(&self, mut buf: &mut [u8], mut offset: u64) -> Result {
    let full = buf.len();
    while !buf.is_empty() {
      match self.file.pread(buf, offset) {
        Ok(0) if buf.len() == full => break, // allow only empty, not partial.
        Ok(0) => return Err(Error::IO(IoError::from(ErrorKind::UnexpectedEof))),
        Ok(n) => {
          let tmp = buf;
          buf = &mut tmp[n..];
          offset += n as u64;
        }
        Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
        Err(e) => return Err(Error::IO(e)),
      }
    }
    Ok(())
  }

  pub fn write_async(&self, buf: &'static [u8], offset: u64) -> TaskHandle<()> {
    self.write_handle.publish_write(
      &self.state,
      &*self.thread,
      &self.file,
      (offset, IoSlice::new(buf)),
    )
  }

  pub fn fdatasync(&self) -> TaskHandle<()> {
    self
      .sync_handle
      .publish_sync(&self.state, &*self.thread, &self.file)
  }

  pub fn fsync(&self) -> Result {
    let _token = match self.state.try_shared() {
      Some(token) => token,
      None => return Ok(()),
    };

    self.file.sync_all().map_err(Error::IO)
  }

  pub fn len(&self) -> Result<u64> {
    Ok(self.file.metadata().map_err(Error::IO)?.len())
  }

  pub fn truncate(&self) -> Result {
    let backoff = Backoff::new();
    while self.state.try_exclusive().map(forget).is_none() {
      backoff.snooze();
    }

    self.base_dir.remove(&*self.filename.l())
  }

  pub fn rename(&self, new_filename: PathBuf) -> Result {
    {
      let mut filename = self.filename.l();
      self.base_dir.rename(&*filename, &new_filename)?;
      *filename = new_filename;
    }
    Ok(())
  }

  pub fn fallocate(&self, offset: u64, len: u64) -> Result {
    self.file.fallocate(offset, len).map_err(Error::IO)
  }
  pub fn filename(&self) -> PathBuf {
    self.filename.l().clone()
  }
}

struct DirHandle {
  file: SBox<File>,
  sync_handle: SBox<TaskPublisher<()>>,
  thread: Arc<IOThread>,
  state: SBox<HandleState>,
  path: PathBuf,
}
impl DirHandle {
  fn ensure(path: &Path, thread: Arc<IOThread>) -> Result<Self> {
    let (file, path) = create_dir_all(path)
      .and_then(|_| path.canonicalize())
      .and_then(|path| File::open(&path).map(|f| (f, path)))
      .map_err(Error::IO)?;
    Ok(Self {
      file: SBox::new(file),
      sync_handle: SBox::new(TaskPublisher::new()),
      thread,
      state: SBox::new(HandleState::new()),
      path,
    })
  }
  fn fdatasync(&self) -> Result {
    self
      .sync_handle
      .publish_sync(&self.state, &*self.thread, &self.file)
      .wait()
  }
  fn get_path(&self) -> &Path {
    self.path.as_path()
  }
  fn read(&self) -> Result<Vec<DirEntry>> {
    let mut entries = Vec::new();

    for entry in read_dir(&self.path).map_err(Error::IO)? {
      let entry = entry.map_err(Error::IO)?;
      entries.push(entry);
    }

    Ok(entries)
  }
  fn remove(&self, filename: &Path) -> Result {
    remove_file(self.path.join(filename)).map_err(Error::IO)
  }
  fn exists(&self, filename: &Path) -> Result<bool> {
    exists(self.path.join(filename)).map_err(Error::IO)
  }
  fn rename(&self, from: &Path, to: &Path) -> Result {
    rename(self.path.join(from), self.path.join(to)).map_err(Error::IO)
  }
}

pub struct AppendIOHandle {
  file: BufWriter<File>,
  filename: PathBuf,
}
impl AppendIOHandle {
  pub fn append(&mut self, buf: &[u8]) -> Result {
    self.file.write_all(buf).map_err(Error::IO)
  }
  pub fn flush(mut self) -> Result<PathBuf> {
    self.file.flush().map_err(Error::IO)?;
    self.file.get_ref().sync_all().map_err(Error::IO)?;
    Ok(self.filename)
  }
}
pub struct ScanIOHandle {
  file: BufReader<File>,
}
impl ScanIOHandle {
  pub fn read(&mut self, bytes: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0; bytes];
    self.file.read_exact(&mut buf).map_err(Error::IO)?;
    Ok(buf)
  }
}
