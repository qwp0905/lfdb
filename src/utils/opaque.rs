use std::io;

use crate::{Error, Result};

pub trait OpaqueRead {
  fn read_opaque(&mut self, buf: &mut [u8]) -> Result;
  fn read_array_opaque<const N: usize>(&mut self) -> Result<[u8; N]> {
    let mut buf = [0; N];
    self.read_opaque(&mut buf)?;
    Ok(buf)
  }
  fn read_to_vec_opaque(&mut self, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0; len];
    self.read_opaque(&mut buf)?;
    Ok(buf)
  }
}
impl<T: io::Read> OpaqueRead for T {
  fn read_opaque(&mut self, buf: &mut [u8]) -> Result {
    io::Read::read_exact(self, buf).map_err(Error::IO)
  }
}
pub trait OpaqueWrite {
  fn write_opaque(&mut self, buf: &[u8]) -> Result;
  fn flush_opaque(&mut self) -> Result;
}
impl<T: io::Write> OpaqueWrite for T {
  fn write_opaque(&mut self, buf: &[u8]) -> Result {
    io::Write::write_all(self, buf).map_err(Error::IO)
  }
  fn flush_opaque(&mut self) -> Result {
    io::Write::flush(self).map_err(Error::IO)
  }
}
