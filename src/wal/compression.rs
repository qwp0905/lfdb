pub struct DecompressError;

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum RecordEncoding {
  Raw = 1u8,
  Lz4 = 2u8,
  Zstd = 3u8,
}
impl RecordEncoding {
  pub const fn from_byte(byte: u8) -> Option<Self> {
    match byte {
      1 => Some(Self::Raw),
      2 => Some(Self::Lz4),
      3 => Some(Self::Zstd),
      _ => None,
    }
  }
  pub fn compress(&self, data: &[u8]) -> Vec<u8> {
    match self {
      Self::Raw => data.to_vec(),
      Self::Lz4 => lz4_flex::compress(data),
      Self::Zstd => zstd::bulk::compress(data, 1).unwrap(),
    }
  }

  pub fn decompress(
    &self,
    data: &[u8],
    original_len: usize,
  ) -> std::result::Result<Vec<u8>, DecompressError> {
    let decoded = match self {
      Self::Raw => Ok(data.to_vec()),
      Self::Lz4 => lz4_flex::decompress(data, original_len).map_err(|_| DecompressError),
      Self::Zstd => {
        zstd::bulk::decompress(data, original_len).map_err(|_| DecompressError)
      }
    }?;
    if decoded.len() != original_len {
      return Err(DecompressError);
    }
    Ok(decoded)
  }
}
