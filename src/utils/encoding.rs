use std::io;

pub struct DecompressError;

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum Encoding {
  Raw = 1u8,
  Lz4 = 2u8,
  Zstd = 3u8,
}
impl Encoding {
  pub const fn from_byte(byte: u8) -> Option<Self> {
    match byte {
      1 => Some(Self::Raw),
      2 => Some(Self::Lz4),
      3 => Some(Self::Zstd),
      _ => None,
    }
  }
  pub fn encoder<W: io::Write>(&self, writer: W) -> Encoder<W> {
    match self {
      Encoding::Raw => Encoder::Raw(writer),
      Encoding::Lz4 => Encoder::Lz4(lz4_flex::frame::FrameEncoder::new(writer)),
      Encoding::Zstd => Encoder::Zstd(zstd::stream::Encoder::new(writer, 1).unwrap()),
    }
  }
  pub fn decoder<R: io::Read>(&self, reader: R) -> Decoder<R> {
    match self {
      Encoding::Raw => Decoder::Raw(reader),
      Encoding::Lz4 => Decoder::Lz4(lz4_flex::frame::FrameDecoder::new(reader)),
      Encoding::Zstd => Decoder::Zstd(zstd::stream::Decoder::new(reader).unwrap()),
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

pub enum Encoder<W: io::Write> {
  Raw(W),
  Lz4(lz4_flex::frame::FrameEncoder<W>),
  Zstd(zstd::stream::Encoder<'static, W>),
}
impl<W: io::Write> io::Write for Encoder<W> {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    match self {
      Encoder::Raw(writer) => writer.write(buf),
      Encoder::Lz4(encoder) => encoder.write(buf),
      Encoder::Zstd(encoder) => encoder.write(buf),
    }
  }

  fn flush(&mut self) -> io::Result<()> {
    match self {
      Encoder::Raw(writer) => writer.flush(),
      Encoder::Lz4(encoder) => encoder.flush(),
      Encoder::Zstd(encoder) => encoder.flush(),
    }
  }
}
pub enum Decoder<R: io::Read> {
  Raw(R),
  Lz4(lz4_flex::frame::FrameDecoder<R>),
  Zstd(zstd::stream::Decoder<'static, io::BufReader<R>>),
}
impl<R: io::Read> io::Read for Decoder<R> {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    match self {
      Decoder::Raw(reader) => reader.read(buf),
      Decoder::Lz4(decoder) => decoder.read(buf),
      Decoder::Zstd(decoder) => decoder.read(buf),
    }
  }
}
