use crate::{
  blob::BlobMetadata,
  disk::{AppendIOHandle, ScanIOHandle},
  utils::{Encoding, OffsetReader, OffsetWriter, OpaqueRead, OpaqueWrite},
  wal::TxId,
  Error, Result,
};

const DEFAULT_ENCODING: Encoding = Encoding::Lz4;

pub struct CheckpointSnapshot {
  pub active_versions: Vec<TxId>,
  pub aborted_versions: Vec<TxId>,
  pub blob_metadata: Vec<BlobMetadata>,
}
impl CheckpointSnapshot {
  pub const fn new(
    active_versions: Vec<TxId>,
    aborted_versions: Vec<TxId>,
    blob_metadata: Vec<BlobMetadata>,
  ) -> Self {
    Self {
      active_versions,
      aborted_versions,
      blob_metadata,
    }
  }
  pub const fn empty() -> Self {
    Self {
      active_versions: Vec::new(),
      aborted_versions: Vec::new(),
      blob_metadata: Vec::new(),
    }
  }

  pub fn read_from(file: &mut ScanIOHandle) -> Result<Self> {
    {
      let mut decoder = DEFAULT_ENCODING.decoder(file);

      let mut active_versions = Vec::new();
      let mut aborted_versions = Vec::new();
      let mut blob_metadata = Vec::new();
      {
        let len = u32::from_le_bytes(decoder.read_array_opaque()?);
        for _ in 0..len {
          let id = TxId::from_le_bytes(decoder.read_array_opaque()?);
          active_versions.push(id);
        }
      }

      {
        let len = u32::from_le_bytes(decoder.read_array_opaque()?);
        for _ in 0..len {
          let id = TxId::from_le_bytes(decoder.read_array_opaque()?);
          aborted_versions.push(id);
        }
      }

      {
        let len = u32::from_le_bytes(decoder.read_array_opaque()?);
        for _ in 0..len {
          let byte_len = u32::from_le_bytes(decoder.read_array_opaque()?) as usize;
          let bytes = decoder.read_to_vec_opaque(byte_len)?;
          let Some(metadata) = BlobMetadata::read_from(&mut OffsetReader::new(&bytes))
          else {
            return Err(Error::InvalidFormat("checkpoint snapshot crashed."));
          };
          blob_metadata.push(metadata);
        }
      }

      Ok(Self {
        active_versions,
        aborted_versions,
        blob_metadata,
      })
    }
  }

  fn write_encoded(&self, file: &mut AppendIOHandle) -> Result<()> {
    let mut encoder = DEFAULT_ENCODING.encoder(file);
    {
      encoder.write_opaque(&(self.active_versions.len() as u32).to_le_bytes())?;
      for id in self.active_versions.iter() {
        encoder.write_opaque(&id.to_le_bytes())?;
      }
    }

    {
      encoder.write_opaque(&(self.aborted_versions.len() as u32).to_le_bytes())?;
      for id in self.aborted_versions.iter() {
        encoder.write_opaque(&id.to_le_bytes())?;
      }
    }

    {
      encoder.write_opaque(&(self.blob_metadata.len() as u32).to_le_bytes())?;
      for metadata in self.blob_metadata.iter() {
        let byte_len = metadata.byte_len();
        encoder.write_opaque(&(byte_len as u32).to_le_bytes())?;

        let mut bytes = vec![0; byte_len];
        metadata.write_at(&mut OffsetWriter::new(&mut bytes));
        encoder.write_opaque(&bytes)?;
      }
    }
    encoder.flush_opaque()?;

    Ok(())
  }

  pub fn write_at(&self, file: &mut AppendIOHandle) -> Result {
    self.write_encoded(file)?;
    file.flush_opaque()?;
    file.sync()?;
    Ok(())
  }
}
