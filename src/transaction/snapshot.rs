use crate::{
  blob::BlobMetadata,
  disk::{AppendIOHandle, ScanIOHandle},
  utils::{OffsetReader, OffsetWriter},
  wal::TxId,
  Error, Result,
};

#[derive(Debug, Clone, Copy)]
pub enum SnapshotFormatVersion {
  V0,
}
impl SnapshotFormatVersion {
  pub const CURRENT: Self = Self::V0;
  // pub const fn from_u16(version: u16) -> Option<Self> {
  //   match version {
  //     0 => Some(Self::V0),
  //     _ => None,
  //   }
  // }
  pub const fn as_u16(&self) -> u16 {
    match self {
      Self::V0 => 0,
    }
  }
}

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
    let mut active_versions = Vec::new();
    let mut aborted_versions = Vec::new();
    let mut blob_metadata = Vec::new();

    {
      let len = u32::from_le_bytes(file.read_array()?);
      for _ in 0..len {
        let id = TxId::from_le_bytes(file.read_array()?);
        active_versions.push(id);
      }
    }

    {
      let len = u32::from_le_bytes(file.read_array()?);
      for _ in 0..len {
        let id = TxId::from_le_bytes(file.read_array()?);
        aborted_versions.push(id);
      }
    }

    {
      let len = u32::from_le_bytes(file.read_array()?);
      for _ in 0..len {
        let byte_len = u32::from_le_bytes(file.read_array()?) as usize;
        let bytes = file.read_to_vec(byte_len)?;
        let Some(metadata) = BlobMetadata::read_from(&mut OffsetReader::new(&bytes))
        else {
          return Err(Error::InvalidFormat("blob metadata crashed."));
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

  pub fn write_at(&self, file: &mut AppendIOHandle) -> Result {
    {
      file.append(&(self.active_versions.len() as u32).to_le_bytes())?;
      for id in self.active_versions.iter() {
        file.append(&id.to_le_bytes())?;
      }
    }

    {
      file.append(&(self.aborted_versions.len() as u32).to_le_bytes())?;
      for id in self.aborted_versions.iter() {
        file.append(&id.to_le_bytes())?;
      }
    }

    {
      file.append(&(self.blob_metadata.len() as u32).to_le_bytes())?;
      for metadata in self.blob_metadata.iter() {
        let byte_len = metadata.byte_len();
        file.append(&(byte_len as u32).to_le_bytes())?;

        let mut bytes = vec![0; byte_len];
        metadata.write_at(&mut OffsetWriter::new(&mut bytes));
        file.append(&bytes)?;
      }
    }

    file.flush_all()?;
    Ok(())
  }
}
