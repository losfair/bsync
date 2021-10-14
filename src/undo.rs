use std::{
  fs::{File, OpenOptions},
  io::{Read, Seek, SeekFrom, Write},
  path::Path,
};

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use fs2::FileExt;
use sha2::{Digest, Sha256};

pub const UNDO_MAGIC: &[u8] = b"BLKPATCH_UNDO_V1\0";
pub const UNDO_BLOCK_SIZE: u64 = 65536;

/// Compute the SHA256 checksum of offset + data.
pub fn checksum_undo_data(offset: u64, old_data: &[u8], new_data: &[u8]) -> [u8; 32] {
  assert_eq!(old_data.len(), new_data.len());

  let mut offset_bytes: [u8; 8] = [0; 8];
  LittleEndian::write_u64(&mut offset_bytes, offset);

  let mut hasher = Sha256::new();
  hasher.update(&offset_bytes);
  hasher.update(old_data);
  hasher.update(new_data);
  let result = hasher.finalize();
  result.into()
}

/// Format a Log Chunk Number (LCN).
pub fn format_lcn(x: u32) -> String {
  if x >= 100000000 {
    panic!("lcn too large");
  }

  format!("{:0>8}", x)
}

pub struct CheckpointFile {
  backing: File,
}

impl CheckpointFile {
  pub fn open(dir: &Path) -> Result<Self> {
    std::fs::create_dir_all(&dir)?;

    let mut checkpoint_path = dir.to_path_buf();
    checkpoint_path.push("checkpoint");

    // Take a lock on the checkpoint file.
    let checkpoint_file = OpenOptions::new()
      .create(true)
      .read(true)
      .write(true)
      .open(&checkpoint_path)?;
    checkpoint_file.try_lock_exclusive()?;
    Ok(Self {
      backing: checkpoint_file,
    })
  }

  pub fn read(&mut self) -> Result<Option<u32>> {
    let mut ckpt_index = String::new();
    self.backing.read_to_string(&mut ckpt_index)?;
    let ckpt_index: Option<u32> = if !ckpt_index.is_empty() {
      Some(ckpt_index.trim().parse()?)
    } else {
      None
    };
    Ok(ckpt_index)
  }

  pub fn write(&mut self, index: u32) -> Result<()> {
    self.backing.set_len(0)?;
    self.backing.seek(SeekFrom::Start(0))?;
    self.backing.write_all(format_lcn(index).as_bytes())?;
    Ok(())
  }
}
