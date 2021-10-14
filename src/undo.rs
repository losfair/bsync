use std::{
  fs::{File, OpenOptions},
  io::{BufReader, ErrorKind, Read, Seek, SeekFrom, Write},
  path::Path,
};

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use fs2::FileExt;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{buffer_writer::BufferWriter, undo_sink::UndoSink};

pub const UNDO_MAGIC: &[u8] = b"BLKPATCH_UNDO_V1\0";

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

pub fn replay_undo_log(image: &mut [u8], log: &mut File, redo: &mut UndoSink) -> Result<bool> {
  const BUF_SIZE: usize = 128;

  #[derive(Error, Debug)]
  #[error("bad magic")]
  struct BadMagic;

  let mut log = BufReader::new(log);

  let mut magic: [u8; UNDO_MAGIC.len()] = [0; UNDO_MAGIC.len()];
  log.read_exact(&mut magic)?;
  if magic != UNDO_MAGIC {
    return Err(BadMagic.into());
  }

  let mut has_checksum_failure = false;
  let mut buf_writer = BufferWriter::new(image);

  loop {
    let mut checksum = [0u8; 32];
    match log.read_exact(&mut checksum) {
      Ok(_) => {}
      Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
      Err(e) => return Err(e.into()),
    }
    let offset = log.read_u64::<LittleEndian>()?;
    let size = log.read_u64::<LittleEndian>()?;
    let mut data = vec![0u8; size as usize];
    log.read_exact(&mut data)?;

    let sink = &buf_writer[offset as usize..(offset + size) as usize];
    if data == sink {
      continue;
    }

    let calculated_checksum = checksum_undo_data(offset, &data, sink);
    if calculated_checksum != checksum {
      log::error!("undo log entry at byte offset {} failed checksum validation. unverified offset = {}, unverified data len = {}", log.stream_position()?, offset, size);
      has_checksum_failure = true;
      continue;
    }

    redo.write(offset, sink, &data)?;
    buf_writer.push(offset as usize, data);
    if buf_writer.len() >= BUF_SIZE {
      redo.commit()?;
      buf_writer.flush();
    }
  }

  redo.commit()?;
  buf_writer.flush();
  Ok(has_checksum_failure)
}
