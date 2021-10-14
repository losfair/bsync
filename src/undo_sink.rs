use std::{
  fs::{File, OpenOptions},
  io::{BufWriter, Write},
};

use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};

use crate::{
  undo::{checksum_undo_data, format_lcn, CheckpointFile, UNDO_MAGIC},
  undo_source::UndoSource,
};

pub struct UndoSink {
  our_log_index: u32,
  checkpoint_file: CheckpointFile,
  backing: BufWriter<File>,
}

impl UndoSink {
  pub fn from_source(source: UndoSource) -> Result<Self> {
    // Create or truncate our new log file.
    let our_log_index = source.checkpoint_index.map(|x| x + 1).unwrap_or(0);
    let mut our_log_file = source.dir.clone();
    our_log_file.push(format_lcn(our_log_index));
    let log_writer = OpenOptions::new()
      .create_new(true)
      .read(true)
      .append(true)
      .open(&our_log_file)?;
    let mut backing = BufWriter::new(log_writer);
    backing.write_all(UNDO_MAGIC)?;
    backing.flush()?;
    Ok(Self {
      our_log_index,
      checkpoint_file: source.checkpoint_file,
      backing,
    })
  }

  pub fn commit(&mut self) -> Result<()> {
    self.backing.flush()?;
    self.backing.get_mut().sync_all()?;
    Ok(())
  }

  pub fn finalize(mut self) -> Result<()> {
    self.commit()?;
    self.checkpoint_file.write(self.our_log_index)?;
    Ok(())
  }

  pub fn write(&mut self, offset: u64, old_data: &[u8], new_data: &[u8]) -> Result<()> {
    if old_data == new_data {
      return Ok(());
    }

    let checksum = checksum_undo_data(offset, old_data, new_data);

    self.backing.write_all(&checksum)?;
    self.backing.write_u64::<LittleEndian>(offset)?;
    self
      .backing
      .write_u64::<LittleEndian>(old_data.len() as u64)?;
    self.backing.write_all(old_data)?;
    Ok(())
  }
}
