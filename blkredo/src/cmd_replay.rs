use std::{
  fs::OpenOptions,
  io::{Seek, SeekFrom, Write},
  os::unix::prelude::FileTypeExt,
  path::{Path, PathBuf},
};

use anyhow::Result;
use structopt::StructOpt;
use thiserror::Error;

use crate::{
  blob::ZERO_BLOCK,
  config::LOG_BLOCK_SIZE,
  db::{ConsistentPoint, Database},
};

/// Replay
#[derive(Debug, StructOpt)]
pub struct Replaycmd {
  #[structopt(short, long)]
  output: PathBuf,

  /// The LSN to use.
  #[structopt(long)]
  lsn: u64,

  /// Path to the database.
  #[structopt(long)]
  db: PathBuf,
}

impl Replaycmd {
  pub fn run(&self) -> Result<()> {
    #[derive(Error, Debug)]
    enum E {
      #[error("the provided LSN is not a consistent point")]
      Inconsistent,
    }

    let db = Database::open_file(&self.db)?;
    let cp_list = db.list_consistent_point();
    let cp = match cp_list.iter().find(|x| x.lsn == self.lsn) {
      Some(x) => x,
      None => return Err(E::Inconsistent.into()),
    };
    write_snapshot(&db, cp, &self.output)?;
    Ok(())
  }
}

fn write_snapshot(db: &Database, cp: &ConsistentPoint, path: &Path) -> Result<()> {
  let snapshot = db.snapshot(cp.lsn)?;
  let mut output = OpenOptions::new()
    .create(true)
    .write(true)
    .truncate(true)
    .open(path)?;
  let output_md = output.metadata()?;
  let blkdev = output_md.file_type().is_block_device();
  let mut last_is_seek = false;
  for offset in (0usize..cp.size as usize).step_by(LOG_BLOCK_SIZE) {
    let write_len = (offset + LOG_BLOCK_SIZE)
      .min(cp.size as usize)
      .checked_sub(offset)
      .unwrap();
    assert!(write_len > 0);
    if let Some(block) = snapshot.read_block(offset as u64 / LOG_BLOCK_SIZE as u64) {
      assert_eq!(block.len(), LOG_BLOCK_SIZE);
      output.write_all(&block[..write_len])?;
      last_is_seek = false;
    } else if blkdev {
      output.write_all(&ZERO_BLOCK)?;
      last_is_seek = false;
    } else {
      output.seek(SeekFrom::Current(write_len as i64)).unwrap();
      last_is_seek = true;
    }
  }

  // It seems that seeking without writing doesn't enlarge the file
  if last_is_seek {
    output.seek(SeekFrom::Current(-1)).unwrap();
    output.write_all(&[0])?;
  }
  drop(output);
  println!("Image written to {}.", path.to_string_lossy());
  Ok(())
}
