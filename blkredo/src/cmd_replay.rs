use std::{
  fs::OpenOptions,
  io::{Seek, SeekFrom, Write},
  path::PathBuf,
};

use anyhow::Result;
use chrono::NaiveDateTime;
use prettytable::{cell, row, Table};
use structopt::StructOpt;
use thiserror::Error;

use crate::{config::LOG_BLOCK_SIZE, db::Database};

/// Incrementally pull updates of an image.
#[derive(Debug, StructOpt)]
pub struct Replaycmd {
  #[structopt(short, long)]
  output: Option<PathBuf>,

  /// The LSN to use.
  #[structopt(long)]
  lsn: Option<u64>,

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

    let db = Database::open_file(&self.db, true)?;
    let cp_list = db.list_consistent_point();

    if let Some(lsn) = self.lsn {
      let cp = match cp_list.iter().find(|x| x.lsn == lsn) {
        Some(x) => x,
        None => return Err(E::Inconsistent.into()),
      };
      let snapshot = db.snapshot(lsn)?;
      if let Some(output_path) = &self.output {
        let mut output = OpenOptions::new()
          .create(true)
          .write(true)
          .truncate(true)
          .open(&output_path)?;
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
        println!("Image written to {}.", output_path.to_string_lossy());
      }
    } else {
      let mut table = Table::new();
      table.set_titles(row!["LSN", "CREATED"]);
      for cp in &cp_list {
        let created_at = NaiveDateTime::from_timestamp(cp.created_at as i64, 0);
        table.add_row(row![cp.lsn, created_at]);
      }
      table.print_tty(false);
    }

    Ok(())
  }
}
