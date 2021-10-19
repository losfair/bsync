use std::path::PathBuf;

use anyhow::Result;
use structopt::StructOpt;
use thiserror::Error;

use crate::db::Database;

/// Squash logs.
#[derive(Debug, StructOpt)]
pub struct SquashCmd {
  /// Start LSN.
  #[structopt(long)]
  start_lsn: u64,

  /// End LSN.
  #[structopt(long)]
  end_lsn: u64,

  /// Data loss confirmation.
  #[structopt(long)]
  data_loss: bool,

  /// Vacuum the database after squash.
  #[structopt(long)]
  vacuum: bool,

  /// Path to the database.
  #[structopt(long)]
  db: PathBuf,
}

impl SquashCmd {
  pub fn run(&self) -> Result<()> {
    #[derive(Error, Debug)]
    enum E {
      #[error("the provided `start_lsn` is not a consistent point")]
      InconsistentStart,

      #[error("the provided `end_lsn` is not a consistent point")]
      InconsistentEnd,

      #[error("squash removes history - please confirm by adding the flag `--data-loss`.")]
      DataLoss,
    }

    let db = Database::open_file(&self.db, false)?;
    let cp_list = db.list_consistent_point();
    if self.start_lsn != 0 {
      match cp_list.iter().find(|x| x.lsn == self.start_lsn) {
        Some(_) => {}
        None => return Err(E::InconsistentStart.into()),
      }
    }
    match cp_list.iter().find(|x| x.lsn == self.end_lsn) {
      Some(_) => {}
      None => return Err(E::InconsistentEnd.into()),
    };

    if !self.data_loss {
      return Err(E::DataLoss.into());
    }

    db.squash(self.start_lsn, self.end_lsn)?;
    db.cas_gc();
    if self.vacuum {
      db.vacuum();
    }
    println!("Success.");

    Ok(())
  }
}
