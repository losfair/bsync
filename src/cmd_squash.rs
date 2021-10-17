use std::path::PathBuf;

use anyhow::Result;
use structopt::StructOpt;
use thiserror::Error;

use crate::db::Database;

/// Incrementally pull updates of an image.
#[derive(Debug, StructOpt)]
pub struct SquashCmd {
  /// The LSN before which all logs will be squashed.
  #[structopt(long)]
  before_lsn: u64,

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
      #[error("the provided LSN is not a consistent point")]
      Inconsistent,

      #[error("squash removes history - please confirm by adding the flag `--data-loss`.")]
      DataLoss,
    }

    let db = Database::open_file(&self.db, false)?;
    let cp_list = db.list_consistent_point();
    match cp_list.iter().find(|x| x.lsn == self.before_lsn) {
      Some(_) => {}
      None => return Err(E::Inconsistent.into()),
    };
    if !self.data_loss {
      return Err(E::DataLoss.into());
    }

    db.squash(self.before_lsn);
    db.cas_gc();
    if self.vacuum {
      db.vacuum();
    }
    println!("Success.");

    Ok(())
  }
}
