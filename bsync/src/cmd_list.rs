use std::path::PathBuf;

use anyhow::Result;
use chrono::NaiveDateTime;
use prettytable::{cell, row, Table};
use structopt::StructOpt;

use crate::db::Database;

/// List all consistent points.
#[derive(Debug, StructOpt)]
pub struct Listcmd {
  /// Path to the database.
  #[structopt(long)]
  db: PathBuf,
}

impl Listcmd {
  pub fn run(&self) -> Result<()> {
    let db = Database::open_file(&self.db, false)?;
    let cp_list = db.list_consistent_point();

    let mut table = Table::new();
    table.set_titles(row!["LSN", "CREATED"]);
    for cp in &cp_list {
      let created_at = NaiveDateTime::from_timestamp(cp.created_at as i64, 0);
      table.add_row(row![cp.lsn, created_at]);
    }
    table.print_tty(false);
    Ok(())
  }
}
