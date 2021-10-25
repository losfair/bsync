use serde::Serialize;
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

  /// Print in json.
  #[structopt(long)]
  json: bool,
}

#[derive(Serialize)]
struct OutputEntry {
  lsn: u64,
  created_at: u64,
  size: u64,
}

impl Listcmd {
  pub fn run(&self) -> Result<()> {
    let db = Database::open_file(&self.db, false)?;
    let cp_list = db.list_consistent_point();

    if self.json {
      let out: Vec<OutputEntry> = cp_list
        .iter()
        .map(|x| OutputEntry {
          lsn: x.lsn,
          created_at: x.created_at,
          size: x.size,
        })
        .collect();
      println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
      let mut table = Table::new();
      table.set_format(*prettytable::format::consts::FORMAT_CLEAN);
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
