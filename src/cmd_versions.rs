use std::path::PathBuf;

use anyhow::Result;
use chrono::NaiveDateTime;
use prettytable::{cell, row, Table};
use structopt::StructOpt;

use crate::config::BackupConfig;

#[derive(StructOpt, Debug)]
pub struct VersionsCmd {
  config: PathBuf,
}

impl VersionsCmd {
  pub fn run(&self) -> Result<()> {
    let config = BackupConfig::must_load_from_file(&self.config);
    let log_store = config.local.open_managed_log(true)?;
    let versions = log_store.list_consistent_logs()?;

    // Add a row per time
    let mut table = Table::new();
    table.set_titles(row!["LCN", "CREATED"]);
    for v in &versions {
      let created_at = NaiveDateTime::from_timestamp(v.created_at as i64, 0);
      table.add_row(row![v.lcn, format!("{}", created_at)]);
    }
    table.print_tty(false);
    Ok(())
  }
}
