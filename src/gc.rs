use std::path::PathBuf;

use anyhow::Result;
use bloomfilter::Bloom;
use rusqlite::params;

use crate::{recover::IncompleteLogRecoveryOptions, store::Store};
use structopt::StructOpt;

use crate::config::BackupConfig;

#[derive(StructOpt, Debug)]
pub struct GcCmd {
  config: PathBuf,
}

impl GcCmd {
  pub fn run(&self) -> Result<()> {
    let config = BackupConfig::must_load_from_file(&self.config);
    let (_, log_store) = config.local.open_managed(
      false,
      Some(IncompleteLogRecoveryOptions {
        undo: true,
        force: false,
      }),
    )?;

    // We can safely remove inactive logs after recovery.
    remove_inactive_logs(&log_store)?;

    gc_cas(&log_store)?;

    println!("Success");
    Ok(())
  }
}

fn remove_inactive_logs(store: &Store) -> Result<()> {
  let db = store.db.lock();
  db.execute_batch(
    r#"
    delete from redo_v1 where not exists (select * from log_list_v1 where lcn = redo_v1.lcn and active = 1);
    delete from undo_v1 where not exists (select * from log_list_v1 where lcn = undo_v1.lcn and active = 1);
  "#,
  )?;
  Ok(())
}

fn gc_cas(store: &Store) -> Result<()> {
  let db = store.db.lock();
  let max_item_count: u64 = db.query_row("select count(*) from cas_v1", params![], |x| x.get(0))?;
  let mut filter: Bloom<[u8; 32]> = Bloom::new_for_fp_rate(max_item_count as usize, 0.01);
  log::debug!(
    "initialized bloom filter of {} bits with estimated item count of {}",
    filter.number_of_bits(),
    max_item_count
  );

  {
    let mut stmt = db.prepare(
      r#"
      select old_data_hash, new_data_hash from redo_v1
      union all
      select old_data_hash, new_data_hash from undo_v1
    "#,
    )?;
    let mut rows = stmt.query(params![])?;
    while let Some(row) = rows.next()? {
      let old_hash: Vec<u8> = row.get(0)?;
      let new_hash: Vec<u8> = row.get(1)?;
      filter.set(&(&old_hash[..]).try_into()?);
      filter.set(&(&new_hash[..]).try_into()?);
    }
  }

  let mut check_count: u64 = 0;
  let mut delete_count: u64 = 0;

  {
    let mut query_stmt = db.prepare("select `hash` from cas_v1")?;
    let mut delete_stmt = db.prepare("delete from cas_v1 where `hash` = ?")?;
    let mut rows = query_stmt.query(params![])?;
    while let Some(row) = rows.next()? {
      let hash: Vec<u8> = row.get(0)?;
      let hash: [u8; 32] = (&hash[..]).try_into()?;
      check_count += 1;
      if !filter.check(&hash) {
        // https://sqlite.org/isolation.html
        // > If an application issues a SELECT statement on a single table like "SELECT rowid, * FROM table WHERE ..."
        // > and starts stepping through the output of that statement using sqlite3_step() and examining each row, then
        // > it is safe for the application to delete the current row or any prior row using "DELETE FROM table WHERE rowid=?".
        delete_stmt.execute(params![&hash[..]])?;
        delete_count += 1;
      }
    }
  }

  log::info!(
    "Deleted {} unreferenced cas entries out of {}.",
    delete_count,
    check_count
  );

  Ok(())
}
