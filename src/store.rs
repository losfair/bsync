use std::{
  borrow::Cow,
  path::Path,
  sync::Arc,
  time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::params;

#[derive(Clone)]
pub struct Store {
  db: Arc<Mutex<rusqlite::Connection>>,
}

pub struct LogEntry<'a> {
  pub offset: u64,
  pub old_data: Cow<'a, [u8]>,
  pub new_data: Cow<'a, [u8]>,
}

pub struct LogEntryMetadata {
  pub offset: u64,
  pub old_data_hash: [u8; 32],
  pub new_data_hash: [u8; 32],
}

impl Store {
  pub fn open_file(path: &Path, read_only: bool) -> Result<Self> {
    let db = rusqlite::Connection::open_with_flags(
      path,
      if read_only {
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
      } else {
        Default::default()
      },
    )?;

    db.execute_batch(include_str!("./init.sql"))?;
    Ok(Self {
      db: Arc::new(Mutex::new(db)),
    })
  }

  pub fn must_read_cas(&self, hash: &[u8; 32]) -> Vec<u8> {
    self.read_cas(hash).expect("cas read failed")
  }

  fn read_cas(&self, hash: &[u8; 32]) -> Result<Vec<u8>> {
    Ok(
      self
        .db
        .lock()
        .prepare_cached("select content from cas_v1 where hash = ?")?
        .query_row(params![&hash[..]], |x| x.get(0))?,
    )
  }

  pub fn write_redo(&self, lcn: u64, batch: &[LogEntry]) -> Result<()> {
    self.write_log_generic("redo_v1", lcn, batch)
  }

  pub fn write_undo(&self, lcn: u64, batch: &[LogEntry]) -> Result<()> {
    self.write_log_generic("undo_v1", lcn, batch)
  }

  pub fn list_redo_for_lcn(&self, lcn: u64) -> Result<Vec<LogEntryMetadata>> {
    self.list_logs_for_lcn_generic("redo_v1", lcn)
  }

  pub fn list_undo_for_lcn(&self, lcn: u64) -> Result<Vec<LogEntryMetadata>> {
    self.list_logs_for_lcn_generic("undo_v1", lcn)
  }

  fn list_logs_for_lcn_generic(&self, log_table: &str, lcn: u64) -> Result<Vec<LogEntryMetadata>> {
    let db = self.db.lock();
    let mut stmt = db.prepare_cached(&format!(
      "select `offset`, `old_data_hash`, `new_data_hash` from {} where lcn = ?",
      log_table
    ))?;
    let mut rows = stmt.query(params![lcn])?;
    let mut result: Vec<LogEntryMetadata> = vec![];
    while let Some(row) = rows.next()? {
      let offset: u64 = row.get(0)?;
      let old_data_hash: Vec<u8> = row.get(1)?;
      let new_data_hash: Vec<u8> = row.get(2)?;
      result.push(LogEntryMetadata {
        offset,
        old_data_hash: (&old_data_hash[..]).try_into()?,
        new_data_hash: (&new_data_hash[..]).try_into()?,
      });
    }

    Ok(result)
  }

  pub fn activate_lcn(&self, lcn: u64) -> Result<()> {
    self
      .db
      .lock()
      .prepare_cached("update log_list_v1 set active = 1 where lcn = ?")?
      .execute(params![lcn])?;
    Ok(())
  }

  pub fn allocate_lcn(&self, link_lcn: u64) -> Result<u64> {
    let db = self.db.lock();
    let now_secs = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_secs();
    db.prepare_cached("insert into log_list_v1 (link, created_at) values(?, ?)")?
      .execute(params![link_lcn, now_secs])?;
    let rowid = db.last_insert_rowid();
    Ok(rowid as u64)
  }

  pub fn last_active_lcn(&self) -> Result<u64> {
    Ok(
      self
        .db
        .lock()
        .prepare_cached("select max(lcn) from log_list_v1 where active = 1")?
        .query_row::<Option<u64>, _, _>(params![], |row| row.get(0))?
        .unwrap_or(0),
    )
  }

  pub fn last_child(&self, lcn: u64) -> Result<u64> {
    Ok(
      self
        .db
        .lock()
        .prepare_cached("select max(lcn) from log_list_v1 where link = ?")?
        .query_row::<Option<u64>, _, _>(params![lcn], |row| row.get(0))?
        .unwrap_or(0),
    )
  }

  pub fn write_image_lcn(&self, image_hash: &[u8; 32], lcn: u64) -> Result<()> {
    self
      .db
      .lock()
      .prepare_cached("insert into image_lcn (lcn, image_hash) values(?, ?)")?
      .execute(params![lcn, &image_hash[..]])?;
    Ok(())
  }

  pub fn list_lcn_by_image(&self, image_hash: &[u8; 32]) -> Result<Vec<u64>> {
    let db = self.db.lock();
    let mut stmt = db.prepare_cached("select lcn from image_lcn where image_hash = ?")?;
    let mut rows = stmt.query(params![&image_hash[..]])?;
    let mut result: Vec<u64> = vec![];
    while let Some(row) = rows.next()? {
      result.push(row.get(0)?);
    }
    Ok(result)
  }

  fn write_log_generic(&self, log_table: &str, lcn: u64, batch: &[LogEntry]) -> Result<()> {
    let mut db = self.db.lock();
    let txn = db.transaction()?;
    {
      let mut cas_insert_stmt =
        txn.prepare_cached("insert or ignore into cas_v1 (`hash`, `content`) values(?, ?)")?;
      let mut insert_stmt = txn.prepare_cached(&format!(
        "insert into {} (`lcn`, `offset`, `old_data_hash`, `new_data_hash`) values(?, ?, ?, ?)",
        log_table
      ))?;
      for entry in batch {
        let old_data_hash: [u8; 32] = blake3::hash(&entry.old_data).into();
        let new_data_hash: [u8; 32] = blake3::hash(&entry.new_data).into();

        cas_insert_stmt.execute(params![&old_data_hash[..], &entry.old_data])?;
        cas_insert_stmt.execute(params![&new_data_hash[..], &entry.new_data])?;
        insert_stmt.execute(params![
          lcn,
          entry.offset,
          &old_data_hash[..],
          &new_data_hash[..]
        ])?;
      }
    }
    txn.commit()?;
    Ok(())
  }
}
