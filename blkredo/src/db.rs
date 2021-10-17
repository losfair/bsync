use std::{
  path::Path,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use thiserror::Error;

use crate::util::align_block;

static SNAPSHOT_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct Database {
  db: Arc<Mutex<Connection>>,
}

#[derive(Clone)]
pub struct ConsistentPoint {
  pub lsn: u64,
  pub size: u64,
  pub created_at: u64,
}

impl Database {
  pub fn open_file(path: &Path, read_only: bool) -> Result<Self> {
    let db = Connection::open_with_flags(
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

  pub fn snapshot(&self, lsn: u64) -> Result<Snapshot> {
    let id = SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);
    let table_name = format!("snapshot_{}", id);
    let db = self.db.lock();
    let start = Instant::now();
    db.execute_batch(&format!(
      r#"
      create temp table {} (
        block_id integer not null primary key,
        hash blob not null
      );
      insert into temp.{} (block_id, hash)
      select block_id, hash from redo_v1
      where lsn in (
        select max(lsn) from redo_v1
        where lsn <= {}
        group by block_id
      );
    "#,
      table_name, table_name, lsn
    ))?;
    log::info!(
      "Materialized snapshot at LSN {} in {:?}.",
      lsn,
      start.elapsed()
    );
    Ok(Snapshot {
      db: self.clone(),
      table_name,
    })
  }

  pub fn write_redo<'a>(
    &self,
    base_lsn: u64,
    data: impl IntoIterator<Item = (u64, &'a [u8])>,
  ) -> Result<u64> {
    #[derive(Error, Debug)]
    #[error("base lsn mismatch: expecting {0}, got {1}")]
    struct LsnMismatch(u64, u64);

    let mut db = self.db.lock();
    let txn = db.transaction().unwrap();
    let max_lsn: Option<u64>;
    {
      let mut get_max_lsn_stmt = txn.prepare_cached("select max(lsn) from redo_v1").unwrap();
      let mut has_cas_stmt = txn
        .prepare_cached("select hash from cas_v1 where hash = ?")
        .unwrap();
      let mut insert_cas_stmt = txn
        .prepare_cached("insert into cas_v1 (hash, content) values(?, ?)")
        .unwrap();
      let mut insert_redo_stmt = txn
        .prepare_cached("insert into redo_v1 (block_id, hash) values(?, ?)")
        .unwrap();

      let prev_max_lsn: Option<u64> = get_max_lsn_stmt.query_row(params![], |r| r.get(0)).unwrap();
      let prev_max_lsn = prev_max_lsn.unwrap_or(0);
      if prev_max_lsn != base_lsn {
        return Err(LsnMismatch(base_lsn, prev_max_lsn).into());
      }

      for (block_id, content) in data {
        let content = align_block(content);
        let hash: [u8; 32] = blake3::hash(&content).into();
        let has_cas: Option<Vec<u8>> = has_cas_stmt
          .query_row(params![&hash[..]], |r| r.get(0))
          .optional()
          .unwrap();
        if has_cas.is_none() {
          insert_cas_stmt
            .execute(params![&hash[..], &content[..]])
            .unwrap();
        }
        insert_redo_stmt
          .execute(params![block_id, &hash[..]])
          .unwrap();
      }
      max_lsn = get_max_lsn_stmt
        .query_row(params![], |r| r.get(0))
        .optional()
        .unwrap();
    }
    txn.commit().unwrap();

    Ok(max_lsn.unwrap_or(0))
  }

  pub fn max_lsn(&self) -> u64 {
    let x: Option<u64> = self
      .db
      .lock()
      .prepare_cached("select max(lsn) from redo_v1")
      .unwrap()
      .query_row(params![], |r| r.get(0))
      .unwrap();
    x.unwrap_or(0)
  }

  pub fn list_consistent_point(&self) -> Vec<ConsistentPoint> {
    let db = self.db.lock();
    let mut stmt = db
      .prepare_cached("select lsn, size, created_at from consistent_point_v1 order by lsn asc")
      .unwrap();
    stmt
      .query_map(params![], |r| {
        Ok(ConsistentPoint {
          lsn: r.get(0)?,
          size: r.get(1)?,
          created_at: r.get(2)?,
        })
      })
      .unwrap()
      .collect::<Result<_, rusqlite::Error>>()
      .unwrap()
  }

  pub fn add_consistent_point(&self, lsn: u64, size: u64) {
    let db = self.db.lock();
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_secs();
    let mut stmt = db
      .prepare_cached("replace into consistent_point_v1 (lsn, size, created_at) values(?, ?, ?)")
      .unwrap();
    stmt.execute(params![lsn, size, now]).unwrap();
  }

  pub fn squash(&self, before_lsn: u64) {
    let mut db = self.db.lock();
    let txn = db.transaction().unwrap();
    txn.execute_batch(&format!(r#"
      delete from consistent_point_v1 where lsn < {};
      create temp table squash (
        `lsn` integer not null primary key
      );
      insert into temp.squash (lsn)
        select max(lsn) from redo_v1
          where lsn <= {}
          group by block_id;
      delete from redo_v1 where lsn <= {} and not exists (select * from temp.squash where lsn = redo_v1.lsn);
      drop table temp.squash;
    "#, before_lsn, before_lsn, before_lsn)).unwrap();
    txn.commit().unwrap();
  }

  pub fn cas_gc(&self) {
    let db = self.db.lock();
    db.execute_batch(
      r#"
      delete from cas_v1 where hash not in (select hash from redo_v1);
    "#,
    )
    .unwrap();
  }

  pub fn vacuum(&self) {
    self.db.lock().execute_batch("vacuum;").unwrap();
  }
}

pub struct Snapshot {
  db: Database,
  table_name: String,
}

impl Snapshot {
  pub fn read_block(&self, block_id: u64) -> Option<Vec<u8>> {
    let db = self.db.db.lock();
    let mut stmt = db
      .prepare_cached(&format!(
        r#"
      select content from cas_v1
      where hash = (select hash from temp.{} where block_id = ?)
    "#,
        self.table_name
      ))
      .unwrap();
    let content: Vec<u8> = stmt
      .query_row(params![block_id], |r| r.get(0))
      .optional()
      .unwrap()?;
    Some(content)
  }

  pub fn read_block_hash(&self, block_id: u64) -> Option<[u8; 32]> {
    let db = self.db.db.lock();
    let mut stmt = db
      .prepare_cached(&format!(
        "select hash from temp.{} where block_id = ?",
        self.table_name
      ))
      .unwrap();
    let hash: Vec<u8> = stmt
      .query_row(params![block_id], |r| r.get(0))
      .optional()
      .unwrap()?;
    Some(hash.try_into().unwrap())
  }
}

impl Drop for Snapshot {
  fn drop(&mut self) {
    self
      .db
      .db
      .lock()
      .execute_batch(&format!(
        r#"
      drop table temp.{};
    "#,
        &self.table_name
      ))
      .unwrap();
  }
}
