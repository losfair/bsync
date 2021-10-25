use std::{
  convert::TryInto,
  path::Path,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use thiserror::Error;

use crate::{blob::ZERO_BLOCK_HASH, util::align_block};

macro_rules! migration {
  ($id:ident, $($version:expr,)*) => {
    static $id: &'static [(&'static str, &'static str)] = &[
      $(($version, include_str!(concat!("./migration/", $version, ".sql"))),)*
    ];
  };
}

migration!(VERSIONS, "000001", "000002", "000003",);

static SNAPSHOT_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct Database {
  db: Arc<Mutex<Connection>>,
  instance_id: Arc<str>,
}

#[derive(Clone)]
pub struct ConsistentPoint {
  pub lsn: u64,
  pub size: u64,
  pub created_at: u64,
}

pub enum RedoContentOrHash<'a> {
  Content(&'a [u8]),
  Hash([u8; 32]),
}

impl Database {
  pub fn open_file(path: &Path, create: bool) -> Result<Self> {
    #[derive(Error, Debug)]
    #[error("migration failed: {0}")]
    struct MigrationError(anyhow::Error);

    let mut flags: OpenFlags = OpenFlags::SQLITE_OPEN_READ_WRITE;
    if create {
      flags |= OpenFlags::SQLITE_OPEN_CREATE;
    }

    let mut db = Connection::open_with_flags(path, flags)?;

    db.execute_batch(
      r#"
      pragma journal_mode = wal;
    "#,
    )?;
    db.busy_handler(Some(|i| {
      log::debug!("Waiting for lock on database (attempt {})", i);
      std::thread::sleep(Duration::from_millis(100));
      true
    }))?;

    run_migration(&mut db).map_err(MigrationError)?;

    let instance_id: String = db
      .query_row(
        "select v from bsync_config where k = 'instance_id'",
        params![],
        |r| r.get(0),
      )
      .expect("missing instance_id in bsync_config");
    log::info!(
      "Opened database at {:?} with instance id {}.",
      path,
      instance_id
    );
    Ok(Self {
      db: Arc::new(Mutex::new(db)),
      instance_id: Arc::from(instance_id.as_str()),
    })
  }

  pub fn instance_id(&self) -> &str {
    &*self.instance_id
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
    data: impl IntoIterator<Item = (u64, RedoContentOrHash<'a>)>,
  ) -> Result<u64> {
    #[derive(Error, Debug)]
    #[error("base lsn mismatch: expecting {0}, got {1}")]
    struct LsnMismatch(u64, u64);

    #[derive(Error, Debug)]
    #[error("block with hash {0} was assumed to exist in CAS but does not exist anymore - did you run `bsync squash` just now? please retry.")]
    struct MissingHash(String);

    let mut db = self.db.lock();
    let txn = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let max_lsn: Option<u64>;
    {
      let mut get_max_lsn_stmt = txn.prepare_cached("select max(lsn) from redo_v1").unwrap();
      let mut has_cas_stmt = txn
        .prepare_cached("select hash from cas_v1 where hash = ?")
        .unwrap();
      let mut insert_cas_compressed_stmt = txn
        .prepare_cached("insert into cas_v1 (hash, content, compressed) values(?, ?, 1)")
        .unwrap();
      let mut insert_redo_stmt = txn
        .prepare_cached("insert into redo_v1 (block_id, hash) values(?, ?)")
        .unwrap();

      let prev_max_lsn: Option<u64> = get_max_lsn_stmt.query_row(params![], |r| r.get(0)).unwrap();
      let prev_max_lsn = prev_max_lsn.unwrap_or(0);
      if prev_max_lsn != base_lsn {
        return Err(LsnMismatch(base_lsn, prev_max_lsn).into());
      }

      for (block_id, body) in data {
        let hash: [u8; 32] = match body {
          RedoContentOrHash::Content(x) => blake3::hash(x).into(),
          RedoContentOrHash::Hash(x) => x,
        };
        let has_cas: Option<Vec<u8>> = has_cas_stmt
          .query_row(params![&hash[..]], |r| r.get(0))
          .optional()
          .unwrap();
        if has_cas.is_none() {
          match body {
            RedoContentOrHash::Content(content) => {
              let content = align_block(content);
              let content = zstd::encode_all(&*content, 3)?;
              insert_cas_compressed_stmt
                .execute(params![&hash[..], &content[..]])
                .unwrap();
            }
            RedoContentOrHash::Hash(_) => return Err(MissingHash(hex::encode(&hash)).into()),
          }
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

  pub fn exists_in_cas(&self, hash: &[u8; 32]) -> bool {
    let v: Option<u32> = self
      .db
      .lock()
      .query_row(
        "select 1 from cas_v1 where hash = ?",
        params![&hash[..]],
        |r| r.get(0),
      )
      .optional()
      .unwrap();
    v.is_some()
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
    // XXX: This doesn't update the size if the block device is extended with zeros.
    let mut stmt = db
      .prepare_cached(
        "insert or ignore into consistent_point_v1 (lsn, size, created_at) values(?, ?, ?)",
      )
      .unwrap();
    stmt.execute(params![lsn, size, now]).unwrap();
  }

  pub fn squash(&self, start_lsn: u64, end_lsn: u64) -> Result<()> {
    let mut db = self.db.lock();
    let txn = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    txn.execute_batch(&format!(r#"
      delete from consistent_point_v1 where lsn > {from} and lsn < {to};
      create temp table squash (
        `lsn` integer not null primary key
      );
      insert into temp.squash (lsn)
        select max(lsn) from redo_v1
          where lsn > {from} and lsn <= {to}
          group by block_id;
      delete from redo_v1 where lsn > {from} and lsn <= {to} and not exists (select * from temp.squash where lsn = redo_v1.lsn);
      drop table temp.squash;
    "#, from = start_lsn, to = end_lsn)).unwrap();
    txn.commit().unwrap();
    Ok(())
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
    let hash = self.read_block_hash(block_id)?;
    if hash == *ZERO_BLOCK_HASH {
      return None;
    }

    let db = self.db.db.lock();
    let mut stmt = db
      .prepare_cached(
        r#"
        select content, compressed from cas_v1 where hash = ?
      "#,
      )
      .unwrap();
    let (content, compressed): (Vec<u8>, bool) = stmt
      .query_row(params![&hash[..]], |r| Ok((r.get(0)?, r.get(1)?)))
      .optional()
      .unwrap()?;
    if compressed {
      let content = zstd::decode_all(&content[..]).expect("read_block: decompression failed");
      Some(content)
    } else {
      Some(content)
    }
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

fn run_migration(db: &mut Connection) -> Result<()> {
  #[derive(Error, Debug)]
  #[error("database schema version is newer than the supported version")]
  struct SchemaTooNew;

  let txn = db.transaction_with_behavior(TransactionBehavior::Immediate)?;

  let table_exists: u32 = txn.query_row(
    "select count(*) from sqlite_master where type='table' and name='bsync_config'",
    params![],
    |r| r.get(0),
  )?;
  let current_version: Option<String> = if table_exists == 1 {
    Some(txn.query_row(
      "select v from bsync_config where k = 'schema_version'",
      params![],
      |r| r.get(0),
    )?)
  } else {
    None
  };
  let current_version: u64 = current_version.map(|x| x.parse()).transpose()?.unwrap_or(0);
  let latest_version: u64 = VERSIONS.last().unwrap().0.parse().unwrap();
  if current_version > latest_version {
    return Err(SchemaTooNew.into());
  }
  for &(version, sql) in VERSIONS {
    let version: u64 = version.parse().unwrap();
    if version > current_version {
      txn.execute_batch(sql)?;
      log::info!("Applied migration {}.", version);
    }
  }
  txn.execute(
    "replace into bsync_config (k, v) values('schema_version', ?)",
    params![format!("{}", latest_version)],
  )?;
  txn.commit()?;
  Ok(())
}
