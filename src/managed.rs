use std::{
  fs::{File, OpenOptions},
  io::{Seek, SeekFrom, Write},
  ops::Deref,
  path::Path,
  sync::Arc,
};

use anyhow::Result;
use fs2::FileExt;

use crate::store::Store;

pub struct ManagedImage {
  file: File,
}

impl ManagedImage {
  pub fn open(path: &Path, read_only: bool) -> Result<Self> {
    let f = OpenOptions::new()
      .create(!read_only)
      .read(true)
      .write(!read_only)
      .open(path)?;
    if read_only {
      f.try_lock_shared()?;
    } else {
      f.try_lock_exclusive()?;
    }
    Ok(Self { file: f })
  }

  pub fn file(&self) -> &File {
    &self.file
  }

  pub fn len(&self) -> Result<u64> {
    Ok(self.file.metadata()?.len())
  }

  pub fn extend_to(&mut self, len: u64) -> Result<()> {
    let current_len = self.len()?;
    assert!(current_len < len);
    self.file.seek(SeekFrom::Start(len - 1))?;
    self.file.write_all(&[0u8])?;
    Ok(())
  }
}

#[derive(Clone)]
pub struct ManagedStore {
  write_lock_file: Option<Arc<File>>,
  log_store: Store,
}

impl ManagedStore {
  pub fn open(dir: &Path, read_only: bool) -> Result<Self> {
    let write_lock_file: Option<Arc<File>>;

    if !read_only {
      std::fs::create_dir_all(dir)?;

      // Acquire exclusive write access.
      let mut write_lock_path = dir.to_path_buf();
      write_lock_path.push("unique_writer.lock");
      let f = OpenOptions::new()
        .create(!read_only)
        .read(true)
        .write(!read_only)
        .open(&write_lock_path)?;
      f.try_lock_exclusive()?;
      write_lock_file = Some(Arc::new(f));
    } else {
      write_lock_file = None;
    }

    // Open the database.
    let mut log_store_path = dir.to_path_buf();
    log_store_path.push("store.db");
    let log_store = Store::open_file(&log_store_path, read_only)?;

    Ok(Self {
      write_lock_file,
      log_store,
    })
  }
}

impl Deref for ManagedStore {
  type Target = Store;
  fn deref(&self) -> &Self::Target {
    &self.log_store
  }
}
