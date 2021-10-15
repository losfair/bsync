use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::Deserialize;

use crate::managed::{ManagedImage, ManagedStore};

pub const LOG_BLOCK_SIZE: u64 = 262144;

#[derive(Deserialize)]
pub struct BackupConfig {
  pub remote: BackupRemoteConfig,
  pub local: BackupLocalConfig,
}

#[derive(Deserialize)]
pub struct BackupRemoteConfig {
  /// Remote address.
  pub server: String,

  /// SSH port number. Defaults to 22.
  pub port: Option<u16>,

  /// SSH username.
  pub user: String,

  /// Path to SSH private key. Agent auth is used if this is empty.
  pub key: Option<String>,

  /// Remote image path.
  pub image: String,
}

#[derive(Deserialize)]
pub struct BackupLocalConfig {
  pub image: String,
  pub log: Option<String>,
}

impl BackupConfig {
  pub fn must_load_from_file(path: &Path) -> Self {
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
      log::error!(
        "cannot open backup config at {}: {}",
        path.to_string_lossy(),
        e
      );
      std::process::exit(1);
    });
    serde_yaml::from_str(&text).unwrap_or_else(|e| {
      log::error!(
        "cannot parse backup config at {}: {}",
        path.to_string_lossy(),
        e
      );
      std::process::exit(1);
    })
  }
}

impl BackupLocalConfig {
  pub fn open_managed(&self, read_only: bool) -> Result<(ManagedImage, ManagedStore)> {
    let image = ManagedImage::open(Path::new(&self.image), read_only)?;

    let log_dir_path = self
      .log
      .as_ref()
      .map(|x| PathBuf::from(&x))
      .unwrap_or_else(|| {
        let mut p = PathBuf::from(&self.image);
        p.pop();
        p.push("log");
        p
      });
    let store = ManagedStore::open(&log_dir_path, read_only)?;
    Ok((image, store))
  }
}
