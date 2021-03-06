use serde::Deserialize;
use std::path::Path;

pub const LOG_BLOCK_SIZE: usize = 262144;

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

  /// Host verification method.
  #[serde(default)]
  pub verify: HostVerification,

  /// Scripts.
  pub scripts: Option<BackupRemoteScripts>,
}

#[derive(Deserialize)]
pub struct BackupRemoteScripts {
  pub no_pull_lock: Option<bool>,
  pub pre_pull: Option<String>,
  pub post_pull: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum HostVerification {
  Insecure,
  Known,
  Dnssec,
}

impl Default for HostVerification {
  fn default() -> Self {
    Self::Known
  }
}

#[derive(Deserialize)]
pub struct BackupLocalConfig {
  /// Local database path.
  pub db: String,

  /// Local pull lock path.
  pub pull_lock: Option<String>,
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
