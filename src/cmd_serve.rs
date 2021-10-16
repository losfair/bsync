use std::path::PathBuf;

use anyhow::Result;
use structopt::StructOpt;

use crate::{
  config::BackupConfig,
  rewind::{ImageRewindLogType, ImageRewindOptions, ImageRewinder},
};

#[derive(StructOpt, Debug)]
pub struct ServeCmd {
  /// The LCN to serve.
  #[structopt(long)]
  lcn: u64,

  config: PathBuf,
}

impl ServeCmd {
  pub fn run(&self) -> Result<()> {
    let config = BackupConfig::must_load_from_file(&self.config);
    let (image, log_store) = config.local.open_managed(true, None)?;
    let mut start_lcn = log_store.last_active_lcn()?;

    let incomplete_lcn = log_store.last_child(start_lcn)?;
    if incomplete_lcn != 0 {
      start_lcn = incomplete_lcn; // recovery
    }

    let path = log_store.lcn_backward_path(start_lcn, self.lcn)?;
    let path = match path {
      Some(x) => x,
      None => {
        log::error!("No path can reach LCN {} from LCN {}.", self.lcn, start_lcn);
        std::process::exit(1);
      }
    };

    if !log_store.lcn_is_consistent(self.lcn)? {
      log::error!("Target LCN {} is inconsistent.", self.lcn);
      std::process::exit(1);
    }

    let _rewinder = ImageRewinder::load(
      image.file().try_clone()?,
      (*log_store).clone(),
      path,
      ImageRewindOptions {
        allow_hash_mismatch_for_first_lcn: false,
        allow_idempotent_writes_for_first_lcn: true,
        log_type: ImageRewindLogType::Undo,
      },
    )?;

    Ok(())
  }
}
