use std::fs::File;

use anyhow::Result;

use crate::{
  rewind::{ImageRewindLogType, ImageRewindOptions, ImageRewinder},
  store::Store,
};

#[derive(Clone, Copy, Debug)]
pub struct IncompleteLogRecoveryOptions {
  pub force: bool,
  pub undo: bool,
}

pub fn recover_incomplete_logs(
  local_image: &File,
  log_store: &Store,
  opts: IncompleteLogRecoveryOptions,
) -> Result<()> {
  let last_active_lcn = log_store.last_active_lcn()?;

  // Revert partially committed data using the log.
  let lcn_to_revert = log_store.last_child(last_active_lcn)?;
  if lcn_to_revert != 0 {
    let rewinder = ImageRewinder::load(
      local_image.try_clone()?,
      (*log_store).clone(),
      vec![lcn_to_revert],
      ImageRewindOptions {
        allow_hash_mismatch_for_first_lcn: opts.force,
        allow_idempotent_writes_for_first_lcn: true,
        log_type: if opts.undo {
          ImageRewindLogType::Undo
        } else {
          ImageRewindLogType::Redo
        },
      },
    )?;

    rewinder.commit()?;

    // Mark that the LCN is committed to the image but is inconsistent.
    if !opts.undo {
      log_store.activate_lcn(lcn_to_revert, false)?;
    }

    log::info!(
      "Recovered incomplete logs (lcn {}, last active lcn {}). Strategy is {}.",
      lcn_to_revert,
      last_active_lcn,
      if opts.undo { "UNDO" } else { "REDO" },
    );
  }

  Ok(())
}
