use std::path::PathBuf;

use anyhow::Result;
use itertools::Itertools;
use memmap2::Mmap;
use structopt::StructOpt;

use crate::config::BackupConfig;

#[derive(StructOpt, Debug)]
pub struct TravelCmd {
  #[structopt(subcommand)]
  subcmd: Subcmd,
}

#[derive(StructOpt, Debug)]
enum Subcmd {
  Backward(TravelOpts),
  Forward(TravelOpts),
}

#[derive(StructOpt, Debug)]
struct TravelOpts {
  config: PathBuf,

  /// Select a start LCN.
  #[structopt(long)]
  image_lcn: Option<u64>,
}

impl TravelCmd {
  pub fn run(&self) -> Result<()> {
    match &self.subcmd {
      Subcmd::Backward(opts) => opts.run(false),
      Subcmd::Forward(opts) => opts.run(true),
    }
  }
}

impl TravelOpts {
  fn run(&self, forward: bool) -> Result<()> {
    let config = BackupConfig::must_load_from_file(&self.config);
    let (managed_image, log_store) = config.local.open_managed(true)?;

    let image_hash: [u8; 32] = blake3::hash(&unsafe { Mmap::map(managed_image.file())? }).into();
    let lcn_list = log_store.list_lcn_by_image(&image_hash)?;
    if lcn_list.is_empty() {
      log::error!("No LCN found for this image");
      std::process::exit(1);
    }

    let lcn = if lcn_list.len() > 1 {
      if self.image_lcn.is_some() && lcn_list.contains(&self.image_lcn.unwrap()) {
        self.image_lcn.unwrap()
      } else {
        log::error!("Multiple LCNs found for this image: {}. Please select one using `--image-lcn [your_lcn]`.", lcn_list.iter().map(|x| x.to_string()).join(", "));
        std::process::exit(1);
      }
    } else {
      lcn_list[0]
    };

    log::info!("Image is at LCN {}.", lcn);

    //let map = ImageRewinder::load(image, store, LOG_BLOCK_SIZE, lcn_list, opts)
    Ok(())
  }
}
