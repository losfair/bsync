use std::{borrow::Cow, collections::HashMap};

use anyhow::Result;

use crate::{config::LOG_BLOCK_SIZE, rewind::ImageRewinder};

pub struct OverlayBlkdev {
  overlay: HashMap<u64, Vec<u8>>,
  rewinder: ImageRewinder,
}

impl OverlayBlkdev {
  pub fn new(rewinder: ImageRewinder) -> Result<Self> {
    Ok(Self {
      overlay: HashMap::new(),
      rewinder,
    })
  }

  fn read_block_aligned(&self, blkid: u64) -> Cow<[u8]> {
    if let Some(x) = self.overlay.get(&blkid) {
      Cow::Borrowed(x.as_slice())
    } else {
      self
        .rewinder
        .read_block_aligned(blkid)
        .expect("read_block returned nothing")
    }
  }

  pub fn read_at(&mut self, start_pos: usize, mut data: &mut [u8]) -> Result<()> {
    todo!()
  }
}
