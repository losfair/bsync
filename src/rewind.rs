use std::{borrow::Cow, fmt::Display, fs::File};

use anyhow::Result;
use fs2::FileExt;
use memmap2::{Mmap, MmapMut};
use thiserror::Error;

use crate::{
  config::LOG_BLOCK_SIZE,
  signals::CRITICAL_WRITE_LOCK,
  store::Store,
  util::{align_block, div_round_up},
};

pub struct ImageRewinder {
  base_file: File,
  base_map: Mmap,
  store: Store,
  block_mappings: Vec<Option<BlockMapping>>,
}

#[derive(Clone)]
struct BlockMapping {
  hash: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct ImageRewindOptions {
  pub allow_hash_mismatch_for_first_lcn: bool,
  pub allow_idempotent_writes_for_first_lcn: bool,
  pub log_type: ImageRewindLogType,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ImageRewindLogType {
  Redo,
  Undo,
}

impl Display for ImageRewindLogType {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{}",
      match self {
        Self::Redo => "redo",
        Self::Undo => "undo",
      }
    )
  }
}

impl ImageRewinder {
  pub fn load(
    base_file: File,
    store: Store,
    lcn_list: Vec<u64>,
    opts: ImageRewindOptions,
  ) -> Result<Self> {
    #[derive(Error, Debug)]
    #[error("hash mismatch")]
    struct HashMismatch;

    base_file.try_lock_exclusive()?;
    let base_map = unsafe { Mmap::map(&base_file)? };
    let block_mappings: Vec<Option<BlockMapping>> =
      vec![None; div_round_up(base_map.len() as u64, LOG_BLOCK_SIZE) as usize];

    let mut me = Self {
      base_file,
      base_map,
      store,
      block_mappings,
    };

    let mut allow_hash_mismatch = opts.allow_hash_mismatch_for_first_lcn;
    let mut allow_idempotent_writes = opts.allow_idempotent_writes_for_first_lcn;

    for lcn in lcn_list {
      let logs = match opts.log_type {
        ImageRewindLogType::Undo => me.store.list_undo_for_lcn(lcn)?,
        ImageRewindLogType::Redo => me.store.list_redo_for_lcn(lcn)?,
      };
      for entry in logs {
        assert!(entry.offset < me.base_map.len() as u64);
        assert!(entry.offset % LOG_BLOCK_SIZE == 0);
        let block_index = entry.offset / LOG_BLOCK_SIZE;

        let prev = me
          .read_block_aligned(block_index)
          .expect("cannot read block");
        let prev_hash: [u8; 32] = blake3::hash(&prev).into();
        if prev_hash != entry.old_data_hash {
          if !allow_idempotent_writes || prev_hash != entry.new_data_hash {
            log::warn!(
              "hash mismatch at image offset {} when applying {} log {}",
              entry.offset,
              opts.log_type,
              lcn
            );
            if !allow_hash_mismatch {
              return Err(HashMismatch.into());
            }
          }
        }

        me.block_mappings[block_index as usize] = Some(BlockMapping {
          hash: entry.new_data_hash,
        });
      }
      allow_hash_mismatch = false;
      allow_idempotent_writes = false;
    }
    Ok(me)
  }

  pub fn len(&self) -> usize {
    self.base_map.len()
  }

  pub fn read_block_aligned<'a>(&'a self, block_index: u64) -> Option<Cow<[u8]>> {
    if block_index >= div_round_up(self.base_map.len() as u64, LOG_BLOCK_SIZE) {
      return None;
    }

    Some(
      if let Some(m) = &self.block_mappings[block_index as usize] {
        Cow::Owned(self.store.must_read_cas_aligned(&m.hash))
      } else {
        let offset = block_index * LOG_BLOCK_SIZE;
        let slice = &self.base_map
          [offset as usize..((offset + LOG_BLOCK_SIZE) as usize).min(self.base_map.len())];
        align_block(slice, LOG_BLOCK_SIZE as usize)
      },
    )
  }

  pub fn commit(self) -> Result<()> {
    drop(self.base_map);
    let mut map = unsafe { MmapMut::map_mut(&self.base_file) }?;
    for (i, m) in self.block_mappings.iter().enumerate() {
      if let Some(m) = m {
        let image_offset = i * LOG_BLOCK_SIZE as usize;
        let data = self.store.must_read_cas_aligned(&m.hash);
        let _guard = CRITICAL_WRITE_LOCK.lock();
        let image_range_end = (image_offset + LOG_BLOCK_SIZE as usize).min(map.len());
        let region = &mut map[image_offset..image_range_end];
        region.copy_from_slice(&data[..image_range_end.checked_sub(image_offset).unwrap()]);
      }
    }
    map.flush()?;
    Ok(())
  }
}
