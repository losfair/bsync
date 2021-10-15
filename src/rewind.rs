use std::{borrow::Cow, collections::BTreeMap, fs::File};

use anyhow::Result;
use fs2::FileExt;
use memmap2::{Mmap, MmapMut};
use thiserror::Error;

use crate::store::Store;

pub struct ImageRewinder {
  base_file: File,
  base_map: Mmap,
  block_size: u64,
  store: Store,
  block_mappings: Vec<Option<BlockMapping>>,
  _overlay: BTreeMap<u64, Vec<u8>>,
}

#[derive(Clone)]
struct BlockMapping {
  hash: [u8; 32],
}

#[derive(Debug, Default, Clone)]
pub struct ImageRewindOptions {
  pub skip_hash_verification_for_first_lcn: bool,
}

impl ImageRewinder {
  pub fn load(
    base_file: File,
    store: Store,
    block_size: u64,
    lcn_list: Vec<u64>,
    opts: ImageRewindOptions,
  ) -> Result<Self> {
    #[derive(Error, Debug)]
    #[error("bad magic in log file {0}")]
    struct BadMagic(String);

    #[derive(Error, Debug)]
    #[error("cannot open log file {0}: {1}")]
    struct CannotOpenLogFile(String, std::io::Error);

    #[derive(Error, Debug)]
    #[error("hash mismatch")]
    struct HashMismatch;

    base_file.try_lock_exclusive()?;
    let base_map = unsafe { Mmap::map(&base_file)? };
    let block_mappings: Vec<Option<BlockMapping>> =
      vec![None; (base_map.len() + block_size as usize - 1) / block_size as usize];

    let mut me = Self {
      base_file,
      base_map,
      store,
      block_size,
      block_mappings,
      _overlay: BTreeMap::new(),
    };

    let mut skip_hash_verification = opts.skip_hash_verification_for_first_lcn;

    for lcn in lcn_list {
      let logs = me.store.list_undo_for_lcn(lcn)?;
      for entry in logs {
        assert!(entry.offset < me.base_map.len() as u64);
        assert!(entry.offset % block_size == 0);
        let block_index = entry.offset / block_size;

        if !skip_hash_verification {
          let prev = me.read_block(block_index).expect("cannot read block");
          let prev_hash: [u8; 32] = blake3::hash(&prev).into();
          if prev_hash != entry.old_data_hash {
            return Err(HashMismatch.into());
          }
        }

        me.block_mappings[block_index as usize] = Some(BlockMapping {
          hash: entry.new_data_hash,
        });
      }
      skip_hash_verification = false;
    }
    Ok(me)
  }

  fn read_block<'a>(&'a self, block_index: u64) -> Option<Cow<[u8]>> {
    if block_index >= self.base_map.len() as u64 / self.block_size {
      return None;
    }

    if let Some(m) = &self.block_mappings[block_index as usize] {
      Some(Cow::Owned(self.store.must_read_cas(&m.hash)))
    } else {
      let offset = block_index * self.block_size;
      Some(Cow::Borrowed(
        &self.base_map[offset as usize..(offset + self.block_size) as usize],
      ))
    }
  }

  pub fn commit(self) -> Result<()> {
    drop(self.base_map);
    let mut map = unsafe { MmapMut::map_mut(&self.base_file) }?;
    for (i, m) in self.block_mappings.iter().enumerate() {
      if let Some(m) = m {
        let image_offset = i * self.block_size as usize;
        let data = self.store.must_read_cas(&m.hash);
        map[image_offset..image_offset + self.block_size as usize].copy_from_slice(&data);
      }
    }
    map.flush()?;
    Ok(())
  }
}
