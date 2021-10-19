use std::borrow::Cow;

use sha2::{Digest, Sha256};

use crate::config::LOG_BLOCK_SIZE;

pub fn align_block(data: &[u8]) -> Cow<[u8]> {
  let block_size = LOG_BLOCK_SIZE as usize;
  assert!(data.len() <= block_size);
  if data.len() < block_size {
    log::debug!(
      "align_block: padding data of length {} to {}",
      data.len(),
      block_size
    );
    let mut v = Vec::with_capacity(block_size);
    v.extend_from_slice(data);
    v.extend(std::iter::repeat(0u8).take(block_size - data.len()));
    Cow::Owned(v)
  } else {
    Cow::Borrowed(data)
  }
}

pub fn sha256hash(data: &[u8]) -> [u8; 32] {
  let mut h = Sha256::new();
  h.update(data);
  h.finalize().into()
}
