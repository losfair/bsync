use std::borrow::Cow;

pub fn div_round_up(value: u64, align: u64) -> u64 {
  (value + align - 1) / align
}

pub fn align_block(data: &[u8], block_size: usize) -> Cow<[u8]> {
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
