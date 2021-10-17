use lazy_static::lazy_static;
use phf::phf_map;

use crate::config::LOG_BLOCK_SIZE;

pub const ARCH_BLKXMIT: phf::Map<&'static str, &'static [u8]> = phf_map! {
  "x86_64" => include_bytes!("../../blkxmit/dist/blkxmit.x86_64-unknown-linux-musl"),
  "aarch64" => include_bytes!("../../blkxmit/dist/blkxmit.aarch64-unknown-linux-musl"),
};

static ZERO_BLOCK: [u8; LOG_BLOCK_SIZE] = [0; LOG_BLOCK_SIZE];

lazy_static! {
  pub static ref ZERO_BLOCK_HASH: [u8; 32] = blake3::hash(&ZERO_BLOCK).into();
}
