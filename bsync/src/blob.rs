use lazy_static::lazy_static;
use phf::phf_map;

use crate::config::LOG_BLOCK_SIZE;

static X86_64_BLKXMIT: &'static [u8] =
  include_bytes!("../bsync-transmit-dist/bsync-transmit.x86_64-unknown-linux-musl");

pub static ARCH_BLKXMIT: phf::Map<&'static str, &'static [u8]> = phf_map! {
  "x86_64" => X86_64_BLKXMIT,
  "amd64" => X86_64_BLKXMIT, // FreeBSD `uname -m` outputs `amd64` instead of `x86_64`
  "aarch64" => include_bytes!("../bsync-transmit-dist/bsync-transmit.aarch64-unknown-linux-musl"),
};

pub static ZERO_BLOCK: [u8; LOG_BLOCK_SIZE] = [0; LOG_BLOCK_SIZE];

lazy_static! {
  pub static ref ZERO_BLOCK_HASH: [u8; 32] = blake3::hash(&ZERO_BLOCK).into();
}
