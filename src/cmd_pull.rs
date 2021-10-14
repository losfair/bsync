use std::{
  borrow::Cow,
  fs::OpenOptions,
  io::{Read, Seek, SeekFrom, Write},
  net::{IpAddr, SocketAddr, TcpStream},
  path::PathBuf,
  str::FromStr,
};

use anyhow::Result;
use fs2::FileExt;
use itertools::Itertools;
use memmap2::MmapMut;
use sha2::{Digest, Sha256};
use shell_escape::unix::escape;
use ssh2::{Channel, Session};
use structopt::StructOpt;
use thiserror::Error;

use crate::{undo::UNDO_BLOCK_SIZE, undo_sink::UndoSink, undo_source::UndoSource};

static BLOCK_SIZES: &'static [u64] = &[2097152, UNDO_BLOCK_SIZE];
const DIFF_BATCH_SIZE: usize = 100;
const DATA_FETCH_BATCH_SIZE: usize = 256; // 16MiB batches

/// Incrementally pull updates of an image.
#[derive(Debug, StructOpt)]
pub struct Pullcmd {
  /// Username for connecting to the server.
  #[structopt(short = "u", long)]
  user: String,

  /// SSH port number.
  #[structopt(short = "p", long)]
  port: Option<u16>,

  /// Remote address.
  #[structopt(short = "s", long)]
  server: String,

  /// Path to SSH private key.
  #[structopt(short = "k", long)]
  key: Option<PathBuf>,

  /// Local path for undo logs. Defaults to `[local_path]/undo/`.
  #[structopt(long = "undo")]
  undo_path: Option<PathBuf>,

  /// Remote path.
  remote_path: String,

  /// Local path.
  local_path: PathBuf,
}

struct LocalBlockMetadata {
  local_hash: [u8; 32],
  data_offset: u64,
}

impl Pullcmd {
  pub fn run(&self) -> Result<()> {
    #[derive(Error, Debug)]
    #[error("detected shrink in remote image from {0} to {1} bytes")]
    struct CannotShrinkLocalFile(u64, u64);
    #[derive(Error, Debug)]
    #[error("received invalid hash from remote: {0}")]
    struct InvalidRemoteHash(String);
    #[derive(Error, Debug)]
    #[error("expecting {0} hashes from remote, got {1}")]
    struct HashCountMismatch(usize, usize);
    #[derive(Error, Debug)]
    #[error("total size mismatch - expecting {0}, got {1}")]
    struct TotalSizeMismatch(u64, u64);

    // Establish SSH session.
    let addr = SocketAddr::new(IpAddr::from_str(&self.server)?, self.port.unwrap_or(22));
    let tcp = TcpStream::connect(addr).unwrap();
    let mut sess = Session::new()?;
    sess.set_tcp_stream(tcp);
    sess.handshake()?;

    if let Some(x) = &self.key {
      sess.userauth_pubkey_file(&self.user, None, x, None)?;
    } else {
      sess.userauth_agent(&self.user)?;
    }

    // Get the size of the remote image.
    let remote_image_size: u64 = exec_oneshot(
      &mut sess,
      &format!(
        "stat --printf=\"%s\" {}",
        escape(Cow::Borrowed(self.remote_path.as_str()))
      ),
    )?
    .parse()?;
    log::info!("Remote image size is {} bytes.", remote_image_size);

    // Get the local image ready.
    let mut local_image = OpenOptions::new()
      .create(true)
      .read(true)
      .write(true)
      .open(&self.local_path)?;
    local_image.try_lock_exclusive()?;
    {
      let orig_len = local_image.metadata()?.len();
      if orig_len < remote_image_size {
        log::info!(
          "Extending local image from {} to {} bytes.",
          orig_len,
          remote_image_size
        );
        local_image.seek(SeekFrom::Start(remote_image_size - 1))?;
        local_image.write_all(&[0u8])?;
      }
      if orig_len > remote_image_size {
        return Err(CannotShrinkLocalFile(orig_len, remote_image_size).into());
      }
    }

    // Prepare the undo log.
    let undo_path = self.undo_path.clone().unwrap_or_else(|| {
      let mut p = self.local_path.clone();
      p.pop();
      p.push("undo");
      p
    });
    let mut undo = UndoSink::from_source(UndoSource::open(&undo_path, &mut local_image)?)?;

    let mut prev_data_offsets: Vec<u64> = vec![0];
    let mut prev_block_size: u64 = remote_image_size;

    // Map the local image into memory.
    let mut map = unsafe { MmapMut::map_mut(&local_image)? };

    // Narrow down the diff
    for &block_size in BLOCK_SIZES {
      let script = format!(
        r#"
set -e
x () {{
  dd if={} bs={} count=1 skip=$1 | sha256sum | cut -d " " -f 1
}}
      "#,
        escape(Cow::Borrowed(self.remote_path.as_str())),
        block_size
      );
      let mut invocations: Vec<String> = vec![];
      let mut local_blocks: Vec<LocalBlockMetadata> = vec![];

      // Build the commands for hashing remote blocks.
      for &data_offset in &prev_data_offsets {
        let block_count = calculate_block_count(prev_block_size, block_size);
        log::debug!("data_offset {}, block_count {}", data_offset, block_count);
        for i in 0..block_count {
          let data_offset = data_offset + i * block_size;
          if data_offset >= remote_image_size {
            break;
          }
          assert!(data_offset % block_size == 0);
          invocations.push(format!("x {}", data_offset / block_size));

          let data_end = (data_offset + block_size).min(remote_image_size);
          let local_data = &map[data_offset as usize..data_end as usize];
          local_blocks.push(LocalBlockMetadata {
            data_offset,
            local_hash: hash_block(local_data),
          });
        }
      }

      let mut output = vec![];
      for i in (0..invocations.len()).step_by(DIFF_BATCH_SIZE) {
        let window = i..(i + DIFF_BATCH_SIZE).min(invocations.len());
        let invocations = &invocations[window.clone()];
        let script = script.clone() + &invocations.join("\n");
        let res = exec_oneshot(&mut sess, &script)?;
        let res = res
          .trim()
          .split("\n")
          .filter(|x| !x.is_empty())
          .map(|x| x.to_string());
        output.extend(res);
      }
      if output.len() != local_blocks.len() {
        return Err(HashCountMismatch(local_blocks.len(), output.len()).into());
      }

      // Compare remote and local hashes.
      prev_data_offsets.clear();
      prev_block_size = block_size;
      for (remote_hash_str, local_block) in output.iter().zip(local_blocks.iter()) {
        let remote_hash =
          hex::decode(remote_hash_str).map_err(|_| InvalidRemoteHash(remote_hash_str.into()))?;
        if remote_hash.len() != 32 {
          return Err(InvalidRemoteHash(remote_hash_str.into()).into());
        }
        if remote_hash != local_block.local_hash {
          prev_data_offsets.push(local_block.data_offset);
        }
      }
      log::info!(
        "Found {} differences at block size {}.",
        prev_data_offsets.len(),
        block_size
      );
    }

    // Fetch the changes
    for data_offset_batch in &prev_data_offsets
      .iter()
      .copied()
      .chunks(DATA_FETCH_BATCH_SIZE)
    {
      let data_offset_batch = data_offset_batch.collect_vec();
      let mut script = format!(
        r#"
set -e
x () {{
  dd if={} bs={} count=1 skip=$1
}}
      "#,
        escape(Cow::Borrowed(self.remote_path.as_str())),
        prev_block_size
      );
      let mut invocations: Vec<String> = vec![];

      for &data_offset in &data_offset_batch {
        assert!(data_offset % prev_block_size == 0);
        invocations.push(format!("x {}", data_offset / prev_block_size));
      }
      script += &invocations.join("\n");
      let output = exec_oneshot_bin(&mut sess, &script)?;

      let data_sizes = data_offset_batch
        .iter()
        .copied()
        .map(|x| {
          (x + prev_block_size)
            .min(remote_image_size)
            .checked_sub(x)
            .expect("block size calculation error")
        })
        .collect_vec();

      // Double check the size
      let expected_total_size: u64 = data_sizes.iter().sum();
      if output.len() as u64 != expected_total_size {
        return Err(TotalSizeMismatch(expected_total_size, output.len() as u64).into());
      }

      // Write the original data to undo logs
      let mut cursor: u64 = 0;
      for (&offset, &len) in data_offset_batch.iter().zip(data_sizes.iter()) {
        undo.write(
          offset,
          &map[offset as usize..(offset + len) as usize],
          &output[cursor as usize..(cursor + len) as usize],
        )?;
        cursor += len;
      }
      undo.commit()?;

      // Write the new data
      let mut cursor: u64 = 0;
      for (&offset, &len) in data_offset_batch.iter().zip(data_sizes.iter()) {
        map[offset as usize..(offset + len) as usize]
          .copy_from_slice(&output[cursor as usize..(cursor + len) as usize]);
        cursor += len;
      }

      log::info!(
        "Committed batch of size {}. Written {} bytes.",
        data_offset_batch.len(),
        output.len()
      );
    }

    // Finalize file writes
    map.flush()?;
    drop(map);
    drop(local_image);
    undo.finalize()?;
    Ok(())
  }
}

fn hash_block(data: &[u8]) -> [u8; 32] {
  let mut hasher = Sha256::new();
  hasher.update(data);
  let result = hasher.finalize();
  result.into()
}

fn calculate_block_count(file_size: u64, block_size: u64) -> u64 {
  (file_size + block_size - 1) / block_size
}

fn exec_oneshot(sess: &mut Session, cmd: &str) -> Result<String> {
  let mut channel = sess.channel_session()?;
  exec_oneshot_in(&mut channel, cmd)
}

fn exec_oneshot_bin(sess: &mut Session, cmd: &str) -> Result<Vec<u8>> {
  let mut channel = sess.channel_session()?;
  exec_oneshot_bin_in(&mut channel, cmd)
}

fn exec_oneshot_in(channel: &mut Channel, cmd: &str) -> Result<String> {
  exec_oneshot_bin_in(channel, cmd).and_then(|x| String::from_utf8(x).map_err(anyhow::Error::from))
}

fn exec_oneshot_bin_in(channel: &mut Channel, cmd: &str) -> Result<Vec<u8>> {
  #[derive(Debug, Error)]
  #[error("remote returned error {0}")]
  struct RemoteError(i32);

  channel.exec(cmd)?;
  let mut data = Vec::new();
  channel.read_to_end(&mut data)?;
  channel.wait_close()?;
  let status = channel.exit_status()?;
  if status != 0 {
    return Err(RemoteError(status).into());
  }
  Ok(data)
}
