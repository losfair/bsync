use std::{
  borrow::Cow,
  io::Read,
  net::{IpAddr, SocketAddr, TcpStream},
  path::{Path, PathBuf},
  str::FromStr,
};

use anyhow::Result;
use blake2::{
  digest::{Update, VariableOutput},
  VarBlake2b,
};
use itertools::Itertools;
use libc::c_void;
use memmap2::MmapMut;
use nix::sys::mman::{madvise, MmapAdvise};
use shell_escape::unix::escape;
use ssh2::{Channel, Session};
use structopt::StructOpt;
use thiserror::Error;

use crate::{
  config::{BackupConfig, LOG_BLOCK_SIZE},
  recover::IncompleteLogRecoveryOptions,
  signals::CRITICAL_WRITE_LOCK,
  store::LogEntry,
};

static BLOCK_SIZES: &'static [u64] = &[1048576 * 8, LOG_BLOCK_SIZE];
const DIFF_BATCH_SIZE: usize = 100;
const DATA_FETCH_BATCH_SIZE: usize = 256; // 16MiB batches

/// Incrementally pull updates of an image.
#[derive(Debug, StructOpt)]
pub struct Pullcmd {
  /// Undo incomplete writes. The default behavior is redo.
  #[structopt(long)]
  undo_incomplete: bool,

  /// Allow hash mismatch during log recovery.
  #[structopt(short, long)]
  force: bool,

  config: PathBuf,
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

    let config = BackupConfig::must_load_from_file(&self.config);
    let remote = &config.remote;
    let local = &config.local;

    // Establish SSH session.
    let addr = SocketAddr::new(IpAddr::from_str(&remote.server)?, remote.port.unwrap_or(22));
    let tcp = TcpStream::connect(addr).unwrap();
    let mut sess = Session::new()?;
    sess.set_tcp_stream(tcp);
    sess.handshake()?;

    if let Some(x) = &remote.key {
      sess.userauth_pubkey_file(&remote.user, None, Path::new(x), None)?;
    } else {
      sess.userauth_agent(&remote.user)?;
    }

    // Get the size of the remote image.
    let remote_image_size: u64 = exec_oneshot(
      &mut sess,
      &format!(
        "stat --printf=\"%s\" {}",
        escape(Cow::Borrowed(remote.image.as_str()))
      ),
    )?
    .parse()?;
    log::info!("Remote image size is {} bytes.", remote_image_size);

    let (mut local_image, log_store) = local.open_managed(
      false,
      Some(IncompleteLogRecoveryOptions {
        undo: self.undo_incomplete,
        force: self.force,
      }),
    )?;

    // Ensure that the sizes are consistent.
    let orig_len = local_image.len()?;
    if orig_len < remote_image_size {
      log::info!(
        "Extending local image from {} to {} bytes.",
        orig_len,
        remote_image_size
      );
      local_image.extend_to(remote_image_size)?;
    }
    if orig_len > remote_image_size {
      return Err(CannotShrinkLocalFile(orig_len, remote_image_size).into());
    }

    let our_lcn = log_store.allocate_lcn(log_store.last_active_lcn()?)?;

    let mut prev_data_offsets: Vec<u64> = vec![0];
    let mut prev_block_size: u64 = remote_image_size;

    // Map the local image into memory.
    let mut map = unsafe { MmapMut::map_mut(local_image.file())? };

    // Narrow down the diff
    for &block_size in BLOCK_SIZES {
      let script = format!(
        r#"
set -e
x () {{
  dd if={} bs={} count=1 skip=$1 | b2sum -l 256 | cut -d " " -f 1
}}
      "#,
        escape(Cow::Borrowed(remote.image.as_str())),
        block_size
      );
      let mut invocations: Vec<String> = vec![];
      let mut local_blocks: Vec<LocalBlockMetadata> = vec![];

      // Build the commands for hashing remote blocks.
      log::info!("Calculating local hashes at block size {}.", block_size);
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
          unsafe {
            madvise(
              local_data.as_ptr() as *const c_void as *mut c_void,
              local_data.len(),
              MmapAdvise::MADV_DONTNEED,
            )?;
          }
        }
      }

      log::info!("Calculating remote block hashes.");
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
        escape(Cow::Borrowed(remote.image.as_str())),
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
      let mut undo_batch: Vec<LogEntry> = vec![];
      let mut redo_batch: Vec<LogEntry> = vec![];
      for (&offset, &len) in data_offset_batch.iter().zip(data_sizes.iter()) {
        undo_batch.push(LogEntry {
          offset,
          old_data: Cow::Borrowed(&output[cursor as usize..(cursor + len) as usize]),
          new_data: Cow::Borrowed(&map[offset as usize..(offset + len) as usize]),
        });
        redo_batch.push(LogEntry {
          offset,
          old_data: Cow::Borrowed(&map[offset as usize..(offset + len) as usize]),
          new_data: Cow::Borrowed(&output[cursor as usize..(cursor + len) as usize]),
        });
        cursor += len;
      }

      log_store.write_undo(our_lcn, &undo_batch)?;
      log_store.write_redo(our_lcn, &redo_batch)?;

      // Write the new data
      let mut cursor: u64 = 0;
      for (&offset, &len) in data_offset_batch.iter().zip(data_sizes.iter()) {
        let _guard = CRITICAL_WRITE_LOCK.lock();
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
    log_store.activate_lcn(our_lcn, true)?;
    Ok(())
  }
}

fn hash_block(data: &[u8]) -> [u8; 32] {
  let mut hasher = VarBlake2b::new(32).unwrap();
  hasher.update(data);
  let result = hasher.finalize_boxed();
  (&result[..]).try_into().unwrap()
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
