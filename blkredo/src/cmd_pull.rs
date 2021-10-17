use std::{
  borrow::Cow,
  io::{Read, Write},
  net::{IpAddr, SocketAddr, TcpStream},
  path::{Path, PathBuf},
  str::FromStr,
};

use anyhow::Result;
use itertools::Itertools;
use shell_escape::unix::escape;
use size_format::SizeFormatterBinary;
use ssh2::{Channel, Session};
use structopt::StructOpt;
use thiserror::Error;

use crate::{
  blob::{ARCH_BLKXMIT, ZERO_BLOCK_HASH},
  config::{BackupConfig, LOG_BLOCK_SIZE},
  db::Database,
};

const DIFF_BATCH_SIZE: usize = 16384;
const DATA_FETCH_BATCH_SIZE: usize = 256; // 16MiB batches

/// Incrementally pull updates from a remote image.
#[derive(Debug, StructOpt)]
pub struct Pullcmd {
  /// Path to the config.
  #[structopt(short, long)]
  config: PathBuf,
}

impl Pullcmd {
  pub fn run(&self) -> Result<()> {
    #[derive(Error, Debug)]
    #[error("received invalid hash from remote: {0}")]
    struct InvalidRemoteHash(String);
    #[derive(Error, Debug)]
    #[error("expecting {0} bytes from remote, got {1}")]
    struct ByteCountMismatch(usize, usize);
    #[derive(Error, Debug)]
    #[error("total size mismatch - expecting {0}, got {1}")]
    struct TotalSizeMismatch(u64, u64);
    #[derive(Error, Debug)]
    #[error("remote architecture not supported: {0}")]
    struct ArchNotSupported(String);

    let config = BackupConfig::must_load_from_file(&self.config);
    let remote = &config.remote;

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

    let db = Database::open_file(Path::new(&config.local.db), false)?;

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

    let remote_arch = exec_oneshot(&mut sess, "uname -m")?;
    let remote_arch = remote_arch.trim();
    log::info!("Remote architecture is {}.", remote_arch);

    let blkxmit_image = *ARCH_BLKXMIT
      .get(&remote_arch)
      .ok_or_else(|| ArchNotSupported(remote_arch.to_string()))?;

    let blkxmit_hash = blake3::hash(blkxmit_image);
    let blkxmit_filename = format!("blkxmit.{}", blkxmit_hash);

    let maybe_upload_path: String = exec_oneshot(
      &mut sess,
      &format!(
        r#"
set -e
if [ ! -f ~/.blkredo/{} ]; then
  mkdir -p ~/.blkredo
  echo -n "$HOME/.blkredo"
fi
"#,
        escape(Cow::Borrowed(blkxmit_filename.as_str()))
      ),
    )?;

    if !maybe_upload_path.is_empty() {
      let upload_path = format!("{}/{}", maybe_upload_path, blkxmit_filename);
      let mut remote_file = sess.scp_send(
        Path::new(&upload_path),
        0o755,
        blkxmit_image.len() as u64,
        None,
      )?;
      remote_file.write_all(blkxmit_image)?;
      remote_file.send_eof()?;
      remote_file.wait_eof()?;
      remote_file.close()?;
      remote_file.wait_close()?;
      println!("Installed blkxmit on remote host at {}.", upload_path);
    }

    let mut lsn = db.max_lsn();
    let snapshot = db.snapshot(lsn)?;
    log::info!("Starting from LSN {}.", lsn);

    let mut fetch_list: Vec<usize> = vec![];

    for chunk in &(0usize..remote_image_size as usize)
      .step_by(LOG_BLOCK_SIZE)
      .chunks(DIFF_BATCH_SIZE)
    {
      let chunk = chunk.collect_vec();
      let script = format!(
        "~/.blkredo/{} {} {} hash {} {}",
        escape(Cow::Borrowed(blkxmit_filename.as_str())),
        escape(Cow::Borrowed(remote.image.as_str())),
        LOG_BLOCK_SIZE,
        chunk[0],
        chunk.len(),
      );
      let output = exec_oneshot_bin(&mut sess, &script)?;
      if output.len() != chunk.len() * 32 {
        return Err(ByteCountMismatch(chunk.len() * 32, output.len()).into());
      }
      let remote_hashes = output.chunks(32);
      let local_hashes = chunk.iter().map(|x| {
        snapshot
          .read_block_hash((*x / LOG_BLOCK_SIZE) as u64)
          .unwrap_or(*ZERO_BLOCK_HASH)
      });
      for (&offset, (lh, rh)) in chunk.iter().zip(local_hashes.zip(remote_hashes)) {
        if lh != rh {
          log::debug!("block at offset {} changed", offset);
          fetch_list.push(offset);
        }
      }
    }

    log::info!("{} blocks changed. Fetching changes.", fetch_list.len());
    let mut total_redo_bytes: usize = 0;
    for chunk in &fetch_list.iter().copied().chunks(DATA_FETCH_BATCH_SIZE) {
      let chunk = chunk.collect_vec();
      let script = format!(
        "~/.blkredo/{} {} {} dump {}",
        escape(Cow::Borrowed(blkxmit_filename.as_str())),
        escape(Cow::Borrowed(remote.image.as_str())),
        LOG_BLOCK_SIZE,
        chunk.iter().map(|x| format!("{}", x)).join(","),
      );
      let output = exec_oneshot_bin(&mut sess, &script)?;
      if output.len() != chunk.len() * LOG_BLOCK_SIZE {
        return Err(ByteCountMismatch(chunk.len() * LOG_BLOCK_SIZE, output.len()).into());
      }
      lsn = db.write_redo(
        lsn,
        chunk
          .iter()
          .copied()
          .zip(output.chunks(LOG_BLOCK_SIZE))
          .map(|(offset, data)| ((offset / LOG_BLOCK_SIZE) as u64, data)),
      )?;
      log::info!(
        "Written {} redo log entries. Total size is {} bytes. Last LSN is {}.",
        chunk.len(),
        output.len(),
        lsn,
      );
      total_redo_bytes += output.len();
    }

    db.add_consistent_point(lsn, remote_image_size);
    println!(
      "Pulled {}B.",
      SizeFormatterBinary::new(total_redo_bytes as u64)
    );
    Ok(())
  }
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
    let mut msg = String::new();
    channel.stderr().read_to_string(&mut msg)?;
    log::error!("remote stderr: {}", msg);
    return Err(RemoteError(status).into());
  }
  Ok(data)
}
