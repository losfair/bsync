use std::{
  borrow::Cow,
  collections::HashSet,
  convert::TryFrom,
  fs::OpenOptions,
  io::{BufRead, BufReader, Read, Write},
  net::{IpAddr, SocketAddr, TcpStream},
  path::{Path, PathBuf},
  str::FromStr,
};

use anyhow::Result;
use fs2::FileExt;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use shell_escape::unix::escape;
use size_format::SizeFormatterBinary;
use ssh2::{Channel, CheckResult, KnownHostFileKind, Session};
use structopt::StructOpt;
use thiserror::Error;

use crate::{
  blob::{ARCH_BLKXMIT, ZERO_BLOCK_HASH},
  config::{BackupConfig, HostVerification, LOG_BLOCK_SIZE},
  db::{Database, RedoContentOrHash},
  util::sha256hash,
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

enum FetchOrAssumeExist {
  Fetch(usize),
  AssumeExistWithHash(usize, [u8; 32]),
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
    #[derive(Error, Debug)]
    #[error("remote os not supported: {0}")]
    struct OsNotSupported(String);

    #[derive(Error, Debug)]
    #[error("`remote.scripts` requested but `local.pull_lock` is not set. If this is really the intended config, set `remote.scripts.no_pull_lock` to `true`.")]
    struct PullLockRequired;

    #[derive(Error, Debug)]
    #[error("cannot acquire pull lock on {0}: {1}")]
    struct LockAcquire(String, std::io::Error);

    #[derive(Error, Debug)]
    #[error("no host key")]
    struct NoHostKey;

    #[derive(Error, Debug)]
    #[error("host key verification error: {0}")]
    struct HostKeyVerifyError(&'static str);

    let config = BackupConfig::must_load_from_file(&self.config);
    let remote = &config.remote;

    // Unique access.
    if let Some(scripts) = &config.remote.scripts {
      if !scripts.no_pull_lock.unwrap_or(false) && config.local.pull_lock.is_none() {
        return Err(PullLockRequired.into());
      }
    }
    let _pull_lock_file = if let Some(path) = &config.local.pull_lock {
      let f = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
      f.try_lock_exclusive()
        .map_err(|e| LockAcquire(path.clone(), e))?;
      log::info!("Acquired pull lock at {}.", path);
      Some(f)
    } else {
      None
    };

    // Establish SSH session.
    let addr = SocketAddr::new(IpAddr::from_str(&remote.server)?, remote.port.unwrap_or(22));
    let tcp = TcpStream::connect(addr).unwrap();
    let mut sess = Session::new()?;
    sess.set_tcp_stream(tcp);
    sess.handshake()?;

    let (host_key, _host_key_type) = sess.host_key().ok_or(NoHostKey)?;
    match config.remote.verify {
      HostVerification::Insecure => {
        log::warn!("`remote.verify` is set to `insecure`, skipping host key verification");
      }
      HostVerification::Known => {
        let mut known_hosts = sess.known_hosts()?;
        if let Some(home) = dirs::home_dir() {
          let _ = known_hosts.read_file(&home.join(".ssh/known_hosts"), KnownHostFileKind::OpenSSH);
        }
        match known_hosts.check(&remote.server, host_key) {
          CheckResult::Match => {}
          CheckResult::NotFound => {
            return Err(
              HostKeyVerifyError("not found - please connect to the remote host once").into(),
            );
          }
          CheckResult::Mismatch => {
            return Err(HostKeyVerifyError("mismatch - possible mitm").into());
          }
          CheckResult::Failure => {
            return Err(HostKeyVerifyError("unknown").into());
          }
        }
      }
      HostVerification::Dnssec => {
        return Err(HostKeyVerifyError("dnssec not yet implemented").into());
      }
    }

    if let Some(x) = &remote.key {
      sess.userauth_pubkey_file(&remote.user, None, Path::new(x), None)?;
    } else {
      sess.userauth_agent(&remote.user)?;
    }

    let db = Database::open_file(Path::new(&config.local.db), true)?;

    let remote_uname = exec_oneshot(&mut sess, "uname -m; uname -s")?;
    let mut remote_uname_segs = remote_uname.split("\n");
    let remote_arch = remote_uname_segs.next().unwrap_or("");
    let remote_os = remote_uname_segs.next().unwrap_or("");

    if remote_os != "Linux" {
      return Err(OsNotSupported(remote_os.to_string()).into());
    }

    log::info!("Remote architecture is {}.", remote_arch);

    let transmit_image = *ARCH_BLKXMIT
      .get(&remote_arch)
      .ok_or_else(|| ArchNotSupported(remote_arch.to_string()))?;
    let transmit_sha256 = hex::encode(sha256hash(transmit_image));
    let transmit_filename = format!("transmit.{}.{}", db.instance_id(), transmit_sha256);

    let maybe_upload_path: String = exec_oneshot(
      &mut sess,
      &format!(
        r#"
if [ -f ~/.bsync/{filename} ]; then
  echo {hash} ~/.bsync/{filename} | sha256sum -c - > /dev/null
  if [ $? -eq 0 ]; then
    exit 0
  fi
fi
mkdir -p ~/.bsync
echo -n "$HOME/.bsync"
"#,
        filename = escape(Cow::Borrowed(transmit_filename.as_str())),
        hash = escape(Cow::Borrowed(transmit_sha256.as_str()))
      ),
    )?;

    if !maybe_upload_path.is_empty() {
      let upload_path = format!("{}/{}", maybe_upload_path, transmit_filename);
      let mut remote_file = sess.scp_send(
        Path::new(&upload_path),
        0o755,
        transmit_image.len() as u64,
        None,
      )?;
      remote_file.write_all(transmit_image)?;
      remote_file.send_eof()?;
      remote_file.wait_eof()?;
      remote_file.close()?;
      remote_file.wait_close()?;
      println!("Installed transmit on remote host at {}.", upload_path);
    }

    if let Some(script) = config
      .remote
      .scripts
      .as_ref()
      .and_then(|x| x.pre_pull.as_ref())
    {
      log::info!("Running pre_pull script.");
      let out = exec_oneshot(&mut sess, script)?;
      log::info!("pre_pull output: {}", out);
      println!("Finished running pre_pull script.");
    }

    // Get the size of the remote image.
    //
    // The image might be created by `pre_pull`.
    let remote_image_size: u64 = exec_oneshot(
      &mut sess,
      &format!(
        "blockdev --getsize64 {} || stat -c \"%s\" {}",
        escape(Cow::Borrowed(remote.image.as_str())),
        escape(Cow::Borrowed(remote.image.as_str())),
      ),
    )?
    .trim()
    .parse()?;
    log::info!("Remote image size is {} bytes.", remote_image_size);

    let mut lsn = db.max_lsn();
    let snapshot = db.snapshot(lsn)?;
    log::info!("Starting from LSN {}.", lsn);

    let mut fetch_list: Vec<FetchOrAssumeExist> = vec![];

    let gen_pb_style = |name: &str| {
      ProgressStyle::default_bar().template(
        &format!("{{spinner:.green}} {} [{{elapsed_precise}}] [{{wide_bar:.cyan/blue}}] {{bytes}}/{{total_bytes}}", name),
      )
      .progress_chars("#>-")
    };

    let bar = ProgressBar::new(remote_image_size);
    bar.set_style(gen_pb_style("Diff"));

    // XXX: This may become large if we are synchronizing a big block device -
    // should we store this in SQLite instead?
    let mut seen_hashes: HashSet<[u8; 32]> = HashSet::new();

    for chunk in &(0usize..remote_image_size as usize)
      .step_by(LOG_BLOCK_SIZE)
      .chunks(DIFF_BATCH_SIZE)
    {
      let chunk = chunk.collect_vec();
      let mut microprogress: usize = 0;
      bar.set_position(chunk[0] as u64);
      let script = format!(
        "~/.bsync/{} {} {} hash {} {}",
        escape(Cow::Borrowed(transmit_filename.as_str())),
        escape(Cow::Borrowed(remote.image.as_str())),
        LOG_BLOCK_SIZE,
        chunk[0],
        chunk.len(),
      );
      let output = exec_oneshot_bin(
        &mut sess,
        &script,
        |inc| {
          microprogress += inc;
          bar.set_position(chunk[0] as u64 + (microprogress as u64 / 32) * LOG_BLOCK_SIZE as u64);
        },
        |x| Box::new(x),
      )?;
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
          let rh = <[u8; 32]>::try_from(rh)?;
          if seen_hashes.contains(&rh) || db.exists_in_cas(&rh) {
            fetch_list.push(FetchOrAssumeExist::AssumeExistWithHash(offset, rh));
          } else {
            fetch_list.push(FetchOrAssumeExist::Fetch(offset));
          }
          seen_hashes.insert(rh);
        }
      }
    }
    bar.finish();
    drop(bar);

    log::info!("{} blocks changed. Fetching changes.", fetch_list.len());
    let bar = ProgressBar::new(
      fetch_list
        .iter()
        .filter(|x| matches!(x, FetchOrAssumeExist::Fetch(_)))
        .count() as u64
        * LOG_BLOCK_SIZE as u64,
    );
    bar.set_style(gen_pb_style("Fetch"));
    let mut total_download_bytes: usize = 0;
    let mut total_reuse_bytes: usize = 0;
    for chunk in &fetch_list.iter().chunks(DATA_FETCH_BATCH_SIZE) {
      let chunk = chunk.collect_vec();
      let fetch_chunk = chunk
        .iter()
        .filter_map(|x| {
          if let FetchOrAssumeExist::Fetch(x) = x {
            Some(*x)
          } else {
            None
          }
        })
        .collect_vec();

      // Don't pass empty string to remote.
      let output: Vec<u8> = if fetch_chunk.len() == 0 {
        vec![]
      } else {
        let script = format!(
          "~/.bsync/{} {} {} dump {}",
          escape(Cow::Borrowed(transmit_filename.as_str())),
          escape(Cow::Borrowed(remote.image.as_str())),
          LOG_BLOCK_SIZE,
          fetch_chunk.iter().map(|x| format!("{}", x)).join(","),
        );
        exec_oneshot_bin(
          &mut sess,
          &script,
          |inc| bar.inc(inc as u64),
          |x| Box::new(snap::read::FrameDecoder::new(x)),
        )?
      };
      if output.len() != fetch_chunk.len() * LOG_BLOCK_SIZE {
        return Err(ByteCountMismatch(fetch_chunk.len() * LOG_BLOCK_SIZE, output.len()).into());
      }
      let mut output_chunks = output.chunks(LOG_BLOCK_SIZE);
      lsn = db.write_redo(
        lsn,
        chunk
          .iter()
          .copied()
          .map(|x| match x {
            FetchOrAssumeExist::Fetch(x) => (
              *x,
              RedoContentOrHash::Content(output_chunks.next().unwrap()),
            ),
            FetchOrAssumeExist::AssumeExistWithHash(x, h) => (*x, RedoContentOrHash::Hash(*h)),
          })
          .map(|(offset, data)| ((offset / LOG_BLOCK_SIZE) as u64, data)),
      )?;
      log::info!(
        "Written {} redo log entries, of which {} are fetched. Total download size is {} bytes. Last LSN is {}.",
        chunk.len(),
        fetch_chunk.len(),
        output.len(),
        lsn,
      );
      total_download_bytes += output.len();
      total_reuse_bytes += (chunk.len() - fetch_chunk.len()) * LOG_BLOCK_SIZE;
    }
    bar.finish();
    drop(bar);

    db.add_consistent_point(lsn, remote_image_size);
    println!(
      "Downloaded {}B and reused {}B.",
      SizeFormatterBinary::new(total_download_bytes as u64),
      SizeFormatterBinary::new(total_reuse_bytes as u64),
    );

    if let Some(script) = config
      .remote
      .scripts
      .as_ref()
      .and_then(|x| x.post_pull.as_ref())
    {
      log::info!("Running post_pull script.");
      let out = exec_oneshot(&mut sess, script)?;
      log::info!("post_pull output: {}", out);
      println!("Finished running post_pull script.");
    }
    Ok(())
  }
}

fn exec_oneshot(sess: &mut Session, cmd: &str) -> Result<String> {
  let mut channel = sess.channel_session()?;
  exec_oneshot_in(&mut channel, cmd)
}

fn exec_oneshot_bin<D: for<'a> FnMut(&'a mut dyn Read) -> Box<dyn Read + 'a>>(
  sess: &mut Session,
  cmd: &str,
  progress: impl FnMut(usize),
  decoder_gen: D,
) -> Result<Vec<u8>> {
  let mut channel = sess.channel_session()?;
  exec_oneshot_bin_in(&mut channel, cmd, progress, decoder_gen)
}

fn exec_oneshot_in(channel: &mut Channel, cmd: &str) -> Result<String> {
  exec_oneshot_bin_in(channel, cmd, |_| (), |x| Box::new(x))
    .and_then(|x| String::from_utf8(x).map_err(anyhow::Error::from))
}

fn exec_oneshot_bin_in<D: for<'a> FnMut(&'a mut dyn Read) -> Box<dyn Read + 'a>>(
  channel: &mut Channel,
  cmd: &str,
  mut progress: impl FnMut(usize),
  mut decoder_gen: D,
) -> Result<Vec<u8>> {
  #[derive(Debug, Error)]
  #[error("remote returned error {0}")]
  struct RemoteError(i32);

  channel.exec(cmd)?;
  let mut data = Vec::new();
  {
    let mut reader = decoder_gen(&mut *channel);
    let mut reader = BufReader::new(&mut *reader);
    loop {
      let buf = reader.fill_buf()?;
      if buf.len() == 0 {
        break;
      }
      data.extend_from_slice(buf);
      let len = buf.len();
      reader.consume(len);
      progress(len);
    }
  }
  channel.wait_close()?;

  let sig = channel.exit_signal()?;
  let status = channel.exit_status()?;
  let mut msg = String::new();
  channel.stderr().read_to_string(&mut msg)?;

  // We get `status == 0` if the program is killed by a signal - so do another check here.
  if let Some(sig) = sig.exit_signal {
    log::error!("remote signal: {}, stderr: {}", sig, msg);
    return Err(RemoteError(1).into());
  }

  if status != 0 {
    log::error!("remote returned error {}, stderr: {}", status, msg);
    return Err(RemoteError(status).into());
  }

  log::debug!("remote stderr: {}", msg);
  Ok(data)
}
