use std::{
  io::{Read, Seek, SeekFrom, Write},
  net::TcpListener,
  os::unix::net::UnixListener,
  path::PathBuf,
  sync::Arc,
};

use anyhow::Result;
use lru::LruCache;
use nbd::{
  server::{handshake, transmission},
  Export,
};
use structopt::StructOpt;
use thiserror::Error;

use crate::{
  blob::ZERO_BLOCK,
  config::LOG_BLOCK_SIZE,
  db::{Database, Snapshot},
};

/// Replay
#[derive(Debug, StructOpt)]
pub struct Servecmd {
  /// The LSN to use.
  #[structopt(long)]
  lsn: u64,

  /// Path to the database.
  #[structopt(long)]
  db: PathBuf,

  #[structopt(short, long)]
  listen: String,
}

struct Service {
  snapshot: Arc<Snapshot>,
  cursor: u64,
  cache: LruCache<usize, Vec<u8>>,
}

impl Service {
  fn read_block<'a>(&'a mut self, index: usize) -> &'a [u8] {
    let cache = &mut self.cache;

    // XXX: Matching with `Some(x)` gives lifetime errors
    if cache.peek(&index).is_some() {
      return cache.get(&index).unwrap();
    } else if let Some(x) = self.snapshot.read_block(index as u64) {
      cache.put(index, x);
      cache.peek(&index).unwrap()
    } else {
      &ZERO_BLOCK[..]
    }
  }
}

impl Read for Service {
  fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    let start_pos = self.cursor as usize;
    let end_pos = start_pos as usize + buf.len();
    let start_block = start_pos / LOG_BLOCK_SIZE;
    let end_block = (end_pos - 1) / LOG_BLOCK_SIZE;

    let mut current_pos = start_pos;
    log::trace!("requested read with pos {} len {}", current_pos, buf.len());

    for blkid in start_block..=end_block {
      let blk = self.read_block(blkid);
      let blk = &blk[current_pos % LOG_BLOCK_SIZE..];
      let buf_offset = current_pos - start_pos;
      let buf_copy_len = buf.len().checked_sub(buf_offset).unwrap().min(blk.len());

      log::trace!(
        "copy {} bytes from block {} offset {} to buf[{}..{}]",
        buf_copy_len,
        blkid,
        current_pos % LOG_BLOCK_SIZE,
        buf_offset,
        buf_offset + buf_copy_len,
      );

      buf[buf_offset..buf_offset + buf_copy_len].copy_from_slice(&blk[..buf_copy_len]);
      current_pos += buf_copy_len;
    }

    self.cursor += buf.len() as u64;
    Ok(buf.len())
  }
}

impl Write for Service {
  fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
    Err(std::io::Error::new(
      std::io::ErrorKind::Other,
      "read only block device",
    ))
  }

  fn flush(&mut self) -> std::io::Result<()> {
    Ok(())
  }
}

impl Seek for Service {
  fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
    match pos {
      SeekFrom::Start(x) => {
        self.cursor = x;
        Ok(x)
      }
      _ => unimplemented!(),
    }
  }
}

impl Servecmd {
  pub fn run(&self) -> Result<()> {
    #[derive(Error, Debug)]
    enum E {
      #[error("the provided LSN is not a consistent point")]
      Inconsistent,
    }

    let db = Database::open_file(&self.db, false)?;
    let cp_list = db.list_consistent_point();
    let cp = match cp_list.iter().find(|x| x.lsn == self.lsn) {
      Some(x) => x,
      None => return Err(E::Inconsistent.into()),
    };
    let snapshot = Arc::new(db.snapshot(cp.lsn)?);

    let listener = do_listen(&self.listen)?;
    for conn in listener.incoming() {
      let mut conn = conn?;
      let svc = Service {
        cache: LruCache::new(100),
        snapshot: snapshot.clone(),
        cursor: 0,
      };
      let e = Export {
        size: cp.size,
        readonly: true,
        ..Default::default()
      };
      std::thread::spawn(move || {
        let res = handshake(&mut conn, &e).and_then(|()| transmission(&mut conn, svc));
        if let Err(e) = res {
          log::error!("error while handling connection: {}", e);
        }
      });
    }
    Ok(())
  }
}

trait ReadAndWrite: Read + Write + Send {}

impl<T: Read + Write + Send> ReadAndWrite for T {}

enum GenericListener {
  Tcp(TcpListener),
  Unix(UnixListener),
}

impl GenericListener {
  fn incoming<'a>(
    &'a self,
  ) -> Box<dyn Iterator<Item = Result<Box<dyn ReadAndWrite>, std::io::Error>> + 'a> {
    match self {
      Self::Tcp(lis) => Box::new(
        lis
          .incoming()
          .map(|x| x.map(|x| Box::new(x) as Box<dyn ReadAndWrite>)),
      ),
      Self::Unix(lis) => Box::new(
        lis
          .incoming()
          .map(|x| x.map(|x| Box::new(x) as Box<dyn ReadAndWrite>)),
      ),
    }
  }
}

fn do_listen(addr: &str) -> Result<GenericListener, std::io::Error> {
  if let Some(path) = addr.strip_prefix("unix:") {
    let _ = std::fs::remove_file(&path);
    Ok(GenericListener::Unix(UnixListener::bind(path)?))
  } else {
    Ok(GenericListener::Tcp(TcpListener::bind(addr)?))
  }
}
