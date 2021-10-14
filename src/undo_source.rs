use std::{
  fs::File,
  io::ErrorKind,
  path::{Path, PathBuf},
  time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use memmap2::MmapMut;
use uuid::Uuid;

use crate::{
  undo::{format_lcn, replay_undo_log, CheckpointFile},
  undo_sink::UndoSink,
};

pub struct UndoSource {
  pub dir: PathBuf,
  pub checkpoint_file: CheckpointFile,
  pub checkpoint_index: Option<u32>,
}

impl UndoSource {
  pub fn open_without_recovery(dir: &Path) -> Result<Self> {
    let mut checkpoint_file = CheckpointFile::open(dir)?;
    let checkpoint_index = checkpoint_file.read()?;
    Ok(Self {
      dir: dir.to_path_buf(),
      checkpoint_file,
      checkpoint_index,
    })
  }

  pub fn recover(&mut self, image: &mut MmapMut) -> Result<()> {
    let next_index_str = format_lcn(self.checkpoint_index.map(|x| x + 1).unwrap_or(0));
    let mut next_log_file_path = self.dir.clone();
    next_log_file_path.push(&next_index_str);
    let mut next_log_file = match File::open(&next_log_file_path) {
      Ok(x) => x,
      Err(e) if e.kind() == ErrorKind::NotFound => {
        return Ok(());
      }
      Err(e) => return Err(e.into()),
    };

    let mut redo_dir_path = self.dir.clone();
    redo_dir_path.push(generate_startup_recover_redo_id(&next_index_str));
    let mut redo_sink = UndoSink::from_source(UndoSource::open_without_recovery(&redo_dir_path)?)?;

    replay_undo_log(image, &mut next_log_file, &mut redo_sink)?;
    image.flush()?;
    std::fs::remove_file(&next_log_file_path)?;
    log::info!(
      "Recovered incomplete undo log {}. Redo log written to {}.",
      next_index_str,
      redo_dir_path.to_string_lossy()
    );

    Ok(())
  }

  pub fn open(dir: &Path, image: &mut MmapMut) -> Result<Self> {
    let mut me = Self::open_without_recovery(dir)?;
    me.recover(image)?;
    Ok(me)
  }
}

fn generate_startup_recover_redo_id(index_str: &str) -> String {
  format!(
    "startup-recover-redo-{}-{}-{}",
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_secs(),
    index_str,
    Uuid::new_v4().to_string()
  )
}
