use std::{
  collections::BTreeMap,
  fs::File,
  io::{Cursor, ErrorKind, Read},
  ops::RangeInclusive,
  path::Path,
  sync::Arc,
};

use crate::{
  undo::{checksum_undo_data, format_lcn, UNDO_MAGIC},
  undo_sink::UndoSink,
};
use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt};
use fs2::FileExt;
use memmap2::{Mmap, MmapMut};
use thiserror::Error;

pub struct ImageRewinder {
  base_file: File,
  base_map: Mmap,
  block_size: u64,
  block_mappings: Vec<Option<BlockMapping>>,
  _overlay: BTreeMap<u64, Vec<u8>>,
}

#[derive(Clone)]
struct BlockMapping {
  log_file: Arc<Mmap>,
  log_offset: u64,
}

struct UndoEntry<'a> {
  checksum: [u8; 32],
  image_offset: u64,
  data_position: u64,
  data: &'a [u8],
}

impl<'a> UndoEntry<'a> {
  fn read(log: &mut Cursor<&'a [u8]>) -> Result<Self, std::io::Error> {
    let mut checksum = [0u8; 32];
    log.read_exact(&mut checksum)?;
    let image_offset = log.read_u64::<LittleEndian>()?;
    let size = log.read_u64::<LittleEndian>()?;
    let data_position = log.position();
    let buf = *log.get_ref();
    if data_position.checked_add(size).unwrap() > buf.len() as u64 {
      return Err(std::io::Error::new(
        ErrorKind::UnexpectedEof,
        "cannot read full data",
      ));
    }
    let data = &buf[data_position as usize..(data_position + size) as usize];
    log.set_position(data_position + size);
    Ok(Self {
      checksum,
      image_offset,
      data_position,
      data,
    })
  }
}

impl ImageRewinder {
  pub fn load(
    base_file: File,
    block_size: u64,
    dir: &Path,
    lcn_range: RangeInclusive<u32>,
  ) -> Result<Self> {
    #[derive(Error, Debug)]
    #[error("bad magic in log file {0}")]
    struct BadMagic(String);

    #[derive(Error, Debug)]
    #[error("cannot open log file {0}: {1}")]
    struct CannotOpenLogFile(String, std::io::Error);

    #[derive(Error, Debug)]
    #[error("undo log entry in file {0} at byte offset {1} failed checksum verification")]
    struct BadChecksum(String, u64);

    #[derive(Error, Debug)]
    #[error("invalid image offset and/or size in log file {0} at byte offset {1}: image_offset={2}, size={3}")]
    struct InvalidOffsetOrSize(String, u64, u64, u64);

    base_file.try_lock_exclusive()?;
    let base_map = unsafe { Mmap::map(&base_file)? };
    let block_mappings: Vec<Option<BlockMapping>> =
      vec![None; (base_map.len() + block_size as usize - 1) / block_size as usize];

    let mut me = Self {
      base_file,
      base_map,
      block_size,
      block_mappings,
      _overlay: BTreeMap::new(),
    };

    for lcn in lcn_range.rev() {
      let mut log_file_path = dir.to_path_buf();
      log_file_path.push(format_lcn(lcn));
      let log_file_path_repr = log_file_path.to_string_lossy().into_owned();
      let log_file =
        File::open(&log_file_path).map_err(|e| CannotOpenLogFile(log_file_path_repr.clone(), e))?;

      let log_map = Arc::new(unsafe { Mmap::map(&log_file)? });

      let mut log = Cursor::new(&log_map[..]);

      let mut magic: [u8; UNDO_MAGIC.len()] = [0; UNDO_MAGIC.len()];
      log.read_exact(&mut magic)?;
      if magic != UNDO_MAGIC {
        return Err(BadMagic(log_file_path_repr).into());
      }

      // Validate checksum and figure out the last valid log entry.
      loop {
        let position = log.position();
        let entry = match UndoEntry::read(&mut log) {
          Ok(x) => x,
          Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
          Err(e) => return Err(e.into()),
        };

        if entry.image_offset >= me.base_map.len() as u64
          || entry.image_offset % block_size != 0
          || entry.data.len() as u64 != block_size
        {
          return Err(
            InvalidOffsetOrSize(
              log_file_path_repr,
              position,
              entry.image_offset,
              entry.data.len() as u64,
            )
            .into(),
          );
        }

        let block_index = entry.image_offset / block_size;
        let current = me.read_block(block_index).unwrap();
        let calculated_checksum = checksum_undo_data(entry.image_offset, &entry.data, current);
        if calculated_checksum != entry.checksum {
          return Err(BadChecksum(log_file_path_repr, position).into());
        }

        me.block_mappings[block_index as usize] = Some(BlockMapping {
          log_file: log_map.clone(),
          log_offset: entry.data_position,
        });
      }
    }
    Ok(me)
  }

  fn read_block<'a>(&'a self, block_index: u64) -> Option<&'a [u8]> {
    if block_index >= self.base_map.len() as u64 / self.block_size {
      return None;
    }

    if let Some(m) = &self.block_mappings[block_index as usize] {
      Some(&m.log_file[m.log_offset as usize..(m.log_offset + self.block_size) as usize])
    } else {
      let offset = block_index * self.block_size;
      Some(&self.base_map[offset as usize..(offset + self.block_size) as usize])
    }
  }

  pub fn commit(self, redo: &mut UndoSink) -> Result<()> {
    drop(self.base_map);
    let mut map = unsafe { MmapMut::map_mut(&self.base_file) }?;
    for (i, m) in self.block_mappings.iter().enumerate() {
      if let Some(m) = m {
        let image_offset = i * self.block_size as usize;
        let old_data = &map[image_offset..image_offset + self.block_size as usize];
        let new_data =
          &m.log_file[m.log_offset as usize..(m.log_offset + self.block_size) as usize];
        redo.write(image_offset as u64, old_data, new_data)?;
      }
    }
    redo.commit()?;
    for (i, m) in self.block_mappings.iter().enumerate() {
      if let Some(m) = m {
        let image_offset = i * self.block_size as usize;
        let new_data =
          &m.log_file[m.log_offset as usize..(m.log_offset + self.block_size) as usize];
        map[image_offset..image_offset + self.block_size as usize].copy_from_slice(new_data);
      }
    }
    map.flush()?;
    Ok(())
  }
}
