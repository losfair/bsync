use std::{
  convert::TryFrom,
  fs::File,
  io::{stdout, BufWriter, Read, Seek, SeekFrom, Write},
};

fn main() {
  let mut args = std::env::args();
  args.next().unwrap();

  let path = args.next().expect("expecting path");
  let chunk_size: usize = args.next().expect("expecting chunk size").parse().unwrap();
  let op = args.next().expect("expecting op");

  assert!(chunk_size > 0);
  let mut f = File::open(&path).unwrap();
  let stdout = stdout();
  let mut stdout = BufWriter::new(stdout.lock());
  let mut buf = vec![0u8; chunk_size];

  match op.as_str() {
    "hash" => {
      let initial_offset: usize = args
        .next()
        .expect("expecting initial offset")
        .parse()
        .unwrap();
      let chunk_count: usize = args.next().expect("expecting chunk count").parse().unwrap();
      assert!(chunk_count > 0);
      assert!(initial_offset % chunk_size == 0);
      let end_offset = initial_offset
        .checked_add(chunk_size.checked_mul(chunk_count).unwrap())
        .unwrap();

      // We're not using `metadata.len` here because of the need to deal with block devices.
      f.seek(SeekFrom::End(0)).unwrap();
      let file_len = f.stream_position().unwrap();
      let end_offset = usize::try_from(file_len).unwrap().min(end_offset);
      f.seek(SeekFrom::Start(initial_offset as u64)).unwrap();

      for offset in (initial_offset..end_offset).step_by(chunk_size) {
        let end_offset = offset.checked_add(chunk_size).unwrap().min(end_offset);
        let read_len = end_offset.checked_sub(offset).unwrap();
        assert!(read_len > 0);
        f.read_exact(&mut buf[..read_len]).unwrap();
        buf[read_len..].fill(0);

        let hash: [u8; 32] = blake3::hash(&buf).into();
        stdout.write_all(&hash[..]).unwrap();
      }
    }
    "dump" => {
      let offset_list: Vec<usize> = args
        .next()
        .expect("expecting offset list")
        .split(",")
        .map(|x| x.parse().expect("bad offset"))
        .collect();
      f.seek(SeekFrom::End(0)).unwrap();
      let file_size = f.stream_position().unwrap();

      for offset in offset_list {
        let end_offset = offset
          .checked_add(chunk_size)
          .unwrap()
          .min(file_size as usize);
        let read_len = end_offset.checked_sub(offset).unwrap();
        assert!(read_len > 0);
        f.seek(SeekFrom::Start(offset as u64)).unwrap();
        f.read_exact(&mut buf[..read_len]).unwrap();
        buf[read_len..].fill(0);
        stdout.write_all(&buf).unwrap();
      }
    }
    _ => panic!("bad op: {}", op),
  }
  stdout.flush().unwrap();
}
