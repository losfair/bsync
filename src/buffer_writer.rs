use std::ops::Deref;

pub struct BufferWriter<'a> {
  inner: &'a mut [u8],
  buffers: Vec<(usize, Vec<u8>)>,
}

impl<'a> Deref for BufferWriter<'a> {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
    self.inner
  }
}
impl<'a> BufferWriter<'a> {
  pub fn new(inner: &'a mut [u8]) -> Self {
    Self {
      inner,
      buffers: vec![],
    }
  }
  pub fn push(&mut self, offset: usize, data: Vec<u8>) {
    self.buffers.push((offset, data));
  }

  pub fn len(&self) -> usize {
    self.buffers.len()
  }

  pub fn flush(&mut self) {
    for (offset, data) in &self.buffers {
      self.inner[*offset..*offset + data.len()].copy_from_slice(data);
    }
    self.buffers.clear();
  }
}
