use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

pub const LOG_BLOCK_SIZE: u64 = 65536;

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
