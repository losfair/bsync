pragma journal_mode = wal;

-- Content-addressable storage for blocks. Keyed by the BLAKE2b hash
-- of `content`.
create table if not exists `cas_v1` (
  `hash` blob not null primary key,
  `content` blob not null
);

create table if not exists `redo_v1` (
  `lsn` integer not null primary key autoincrement,
  `block_id` integer not null,
  `hash` blob not null
);

create table if not exists `consistent_point_v1` (
  `lsn` integer not null primary key,
  `size` integer not null,
  `created_at` integer not null
);
