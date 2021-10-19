-- Content-addressable storage for blocks. Keyed by the BLAKE3 hash
-- of `content`.
create table `cas_v1` (
  `hash` blob not null primary key,
  `content` blob not null
);

create table `redo_v1` (
  `lsn` integer not null primary key autoincrement,
  `block_id` integer not null,
  `hash` blob not null
);

create table `consistent_point_v1` (
  `lsn` integer not null primary key,
  `size` integer not null,
  `created_at` integer not null
);

create table `blkredo_config` (
  `k` text not null primary key,
  `v` text not null
);
