pragma journal_mode = wal;
pragma synchronous = full;

-- Content-addressable storage for blocks. Keyed by the BLAKE3 hash
-- of `content`.
create table if not exists `cas_v1` (
  `hash` blob not null primary key,
  `content` blob not null
);

-- Redo logs.
create table if not exists `redo_v1` (
  `lcn` integer not null,
  `offset` integer not null,
  `old_data_hash` blob not null,
  `new_data_hash` blob not null,
  primary key (`lcn`, `offset`)
);

-- Undo logs.
create table if not exists `undo_v1` (
  `lcn` integer not null,
  `offset` integer not null,
  `old_data_hash` blob not null,
  `new_data_hash` blob not null,
  primary key (`lcn`, `offset`)
);

-- Linked list for undo and redo logs.
create table if not exists `log_list_v1` (
  `lcn` integer not null primary key autoincrement,
  `link` integer not null,
  `active` integer not null default 0
);
