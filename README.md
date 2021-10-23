# bsync

Incremental, multi-version remote backup tool for block devices.

The on-disk backup format is a SQLite database and I've been dogfooding this on my homelab servers for a while, so I consider this quite stable.

## Install

bsync implements pull-style synchronization and should be installed on the backup destination (pull-side) host. Get the latest binary from [Releases](https://github.com/losfair/bsync/releases).

The [release workflow](https://github.com/losfair/bsync/blob/main/.github/workflows/ci.yml) builds `bsync` for:

- Linux (x86_64, binary and `.deb`)
- macOS (x86_64, binary)
- FreeBSD (x86_64, binary)

## Usage

`bsync` works over SSH and pulls changes to a block device from the remote host (backup source). Linux (x86\_64, AArch64) is currently the only supported OS on the backup source.

Pull changes (see "Example config" below for an example of `config.yaml`):

```
$ bsync pull -c ./config.yaml
```

List local versions:

```
$ bsync list --db ./backup.db
+-------+---------------------+
| LSN   | CREATED             |
+=======+=====================+
| 21800 | 2021-10-19 08:21:51 |
+-------+---------------------+
| 22267 | 2021-10-19 08:46:24 |
+-------+---------------------+
| 30245 | 2021-10-20 00:22:38 |
+-------+---------------------+
| 35319 | 2021-10-20 08:22:15 |
+-------+---------------------+
```

Build an image of the block device at a given point in time:

```
# Find `lsn` from `bsync list` output
$ bsync replay --db ./backup.db --lsn 30245 --output ./replay.img
```

Start an NBD server to serve a read-only version of the block device at a given point in time:

```
$ bsync serve --db ./backup.db --lsn 30245 --listen 127.0.0.1:2939
# Or, to listen on a unix socket
$ bsync serve --db ./backup.db --lsn 30245 --listen unix:/tmp/bsync.sock
```

Squash the backup to remove historic versions and free up space:

```
# Remove versions between LSN 21800 and 30245 (boundaries excluded) so that the remaining versions are
# 21800, 30245, 35319
$ bsync squash --db ./backup.db --start-lsn 21800 --end-lsn 30245 
```

## Example config

The schema of the config file is defined as `BackupConfig` in [src/config.rs](https://github.com/losfair/bsync/blob/main/bsync/src/config.rs) and can be used as a reference.

Note that bsync doesn't automatically snapshot your volumes yet so please add your own snapshot logic (LVM, zvol, etc.) in `remote.scripts.pre_pull` to ensure data consistency. An example for backing up LVM thin volumes (taken from my homelab servers):

```yaml
remote:
  server: 192.168.1.1
  user: root
  image: /dev/mapper/VG_data01-data--auto--snapshot--do--not--touch
  scripts:
    pre_pull: |
      set -e
      lvremove -y VG_data01/data-auto-snapshot-do-not-touch || true
      lvcreate -s VG_data01/data -n data-auto-snapshot-do-not-touch
      lvchange -ay -Ky VG_data01/data-auto-snapshot-do-not-touch
    post_pull: |
      lvremove -y VG_data01/data-auto-snapshot-do-not-touch
local:
  db: /backup/store.db
  pull_lock: /backup/store.lock
```
