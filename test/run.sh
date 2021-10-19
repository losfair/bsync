#!/bin/bash

run_ssh () {
  ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=./known_hosts -i ./id_ed25519 -p 7219 root@127.0.0.1 "$1"
}

set -euxo pipefail
cd "$(mktemp -t -d bsync-test.XXXXXXXX)"
tmpdir="$PWD"

export RUST_LOG=info

cp "$OLDPWD/target/release/bsync" ./

ssh-keygen -t ed25519 -f ./id_ed25519 -q -N ""
cp id_ed25519.pub authorized_keys
container_id=$(docker run --rm -d -e SSH_ENABLE_ROOT=true -p 127.0.0.1:7219:22 -v "$PWD/authorized_keys:/root/.ssh/authorized_keys" panubo/sshd)
echo "Container $container_id is up."
trap "rm -rf \"$tmpdir\"; docker rm -f $container_id" EXIT

set +e
while true; do
  run_ssh "exit"
  if [ $? -eq 0 ]; then
    break
  fi
  sleep 1
done
set -e
echo "SSH is up."

run_ssh "dd if=/dev/urandom of=/root/test.img bs=1M count=30 seek=990"
cat > bsync.yaml << EOF
remote:
  server: 127.0.0.1
  port: 7219
  user: root
  key: ./id_ed25519
  verify: insecure
  image: /root/test.img
local:
  db: ./backup.db
EOF

# First pull
./bsync pull -c ./bsync.yaml
lsn_1="$(./bsync list --db ./backup.db --json | jq ".[-1].lsn")"
./bsync replay --db ./backup.db --lsn "$lsn_1" --output ./replay.img
remote_hash_1="$(run_ssh "sha256sum /root/test.img" | cut -d ' ' -f 1)"
local_hash_1="$(sha256sum ./replay.img | cut -d ' ' -f 1)"

if [ "$remote_hash_1" != "$local_hash_1" ]; then
  echo "[-] lsn_1 hash mismatch"
  exit 1
fi

# Incremental update
run_ssh "dd if=/dev/urandom of=/root/test.img bs=1M count=100 seek=600 conv=notrunc"
./bsync pull -c ./bsync.yaml
lsn_2="$(./bsync list --db ./backup.db --json | jq ".[-1].lsn")"
./bsync replay --db ./backup.db --lsn "$lsn_2" --output ./replay.img
remote_hash_2="$(run_ssh "sha256sum /root/test.img" | cut -d ' ' -f 1)"
local_hash_2="$(sha256sum ./replay.img | cut -d ' ' -f 1)"

if [ "$remote_hash_2" != "$local_hash_2" ] || [ "$remote_hash_2" = "$remote_hash_1" ]; then
  echo "[-] lsn_2 hash mismatch"
  exit 1
fi

# Back to the past
./bsync replay --db ./backup.db --lsn "$lsn_1" --output ./replay.img
local_hash_1_1="$(sha256sum ./replay.img | cut -d ' ' -f 1)"
if [ "$local_hash_1_1" != "$local_hash_1" ]; then
  echo "[-] local_hash_1_1 mismatch"
  exit 1
fi

# Another incremental update with some zeros. Test cas reuse.
run_ssh "dd if=/dev/zero of=/root/test.img bs=1M count=5 seek=650 conv=notrunc"
run_ssh "dd if=/dev/urandom of=/root/test.img bs=1M count=42 seek=690 conv=notrunc"
./bsync pull -c ./bsync.yaml
lsn_3="$(./bsync list --db ./backup.db --json | jq ".[-1].lsn")"
./bsync replay --db ./backup.db --lsn "$lsn_3" --output ./replay.img
remote_hash_3="$(run_ssh "sha256sum /root/test.img" | cut -d ' ' -f 1)"
local_hash_3="$(sha256sum ./replay.img | cut -d ' ' -f 1)"

if [ "$remote_hash_3" != "$local_hash_3" ]; then
  echo "[-] lsn_3 hash mismatch"
  exit 1
fi

# Check data integrity after squash
./bsync squash --db ./backup.db --start-lsn "$lsn_1" --end-lsn "$lsn_3" --data-loss
./bsync replay --db ./backup.db --lsn "$lsn_3" --output ./replay.img
local_hash_3_1="$(sha256sum ./replay.img | cut -d ' ' -f 1)"

if [ "$local_hash_3_1" != "$local_hash_3" ]; then
  echo "[-] local_hash_3_1 hash mismatch"
  exit 1
fi

./bsync replay --db ./backup.db --lsn "$lsn_1" --output ./replay.img
local_hash_1_2="$(sha256sum ./replay.img | cut -d ' ' -f 1)"
if [ "$local_hash_1_2" != "$local_hash_1" ]; then
  echo "[-] local_hash_1_2 mismatch"
  exit 1
fi

echo "[+] Test completed."
