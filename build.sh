#!/bin/bash

set -euxo pipefail
cd "$(dirname $0)"

cd bsync-transmit
rm -r ../bsync/bsync-transmit-dist || true
mkdir ../bsync/bsync-transmit-dist
cargo build --release --target x86_64-unknown-linux-musl
cp ../target/x86_64-unknown-linux-musl/release/bsync-transmit ../bsync/bsync-transmit-dist/bsync-transmit.x86_64-unknown-linux-musl
cargo build --release --target aarch64-unknown-linux-musl
cp ../target/aarch64-unknown-linux-musl/release/bsync-transmit ../bsync/bsync-transmit-dist/bsync-transmit.aarch64-unknown-linux-musl
llvm-strip ../bsync/bsync-transmit-dist/*
brandelf -t Linux ../bsync/bsync-transmit-dist/*
cd ..

cd bsync
cargo build --release
cargo deb --no-strip
cd ..

echo "Done."
