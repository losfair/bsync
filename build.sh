#!/bin/bash

set -euxo pipefail
cd "$(dirname $0)"

cd blkxmit
rm -r ../blkredo/blkxmit-dist || true
mkdir ../blkredo/blkxmit-dist
cargo build --release --target x86_64-unknown-linux-musl
cp ../target/x86_64-unknown-linux-musl/release/blkxmit ../blkredo/blkxmit-dist/blkxmit.x86_64-unknown-linux-musl
cargo build --release --target aarch64-unknown-linux-musl
cp ../target/aarch64-unknown-linux-musl/release/blkxmit ../blkredo/blkxmit-dist/blkxmit.aarch64-unknown-linux-musl
llvm-strip ../blkredo/blkxmit-dist/*
cd ..

cd blkredo
cargo build --release
cargo deb --no-strip
cd ..

echo "Done."
