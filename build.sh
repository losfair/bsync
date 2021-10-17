#!/bin/bash

set -euxo pipefail
cd "$(dirname $0)"

cd blkxmit
rm -r ./dist || true
mkdir dist
cargo build --release --target x86_64-unknown-linux-musl
cp ../target/x86_64-unknown-linux-musl/release/blkxmit ./dist/blkxmit.x86_64-unknown-linux-musl
cargo build --release --target aarch64-unknown-linux-musl
cp ../target/aarch64-unknown-linux-musl/release/blkxmit ./dist/blkxmit.aarch64-unknown-linux-musl
llvm-strip ./dist/*
cd ..

cd blkredo
cargo build --release
cd ..

echo "Done."
