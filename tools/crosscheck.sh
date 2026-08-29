#!/usr/bin/env bash
# Write one Avro file per codec with avro.mojo, then read them with fastavro.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p build/crosscheck
mojo build tools/write_crosscheck.mojo -I src -I ../snappy.mojo/src \
    -I ../zstd.mojo/src -o build/avro-crosscheck
./build/avro-crosscheck build/crosscheck
if [ ! -x build/crosscheck-venv/bin/python ]; then
    uv venv --python 3.12 build/crosscheck-venv
    uv pip install --python build/crosscheck-venv/bin/python \
        fastavro python-snappy zstandard backports.zstd
fi
build/crosscheck-venv/bin/python tools/crosscheck.py build/crosscheck
