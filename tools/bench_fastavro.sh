#!/usr/bin/env bash
# fastavro over the files `pixi run bench` leaves in build/bench/.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ ! -d build/bench ]; then
    echo "no build/bench — running the Mojo bench first" >&2
    mkdir -p build && mojo build bench/bench_avro.mojo -I src -o build/avro-bench
    ./build/avro-bench > /dev/null
fi
if [ ! -x build/bench-venv/bin/python ]; then
    uv venv --python 3.12 build/bench-venv
    uv pip install --python build/bench-venv/bin/python fastavro
fi
build/bench-venv/bin/python tools/bench_fastavro.py
