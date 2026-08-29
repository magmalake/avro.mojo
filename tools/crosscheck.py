#!/usr/bin/env python3
"""Read the files avro.mojo wrote, with Python fastavro, and check them.

    pixi run -e codecs crosscheck

`tools/write_crosscheck.mojo` writes one Object Container File per codec;
this reads each one with fastavro and compares every record against the same
expectations `tools/gen_fixtures.py` uses. Needs a venv with fastavro,
python-snappy, zstandard and backports.zstd:

    uv venv .venv && uv pip install --python .venv/bin/python \
        fastavro python-snappy zstandard backports.zstd
"""
import datetime
import decimal
import os
import sys

import fastavro

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gen_fixtures import N, SCHEMA, record  # noqa: E402


def check(path):
    with open(path, "rb") as fh:
        reader = fastavro.reader(fh)
        codec = reader.metadata.get("avro.codec", "null")
        assert reader.metadata.get("written-by") == "avro.mojo", reader.metadata
        got_schema = reader.writer_schema
        rows = list(reader)

    assert got_schema["name"] == "org.magmalake.avro.Everything", got_schema["name"]
    assert len(got_schema["fields"]) == len(SCHEMA["fields"])
    assert len(rows) == N, "%d records, wanted %d" % (len(rows), N)

    for i, got in enumerate(rows):
        want = record(i)
        for key in want:
            g, w = got[key], want[key]
            # fastavro materialises logical types on read; undo that so the
            # comparison is against what was actually encoded.
            if key == "day":
                g = (g - datetime.date(1970, 1, 1)).days
            elif key == "ts":
                epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
                g = round((g - epoch).total_seconds() * 1_000_000)
            elif key == "amount":
                w = decimal.Decimal(
                    int.from_bytes(w, "big", signed=True)
                ).scaleb(-2)
            if key == "f32":
                ok = abs(g - w) < 1e-6
            else:
                ok = g == w
            if not ok:
                raise AssertionError(
                    "%s record %d field %r: fastavro read %r, wanted %r"
                    % (os.path.basename(path), i, key, g, w)
                )
    print(
        "  %-10s %6d bytes  %3d records  OK"
        % (codec, os.path.getsize(path), len(rows))
    )
    return codec


def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "build/crosscheck"
    print("fastavro %s reading avro.mojo's output:" % fastavro.__version__)
    seen = []
    for codec in ("null", "deflate", "snappy", "zstandard"):
        path = os.path.join(out_dir, "avro_mojo_%s.avro" % codec)
        if not os.path.exists(path):
            print("  %-10s MISSING (%s)" % (codec, path))
            return 1
        seen.append(check(path))
    print("all %d codecs round-trip through fastavro" % len(seen))
    return 0


if __name__ == "__main__":
    sys.exit(main())
