#!/usr/bin/env python3
"""Write the fastavro-produced test fixtures into tests/fixtures/.

Run with the scratch venv built by tools/README-crosscheck.md:

    uv venv venv && uv pip install --python venv/bin/python fastavro \
        python-snappy zstandard
    venv/bin/python tools/gen_fixtures.py

The files are committed, so this only needs re-running when the fixture
schema or data changes.
"""
import os
import sys

import fastavro

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURES = os.path.join(os.path.dirname(HERE), "tests", "fixtures")

SCHEMA = {
    "type": "record",
    "name": "Everything",
    "namespace": "org.magmalake.avro",
    "doc": "one of each Avro type, for the fastavro cross-check",
    "fields": [
        {"name": "nothing", "type": "null"},
        {"name": "flag", "type": "boolean"},
        {"name": "i32", "type": "int"},
        {"name": "i64", "type": "long"},
        {"name": "f32", "type": "float"},
        {"name": "f64", "type": "double"},
        {"name": "blob", "type": "bytes"},
        {"name": "text", "type": "string"},
        {
            "name": "suit",
            "type": {"type": "enum", "name": "Suit", "symbols": ["C", "D", "H", "S"]},
        },
        {"name": "key", "type": {"type": "fixed", "name": "K4", "size": 4}},
        {"name": "xs", "type": {"type": "array", "items": "long"}},
        {"name": "props", "type": {"type": "map", "values": "int"}},
        {"name": "maybe", "type": ["null", "string"], "default": None},
        {"name": "choice", "type": ["int", "string", "boolean"]},
        {
            "name": "inner",
            "type": {
                "type": "record",
                "name": "Inner",
                "fields": [
                    {"name": "a", "type": "int"},
                    {"name": "b", "type": {"type": "array", "items": "string"}},
                ],
            },
        },
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {
            "name": "ts",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
        },
        {
            "name": "amount",
            "type": {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 9,
                "scale": 2,
            },
        },
    ],
}

N = 240


def record(i):
    return {
        "nothing": None,
        "flag": i % 2 == 0,
        "i32": i * 7 - 500,
        "i64": (i * 1_000_003) - 2_000_000_000,
        "f32": float(i) * 0.5,
        "f64": float(i) * -1.25,
        "blob": bytes([i % 256, (i * 3) % 256, 0, 255]),
        "text": "row-%d-☺" % i,
        "suit": ["C", "D", "H", "S"][i % 4],
        "key": bytes([i % 256, 1, 2, 3]),
        "xs": [i, i + 1, i + 2][: (i % 3) + 1],
        "props": {"a": i, "b": i * 2},
        "maybe": None if i % 3 == 0 else "yes-%d" % i,
        "choice": i if i % 3 == 0 else ("s%d" % i if i % 3 == 1 else i % 2 == 0),
        "inner": {"a": i * 2, "b": ["x%d" % i, "y%d" % i]},
        "day": 19000 + i,
        "ts": 1_700_000_000_000_000 + i * 1_000_000,
        "amount": (i * 137 - 5000).to_bytes(8, "big", signed=True),
    }


def main():
    os.makedirs(FIXTURES, exist_ok=True)
    parsed = fastavro.parse_schema(SCHEMA)
    written = []
    for codec in ("null", "deflate", "snappy", "zstandard"):
        path = os.path.join(FIXTURES, "fastavro_%s.avro" % codec)
        with open(path, "wb") as fh:
            fastavro.writer(
                fh,
                parsed,
                (record(i) for i in range(N)),
                codec=codec,
                sync_interval=800,
                metadata={"written-by": "fastavro", "note": "avro.mojo fixture"},
            )
        written.append((codec, path, os.path.getsize(path)))

    with open(os.path.join(FIXTURES, "fastavro_null.avro"), "rb") as fh:
        rows = list(fastavro.reader(fh))

    for codec, path, size in written:
        print("%-10s %7d bytes  %s" % (codec, size, os.path.basename(path)))
    print("records:", len(rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
