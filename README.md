# avro.mojo

[![CI](https://github.com/magmalake/avro.mojo/actions/workflows/ci.yml/badge.svg)](https://github.com/magmalake/avro.mojo/actions/workflows/ci.yml) [![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> Part of **magmalake** — data lake building blocks in Mojo.

A pure-[Mojo](https://www.modular.com/mojo) implementation of
[Apache Avro](https://avro.apache.org/docs/1.12.0/specification/): schema
parsing, the binary encoding, Object Container Files (read and write), and
schema resolution.

The core has **no dependencies at all** — not even FFI. A consumer needs
nothing but `-I ../avro.mojo/src`.

```mojo
from avro import DataFileReader

var r = DataFileReader.open("manifest.avro")
while r.has_next():
    var rec = r.next()
    print(rec.field("manifest_path").as_string())
```

## Why

Apache Iceberg's **manifest lists** and **manifests** are Avro Object
Container Files. Reading them is the first thing a native Iceberg
implementation in Mojo has to do, so this library is built around what that
needs:

- **Field ids survive parsing.** Iceberg hangs `field-id`, `element-id`,
  `key-id` and `value-id` off the schema JSON; they are kept as properties
  and read back with `field.field_id()`, `schema.element_id(i)`, and friends.
- **File metadata beyond `avro.*` is reachable.** Iceberg writes `schema`,
  `partition-spec`, `format-version` and `content` into the OCF metadata map.
  `reader.metadata` exposes the raw bytes; `reader.metadata_string(key)` the
  text.
- **`["null", T]` is recognised as optional** — `schema.is_optional(i)` and
  `schema.optional_branch(i)`.
- **Maps with non-string keys**, which Avro (and Iceberg) spell as an array
  of two-field records tagged `"logicalType": "map"`, are flagged by
  `schema.is_logical_map(i)`.

## API

```mojo
from avro import (
    Schema, parse_schema,          # schema tree, JSON in and out
    Value, RecordBuilder,          # a dynamic datum, and how to build one
    Decoder, Encoder, encode_value,# the binary encoding
    DataFileReader, DataFileWriter,# Object Container Files
    resolve,                       # writer schema -> reader schema
    deflate, inflate, crc32,       # the pieces the codecs need
)
```

### Schema

`parse_schema(json)` returns a `Schema`: an arena of nodes plus a root index.
Named-type references (including a record that refers to itself) are just the
same arena index appearing twice, so recursive schemas parse and print
without any special casing.

| call | meaning |
|---|---|
| `s.root`, `s.root_kind()` | the root node index and its kind |
| `s.kind(i)`, `s.name(i)`, `s.logical_type(i)`, `s.size(i)` | node basics |
| `s.num_fields(i)`, `s.field(i, k)`, `s.field_index(i, name)`, `s.field_type(i, k)` | records |
| `s.symbols(i)` | enums |
| `s.items(i)`, `s.values(i)` | arrays and maps |
| `s.num_branches(i)`, `s.branch(i, k)`, `s.is_optional(i)`, `s.optional_branch(i)` | unions |
| `s.props(i)`, `s.element_id(i)`, `s.key_id(i)`, `s.value_id(i)` | extra JSON attributes |
| `s.to_json()`, `s.parsing_canonical_form()`, `s.fingerprint()` | printing; `fingerprint` is Avro's `CRC-64-AVRO` Rabin fingerprint |

Kinds are the constants `NULL`, `BOOLEAN`, `INT`, `LONG`, `FLOAT`, `DOUBLE`,
`BYTES`, `STRING`, `RECORD`, `ENUM`, `ARRAY`, `MAP`, `UNION`, `FIXED`.

Logical types (`decimal`, `uuid`, `date`, `time-millis/micros`,
`timestamp-millis/micros/nanos`, `local-timestamp-*`, `duration`) are parsed
and exposed — `s.logical_type(i)` plus `precision`/`scale` on the node — but
not converted: a `decimal` still decodes to its `bytes`, a `date` to its
`int`. Interpreting them is the consumer's call.

### Value

A `Value` is a dynamically typed datum. The node arena sits behind an
`ArcPointer`, so navigating is a refcount bump, not a copy.

```mojo
rec.field("data_file").field("record_count").as_long()
rec.field_raw("key_metadata").is_null()      # keep the union wrapper
rec.field("partitions").at(0).field("lower_bound").as_bytes()
rec.to_json()                                 # debug / cross-check rendering
```

Constructors: `Value.null()`, `.boolean`, `.int`, `.long`, `.float`,
`.double`, `.bytes`, `.string`, `.fixed`, `.enum`, `.array`, `.map`,
`.record`, `.union`; plus `RecordBuilder`, `ArrayBuilder` and `MapBuilder`
for building one field at a time.

### Object Container Files

```mojo
var w = DataFileWriter(parse_schema(text), "deflate")
w.set_metadata_string("app", "avro.mojo")
w.append(row)
var raw = w.bytes()          # or w.save("out.avro")

var r = DataFileReader.from_bytes(Span(raw))
r.schema          # parsed avro.schema
r.codec           # avro.codec
r.metadata        # Dict[String, List[UInt8]] — every key, avro.* included
r.sync_marker     # the file's 16 bytes
```

The writer starts a new block every `sync_interval` uncompressed bytes
(64 000 by default) and writes the sync marker after each one; the reader
verifies it.

### Schema resolution

`resolve(writer, reader)` walks both schemas once and returns a
`ResolvedReader` — a flat decoding plan, so per-record decoding does no name
lookups. Supported:

- field reordering, and writer fields the reader dropped (skipped in place);
- reader fields the writer never wrote, filled from the field's `default`;
- promotions `int → long → float → double`;
- `string ↔ bytes`;
- writer union → reader non-union, and writer non-union → reader union;
- enum symbols the reader lacks, falling back to the reader enum's `default`;
- records, enums and fixed matched by unqualified name and by reader
  `aliases`.

A writer union branch the reader cannot accept is not a planning failure — it
becomes an error only if that branch actually shows up on the wire.

## Codecs

| codec | where it lives | needs |
|---|---|---|
| `null` | `avro` — built in | — |
| `deflate` | `avro.deflate` — RFC 1951, both directions | — |
| `snappy` | `avro_codecs` | [snappy.mojo](https://github.com/magmalake/snappy.mojo) |
| `zstandard` | `avro_codecs` | [zstd.mojo](https://github.com/magmalake/zstd.mojo) |

`deflate` is implemented here rather than bound to zlib so the core library
stays dependency-free: `inflate` reads all three block types (stored,
fixed-Huffman, dynamic-Huffman) and `deflate` emits fixed-Huffman blocks from
a hash-chain LZ77 matcher. Avro wants *raw* DEFLATE — no zlib header, no
trailing Adler-32 — which is what these two produce and accept.

`snappy` and `zstandard` live in a **separate package**, `avro_codecs`, which
nothing in `avro` imports. That is what keeps the core dependency-free: a
consumer that only reads Iceberg manifests never pulls in the sibling tins.
Reach for them by naming a `CodecSet`:

```mojo
from avro import DataFileReader
from avro_codecs import AllCodecs      # or SnappyCodecs / ZstdCodecs

var r = DataFileReader[AllCodecs].open("data.avro")
```

Avro's `snappy` block is a raw Snappy block with the big-endian CRC-32 of the
*uncompressed* data appended — four bytes that are not part of Snappy itself,
so `avro_codecs` adds and verifies them (`avro.crc32` is in-repo for exactly
this).

The sibling tins arrive as pixi **git source dependencies** in the `codecs`
feature, not path dependencies: pixi solves every environment even when it
installs only one, so a path dependency to `../zstd.mojo` would break
`pixi install` for anyone who cloned just this repo. Both tins also install a
`lib/mojo/*.mojopkg`, but those are built with stable Mojo 1.0.0 and the
nightly compiler cannot load them, so `test-codecs` compiles the siblings
from source (`-I ../snappy.mojo/src -I ../zstd.mojo/src`) and CI checks the
two repos out next to this one.

## Interoperability

Both directions are checked against Python
[fastavro](https://github.com/fastavro/fastavro) 1.12.2.

**Python → us.** `tests/fixtures/fastavro_{null,deflate,snappy,zstandard}.avro`
are written by `tools/gen_fixtures.py`: 240 records of a schema with one of
every Avro type — including an enum, a fixed, an array, a map, a three-way
union, a nested record, and `date` / `timestamp-micros` / `decimal(9,2)`
logical types — at `sync_interval=800` so each file has ~30 blocks. The tests
decode all four and check every field of every record.

`tests/fixtures/iceberg_manifest.avro` and `iceberg_manifest_list.avro` are
real Apache Iceberg v2 metadata, produced by
[iceberg-rs.mojo](https://github.com/magmalake/iceberg-rs.mojo)'s test
warehouse. They decode with field ids intact and match what fastavro reads.

**Us → Python.** `pixi run -e codecs crosscheck` writes the same 240 records
with each codec and has fastavro read them back:

```
fastavro 1.12.2 reading avro.mojo's output:
  null        25499 bytes  240 records  OK
  deflate     17544 bytes  240 records  OK
  snappy      20030 bytes  240 records  OK
  zstandard   16726 bytes  240 records  OK
all 4 codecs round-trip through fastavro
```

## Performance

`pixi run bench` — 100 000 six-field records (a long, a string, a double, a
boolean, a two-element array and an optional string), on an Apple M-series
Mac (`osx-arm64`, nightly Mojo). MB/s is measured against the uncompressed
datum stream (4.0 MiB).

| operation | throughput | rows/s |
|---|---|---|
| encode (datum stream) | 217 MB/s | 5.5 M |
| decode (datum stream) | 44 MB/s | 1.1 M |
| OCF write, `null` | 231 MB/s | 5.9 M |
| **OCF read, `null`** | **45 MB/s** | **1.1 M** |
| OCF write, `deflate` | 39 MB/s | 1.0 M |
| OCF read, `deflate` | 31 MB/s | 0.8 M |
| `deflate` alone | 45 MB/s | — |
| `inflate` alone | 104 MB/s | — |

Decoding is the slower direction because every record materialises a fresh
`Value` arena — the price of a dynamically typed datum. A schema-specialised
reader (decode straight into the consumer's own struct) would skip that, and
is the obvious next step if manifest reading ever shows up in a profile.

## Test

```sh
pixi run -e stable test           # core: stable Mojo 1.0.0
pixi run -e default test          # core: nightly
pixi run -e codecs-stable test-codecs   # + snappy / zstandard
pixi run -e codecs test-codecs          # ditto, nightly
pixi run bench
pixi run -e codecs crosscheck     # our files, read by fastavro (needs uv)
```

The core suite is 40 tests; the codec suite adds 8. Both run on stable 1.0.0
and on nightly, on `osx-arm64` and `linux-64`.

## Limitations

- **Logical types are parsed, not converted.** `decimal` decodes to its
  `bytes`, `date` to its `int`, `duration` to its 12-byte `fixed`. The schema
  tells you which they are; interpreting them is the consumer's call.
- **No JSON encoding of data.** Avro's JSON *datum* encoding is not
  implemented; `Value.to_json()` is a debug rendering (unions collapse to
  their branch, `bytes` render as `"0x…"` hex), not the spec's format.
- **No single-object encoding or schema registry framing.** The Rabin
  fingerprint (`schema.fingerprint()`) is there, the `C3 01` envelope is not.
- **`bzip2` and `xz` codecs** are not implemented.
- **Resolution does not do record aliases across namespaces** beyond
  unqualified-name and `aliases` matching, and does not reorder union
  branches by name.
- **The writer emits one fixed-Huffman DEFLATE block per Avro block**, so
  `deflate` files are a little larger than zlib's (level 9) would be — about
  1.05x on the fixtures. Anything can read them.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
