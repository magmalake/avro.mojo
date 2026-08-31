# avro.mojo

[![mojoshelf](https://mojoshelf.org/badge/avro-mojo.svg)](https://mojoshelf.org/tins/avro-mojo) [![mojo nightly](https://mojoshelf.org/badge/avro-mojo/nightly.svg)](https://mojoshelf.org/tins/avro-mojo)

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

There are two readers. `Value` above is the general one: a dynamically typed
datum, good for anything. When the file is large and its schema is known,
`RecordCursor` compiles that schema into a decode plan once and reads records
into buffers it reuses — about **eleven times faster**, and with no
allocation per record at all:

```mojo
from avro import RecordCursor

var c = RecordCursor.open("manifest.avro", ["manifest_path", "added_snapshot_id"])
var path = c.plan.slot_of("manifest_path")
var snap = c.plan.slot_of("added_snapshot_id")
while c.next():
    print(c.get_str(path), c.get_long(snap))
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
  `reader.metadata_keys` / `metadata_vals` expose the raw bytes;
  `reader.metadata_string(key)` the text.
- **Opening five hundred small files is its own problem.** They all carry the
  same schema, so `PlanCache` parses and compiles it once — see
  [One plan, many files](#recordcursor).
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
    RecordCursor, DecodePlan,      # the schema-compiled reader
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

### RecordCursor

A `Value` is convenient and costs about sixty heap allocations per record.
`RecordCursor` trades that convenience for a decode **plan**: the schema is
compiled once into a flat program of ops, every value the plan keeps lands in
a numbered **slot**, and the slot buffers are reused from record to record.
The hot path allocates nothing.

```mojo
var c = RecordCursor.open("manifest.avro")          # or .from_bytes / .of_bytes
var path  = c.plan.slot_of("data_file.file_path")   # name lookup, once
var rows  = c.plan.slot_of("data_file.record_count")
while c.next():
    print(c.get_str(path), c.get_long(rows))
```

**Slots are paths.** Nesting is flattened with dots. A record field is
`parent.child`; an array's elements live under `path.element` and a map's
under `path.key` / `path.value`. `plan.slot_of(path)` raises for a path the
schema does not have, `plan.try_slot(path)` returns -1 — which is how a
reader stays version-tolerant. `plan.paths()` lists them all.

**Repeated fields.** The array's own slot holds the element count; a slot
inside the array holds one value per element, addressed by the accessors'
second argument:

```mojo
var lb  = c.plan.slot_of("data_file.lower_bounds")
var key = c.plan.slot_of("data_file.lower_bounds.element.key")
var val = c.plan.slot_of("data_file.lower_bounds.element.value")
for k in range(c.array_len(lb)):
    print(c.get_long(key, k), c.get_bytes(val, k))
```

**Selection.** `RecordCursor.open(path, ["a", "b.c"])` keeps only those paths
and everything under them. The fields nobody asked for still have ops — the
decoder has to step over their bytes — but they get no slot and nothing is
stored, which is worth another ~40%.

**Accessors.** `get_bool`, `get_long`, `get_int`, `get_double`, `get_float`,
`get_str`, `get_string`, `get_bytes`, `get_bytes_copy`, `get_symbol`,
`enum_index`, `union_branch`, `array_len`, `is_null`, `count`. `get_str` and
`get_bytes` are **views into the block buffer the cursor is holding**: no
copy, and the compiler ties them to the cursor so one cannot outlive it. They
are valid until the next `next()`; `get_string` / `get_bytes_copy` when the
value has to live longer.

**A union** `["null", T]` is one slot — `is_null(slot)` answers it. Any other
union also gets a `path.$branch` slot, read with `union_branch`. A record is
given a slot only where it can be absent, which is as a union branch: for
`["null", record]` the record's own path answers `is_null`, and a record
reached from a field is always there and costs nothing. Whichever branch the
data takes, the union also writes a null into the slots its *other* branches
own — without that, a `["null", record]` inside an array would leave the
element after a missing one reading the wrong value.

**The core suite is the oracle.** `tests/test_cursor.mojo` decodes the same
files both ways and compares field for field, including files where an
optional record is present in some rows and absent in others, and absent in
the middle of an array.

**One plan, many files.** A scan planner opens hundreds of files written by
one writer, each carrying a byte-identical copy of the same schema, and
parsing that JSON and compiling it is most of what opening a small Avro file
costs. `PlanCache` is keyed on the raw `avro.schema` **bytes, before they are
parsed** — byte equality is the cheap sufficient condition — and hands out a
plan rather than compiling one:

```mojo
var cache = PlanCache()                     # caller-owned; there is no global
for k in range(len(paths)):
    var c = RecordCursor.open_cached(paths[k], cache)
    while c.next():
        ...
print(cache.hits, "hits,", cache.misses, "misses")
```

`open_cached`, `of_bytes_cached` and `of_cached` are the three cached
openers; the selection list is part of the key, because it changes the plan.
On a hit the file's schema is never parsed at all — the reader is opened with
`defer_schema=True` and only reads its header. The hash over the key bytes is
only a *filter*: a hit is confirmed by comparing the bytes themselves, so a
collision costs a memcmp and never a wrong answer, which
`test_a_hash_collision_is_caught_by_the_bytes` proves by forcing every key to
hash to zero. `PlanCache.disabled()` turns the whole thing off without
changing the code path.

A `DecodePlan` is a **shared handle**: copying one bumps a refcount rather
than duplicating a few hundred slot paths, and a cursor holds its own
reference, so a cached plan outlives every cursor built from it no matter
what order they are destroyed in.

**Resolution** is done at plan-build time, so it costs nothing per record:

```mojo
var rr = resolve(writer_schema, reader_schema)
var c = RecordCursor.resolved(DataFileReader.open("f.avro"), rr^)
```

Promotions, field reordering, writer fields the reader dropped, enum
remapping, union narrowing and scalar defaults are all baked into the plan. A
default that is itself a record, array or map is the one case the cursor
refuses — read that file through `Value`.

### Object Container Files

```mojo
var w = DataFileWriter(parse_schema(text), "deflate")
w.set_metadata_string("app", "avro.mojo")
w.append(row)
var raw = w.bytes()          # or w.save("out.avro")

var r = DataFileReader.from_bytes(Span(raw))
r.schema          # parsed avro.schema
r.codec           # avro.codec
r.metadata_keys   # every key in the order the file wrote them, avro.* included
r.metadata_vals   # the values, parallel to the keys
r.sync_marker     # the file's 16 bytes

r.metadata_index("schema")      # -1 when absent; no String is built to ask
r.metadata_string("schema")     # the entry as text
r.metadata_map()                # Dict[String, List[UInt8]], for that shape
```

The metadata is two parallel lists rather than a `Dict`: a file has a handful
of entries, so a linear scan over `StringSlice`s beats hashing — and, unlike
a `Dict` lookup, asking for one does not have to allocate a `String` first.

The writer starts a new block every `sync_interval` uncompressed bytes
(64 000 by default) and writes the sync marker after each one; the reader
verifies it. `w.set_sync_marker(...)` pins the marker for reproducible files,
and `w.set_schema_json(text)` writes an exact schema text as `avro.schema`
instead of the printed form — for a writer that wants its manifests to match
another implementation's bytes.

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

### Bringing your own

`CodecSet` is a plain trait, and `DataFileReader` / `DataFileWriter` are
parametrised on it, so a consumer can substitute any implementation of any
codec — including `deflate`:

```mojo
struct MyCodecs(CodecSet):
    @staticmethod
    def supports(name: StringSlice) -> Bool:
        return not name or name == "null" or name == "deflate"

    @staticmethod
    def decompress(name: StringSlice, data: Span[UInt8, _]) raises -> List[UInt8]:
        if name == "deflate":
            return my_zlib_inflate(data)          # raw DEFLATE, no header
        return DefaultCodecs.decompress(name, data)

    @staticmethod
    def compress(name: StringSlice, data: Span[UInt8, _]) raises -> List[UInt8]:
        return DefaultCodecs.compress(name, data)

var r = DataFileReader[MyCodecs].open("manifest.avro")
var c = RecordCursor[MyCodecs].open("manifest.avro")
```

Two tests exercise exactly this seam in both directions: a `CodecSet` whose
`deflate` emits RFC 1951 *stored* blocks writes a file the built-in codec
reads, and reads a file the built-in codec wrote.

**This repo does not ship a zlib-backed `deflate`, on purpose.** It used to
be the obvious thing to add — `inflate` was 104 MB/s and every gzip manifest
paid it. At 860 MB/s it is no longer what makes a manifest read slow: in a
500-manifest Iceberg scan the decompression does not show up at all next to
opening the files and parsing their schemas. Trading "no dependencies at all"
for the remaining ~2.5x against the platform's zlib is not a good deal for
this library — but it might be for a consumer with a different profile, and
the seam above is how they take it.

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

Apple M4, one core, `osx-arm64`, stable Mojo 1.0.0. MB/s is always against
the **uncompressed** datum stream, so the `null` and `deflate` rows compare.

### `Value` against `RecordCursor`

`pixi run bench` writes both files and reads each three ways;
`pixi run bench-fastavro` then reads the very same files with
[fastavro](https://github.com/fastavro/fastavro) 1.12.2, whose core is
Cython — the honest reference, not a Python strawman.

100 000 records of a manifest-shaped record (two longs, a path-like string, a
double, an optional long):

| reader | `null` | `deflate` |
|---|---|---|
| `Value` | 82 MB/s, 1.74 M rows/s | 78 MB/s, 1.65 M rows/s |
| **`RecordCursor`** | **912 MB/s, 19.2 M rows/s** | **502 MB/s, 10.6 M rows/s** |
| `RecordCursor`, 1 of 5 fields selected | 1302 MB/s, 27.5 M rows/s | 602 MB/s, 12.7 M rows/s |
| fastavro | 83 MB/s, 1.74 M rows/s | 81 MB/s, 1.71 M rows/s |

100 000 real Iceberg `manifest_entry` records — the fixture's own schema and
entries, nested `data_file`, four metric maps, partition struct and all:

| reader | `null` | `deflate` |
|---|---|---|
| `Value` | 42 MB/s, 126 k rows/s | 41 MB/s, 122 k rows/s |
| **`RecordCursor`** | **444 MB/s, 1.31 M rows/s** | **368 MB/s, 1.08 M rows/s** |
| `RecordCursor`, 3 fields selected | 612 MB/s, 1.80 M rows/s | 471 MB/s, 1.39 M rows/s |
| fastavro | 35 MB/s, 103 k rows/s | 35 MB/s, 103 k rows/s |

So the cursor is **10-11x** the `Value` path on either shape, 14-16x with a
selection, and 11-13x fastavro. The `Value` path itself is already at
fastavro's speed — which is the honest reason the cursor exists. There was no
more room inside a dynamically typed datum; the win had to come from not
building one.

### The rest

`pixi run bench` — 100 000 six-field records (a long, a string, a double, a
boolean, a two-element array and an optional string), 4.0 MiB of datum stream:

| operation | throughput | rows/s |
|---|---|---|
| encode (datum stream) | 215 MB/s | 5.5 M |
| decode (datum stream) | 46 MB/s | 1.2 M |
| OCF write, `null` | 233 MB/s | 5.9 M |
| OCF write, `deflate` | 40 MB/s | 1.0 M |
| `deflate` alone | 48 MB/s | — |
| **`inflate` alone** | **860-1000 MB/s** | — |

`inflate` used to be 104 MB/s — a bit-at-a-time "puff" decoder. It now reads
Huffman codes out of a 512-entry table, refills a 64-bit bit buffer with one
unaligned eight-byte read, and writes into a window it keeps valid to
capacity. For scale, the platform's own zlib inflates the same stream at
about 2200 MB/s, so a `CodecSet` backed by FFI still has something to offer
(see **Codecs**) — but the pure-Mojo path is no longer the reason a manifest
read is slow.

Writing is still the slower direction for `deflate`: the encoder is a
single-slot hash-chain matcher emitting fixed-Huffman blocks, and nothing
here has needed it to be faster yet.

### Opening many small files

Reading a 4 KB Avro file is not the same problem as reading a big one: the
per-record cost is nothing and the *fixed* cost is everything. In
[iceberg.mojo](https://github.com/magmalake/iceberg.mojo), planning a scan
over 500 manifests written by one writer used to spend about 114 µs per
manifest before decoding a single entry, most of it parsing 500
byte-identical copies of the same 5 KB `avro.schema` and recompiling the same
plan. With a `PlanCache` across those 500 files that falls to **32 µs**, of
which 11 µs is the `open`/`read`/`close` itself:

| 500 manifests, 4 entries each | fixed per manifest | wall |
|---|---|---|
| a fresh plan per file | 114 µs | 62.3 ms |
| **one `PlanCache`** | **32 µs** | **21.4 ms** |

A hit costs a hash and a memcmp over the schema bytes and a refcount bump.

## Test

```sh
pixi run -e stable test           # core: stable Mojo 1.0.0
pixi run -e stable test-cursor    # the schema-compiled reader
pixi run -e default test          # core: nightly
pixi run -e default test-cursor
pixi run -e codecs-stable test-codecs   # + snappy / zstandard
pixi run -e codecs test-codecs          # ditto, nightly
pixi run bench
pixi run bench-fastavro           # the same files, read by fastavro (needs uv)
pixi run -e codecs crosscheck     # our files, read by fastavro (needs uv)
```

The core suite is 41 tests, the cursor suite 26, the codec suite 8. All run on
stable 1.0.0 and on nightly, on `osx-arm64` and `linux-64`.

The cursor suite's oracle is the `Value` path: both readers decode the same
files and every field is compared, over every Avro type on the fastavro
fixtures and over two real Iceberg manifests. It also asserts the
no-allocation promise — a second pass over the same bytes must not move
`slot_watermark()`, which only changes when a slot buffer grows.

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
- **A `RecordCursor` slot span is 32-bit**, so a single decompressed block
  wider than 2 GiB is refused (Avro blocks are kilobytes; the default sync
  interval is 64 000 bytes). A container-typed schema default cannot be
  filled by the cursor either. Both raise rather than mislead, and the
  `Value` path reads such a file fine.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
