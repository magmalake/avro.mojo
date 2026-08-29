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
| `null` | built in | — |
| `deflate` | built in (`avro.deflate`, RFC 1951, both directions) | — |

`deflate` is implemented here rather than bound to zlib so the core library
stays dependency-free: `inflate` reads all three block types (stored,
fixed-Huffman, dynamic-Huffman) and `deflate` emits fixed-Huffman blocks from
a hash-chain LZ77 matcher.

## Test

```sh
pixi run -e stable test    # stable Mojo 1.0.0
pixi run -e default test   # nightly
```

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
