"""avro.mojo — Apache Avro in pure Mojo.

Part of magmalake: data lake building blocks in Mojo.

```mojo
from avro import DataFileReader

var r = DataFileReader.open("manifest.avro")
while r.has_next():
    var rec = r.next()
    print(rec.field("manifest_path").as_string())
```

`Value` is the general reader. When the schema is known and the file is
large — a scan planner walking manifests, say — `RecordCursor` compiles the
schema into a decode plan once and then reads records into reused buffers,
about ten times faster and with no per-record allocation:

```mojo
from avro import RecordCursor

var c = RecordCursor.open("manifest.avro", ["manifest_path"])
var path = c.plan.slot_of("manifest_path")
while c.next():
    print(c.get_str(path))
```

Everything reachable from this module is dependency-free (the `null` and
`deflate` block codecs are implemented in this repo). `snappy` and
`zstandard` live in `avro.ext_snappy` / `avro.ext_zstd`, which pull in the
sibling `snappy.mojo` and `zstd.mojo` tins.
"""

from avro.codec import CodecSet, DefaultCodecs, unknown_codec
from avro.crc32 import crc32
from avro.datafile import (
    DEFAULT_SYNC_INTERVAL,
    SYNC_SIZE,
    DataFileReader,
    DataFileWriter,
    random_sync_marker,
    read_file_bytes,
    write_file_bytes,
)
from avro.cursor import (
    DecodePlan,
    PlanCache,
    PlanData,
    PlanOp,
    RecordCursor,
    SlotInfo,
    SlotVal,
    schema_hash,
)
from avro.decoder import Decoder
from avro.deflate import deflate, inflate
from avro.encoder import Encoder, encode_value
from avro.json import JsonDoc, parse_json
from avro.resolve import ResolvedReader, resolve
from avro.schema import (
    ARRAY,
    BOOLEAN,
    BYTES,
    DOUBLE,
    ENUM,
    FIXED,
    FLOAT,
    INT,
    LONG,
    MAP,
    NULL,
    RECORD,
    STRING,
    UNION,
    Field,
    Props,
    Schema,
    SchemaNode,
    crc64_avro,
    is_named,
    is_primitive,
    kind_name,
    parse_schema,
    primitive_kind,
)
from avro.value import ArrayBuilder, MapBuilder, RecordBuilder, Value, ValueNode
