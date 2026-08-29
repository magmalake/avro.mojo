"""Avro Object Container Files — the reader and the writer.

An OCF is `Obj\\x01`, a `map<bytes>` of file metadata (carrying at least
`avro.schema`, and usually `avro.codec`), a 16-byte sync marker, and then a
run of blocks, each `count · size · data · sync`.

Apache Iceberg stores its own keys in that same metadata map — `schema`,
`partition-spec`, `format-version`, `content` — so `metadata` is exposed as
raw bytes and `metadata_string` as text, alongside the parsed `avro.schema`.
"""

from std.collections import Dict
from std.random import random_ui64

from avro.codec import CodecSet, DefaultCodecs
from avro.decoder import Decoder
from avro.encoder import Encoder
from avro.schema import Schema, parse_schema
from avro.value import Value

comptime MAGIC0: UInt8 = 79  # 'O'
comptime MAGIC1: UInt8 = 98  # 'b'
comptime MAGIC2: UInt8 = 106  # 'j'
comptime MAGIC3: UInt8 = 1
comptime SYNC_SIZE: Int = 16
comptime DEFAULT_SYNC_INTERVAL: Int = 64000


def read_file_bytes(path: StringSlice) raises -> List[UInt8]:
    var f = open(path, "r")
    var data = f.read_bytes()
    f.close()
    return data^


def write_file_bytes(path: StringSlice, data: Span[UInt8, _]) raises:
    var f = open(path, "w")
    f.write_bytes(data)
    f.close()


# ── reader ─────────────────────────────────────────────────────────────────


struct DataFileReader[C: CodecSet = DefaultCodecs](Copyable, Movable):
    """Reads an Avro Object Container File held in memory."""

    var data: List[UInt8]
    var schema: Schema
    var metadata: Dict[String, List[UInt8]]
    var metadata_keys: List[String]
    """Metadata keys in the order the file wrote them."""
    var codec: String
    var sync_marker: List[UInt8]
    var pos: Int
    """Offset of the next block header in `data`."""
    var block: List[UInt8]
    var block_pos: Int
    var block_left: Int
    var block_index: Int

    def __init__(out self, var data: List[UInt8]) raises:
        self.data = data^
        self.schema = Schema()
        self.metadata = Dict[String, List[UInt8]]()
        self.metadata_keys = List[String]()
        self.codec = String("null")
        self.sync_marker = List[UInt8]()
        self.pos = 0
        self.block = List[UInt8]()
        self.block_pos = 0
        self.block_left = 0
        self.block_index = -1
        self._read_header()

    def __init__(out self, *, copy: Self):
        self.data = copy.data.copy()
        self.schema = copy.schema.copy()
        self.metadata = copy.metadata.copy()
        self.metadata_keys = copy.metadata_keys.copy()
        self.codec = copy.codec
        self.sync_marker = copy.sync_marker.copy()
        self.pos = copy.pos
        self.block = copy.block.copy()
        self.block_pos = copy.block_pos
        self.block_left = copy.block_left
        self.block_index = copy.block_index

    def __init__(out self, *, deinit move: Self):
        self.data = move.data^
        self.schema = move.schema^
        self.metadata = move.metadata^
        self.metadata_keys = move.metadata_keys^
        self.codec = move.codec^
        self.sync_marker = move.sync_marker^
        self.pos = move.pos
        self.block = move.block^
        self.block_pos = move.block_pos
        self.block_left = move.block_left
        self.block_index = move.block_index

    @staticmethod
    def open(path: StringSlice) raises -> Self:
        """Read the whole file, then parse its header."""
        return Self(read_file_bytes(path))

    @staticmethod
    def from_bytes(data: Span[UInt8, _]) raises -> Self:
        var owned = List[UInt8](capacity=len(data))
        owned.extend(data)
        return Self(owned^)

    def _read_header(mut self) raises:
        var d = Decoder(Span(self.data))
        if len(self.data) < 4:
            raise Error("avro.DataFileReader: file is too short to be an OCF")
        if (
            self.data[0] != MAGIC0
            or self.data[1] != MAGIC1
            or self.data[2] != MAGIC2
            or self.data[3] != MAGIC3
        ):
            raise Error("avro.DataFileReader: bad magic, not an Avro container file")
        d.pos = 4
        while True:
            var count = Int(d.read_block_count())
            if count == 0:
                break
            for _k in range(count):
                var key = d.read_string()
                var val = d.read_bytes()
                self.metadata_keys.append(key)
                self.metadata[key] = val^
        self.sync_marker = d.read_fixed(SYNC_SIZE)
        self.pos = d.pos
        if "avro.schema" not in self.metadata:
            raise Error("avro.DataFileReader: file metadata has no 'avro.schema'")
        ref raw = self.metadata["avro.schema"]
        self.schema = parse_schema(StringSlice(unsafe_from_utf8=Span(raw)))
        if "avro.codec" in self.metadata:
            ref c = self.metadata["avro.codec"]
            self.codec = String(from_utf8_lossy=Span(c))
        if not self.codec:
            self.codec = String("null")
        if not Self.C.supports(self.codec):
            raise Error(
                String(
                    "avro.DataFileReader: codec '",
                    self.codec,
                    "' is not in the CodecSet this reader was built with",
                )
            )

    # ── metadata ───────────────────────────────────────────────────────────

    def metadata_string(self, key: StringSlice) raises -> String:
        """A metadata entry decoded as UTF-8; "" when the key is absent."""
        var k = String(key)
        if k not in self.metadata:
            return String()
        ref raw = self.metadata[k]
        return String(from_utf8_lossy=Span(raw))

    def has_metadata(self, key: StringSlice) -> Bool:
        return String(key) in self.metadata

    def schema_json(self) raises -> String:
        """The schema exactly as the file spelled it."""
        return self.metadata_string("avro.schema")

    # ── iteration ──────────────────────────────────────────────────────────

    def _load_block(mut self) raises -> Bool:
        if self.pos >= len(self.data):
            return False
        var d = Decoder(Span(self.data), self.pos)
        var count = Int(d.read_long())
        var size = Int(d.read_long())
        if size < 0 or d.pos + size + SYNC_SIZE > len(self.data):
            raise Error("avro.DataFileReader: truncated data block")
        var raw = Span(self.data)[d.pos : d.pos + size]
        self.block = Self.C.decompress(self.codec, raw)
        d.pos += size
        var sync = d.read_fixed(SYNC_SIZE)
        for k in range(SYNC_SIZE):
            if sync[k] != self.sync_marker[k]:
                raise Error(
                    "avro.DataFileReader: sync marker mismatch after a data block"
                )
        self.pos = d.pos
        self.block_pos = 0
        self.block_left = count
        self.block_index += 1
        return True

    def has_next(mut self) raises -> Bool:
        while self.block_left == 0:
            if not self._load_block():
                return False
        return True

    def next(mut self) raises -> Value:
        """Decode the next record; raises when the file is exhausted."""
        if not self.has_next():
            raise Error("avro.DataFileReader: no more records")
        var d = Decoder(Span(self.block), self.block_pos)
        var v = d.read_value(self.schema)
        self.block_pos = d.pos
        self.block_left -= 1
        return v^

    def read_all(mut self) raises -> List[Value]:
        var out = List[Value]()
        while self.has_next():
            out.append(self.next())
        return out^

    def count_blocks(self) raises -> Int:
        """Walk every block header, without decompressing anything.

        Independent of how far iteration has got: it re-reads the header and
        then strides over the blocks.
        """
        var d = Decoder(Span(self.data))
        d.pos = 4
        while True:
            var count = Int(d.read_block_count())
            if count == 0:
                break
            for _k in range(count):
                _ = d.read_string()
                _ = d.read_bytes()
        _ = d.read_fixed(SYNC_SIZE)
        var blocks = 0
        while d.pos < len(self.data):
            _ = d.read_long()
            var size = Int(d.read_long())
            d.skip(size + SYNC_SIZE)
            blocks += 1
        return blocks


# ── writer ─────────────────────────────────────────────────────────────────


def random_sync_marker() -> List[UInt8]:
    var out = List[UInt8](capacity=SYNC_SIZE)
    for _k in range(2):
        var r = random_ui64(0, UInt64.MAX)
        for b in range(8):
            out.append(UInt8((r >> UInt64(8 * b)) & 0xFF))
    return out^


struct DataFileWriter[C: CodecSet = DefaultCodecs](Copyable, Movable):
    """Builds an Avro Object Container File in memory."""

    var schema: Schema
    var codec: String
    var sync_marker: List[UInt8]
    var sync_interval: Int
    """Approximate uncompressed bytes per block before an automatic flush."""
    var meta_keys: List[String]
    var meta_vals: List[List[UInt8]]
    var out: List[UInt8]
    var buf: Encoder
    var block_count: Int
    var started: Bool
    var closed: Bool
    var schema_json: String
    """What to write as `avro.schema`; the printed schema unless overridden."""

    def __init__(
        out self,
        var schema: Schema,
        codec: StringSlice = "null",
        sync_interval: Int = DEFAULT_SYNC_INTERVAL,
    ) raises:
        if not Self.C.supports(codec):
            raise Error(
                String(
                    "avro.DataFileWriter: codec '",
                    codec,
                    "' is not in the CodecSet this writer was built with",
                )
            )
        self.schema = schema^
        self.codec = String(codec)
        self.sync_marker = random_sync_marker()
        self.sync_interval = sync_interval
        self.meta_keys = List[String]()
        self.meta_vals = List[List[UInt8]]()
        self.out = List[UInt8]()
        self.buf = Encoder()
        self.block_count = 0
        self.started = False
        self.closed = False
        self.schema_json = String()

    def __init__(out self, *, copy: Self):
        self.schema = copy.schema.copy()
        self.codec = copy.codec
        self.sync_marker = copy.sync_marker.copy()
        self.sync_interval = copy.sync_interval
        self.meta_keys = copy.meta_keys.copy()
        self.meta_vals = copy.meta_vals.copy()
        self.out = copy.out.copy()
        self.buf = copy.buf.copy()
        self.block_count = copy.block_count
        self.started = copy.started
        self.closed = copy.closed
        self.schema_json = copy.schema_json

    def __init__(out self, *, deinit move: Self):
        self.schema = move.schema^
        self.codec = move.codec^
        self.sync_marker = move.sync_marker^
        self.sync_interval = move.sync_interval
        self.meta_keys = move.meta_keys^
        self.meta_vals = move.meta_vals^
        self.out = move.out^
        self.buf = move.buf^
        self.block_count = move.block_count
        self.started = move.started
        self.closed = move.closed
        self.schema_json = move.schema_json^

    def set_sync_marker(mut self, marker: Span[UInt8, _]) raises:
        """Pin the sync marker — the tests need reproducible files."""
        if len(marker) != SYNC_SIZE:
            raise Error("avro.DataFileWriter: a sync marker is exactly 16 bytes")
        if self.started:
            raise Error("avro.DataFileWriter: the header is already written")
        self.sync_marker = List[UInt8](capacity=SYNC_SIZE)
        self.sync_marker.extend(marker)

    def set_metadata(mut self, key: StringSlice, value: Span[UInt8, _]) raises:
        """Add a file-level metadata entry. `avro.*` keys are reserved."""
        if self.started:
            raise Error("avro.DataFileWriter: the header is already written")
        var k = String(key)
        if k == "avro.schema" or k == "avro.codec":
            raise Error(String("avro.DataFileWriter: '", k, "' is written for you"))
        var v = List[UInt8](capacity=len(value))
        v.extend(value)
        for i in range(len(self.meta_keys)):
            if self.meta_keys[i] == k:
                self.meta_vals[i] = v^
                return
        self.meta_keys.append(k^)
        self.meta_vals.append(v^)

    def set_metadata_string(mut self, key: StringSlice, value: StringSlice) raises:
        self.set_metadata(key, value.as_bytes())

    def set_schema_json(mut self, text: StringSlice) raises:
        """Write `text` verbatim as `avro.schema`.

        `to_json()` prints an equivalent schema, not a byte-identical one.
        Apache Iceberg writers that want their manifests to match another
        implementation's bytes can hand over the exact JSON here; it is the
        caller's job to keep it equivalent to the schema being written.
        """
        if self.started:
            raise Error("avro.DataFileWriter: the header is already written")
        self.schema_json = String(text)

    def _write_header(mut self) raises:
        var e = Encoder()
        e.out.append(MAGIC0)
        e.out.append(MAGIC1)
        e.out.append(MAGIC2)
        e.out.append(MAGIC3)
        var schema_json = (
            self.schema_json if self.schema_json else self.schema.to_json()
        )
        var n = len(self.meta_keys) + 2
        e.write_long(Int64(n))
        e.write_string("avro.schema")
        e.write_bytes(schema_json.as_bytes())
        e.write_string("avro.codec")
        e.write_bytes(self.codec.as_bytes())
        for i in range(len(self.meta_keys)):
            e.write_string(self.meta_keys[i])
            e.write_bytes(Span(self.meta_vals[i]))
        e.write_long(0)
        e.write_fixed(Span(self.sync_marker))
        self.out.extend(Span(e.out))
        self.started = True

    def append(mut self, value: Value) raises:
        if self.closed:
            raise Error("avro.DataFileWriter: writer is closed")
        if not self.started:
            self._write_header()
        self.buf.write_value(self.schema, value)
        self.block_count += 1
        if len(self.buf) >= self.sync_interval:
            self.flush()

    def flush(mut self) raises:
        """Close off the current block, if it has anything in it."""
        if not self.started:
            self._write_header()
        if self.block_count == 0:
            return
        var body = Self.C.compress(self.codec, Span(self.buf.out))
        var head = Encoder()
        head.write_long(Int64(self.block_count))
        head.write_long(Int64(len(body)))
        self.out.extend(Span(head.out))
        self.out.extend(Span(body))
        self.out.extend(Span(self.sync_marker))
        self.buf.reset()
        self.block_count = 0

    def close(mut self) raises:
        if self.closed:
            return
        self.flush()
        self.closed = True

    def bytes(mut self) raises -> List[UInt8]:
        """Finish the file and hand back its bytes."""
        self.close()
        return self.out.copy()

    def save(mut self, path: StringSlice) raises:
        self.close()
        write_file_bytes(path, Span(self.out))
