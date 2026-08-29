"""The Avro binary decoder.

A `Decoder` is a cursor over a `Span[UInt8]`: the primitive readers follow
the spec's binary encoding (zigzag varints, little-endian IEEE floats,
length-prefixed bytes, block-encoded arrays and maps), and `read_value`
drives them from a parsed `Schema`.
"""

from std.memory import ArcPointer, bitcast

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
    Schema,
    kind_name,
)
from avro.value import Value, ValueNode


struct Decoder[origin: ImmOrigin](Copyable, Movable):
    """A cursor over Avro binary data."""

    var data: Span[UInt8, Self.origin]
    var pos: Int

    def __init__(out self, data: Span[UInt8, Self.origin]):
        self.data = data
        self.pos = 0

    def __init__(out self, data: Span[UInt8, Self.origin], pos: Int):
        self.data = data
        self.pos = pos

    def remaining(self) -> Int:
        return len(self.data) - self.pos

    def at_end(self) -> Bool:
        return self.pos >= len(self.data)

    def _need(self, n: Int) raises:
        if self.pos + n > len(self.data):
            raise Error(
                String(
                    "avro.Decoder: truncated input, wanted ",
                    n,
                    " byte(s) at offset ",
                    self.pos,
                    " of ",
                    len(self.data),
                )
            )

    # ── primitives ─────────────────────────────────────────────────────────

    def read_byte(mut self) raises -> UInt8:
        self._need(1)
        var b = self.data[self.pos]
        self.pos += 1
        return b

    def read_boolean(mut self) raises -> Bool:
        var b = self.read_byte()
        if b > 1:
            raise Error(String("avro.Decoder: bad boolean byte ", Int(b)))
        return b == 1

    def read_long(mut self) raises -> Int64:
        """A zigzag-encoded variable-length integer."""
        var value: UInt64 = 0
        var shift: UInt64 = 0
        while True:
            var b = self.read_byte()
            value |= (UInt64(b) & 0x7F) << shift
            if b < 0x80:
                break
            shift += 7
            if shift > 63:
                raise Error("avro.Decoder: varint longer than 10 bytes")
        return Int64((value >> 1) ^ (0 - (value & 1)))

    def read_int(mut self) raises -> Int32:
        return Int32(self.read_long())

    def read_float(mut self) raises -> Float32:
        self._need(4)
        var bits: UInt32 = 0
        for k in range(4):
            bits |= UInt32(self.data[self.pos + k]) << UInt32(8 * k)
        self.pos += 4
        return bitcast[DType.float32](bits)

    def read_double(mut self) raises -> Float64:
        self._need(8)
        var bits: UInt64 = 0
        for k in range(8):
            bits |= UInt64(self.data[self.pos + k]) << UInt64(8 * k)
        self.pos += 8
        return bitcast[DType.float64](bits)

    def read_fixed(mut self, n: Int) raises -> List[UInt8]:
        self._need(n)
        var out = List[UInt8](capacity=n)
        out.extend(self.data[self.pos : self.pos + n])
        self.pos += n
        return out^

    def read_bytes(mut self) raises -> List[UInt8]:
        var n = Int(self.read_long())
        if n < 0:
            raise Error(String("avro.Decoder: negative byte length ", n))
        return self.read_fixed(n)

    def read_string(mut self) raises -> String:
        var raw = self.read_bytes()
        return String(from_utf8_lossy=Span(raw))

    def skip(mut self, n: Int) raises:
        self._need(n)
        self.pos += n

    # ── block headers ──────────────────────────────────────────────────────

    def read_block_count(mut self) raises -> Int64:
        """Read one array/map block header.

        A negative count means the writer also wrote the block's byte size,
        which is consumed here and discarded (it is only a skip hint).
        """
        var count = self.read_long()
        if count < 0:
            _ = self.read_long()  # block size in bytes
            count = -count
        return count

    # ── schema-driven decoding ─────────────────────────────────────────────

    def read_value(mut self, schema: Schema) raises -> Value:
        """Decode one datum described by `schema`'s root."""
        return self.read_value_at(schema, schema.root)

    def read_value_at(mut self, schema: Schema, i: Int) raises -> Value:
        var arena = List[ValueNode]()
        _ = self._decode(schema, i, arena)
        return Value(ArcPointer(arena^), 0)

    def _decode(
        mut self, schema: Schema, i: Int, mut arena: List[ValueNode]
    ) raises -> Int:
        var kind = schema.kind(i)
        var here = len(arena)
        if kind == NULL:
            arena.append(ValueNode.of(NULL))
            return here
        if kind == BOOLEAN:
            var n = ValueNode.of(BOOLEAN)
            n.b = self.read_boolean()
            arena.append(n^)
            return here
        if kind == INT or kind == LONG:
            var n = ValueNode.of(kind)
            n.i = self.read_long()
            arena.append(n^)
            return here
        if kind == FLOAT:
            var n = ValueNode.of(FLOAT)
            n.d = Float64(self.read_float())
            arena.append(n^)
            return here
        if kind == DOUBLE:
            var n = ValueNode.of(DOUBLE)
            n.d = self.read_double()
            arena.append(n^)
            return here
        if kind == BYTES:
            var n = ValueNode.of(BYTES)
            n.data = self.read_bytes()
            arena.append(n^)
            return here
        if kind == STRING:
            var n = ValueNode.of(STRING)
            n.s = self.read_string()
            arena.append(n^)
            return here
        if kind == FIXED:
            var n = ValueNode.of(FIXED)
            n.data = self.read_fixed(schema.size(i))
            arena.append(n^)
            return here
        if kind == ENUM:
            var n = ValueNode.of(ENUM)
            var idx = Int(self.read_long())
            var syms = schema.symbols(i)
            if idx < 0 or idx >= len(syms):
                raise Error(String("avro.Decoder: enum index ", idx, " out of range"))
            n.i = Int64(idx)
            n.s = syms[idx]
            arena.append(n^)
            return here
        if kind == UNION:
            var branch = Int(self.read_long())
            if branch < 0 or branch >= schema.num_branches(i):
                raise Error(
                    String("avro.Decoder: union index ", branch, " out of range")
                )
            var n = ValueNode.of(UNION)
            n.i = Int64(branch)
            arena.append(n^)
            var child = self._decode_child(schema, schema.branch(i, branch), arena)
            arena[here].kids.append(child)
            return here
        if kind == RECORD:
            var n = ValueNode.of(RECORD)
            var nf = schema.num_fields(i)
            var names = List[String](capacity=nf)
            for k in range(nf):
                names.append(schema.field(i, k).name)
            n.keys = names^
            arena.append(n^)
            var kids = List[Int](capacity=nf)
            for k in range(nf):
                var c = self._decode_child(schema, schema.field_type(i, k), arena)
                kids.append(c)
            arena[here].kids = kids^
            return here
        if kind == ARRAY:
            arena.append(ValueNode.of(ARRAY))
            var items = schema.items(i)
            var kids = List[Int]()
            while True:
                var count = Int(self.read_block_count())
                if count == 0:
                    break
                for _k in range(count):
                    var c = self._decode_child(schema, items, arena)
                    kids.append(c)
            arena[here].kids = kids^
            return here
        if kind == MAP:
            arena.append(ValueNode.of(MAP))
            var values = schema.values(i)
            var keys = List[String]()
            var kids = List[Int]()
            while True:
                var count = Int(self.read_block_count())
                if count == 0:
                    break
                for _k in range(count):
                    var key = self.read_string()
                    keys.append(key)
                    var c = self._decode_child(schema, values, arena)
                    kids.append(c)
            arena[here].keys = keys^
            arena[here].kids = kids^
            return here
        raise Error(String("avro.Decoder: cannot decode kind ", kind_name(kind)))

    def _decode_child(
        mut self, schema: Schema, i: Int, mut arena: List[ValueNode]
    ) raises -> Int:
        return self._decode(schema, i, arena)

    # ── skipping ───────────────────────────────────────────────────────────

    def skip_value(mut self, schema: Schema, i: Int) raises:
        """Advance past one datum without materialising it."""
        var kind = schema.kind(i)
        if kind == NULL:
            return
        if kind == BOOLEAN:
            _ = self.read_byte()
        elif kind == INT or kind == LONG:
            _ = self.read_long()
        elif kind == FLOAT:
            self.skip(4)
        elif kind == DOUBLE:
            self.skip(8)
        elif kind == BYTES or kind == STRING:
            var n = Int(self.read_long())
            self.skip(n)
        elif kind == FIXED:
            self.skip(schema.size(i))
        elif kind == ENUM:
            _ = self.read_long()
        elif kind == UNION:
            var b = Int(self.read_long())
            self._skip_child(schema, schema.branch(i, b))
        elif kind == RECORD:
            for k in range(schema.num_fields(i)):
                self._skip_child(schema, schema.field_type(i, k))
        elif kind == ARRAY:
            while True:
                var count = Int(self.read_block_count())
                if count == 0:
                    break
                for _k in range(count):
                    self._skip_child(schema, schema.items(i))
        elif kind == MAP:
            while True:
                var count = Int(self.read_block_count())
                if count == 0:
                    break
                for _k in range(count):
                    var n = Int(self.read_long())
                    self.skip(n)
                    self._skip_child(schema, schema.values(i))
        else:
            raise Error(String("avro.Decoder: cannot skip kind ", kind_name(kind)))

    def _skip_child(mut self, schema: Schema, i: Int) raises:
        self.skip_value(schema, i)
