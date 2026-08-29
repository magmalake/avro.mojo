"""The Avro binary encoder — the inverse of `avro.decoder`."""

from std.memory import bitcast

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
from avro.value import Value


struct Encoder(Copyable, Movable, Defaultable, Sized):
    """Appends Avro binary data to an owned `List[UInt8]`."""

    var out: List[UInt8]

    def __init__(out self):
        self.out = List[UInt8]()

    def __init__(out self, capacity: Int):
        self.out = List[UInt8](capacity=capacity)

    def __init__(out self, *, copy: Self):
        self.out = copy.out.copy()

    def __init__(out self, *, deinit move: Self):
        self.out = move.out^

    def __len__(self) -> Int:
        return len(self.out)

    def take(deinit self) -> List[UInt8]:
        return self.out^

    def bytes(self) -> List[UInt8]:
        return self.out.copy()

    def reset(mut self):
        self.out.clear()

    # ── primitives ─────────────────────────────────────────────────────────

    def write_boolean(mut self, v: Bool):
        self.out.append(UInt8(1) if v else UInt8(0))

    def write_long(mut self, v: Int64):
        """Zigzag varint."""
        var n = (UInt64(v) << 1) ^ UInt64(v >> 63)
        while n >= 0x80:
            self.out.append(UInt8((n & 0x7F) | 0x80))
            n >>= 7
        self.out.append(UInt8(n))

    def write_int(mut self, v: Int32):
        self.write_long(Int64(v))

    def write_float(mut self, v: Float32):
        var bits = bitcast[DType.uint32](v)
        for k in range(4):
            self.out.append(UInt8((bits >> UInt32(8 * k)) & 0xFF))

    def write_double(mut self, v: Float64):
        var bits = bitcast[DType.uint64](v)
        for k in range(8):
            self.out.append(UInt8((bits >> UInt64(8 * k)) & 0xFF))

    def write_fixed(mut self, v: Span[UInt8, _]):
        self.out.extend(v)

    def write_bytes(mut self, v: Span[UInt8, _]):
        self.write_long(Int64(len(v)))
        self.out.extend(v)

    def write_string(mut self, v: StringSlice):
        self.write_bytes(v.as_bytes())

    # ── schema-driven encoding ─────────────────────────────────────────────

    def write_value(mut self, schema: Schema, value: Value) raises:
        self.write_value_at(schema, schema.root, value)

    def write_value_at(mut self, schema: Schema, i: Int, value: Value) raises:
        var kind = schema.kind(i)
        if kind == NULL:
            return
        if kind == BOOLEAN:
            self.write_boolean(value.as_bool())
        elif kind == INT:
            self.write_long(value.as_long())
        elif kind == LONG:
            self.write_long(value.as_long())
        elif kind == FLOAT:
            self.write_float(value.as_float())
        elif kind == DOUBLE:
            self.write_double(value.as_double())
        elif kind == BYTES:
            self.write_bytes(Span(value.as_bytes()))
        elif kind == STRING:
            self.write_string(value.as_string())
        elif kind == FIXED:
            var raw = value.as_bytes()
            if len(raw) != schema.size(i):
                raise Error(
                    String(
                        "avro.Encoder: fixed '",
                        schema.name(i),
                        "' wants ",
                        schema.size(i),
                        " bytes, got ",
                        len(raw),
                    )
                )
            self.write_fixed(Span(raw))
        elif kind == ENUM:
            var syms = schema.symbols(i)
            var idx = value.enum_index()
            var sym = value.symbol()
            if sym:
                idx = -1
                for k in range(len(syms)):
                    if syms[k] == sym:
                        idx = k
                        break
            if idx < 0 or idx >= len(syms):
                raise Error(
                    String("avro.Encoder: '", sym, "' is not a symbol of ", schema.name(i))
                )
            self.write_long(Int64(idx))
        elif kind == UNION:
            var branch: Int
            var inner: Value
            if value.kind() == UNION:
                branch = value.union_index()
                inner = value.unwrap()
            else:
                branch = _match_branch(schema, i, value)
                inner = value.copy()
            if branch < 0 or branch >= schema.num_branches(i):
                raise Error("avro.Encoder: no matching union branch for the value")
            self.write_long(Int64(branch))
            self._write_child(schema, schema.branch(i, branch), inner)
        elif kind == RECORD:
            for k in range(schema.num_fields(i)):
                var f = schema.field(i, k)
                var pos = value.index_of(f.name)
                if pos < 0:
                    raise Error(
                        String(
                            "avro.Encoder: record '",
                            schema.name(i),
                            "' is missing field '",
                            f.name,
                            "'",
                        )
                    )
                self._write_child(schema, f.type_index, value.at(pos))
        elif kind == ARRAY:
            var n = len(value)
            if n:
                self.write_long(Int64(n))
                for k in range(n):
                    self._write_child(schema, schema.items(i), value.at(k))
            self.write_long(0)
        elif kind == MAP:
            var n = len(value)
            if n:
                self.write_long(Int64(n))
                for k in range(n):
                    self.write_string(value.key(k))
                    self._write_child(schema, schema.values(i), value.at(k))
            self.write_long(0)
        else:
            raise Error(String("avro.Encoder: cannot encode kind ", kind_name(kind)))

    def _write_child(mut self, schema: Schema, i: Int, value: Value) raises:
        self.write_value_at(schema, i, value)


def _match_branch(schema: Schema, i: Int, value: Value) -> Int:
    """Pick the union branch that fits a value carrying no branch index."""
    var vk = value.kind()
    for k in range(schema.num_branches(i)):
        if schema.kind(schema.branch(i, k)) == vk:
            return k
    # Numeric and string/bytes values are interchangeable enough to retry.
    for k in range(schema.num_branches(i)):
        var bk = schema.kind(schema.branch(i, k))
        if (vk == INT or vk == LONG) and (
            bk == INT or bk == LONG or bk == FLOAT or bk == DOUBLE
        ):
            return k
        if (vk == FLOAT or vk == DOUBLE) and (bk == FLOAT or bk == DOUBLE):
            return k
        if (vk == STRING or vk == BYTES) and (
            bk == STRING or bk == BYTES or bk == FIXED
        ):
            return k
    return -1


def encode_value(schema: Schema, value: Value) raises -> List[UInt8]:
    """One-shot: encode `value` against `schema` into a fresh buffer."""
    var e = Encoder()
    e.write_value(schema, value)
    return e^.take()
