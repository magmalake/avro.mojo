"""`Value` — a dynamically typed Avro datum.

Like `Schema`, a `Value` is an arena of nodes plus a root index, but the
arena sits behind an `ArcPointer` so navigating (`v.field("x").at(0)`) is a
refcount bump rather than a deep copy. Building a value copies its children
into a fresh arena; reading one never copies.
"""

from std.memory import ArcPointer

from avro.json import write_json_string
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
    kind_name,
)


@fieldwise_init
struct ValueNode(Copyable, Movable):
    var kind: Int
    var b: Bool
    var i: Int64
    """Int / long payload, and the enum symbol or union branch index."""
    var d: Float64
    """Float / double payload."""
    var data: List[UInt8]
    """Bytes / fixed payload."""
    var s: String
    """String payload, and the enum symbol name."""
    var keys: List[String]
    """Record field names or map keys, in order."""
    var kids: List[Int]

    @staticmethod
    def of(kind: Int) -> ValueNode:
        return ValueNode(
            kind, False, 0, 0.0, List[UInt8](), String(), List[String](), List[Int]()
        )


struct Value(Copyable, Movable, Writable, Equatable, Sized):
    """One Avro datum. Cheap to copy: the node arena is shared."""

    var arena: ArcPointer[List[ValueNode]]
    var idx: Int

    def __init__(out self, var arena: ArcPointer[List[ValueNode]], idx: Int):
        self.arena = arena^
        self.idx = idx

    def __init__(out self, *, copy: Self):
        self.arena = copy.arena.copy()
        self.idx = copy.idx

    def __init__(out self, *, deinit move: Self):
        self.arena = move.arena^
        self.idx = move.idx

    def __init__(out self):
        self.arena = ArcPointer(List[ValueNode]())
        self.arena[].append(ValueNode.of(NULL))
        self.idx = 0

    # ── construction ───────────────────────────────────────────────────────

    @staticmethod
    def _single(var node: ValueNode) -> Value:
        var arena = List[ValueNode]()
        arena.append(node^)
        return Value(ArcPointer(arena^), 0)

    @staticmethod
    def null() -> Value:
        return Value._single(ValueNode.of(NULL))

    @staticmethod
    def boolean(v: Bool) -> Value:
        var n = ValueNode.of(BOOLEAN)
        n.b = v
        return Value._single(n^)

    @staticmethod
    def int(v: Int64) -> Value:
        var n = ValueNode.of(INT)
        n.i = v
        return Value._single(n^)

    @staticmethod
    def long(v: Int64) -> Value:
        var n = ValueNode.of(LONG)
        n.i = v
        return Value._single(n^)

    @staticmethod
    def float(v: Float32) -> Value:
        var n = ValueNode.of(FLOAT)
        n.d = Float64(v)
        return Value._single(n^)

    @staticmethod
    def double(v: Float64) -> Value:
        var n = ValueNode.of(DOUBLE)
        n.d = v
        return Value._single(n^)

    @staticmethod
    def bytes(v: Span[UInt8, _]) -> Value:
        var n = ValueNode.of(BYTES)
        n.data.extend(v)
        return Value._single(n^)

    @staticmethod
    def fixed(v: Span[UInt8, _]) -> Value:
        var n = ValueNode.of(FIXED)
        n.data.extend(v)
        return Value._single(n^)

    @staticmethod
    def string(v: StringSlice) -> Value:
        var n = ValueNode.of(STRING)
        n.s = String(v)
        return Value._single(n^)

    @staticmethod
    def enum(index: Int, symbol: StringSlice) -> Value:
        var n = ValueNode.of(ENUM)
        n.i = Int64(index)
        n.s = String(symbol)
        return Value._single(n^)

    @staticmethod
    def _compound(
        kind: Int, var keys: List[String], items: List[Value], index: Int
    ) -> Value:
        var arena = List[ValueNode]()
        var n = ValueNode.of(kind)
        n.i = Int64(index)
        n.keys = keys^
        arena.append(n^)
        var kids = List[Int]()
        for i in range(len(items)):
            kids.append(_graft(arena, items[i]))
        arena[0].kids = kids^
        return Value(ArcPointer(arena^), 0)

    @staticmethod
    def array(items: List[Value]) -> Value:
        return Value._compound(ARRAY, List[String](), items, 0)

    @staticmethod
    def map(var keys: List[String], values: List[Value]) -> Value:
        return Value._compound(MAP, keys^, values, 0)

    @staticmethod
    def record(var names: List[String], values: List[Value]) -> Value:
        return Value._compound(RECORD, names^, values, 0)

    @staticmethod
    def union(index: Int, branch: Value) -> Value:
        var items = List[Value](capacity=1)
        items.append(branch.copy())
        return Value._compound(UNION, List[String](), items, index)

    # ── inspection ─────────────────────────────────────────────────────────

    def kind(self) -> Int:
        return self.arena[][self.idx].kind

    def type_name(self) -> String:
        return kind_name(self.kind())

    def is_null(self) -> Bool:
        var k = self.kind()
        if k == NULL:
            return True
        if k == UNION:
            return self.unwrap().is_null()
        return False

    def as_bool(self) -> Bool:
        return self.arena[][self.idx].b

    def as_long(self) -> Int64:
        ref n = self.arena[][self.idx]
        if n.kind == FLOAT or n.kind == DOUBLE:
            return Int64(n.d)
        return n.i

    def as_int(self) -> Int32:
        return Int32(self.as_long())

    def as_double(self) -> Float64:
        ref n = self.arena[][self.idx]
        if n.kind == INT or n.kind == LONG:
            return Float64(n.i)
        return n.d

    def as_float(self) -> Float32:
        return Float32(self.as_double())

    def as_string(self) -> String:
        ref n = self.arena[][self.idx]
        if n.kind == BYTES or n.kind == FIXED:
            return String(from_utf8_lossy=Span(n.data))
        return n.s

    def as_bytes(self) -> List[UInt8]:
        ref n = self.arena[][self.idx]
        if n.kind == STRING:
            var out = List[UInt8]()
            out.extend(n.s.as_bytes())
            return out^
        return n.data.copy()

    def enum_index(self) -> Int:
        return Int(self.arena[][self.idx].i)

    def symbol(self) -> String:
        return self.arena[][self.idx].s

    def union_index(self) -> Int:
        return Int(self.arena[][self.idx].i)

    def unwrap(self) -> Value:
        """The branch value of a union; `self` for anything else."""
        ref n = self.arena[][self.idx]
        if n.kind == UNION and len(n.kids):
            return Value(self.arena.copy(), n.kids[0])
        return self.copy()

    def __len__(self) -> Int:
        return len(self.arena[][self.idx].kids)

    def at(self, k: Int) -> Value:
        """Child `k` — an array element, map value or record field value."""
        return Value(self.arena.copy(), self.arena[][self.idx].kids[k])

    def key(self, k: Int) -> String:
        """Name of child `k` — a record field name or a map key."""
        return self.arena[][self.idx].keys[k]

    def index_of(self, name: StringSlice) -> Int:
        """Position of a record field or map key, or -1 when absent."""
        ref n = self.arena[][self.idx]
        for k in range(len(n.keys)):
            if n.keys[k] == name:
                return k
        return -1

    def has(self, name: StringSlice) -> Bool:
        return self.index_of(name) >= 0

    def field(self, name: StringSlice) raises -> Value:
        """A record field (or map entry) by name, unwrapping unions."""
        var k = self.index_of(name)
        if k < 0:
            raise Error(String("avro.Value: no field '", name, "'"))
        return self.at(k).unwrap()

    def field_raw(self, name: StringSlice) raises -> Value:
        """A record field by name, keeping any union wrapper."""
        var k = self.index_of(name)
        if k < 0:
            raise Error(String("avro.Value: no field '", name, "'"))
        return self.at(k)

    # ── equality and printing ──────────────────────────────────────────────

    def __eq__(self, other: Value) -> Bool:
        return _node_eq(self.arena[], self.idx, other.arena[], other.idx)

    def __ne__(self, other: Value) -> Bool:
        return not (self == other)

    def to_json(self) -> String:
        """A JSON rendering used by the tests and the fastavro cross-check.

        Unions collapse to their branch value and `bytes`/`fixed` render as
        `"0x…"` hex, so the output lines up with a Python dict dumped the
        same way.
        """
        var out = String()
        _write_json(self.arena[], self.idx, out)
        return out^

    def write_to(self, mut writer: Some[Writer]):
        writer.write(self.to_json())


def _graft(mut arena: List[ValueNode], src: Value) -> Int:
    """Deep-copy `src`'s subtree into `arena`, returning its new index."""
    return _graft_node(arena, src.arena[], src.idx)


def _graft_node(
    mut arena: List[ValueNode], src: List[ValueNode], si: Int, depth: Int = 0
) -> Int:
    var n = ValueNode.of(src[si].kind)
    n.b = src[si].b
    n.i = src[si].i
    n.d = src[si].d
    n.data = src[si].data.copy()
    n.s = src[si].s
    n.keys = src[si].keys.copy()
    var here = len(arena)
    arena.append(n^)
    var kids = List[Int]()
    for k in range(len(src[si].kids)):
        var ci = src[si].kids[k]
        var child = _graft_child(arena, src, ci, depth + 1)
        kids.append(child)
    arena[here].kids = kids^
    return here


def _graft_child(
    mut arena: List[ValueNode], src: List[ValueNode], si: Int, depth: Int
) -> Int:
    return _graft_node(arena, src, si, depth)


def _node_eq(a: List[ValueNode], ai: Int, b: List[ValueNode], bi: Int) -> Bool:
    ref x = a[ai]
    ref y = b[bi]
    if x.kind != y.kind:
        return False
    if x.b != y.b or x.i != y.i or x.s != y.s:
        return False
    if x.kind == FLOAT or x.kind == DOUBLE:
        if x.d != y.d:
            return False
    if len(x.data) != len(y.data):
        return False
    for k in range(len(x.data)):
        if x.data[k] != y.data[k]:
            return False
    if len(x.keys) != len(y.keys):
        return False
    for k in range(len(x.keys)):
        if x.keys[k] != y.keys[k]:
            return False
    if len(x.kids) != len(y.kids):
        return False
    for k in range(len(x.kids)):
        if not _node_eq(a, x.kids[k], b, y.kids[k]):
            return False
    return True


comptime _HEXD = "0123456789abcdef"


def _write_hex(data: List[UInt8], mut out: String):
    out += '"0x'
    for b in data:
        out += _HEXD[byte= Int(b >> 4)]
        out += _HEXD[byte= Int(b & 0xF)]
    out += '"'


def _write_json(arena: List[ValueNode], i: Int, mut out: String):
    ref n = arena[i]
    if n.kind == NULL:
        out += "null"
    elif n.kind == BOOLEAN:
        out += "true" if n.b else "false"
    elif n.kind == INT or n.kind == LONG:
        out += String(n.i)
    elif n.kind == FLOAT or n.kind == DOUBLE:
        out += String(n.d)
    elif n.kind == STRING:
        write_json_string(n.s, out)
    elif n.kind == ENUM:
        write_json_string(n.s, out)
    elif n.kind == BYTES or n.kind == FIXED:
        _write_hex(n.data, out)
    elif n.kind == UNION:
        if len(n.kids):
            _write_json(arena, n.kids[0], out)
        else:
            out += "null"
    elif n.kind == ARRAY:
        out += "["
        for k in range(len(n.kids)):
            if k:
                out += ","
            _write_json(arena, n.kids[k], out)
        out += "]"
    else:
        out += "{"
        for k in range(len(n.kids)):
            if k:
                out += ","
            write_json_string(n.keys[k], out)
            out += ":"
            _write_json(arena, n.kids[k], out)
        out += "}"


# ── builders ───────────────────────────────────────────────────────────────


struct RecordBuilder(Copyable, Movable, Defaultable):
    """Accumulate named fields, then `build()` a record `Value`.

    ```mojo
    var b = RecordBuilder()
    b.add("id", Value.long(7))
    b.add("name", Value.string("row"))
    var v = b^.build()
    ```
    """

    var names: List[String]
    var values: List[Value]

    def __init__(out self):
        self.names = List[String]()
        self.values = List[Value]()

    def __init__(out self, *, copy: Self):
        self.names = copy.names.copy()
        self.values = copy.values.copy()

    def __init__(out self, *, deinit move: Self):
        self.names = move.names^
        self.values = move.values^

    def add(mut self, name: StringSlice, value: Value):
        self.names.append(String(name))
        self.values.append(value.copy())

    def build(deinit self) -> Value:
        return Value.record(self.names^, self.values)


struct ArrayBuilder(Copyable, Movable, Defaultable):
    """Accumulate elements, then `build()` an array `Value`."""

    var values: List[Value]

    def __init__(out self):
        self.values = List[Value]()

    def __init__(out self, *, copy: Self):
        self.values = copy.values.copy()

    def __init__(out self, *, deinit move: Self):
        self.values = move.values^

    def add(mut self, value: Value):
        self.values.append(value.copy())

    def build(deinit self) -> Value:
        return Value.array(self.values)


struct MapBuilder(Copyable, Movable, Defaultable):
    """Accumulate key/value pairs, then `build()` a map `Value`."""

    var keys: List[String]
    var values: List[Value]

    def __init__(out self):
        self.keys = List[String]()
        self.values = List[Value]()

    def __init__(out self, *, copy: Self):
        self.keys = copy.keys.copy()
        self.values = copy.values.copy()

    def __init__(out self, *, deinit move: Self):
        self.keys = move.keys^
        self.values = move.values^

    def add(mut self, key: StringSlice, value: Value):
        self.keys.append(String(key))
        self.values.append(value.copy())

    def build(deinit self) -> Value:
        return Value.map(self.keys^, self.values)
