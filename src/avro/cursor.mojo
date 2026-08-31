"""`RecordCursor` — a schema-compiled, allocation-free Avro reader.

`DataFileReader.next()` hands back a `Value`: a fresh node arena behind an
`ArcPointer`, every node carrying a `String`, two `List`s and a `List[UInt8]`.
That is the right shape for a dynamically typed datum, and the wrong shape
for a scan planner reading a million manifest entries — a five-field record
costs about sixty heap allocations and every `field("name")` walks a list of
`String`s comparing bytes.

A `RecordCursor` compiles the schema **once** into a flat program of ops and
then runs it per record into pre-sized, reused buffers:

```mojo
from avro.cursor import RecordCursor

var c = RecordCursor.open("manifest.avro", ["status", "data_file.file_path"])
var path = c.plan.slot_of("data_file.file_path")
var status = c.plan.slot_of("status")
while c.next():
    print(c.get_long(status), c.get_str(path))
```

Three things make it fast:

* **Slots, not names.** Every value the plan decodes lands in a numbered
  slot. `slot_of` is a one-off name lookup at plan-build time; the hot loop
  only ever indexes.
* **Nothing is allocated per record.** Slot storage is `List[List[SlotVal]]`
  with a parallel `counts` array; a new record resets the counts to zero and
  overwrites what is already there, so after the first few records every
  `List` is already big enough and the steady state does no allocation at
  all. `tests/test_cursor.mojo` asserts this by counting allocations.
* **Strings and bytes are views.** `get_str` and `get_bytes` return a slice
  of the decompressed block the cursor is holding — no copy. They borrow
  from `self.reader.block`, so the compiler ties them to the cursor: a view
  cannot outlive it, and it is only valid until the next `next()`.

Selection prunes work but not correctness: an unselected field still has an
op, because the decoder has to step over its bytes, but it gets no slot and
nothing is stored.

Nesting is flattened. A field inside a record is `parent.child`; array
elements live under `path.element`, map entries under `path.key` and
`path.value`. A slot inside an array holds one value per element, read with
the second argument of the accessors, and the array's own slot holds the
element count:

```mojo
var n = c.array_len(bounds)
for k in range(n):
    print(c.get_long(bound_key, k), c.get_bytes(bound_val, k))
```
"""

from std.collections import Dict
from std.memory import bitcast

from avro.codec import CodecSet, DefaultCodecs
from avro.datafile import DataFileReader, read_file_bytes
from avro.resolve import (
    RA_ARRAY,
    RA_DIRECT,
    RA_ENUM,
    RA_ERROR,
    RA_MAP,
    RA_PROMOTE,
    RA_RECORD,
    RA_SKIP,
    RA_UNION_READER,
    RA_UNION_WRITER,
    ResolvedReader,
)
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

# ── the op codes ───────────────────────────────────────────────────────────
comptime OP_NULL: Int = 0
"""Nothing on the wire; the slot records a present-but-null value."""
comptime OP_BOOL: Int = 1
comptime OP_VARINT: Int = 2
"""A zigzag varint — `int` and `long`."""
comptime OP_FLOAT: Int = 3
comptime OP_DOUBLE: Int = 4
comptime OP_BYTES: Int = 5
"""Length-prefixed bytes — `bytes` and `string`, which share a wire form."""
comptime OP_FIXED: Int = 6
"""`aux` bytes with no prefix."""
comptime OP_ENUM: Int = 7
"""A varint symbol index; `aux` indexes `DecodePlan.enum_maps`, or -1."""
comptime OP_RECORD: Int = 8
comptime OP_ARRAY: Int = 9
comptime OP_MAP: Int = 10
"""`aux` is the key slot."""
comptime OP_UNION: Int = 11
"""`aux` is the branch-index slot, or -1."""
comptime OP_CONST: Int = 12
"""No wire bytes: store `DecodePlan.consts[aux]`. A resolution default."""
comptime OP_ERROR: Int = 13
"""A writer union branch the reader cannot accept; `aux` indexes `messages`."""
comptime OP_VARINT_D: Int = 14
"""A varint an `int -> float` / `long -> double` promotion turns into a float.
Its own op so the hot `OP_VARINT` never has to consult the slot table."""


@fieldwise_init
struct PlanOp(Copyable, Movable):
    """One instruction. Plain integers, so a plan is cheap to copy."""

    var op: Int
    var slot: Int
    """Where the value lands, or -1 to decode and discard."""
    var aux: Int
    var kid0: Int
    """First child: an index into `DecodePlan.kids`."""
    var nkid: Int


@fieldwise_init
struct SlotInfo(Copyable, Movable):
    """What a slot means. Built once; never touched on the hot path."""

    var path: String
    """Dotted path, e.g. `data_file.lower_bounds.element.key`."""
    var kind: Int
    """The reader-visible Avro kind."""
    var depth: Int
    """Repetition depth: 0 is one value per record, 1 is one per element."""
    var symbols: Int
    """Index into `DecodePlan.symbols` for an enum slot, else -1."""


@fieldwise_init
struct SlotVal(Copyable, ImplicitlyCopyable, Movable):
    """One decoded value — 24 bytes, so a slot buffer stays cache-friendly.

    `i` carries integers directly and floats bit-for-bit (`bitcast`, never
    `SIMD.cast`, which converts by value); `off`/`ln` locate a string or a
    `bytes` in the block buffer. `off < 0` marks the rare value that lives on
    the plan instead — a `string` filled in from a schema default.
    """

    var i: Int64
    var off: Int32
    var ln: Int32
    var nul: Bool

    @staticmethod
    @always_inline
    def none() -> SlotVal:
        return SlotVal(0, 0, 0, True)

    @staticmethod
    @always_inline
    def of_long(v: Int64) -> SlotVal:
        return SlotVal(v, 0, 0, False)

    @staticmethod
    @always_inline
    def of_double(v: Float64) -> SlotVal:
        return SlotVal(bitcast[DType.int64](v), 0, 0, False)

    @staticmethod
    @always_inline
    def of_span(off: Int, ln: Int) -> SlotVal:
        return SlotVal(0, Int32(off), Int32(ln), False)

    @always_inline
    def as_double(self) -> Float64:
        return bitcast[DType.float64](self.i)


# ── the plan ───────────────────────────────────────────────────────────────


struct DecodePlan(Copyable, Movable):
    """A schema compiled into a decode program plus its slot table."""

    var ops: List[PlanOp]
    var kids: List[Int]
    """Child op indices, referenced by `PlanOp.kid0` / `PlanOp.nkid`."""
    var slots: List[SlotInfo]
    var symbols: List[List[String]]
    var enum_maps: List[List[Int]]
    """Writer symbol index -> reader symbol index, for a resolved enum."""
    var consts: List[SlotVal]
    var const_strings: List[String]
    """Text of a `string`/`bytes` resolution default, by slot."""
    var messages: List[String]
    var root: Int
    var by_path: Dict[String, Int]

    def __init__(out self):
        self.ops = List[PlanOp]()
        self.kids = List[Int]()
        self.slots = List[SlotInfo]()
        self.symbols = List[List[String]]()
        self.enum_maps = List[List[Int]]()
        self.consts = List[SlotVal]()
        self.const_strings = List[String]()
        self.messages = List[String]()
        self.root = -1
        self.by_path = Dict[String, Int]()

    def __init__(out self, *, copy: Self):
        self.ops = copy.ops.copy()
        self.kids = copy.kids.copy()
        self.slots = copy.slots.copy()
        self.symbols = copy.symbols.copy()
        self.enum_maps = copy.enum_maps.copy()
        self.consts = copy.consts.copy()
        self.const_strings = copy.const_strings.copy()
        self.messages = copy.messages.copy()
        self.root = copy.root
        self.by_path = copy.by_path.copy()

    def __init__(out self, *, deinit move: Self):
        self.ops = move.ops^
        self.kids = move.kids^
        self.slots = move.slots^
        self.symbols = move.symbols^
        self.enum_maps = move.enum_maps^
        self.consts = move.consts^
        self.const_strings = move.const_strings^
        self.messages = move.messages^
        self.root = move.root
        self.by_path = move.by_path^

    # ── the slot table ─────────────────────────────────────────────────────

    def num_slots(self) -> Int:
        return len(self.slots)

    def try_slot(self, path: StringSlice) raises -> Int:
        """The slot for `path`, or -1 when the schema has no such field."""
        var k = String(path)
        if k in self.by_path:
            return self.by_path[k]
        return -1

    def has_slot(self, path: StringSlice) raises -> Bool:
        return self.try_slot(path) >= 0

    def slot_of(self, path: StringSlice) raises -> Int:
        var s = self.try_slot(path)
        if s < 0:
            raise Error(
                String(
                    "avro.DecodePlan: no slot '",
                    path,
                    "' — it is not in the schema, or not selected",
                )
            )
        return s

    def slot_path(self, slot: Int) -> String:
        return self.slots[slot].path

    def slot_kind(self, slot: Int) -> Int:
        return self.slots[slot].kind

    def slot_depth(self, slot: Int) -> Int:
        return self.slots[slot].depth

    def paths(self) -> List[String]:
        """Every slot path, in slot order — handy in tests and REPL work."""
        var out = List[String](capacity=len(self.slots))
        for k in range(len(self.slots)):
            out.append(self.slots[k].path)
        return out^

    # ── building ───────────────────────────────────────────────────────────

    @staticmethod
    def build(schema: Schema, select: List[String] = []) raises -> DecodePlan:
        """Compile `schema`, keeping only the selected paths (all if empty)."""
        var b = _Builder(select)
        var plan = DecodePlan()
        b.plan = plan^
        b.plan.root = b.compile(schema, schema.root, String(), 0, True)
        return b^.finish()

    @staticmethod
    def build_resolved(
        resolved: ResolvedReader, select: List[String] = []
    ) raises -> DecodePlan:
        """Compile a `resolve(writer, reader)` plan: the data is decoded as
        the writer wrote it and lands in the reader's shape."""
        var b = _Builder(select)
        var plan = DecodePlan()
        b.plan = plan^
        b.plan.root = b.compile_resolved(
            resolved, resolved.root, String(), 0, True
        )
        return b^.finish()


def _child_path(parent: String, name: StringSlice) -> String:
    if not parent:
        return String(name)
    return String(parent, ".", name)


def _under(path: String, prefix: String) -> Bool:
    """`path` is `prefix` itself or something inside it."""
    var n = prefix.byte_length()
    if path.byte_length() < n:
        return False
    if String(path[byte=0:n]) != prefix:
        return False
    if path.byte_length() == n:
        return True
    return path[byte=n] == "."


struct _Builder(Copyable, Movable):
    """Walks a schema (or a resolution plan) and emits ops and slots."""

    var plan: DecodePlan
    var select: List[String]

    def __init__(out self, select: List[String]):
        self.plan = DecodePlan()
        self.select = select.copy()

    def __init__(out self, *, copy: Self):
        self.plan = copy.plan.copy()
        self.select = copy.select.copy()

    def __init__(out self, *, deinit move: Self):
        self.plan = move.plan^
        self.select = move.select^

    def finish(deinit self) -> DecodePlan:
        return self.plan^

    def selected(self, path: String) -> Bool:
        """Keep `path` when it is on, above, or below something selected."""
        if len(self.select) == 0:
            return True
        if not path:
            return True
        for k in range(len(self.select)):
            ref s = self.select[k]
            if _under(path, s) or _under(s, path):
                return True
        return False

    def slot(
        mut self, path: String, kind: Int, depth: Int, symbols: Int
    ) raises -> Int:
        """The slot for `path`, creating it the first time.

        A union's branches share their parent's path, so `["null","long"]`
        gives one slot that is null on one branch and a long on the other.
        The `null` branch must not win the slot's kind.
        """
        if path in self.plan.by_path:
            var s = self.plan.by_path[path]
            if self.plan.slots[s].kind == NULL and kind != NULL:
                self.plan.slots[s].kind = kind
                self.plan.slots[s].symbols = symbols
            return s
        var s = len(self.plan.slots)
        self.plan.slots.append(SlotInfo(path, kind, depth, symbols))
        self.plan.by_path[path] = s
        return s

    def push(mut self, var op: PlanOp) -> Int:
        self.plan.ops.append(op^)
        return len(self.plan.ops) - 1

    def push_kids(mut self, kids: List[Int]) -> Int:
        var at = len(self.plan.kids)
        self.plan.kids.extend(kids.copy())
        return at

    def add_symbols(mut self, syms: List[String]) -> Int:
        self.plan.symbols.append(syms.copy())
        return len(self.plan.symbols) - 1

    # ── compiling a plain schema ───────────────────────────────────────────

    def compile(
        mut self, schema: Schema, i: Int, path: String, depth: Int, keep: Bool
    ) raises -> Int:
        var kind = schema.kind(i)
        var want = keep and self.selected(path)

        if kind == RECORD:
            var kids = List[Int]()
            ref node = schema.nodes[i]
            for f in range(len(node.fields)):
                var fp = _child_path(path, schema.nodes[i].fields[f].name)
                kids.append(
                    self.compile(
                        schema,
                        schema.nodes[i].fields[f].type_index,
                        fp,
                        depth,
                        want,
                    )
                )
            var at = self.push_kids(kids)
            return self.push(PlanOp(OP_RECORD, -1, 0, at, len(kids)))

        if kind == UNION:
            var nb = schema.num_branches(i)
            var bslot = -1
            if want and _needs_branch_slot(schema, i):
                bslot = self.slot(String(path, ".$branch"), INT, depth, -1)
            var kids = List[Int]()
            for b in range(nb):
                kids.append(
                    self.compile(schema, schema.branch(i, b), path, depth, keep)
                )
            var at = self.push_kids(kids)
            return self.push(PlanOp(OP_UNION, -1, bslot, at, len(kids)))

        if kind == ARRAY:
            var s = self.slot(path, ARRAY, depth, -1) if want else -1
            var kid = self.compile(
                schema,
                schema.items(i),
                _child_path(path, "element"),
                depth + 1,
                keep,
            )
            var at = self.push_kids([kid])
            return self.push(PlanOp(OP_ARRAY, s, 0, at, 1))

        if kind == MAP:
            var s = self.slot(path, MAP, depth, -1) if want else -1
            var ks = -1
            if want:
                ks = self.slot(_child_path(path, "key"), STRING, depth + 1, -1)
            var kid = self.compile(
                schema,
                schema.values(i),
                _child_path(path, "value"),
                depth + 1,
                keep,
            )
            var at = self.push_kids([kid])
            return self.push(PlanOp(OP_MAP, s, ks, at, 1))

        return self.leaf(schema, i, path, depth, want)

    def leaf(
        mut self, schema: Schema, i: Int, path: String, depth: Int, want: Bool
    ) raises -> Int:
        """A scalar op, with a slot only when the path is wanted."""
        var kind = schema.kind(i)
        var syms = -1
        if kind == ENUM and want:
            syms = self.add_symbols(schema.nodes[i].symbols)
        var s = self.slot(path, kind, depth, syms) if want else -1
        if kind == NULL:
            return self.push(PlanOp(OP_NULL, s, 0, 0, 0))
        if kind == BOOLEAN:
            return self.push(PlanOp(OP_BOOL, s, 0, 0, 0))
        if kind == INT or kind == LONG:
            return self.push(PlanOp(OP_VARINT, s, 0, 0, 0))
        if kind == FLOAT:
            return self.push(PlanOp(OP_FLOAT, s, 0, 0, 0))
        if kind == DOUBLE:
            return self.push(PlanOp(OP_DOUBLE, s, 0, 0, 0))
        if kind == BYTES or kind == STRING:
            return self.push(PlanOp(OP_BYTES, s, 0, 0, 0))
        if kind == FIXED:
            return self.push(PlanOp(OP_FIXED, s, schema.size(i), 0, 0))
        if kind == ENUM:
            return self.push(PlanOp(OP_ENUM, s, -1, 0, 0))
        raise Error(
            String("avro.DecodePlan: cannot compile kind ", kind_name(kind))
        )

    # ── compiling a resolution plan ────────────────────────────────────────

    def compile_resolved(
        mut self,
        rr: ResolvedReader,
        p: Int,
        path: String,
        depth: Int,
        keep: Bool,
    ) raises -> Int:
        ref n = rr.plan[p]
        var want = keep and self.selected(path)

        if n.action == RA_DIRECT or n.action == RA_SKIP:
            var k = keep if n.action == RA_DIRECT else False
            return self.compile(rr.writer, n.w, path, depth, k)

        if n.action == RA_PROMOTE:
            var wk = rr.writer.kind(n.w)
            var rk = rr.reader.kind(n.r)
            var s = self.slot(path, rk, depth, -1) if want else -1
            if wk == INT or wk == LONG:
                if rk == FLOAT or rk == DOUBLE:
                    return self.push(PlanOp(OP_VARINT_D, s, 0, 0, 0))
                return self.push(PlanOp(OP_VARINT, s, 0, 0, 0))
            if wk == FLOAT:
                return self.push(PlanOp(OP_FLOAT, s, 0, 0, 0))
            if wk == DOUBLE:
                return self.push(PlanOp(OP_DOUBLE, s, 0, 0, 0))
            if wk == STRING or wk == BYTES:
                return self.push(PlanOp(OP_BYTES, s, 0, 0, 0))
            raise Error(
                String("avro.DecodePlan: cannot promote ", kind_name(wk))
            )

        if n.action == RA_ENUM:
            var syms = -1
            if want:
                syms = self.add_symbols(rr.reader.nodes[n.r].symbols)
            var s = self.slot(path, ENUM, depth, syms) if want else -1
            var m = List[Int]()
            for k in range(len(n.enum_map)):
                var mapped = n.enum_map[k]
                if mapped < 0:
                    mapped = n.enum_fallback
                m.append(mapped)
            self.plan.enum_maps.append(m^)
            return self.push(
                PlanOp(OP_ENUM, s, len(self.plan.enum_maps) - 1, 0, 0)
            )

        if n.action == RA_UNION_WRITER:
            var bslot = -1
            if want and _needs_branch_slot(rr.writer, n.w):
                bslot = self.slot(String(path, ".$branch"), INT, depth, -1)
            var kids = List[Int]()
            for b in range(len(n.kids)):
                kids.append(
                    self.compile_resolved(rr, n.kids[b], path, depth, keep)
                )
            var at = self.push_kids(kids)
            return self.push(PlanOp(OP_UNION, -1, bslot, at, len(kids)))

        if n.action == RA_UNION_READER:
            # The reader's union wrapper is invisible in a flat cursor: the
            # branch is fixed at plan time, so only its payload is decoded.
            return self.compile_resolved(rr, n.kids[0], path, depth, keep)

        if n.action == RA_ARRAY:
            var s = self.slot(path, ARRAY, depth, -1) if want else -1
            var kid = self.compile_resolved(
                rr, n.kids[0], _child_path(path, "element"), depth + 1, keep
            )
            var at = self.push_kids([kid])
            return self.push(PlanOp(OP_ARRAY, s, 0, at, 1))

        if n.action == RA_MAP:
            var s = self.slot(path, MAP, depth, -1) if want else -1
            var ks = -1
            if want:
                ks = self.slot(_child_path(path, "key"), STRING, depth + 1, -1)
            var kid = self.compile_resolved(
                rr, n.kids[0], _child_path(path, "value"), depth + 1, keep
            )
            var at = self.push_kids([kid])
            return self.push(PlanOp(OP_MAP, s, ks, at, 1))

        if n.action == RA_RECORD:
            var kids = List[Int]()
            for wf in range(len(n.field_plan)):
                var rf = n.field_slot[wf]
                if rf < 0:
                    # A writer field the reader dropped: still stepped over.
                    kids.append(
                        self.compile(
                            rr.writer,
                            rr.writer.field_type(n.w, wf),
                            _child_path(path, "$skip"),
                            depth,
                            False,
                        )
                    )
                else:
                    var fp = _child_path(
                        path, rr.reader.nodes[n.r].fields[rf].name
                    )
                    kids.append(
                        self.compile_resolved(
                            rr, n.field_plan[wf], fp, depth, keep
                        )
                    )
            for k in range(len(n.default_slots)):
                var rf = n.default_slots[k]
                var fp = _child_path(path, rr.reader.nodes[n.r].fields[rf].name)
                kids.append(
                    self.const_op(
                        n.default_values[k],
                        fp,
                        depth,
                        keep and self.selected(fp),
                    )
                )
            var at = self.push_kids(kids)
            return self.push(PlanOp(OP_RECORD, -1, 0, at, len(kids)))

        if n.action == RA_ERROR:
            self.plan.messages.append(n.message)
            return self.push(
                PlanOp(OP_ERROR, -1, len(self.plan.messages) - 1, 0, 0)
            )

        raise Error("avro.DecodePlan: unreachable resolution action")

    def const_op(
        mut self, value: Value, path: String, depth: Int, want: Bool
    ) raises -> Int:
        """A reader field the writer never wrote, filled from its default."""
        var v = value.unwrap()
        var kind = v.kind()
        if kind == RECORD or kind == ARRAY or kind == MAP:
            raise Error(
                String(
                    (
                        "avro.DecodePlan: a cursor cannot fill the container"
                        " default for reader field '"
                    ),
                    path,
                    "' — read this file through the Value API",
                )
            )
        var s = -1
        if want:
            var syms = -1
            if kind == ENUM:
                syms = self.add_symbols([v.symbol()])
            s = self.slot(path, kind, depth, syms)
        var sv = SlotVal.none()
        if kind == BOOLEAN:
            sv = SlotVal.of_long(Int64(1) if v.as_bool() else Int64(0))
        elif kind == INT or kind == LONG or kind == ENUM:
            sv = SlotVal.of_long(
                Int64(v.enum_index()) if kind == ENUM else v.as_long()
            )
        elif kind == FLOAT or kind == DOUBLE:
            sv = SlotVal.of_double(v.as_double())
        elif kind == BYTES or kind == STRING or kind == FIXED:
            # Constant text has no home in the block buffer, so it is kept on
            # the plan and served by `get_string` / `get_bytes_copy`.
            var text = v.as_string()
            self.plan.const_strings.append(text)
            sv = SlotVal(Int64(len(self.plan.const_strings) - 1), -1, 0, False)
        self.plan.consts.append(sv)
        return self.push(PlanOp(OP_CONST, s, len(self.plan.consts) - 1, 0, 0))


def _needs_branch_slot(schema: Schema, i: Int) -> Bool:
    """True unless the union is Iceberg's plain `["null", T]` optional.

    For an optional the branch index says nothing `is_null` does not, and
    both branches share one slot; anything else needs the branch to be
    readable.
    """
    if schema.num_branches(i) != 2:
        return True
    return not schema.is_optional(i)


# ── decoding ───────────────────────────────────────────────────────────────


@always_inline
def _need(blk: Span[UInt8, _], pos: Int, n: Int) raises:
    if pos + n > len(blk):
        raise Error(
            String(
                "avro.RecordCursor: truncated block, wanted ",
                n,
                " byte(s) at offset ",
                pos,
                " of ",
                len(blk),
            )
        )


@always_inline
def _rd_varint(blk: Span[UInt8, _], mut pos: Int) raises -> Int64:
    _need(blk, pos, 1)
    var b0 = blk[pos]
    if b0 < 0x80:
        pos += 1
        var u = UInt64(b0)
        return Int64((u >> 1) ^ (0 - (u & 1)))
    var value = UInt64(b0) & 0x7F
    var shift: UInt64 = 7
    pos += 1
    while True:
        _need(blk, pos, 1)
        var b = blk[pos]
        pos += 1
        value |= (UInt64(b) & 0x7F) << shift
        if b < 0x80:
            break
        shift += 7
        if shift > 63:
            raise Error("avro.RecordCursor: varint longer than 10 bytes")
    return Int64((value >> 1) ^ (0 - (value & 1)))


@always_inline
def _rd_block_count(blk: Span[UInt8, _], mut pos: Int) raises -> Int:
    """One array/map block header; a negative count carries a byte size."""
    var count = _rd_varint(blk, pos)
    if count < 0:
        _ = _rd_varint(blk, pos)
        count = -count
    return Int(count)


@always_inline
def _load_le64(blk: Span[UInt8, _], pos: Int) -> UInt64:
    var bits: UInt64 = 0
    for k in range(8):
        bits |= UInt64(blk[pos + k]) << UInt64(8 * k)
    return bits


@always_inline
def _load_le32(blk: Span[UInt8, _], pos: Int) -> UInt32:
    var bits: UInt32 = 0
    for k in range(4):
        bits |= UInt32(blk[pos + k]) << UInt32(8 * k)
    return bits


@always_inline
def _put(
    mut vals: List[List[SlotVal]],
    mut counts: List[Int],
    slot: Int,
    v: SlotVal,
):
    """Store into slot `slot`, reusing the buffer this record's predecessor
    left behind. This is the reason the hot path allocates nothing."""
    var c = counts[slot]
    if c < len(vals[slot]):
        vals[slot][c] = v
    else:
        vals[slot].append(v)
    counts[slot] = c + 1


@always_inline
def _leaf(
    plan: DecodePlan,
    op: PlanOp,
    blk: Span[UInt8, _],
    mut pos: Int,
    mut vals: List[List[SlotVal]],
    mut counts: List[Int],
) raises -> Bool:
    """Run `op` if it is a scalar; False means it needs the full `_run`.

    Inlined into the record loop: a flat record is then decoded without a
    function call per field, which is most of what the cursor is for.
    """
    var code = op.op
    var slot = op.slot

    if code == OP_VARINT:
        var v = _rd_varint(blk, pos)
        if slot >= 0:
            _put(vals, counts, slot, SlotVal.of_long(v))
        return True

    if code == OP_BYTES:
        var n = Int(_rd_varint(blk, pos))
        if n < 0:
            raise Error(String("avro.RecordCursor: negative length ", n))
        _need(blk, pos, n)
        if slot >= 0:
            _put(vals, counts, slot, SlotVal.of_span(pos, n))
        pos += n
        return True

    if code == OP_DOUBLE:
        _need(blk, pos, 8)
        var bits = _load_le64(blk, pos)
        pos += 8
        if slot >= 0:
            _put(
                vals,
                counts,
                slot,
                SlotVal.of_double(bitcast[DType.float64](bits)),
            )
        return True

    if code == OP_NULL:
        if slot >= 0:
            _put(vals, counts, slot, SlotVal.none())
        return True

    if code == OP_BOOL:
        _need(blk, pos, 1)
        var b = blk[pos]
        pos += 1
        if b > 1:
            raise Error(String("avro.RecordCursor: bad boolean byte ", Int(b)))
        if slot >= 0:
            _put(vals, counts, slot, SlotVal.of_long(Int64(b)))
        return True

    if code == OP_FLOAT:
        _need(blk, pos, 4)
        var bits = _load_le32(blk, pos)
        pos += 4
        if slot >= 0:
            _put(
                vals,
                counts,
                slot,
                SlotVal.of_double(Float64(bitcast[DType.float32](bits))),
            )
        return True

    if code == OP_FIXED:
        _need(blk, pos, op.aux)
        if slot >= 0:
            _put(vals, counts, slot, SlotVal.of_span(pos, op.aux))
        pos += op.aux
        return True

    if code == OP_VARINT_D:
        var v = _rd_varint(blk, pos)
        if slot >= 0:
            _put(vals, counts, slot, SlotVal.of_double(Float64(v)))
        return True

    if code == OP_ENUM:
        var idx = Int(_rd_varint(blk, pos))
        if op.aux >= 0:
            ref m = plan.enum_maps[op.aux]
            if idx < 0 or idx >= len(m):
                raise Error(
                    String(
                        "avro.RecordCursor: enum index ", idx, " out of range"
                    )
                )
            idx = m[idx]
            if idx < 0:
                raise Error(
                    "avro.RecordCursor: the writer's symbol is not in the"
                    " reader's enum and it has no default"
                )
        if slot >= 0:
            _put(vals, counts, slot, SlotVal.of_long(Int64(idx)))
        return True

    if code == OP_CONST:
        if slot >= 0:
            _put(vals, counts, slot, plan.consts[op.aux])
        return True

    return False


def _run(
    plan: DecodePlan,
    oi: Int,
    blk: Span[UInt8, _],
    mut pos: Int,
    mut vals: List[List[SlotVal]],
    mut counts: List[Int],
) raises:
    ref op = plan.ops[oi]
    var code = op.op
    var slot = op.slot

    if code == OP_RECORD:
        for k in range(op.nkid):
            var ki = plan.kids[op.kid0 + k]
            if not _leaf(plan, plan.ops[ki], blk, pos, vals, counts):
                _run(plan, ki, blk, pos, vals, counts)
        return

    if code == OP_UNION:
        var b = Int(_rd_varint(blk, pos))
        if b < 0 or b >= op.nkid:
            raise Error(
                String("avro.RecordCursor: union index ", b, " out of range")
            )
        if op.aux >= 0:
            _put(vals, counts, op.aux, SlotVal.of_long(Int64(b)))
        var ki = plan.kids[op.kid0 + b]
        if not _leaf(plan, plan.ops[ki], blk, pos, vals, counts):
            _run(plan, ki, blk, pos, vals, counts)
        return

    if code == OP_ARRAY:
        var total = 0
        var kid = plan.kids[op.kid0]
        while True:
            var count = _rd_block_count(blk, pos)
            if count == 0:
                break
            for _k in range(count):
                if not _leaf(plan, plan.ops[kid], blk, pos, vals, counts):
                    _run(plan, kid, blk, pos, vals, counts)
            total += count
        if slot >= 0:
            _put(vals, counts, slot, SlotVal.of_long(Int64(total)))
        return

    if code == OP_MAP:
        var total = 0
        var kid = plan.kids[op.kid0]
        while True:
            var count = _rd_block_count(blk, pos)
            if count == 0:
                break
            for _k in range(count):
                var n = Int(_rd_varint(blk, pos))
                if n < 0:
                    raise Error(
                        String("avro.RecordCursor: negative key length ", n)
                    )
                _need(blk, pos, n)
                if op.aux >= 0:
                    _put(vals, counts, op.aux, SlotVal.of_span(pos, n))
                pos += n
                if not _leaf(plan, plan.ops[kid], blk, pos, vals, counts):
                    _run(plan, kid, blk, pos, vals, counts)
            total += count
        if slot >= 0:
            _put(vals, counts, slot, SlotVal.of_long(Int64(total)))
        return

    if code == OP_ERROR:
        raise Error(plan.messages[op.aux])

    if _leaf(plan, op, blk, pos, vals, counts):
        return
    raise Error(String("avro.RecordCursor: unknown op ", code))


# ── the cursor ─────────────────────────────────────────────────────────────


struct RecordCursor[C: CodecSet = DefaultCodecs](Movable):
    """A schema-compiled reader over an Object Container File.

    The cursor owns the `DataFileReader`, and therefore the decompressed
    block that `get_str` and `get_bytes` return views of. That is the whole
    lifetime story: a view borrows from `self.reader.block`, so it cannot
    outlive the cursor, and it is only meaningful until the next `next()`
    overwrites the position — or until a block boundary replaces the buffer.
    Copy with `get_string` / `get_bytes_copy` if it has to live longer.
    """

    var reader: DataFileReader[Self.C]
    var plan: DecodePlan
    var vals: List[List[SlotVal]]
    var counts: List[Int]
    var index: Int
    """How many records `next()` has returned."""

    def __init__(
        out self, var reader: DataFileReader[Self.C], var plan: DecodePlan
    ):
        self.reader = reader^
        self.plan = plan^
        self.vals = List[List[SlotVal]]()
        self.counts = List[Int]()
        for _k in range(self.plan.num_slots()):
            self.vals.append(List[SlotVal]())
            self.counts.append(0)
        self.index = 0

    def __init__(out self, *, deinit move: Self):
        self.reader = move.reader^
        self.plan = move.plan^
        self.vals = move.vals^
        self.counts = move.counts^
        self.index = move.index

    @staticmethod
    def of(
        var reader: DataFileReader[Self.C], select: List[String] = []
    ) raises -> Self:
        """Take over `reader` and compile its own schema."""
        var plan = DecodePlan.build(reader.schema, select)
        return Self(reader^, plan^)

    @staticmethod
    def open(path: StringSlice, select: List[String] = []) raises -> Self:
        return Self.of(DataFileReader[Self.C].open(path), select)

    @staticmethod
    def from_bytes(
        data: Span[UInt8, _], select: List[String] = []
    ) raises -> Self:
        return Self.of(DataFileReader[Self.C].from_bytes(data), select)

    @staticmethod
    def of_bytes(
        var data: List[UInt8], select: List[String] = []
    ) raises -> Self:
        """Take ownership of `data` — no copy, unlike `from_bytes`."""
        return Self.of(DataFileReader[Self.C](data^), select)

    @staticmethod
    def resolved(
        var reader: DataFileReader[Self.C],
        var resolution: ResolvedReader,
        select: List[String] = [],
    ) raises -> Self:
        """Read `reader` — whose schema must be `resolution`'s writer — into
        the reader schema `resolution` was built with."""
        var plan = DecodePlan.build_resolved(resolution, select)
        return Self(reader^, plan^)

    # ── iteration ──────────────────────────────────────────────────────────

    def next(mut self) raises -> Bool:
        """Decode the next record into the slots; False at end of file."""
        if not self.reader.has_next():
            return False
        if len(self.reader.block) > 0x7FFFFFFF:
            raise Error(
                "avro.RecordCursor: a block wider than 2 GiB cannot be"
                " addressed by a slot span — read this file through the"
                " Value API"
            )
        for s in range(len(self.counts)):
            self.counts[s] = 0
        var pos = self.reader.block_pos
        _run(
            self.plan,
            self.plan.root,
            Span(self.reader.block),
            pos,
            self.vals,
            self.counts,
        )
        self.reader.block_pos = pos
        self.reader.block_left -= 1
        self.index += 1
        return True

    def skip(mut self) raises -> Bool:
        """Step over a record without filling any slot."""
        return self.next()

    # ── reading slots ──────────────────────────────────────────────────────

    def slot_watermark(self) -> Int:
        """Total values the slot buffers have ever held.

        The only place the hot path can allocate is a slot buffer growing, so
        a run where this does not change is a run that did not allocate.
        `tests/test_cursor.mojo` leans on that.
        """
        var n = 0
        for k in range(len(self.vals)):
            n += len(self.vals[k])
        return n

    def count(self, slot: Int) -> Int:
        """How many values this record put in `slot` — the repetition count."""
        return self.counts[slot]

    def is_null(self, slot: Int, k: Int = 0) -> Bool:
        """True when the field was absent, or a union took its `null` branch."""
        if slot < 0 or k >= self.counts[slot]:
            return True
        return self.vals[slot][k].nul

    def array_len(self, slot: Int, k: Int = 0) -> Int:
        """Elements in the array (or entries in the map) at `slot`."""
        if slot < 0 or k >= self.counts[slot]:
            return 0
        return Int(self.vals[slot][k].i)

    def get_bool(self, slot: Int, k: Int = 0) -> Bool:
        if k >= self.counts[slot]:
            return False
        return self.vals[slot][k].i != 0

    def get_long(self, slot: Int, k: Int = 0) -> Int64:
        if k >= self.counts[slot]:
            return 0
        ref v = self.vals[slot][k]
        var kind = self.plan.slots[slot].kind
        if kind == FLOAT or kind == DOUBLE:
            return Int64(v.as_double())
        return v.i

    def get_int(self, slot: Int, k: Int = 0) -> Int32:
        return Int32(self.get_long(slot, k))

    def get_double(self, slot: Int, k: Int = 0) -> Float64:
        if k >= self.counts[slot]:
            return 0.0
        ref v = self.vals[slot][k]
        var kind = self.plan.slots[slot].kind
        if kind == FLOAT or kind == DOUBLE:
            return v.as_double()
        return Float64(v.i)

    def get_float(self, slot: Int, k: Int = 0) -> Float32:
        return Float32(self.get_double(slot, k))

    def get_bytes(
        self, slot: Int, k: Int = 0
    ) raises -> Span[UInt8, origin_of(self.reader.block)]:
        """A view of the block buffer — valid until the next `next()`."""
        if k >= self.counts[slot]:
            return Span(self.reader.block)[0:0]
        ref v = self.vals[slot][k]
        if v.off < 0:
            raise Error(
                String(
                    "avro.RecordCursor: slot '",
                    self.plan.slots[slot].path,
                    (
                        "' is filled from a schema default, which lives on the"
                        " plan rather than in the block — use get_string() or"
                        " get_bytes_copy()"
                    ),
                )
            )
        return Span(self.reader.block)[Int(v.off) : Int(v.off) + Int(v.ln)]

    def get_str(
        self, slot: Int, k: Int = 0
    ) raises -> StringSlice[origin_of(self.reader.block)]:
        """The field as text, without copying. Same lifetime as `get_bytes`."""
        return StringSlice(unsafe_from_utf8=self.get_bytes(slot, k))

    def get_string(self, slot: Int, k: Int = 0) -> String:
        """An owned copy of the field's text."""
        if k >= self.counts[slot]:
            return String()
        ref v = self.vals[slot][k]
        if v.off < 0:
            return self.plan.const_strings[Int(v.i)]
        return String(
            from_utf8_lossy=Span(self.reader.block)[
                Int(v.off) : Int(v.off) + Int(v.ln)
            ]
        )

    def get_bytes_copy(self, slot: Int, k: Int = 0) -> List[UInt8]:
        """An owned copy of the field's bytes."""
        var out = List[UInt8]()
        if k >= self.counts[slot]:
            return out^
        ref v = self.vals[slot][k]
        if v.off < 0:
            out.extend(self.plan.const_strings[Int(v.i)].as_bytes())
            return out^
        out.reserve(Int(v.ln))
        out.extend(Span(self.reader.block)[Int(v.off) : Int(v.off) + Int(v.ln)])
        return out^

    def enum_index(self, slot: Int, k: Int = 0) -> Int:
        if k >= self.counts[slot]:
            return -1
        return Int(self.vals[slot][k].i)

    def get_symbol(self, slot: Int, k: Int = 0) raises -> String:
        """The enum symbol's name."""
        var s = self.plan.slots[slot].symbols
        var idx = self.enum_index(slot, k)
        if s < 0 or idx < 0 or idx >= len(self.plan.symbols[s]):
            raise Error(
                String(
                    "avro.RecordCursor: slot '",
                    self.plan.slots[slot].path,
                    "' has no symbol ",
                    idx,
                )
            )
        return self.plan.symbols[s][idx]

    def union_branch(self, slot: Int, k: Int = 0) -> Int:
        """The wire branch of the union whose `$branch` slot this is."""
        if slot < 0 or k >= self.counts[slot]:
            return -1
        return Int(self.vals[slot][k].i)
