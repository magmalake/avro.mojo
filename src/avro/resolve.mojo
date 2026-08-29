"""Schema resolution — reading data written with one schema as another.

`resolve(writer, reader)` walks the two schemas once and builds a *plan*: a
flat list of `ResolveNode`s that says, for every position in the writer's
data, what to decode and where it lands in the reader's shape. Decoding then
follows the plan, so the per-record cost has no name lookups in it.

Supported (the common cases from the Avro spec's "Schema Resolution"):

- field reordering, and writer fields the reader dropped (skipped in place);
- reader fields the writer never wrote, filled from the field's `default`;
- numeric promotions `int → long → float → double`;
- `string ↔ bytes`;
- union member selection in both directions, plus a non-union writer read
  into a reader union;
- enum symbols missing from the reader, falling back to the reader enum's
  own `default`;
- records/enums/fixed matched by unqualified name, and by the reader's
  `aliases`.
"""

from std.memory import ArcPointer

from avro.decoder import Decoder
from avro.json import (
    JSON_ARRAY,
    JSON_OBJECT,
    JsonDoc,
    parse_json,
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
from avro.value import Value, ValueNode

comptime RA_DIRECT: Int = 0
"""Kinds match exactly — decode with the writer schema."""
comptime RA_PROMOTE: Int = 1
comptime RA_RECORD: Int = 2
comptime RA_ENUM: Int = 3
comptime RA_ARRAY: Int = 4
comptime RA_MAP: Int = 5
comptime RA_UNION_WRITER: Int = 6
comptime RA_UNION_READER: Int = 7
comptime RA_SKIP: Int = 8
comptime RA_ERROR: Int = 9
"""A writer union branch the reader cannot accept — only fatal if it is
actually the branch on the wire."""


@fieldwise_init
struct ResolveNode(Copyable, Movable):
    var action: Int
    var w: Int
    """Writer schema node index."""
    var r: Int
    """Reader schema node index (-1 for a skip)."""
    var kids: List[Int]
    """Child plan indices: array items, map values, union branches."""
    var field_plan: List[Int]
    """Per writer field: the child plan index, or -1 to skip the field."""
    var field_slot: List[Int]
    """Per writer field: the reader field position, or -1 to skip."""
    var default_slots: List[Int]
    """Reader field positions filled from the field's `default`."""
    var default_values: List[Value]
    var enum_map: List[Int]
    """Per writer symbol: the reader symbol index, or -1 for the fallback."""
    var enum_fallback: Int
    var branch: Int
    """Chosen reader union branch for RA_UNION_READER."""
    var message: String
    """Why RA_ERROR was planted."""

    @staticmethod
    def of(action: Int, w: Int, r: Int) -> ResolveNode:
        return ResolveNode(
            action,
            w,
            r,
            List[Int](),
            List[Int](),
            List[Int](),
            List[Int](),
            List[Value](),
            List[Int](),
            -1,
            -1,
            String(),
        )


struct ResolvedReader(Copyable, Movable):
    """A decoding plan produced by `resolve`."""

    var writer: Schema
    var reader: Schema
    var plan: List[ResolveNode]
    var root: Int

    def __init__(out self, var writer: Schema, var reader: Schema):
        self.writer = writer^
        self.reader = reader^
        self.plan = List[ResolveNode]()
        self.root = -1

    def __init__(out self, *, copy: Self):
        self.writer = copy.writer.copy()
        self.reader = copy.reader.copy()
        self.plan = copy.plan.copy()
        self.root = copy.root

    def __init__(out self, *, deinit move: Self):
        self.writer = move.writer^
        self.reader = move.reader^
        self.plan = move.plan^
        self.root = move.root

    def read_value(self, mut d: Decoder) raises -> Value:
        """Decode one datum written with `writer`, shaped as `reader`."""
        var arena = List[ValueNode]()
        _ = self._decode(d, self.root, arena)
        return Value(ArcPointer(arena^), 0)

    def _decode(
        self, mut d: Decoder, p: Int, mut arena: List[ValueNode]
    ) raises -> Int:
        ref n = self.plan[p]
        if n.action == RA_DIRECT:
            return d._decode(self.writer, n.w, arena)
        if n.action == RA_PROMOTE:
            var wk = self.writer.kind(n.w)
            var rk = self.reader.kind(n.r)
            var node = ValueNode.of(rk)
            if wk == INT or wk == LONG:
                var v = d.read_long()
                if rk == FLOAT or rk == DOUBLE:
                    node.d = Float64(v)
                else:
                    node.i = v
            elif wk == FLOAT:
                node.d = Float64(d.read_float())
            elif wk == DOUBLE:
                node.d = d.read_double()
            elif wk == STRING or wk == BYTES:
                var raw = d.read_bytes()
                if rk == STRING:
                    node.s = String(from_utf8_lossy=Span(raw))
                else:
                    node.data = raw^
            else:
                raise Error(
                    String("avro.resolve: cannot promote ", kind_name(wk))
                )
            var here = len(arena)
            arena.append(node^)
            return here
        if n.action == RA_ENUM:
            var idx = Int(d.read_long())
            var syms = self.writer.symbols(n.w)
            if idx < 0 or idx >= len(syms):
                raise Error("avro.resolve: enum index out of range")
            var mapped = n.enum_map[idx]
            if mapped < 0:
                mapped = n.enum_fallback
            if mapped < 0:
                raise Error(
                    String(
                        "avro.resolve: symbol '",
                        syms[idx],
                        "' is not in the reader's enum and it has no default",
                    )
                )
            var node = ValueNode.of(ENUM)
            node.i = Int64(mapped)
            node.s = self.reader.symbols(n.r)[mapped]
            var here = len(arena)
            arena.append(node^)
            return here
        if n.action == RA_UNION_WRITER:
            var branch = Int(d.read_long())
            if branch < 0 or branch >= len(n.kids):
                raise Error("avro.resolve: union index out of range")
            return self._decode_child(d, n.kids[branch], arena)
        if n.action == RA_UNION_READER:
            var node = ValueNode.of(UNION)
            node.i = Int64(n.branch)
            var here = len(arena)
            arena.append(node^)
            var c = self._decode_child(d, n.kids[0], arena)
            arena[here].kids.append(c)
            return here
        if n.action == RA_ARRAY:
            var here = len(arena)
            arena.append(ValueNode.of(ARRAY))
            var kids = List[Int]()
            while True:
                var count = Int(d.read_block_count())
                if count == 0:
                    break
                for _k in range(count):
                    var c = self._decode_child(d, n.kids[0], arena)
                    kids.append(c)
            arena[here].kids = kids^
            return here
        if n.action == RA_MAP:
            var here = len(arena)
            arena.append(ValueNode.of(MAP))
            var keys = List[String]()
            var kids = List[Int]()
            while True:
                var count = Int(d.read_block_count())
                if count == 0:
                    break
                for _k in range(count):
                    keys.append(d.read_string())
                    var c = self._decode_child(d, n.kids[0], arena)
                    kids.append(c)
            arena[here].keys = keys^
            arena[here].kids = kids^
            return here
        if n.action == RA_RECORD:
            var here = len(arena)
            var node = ValueNode.of(RECORD)
            var nrf = self.reader.num_fields(n.r)
            var names = List[String](capacity=nrf)
            for k in range(nrf):
                names.append(self.reader.field(n.r, k).name)
            node.keys = names^
            arena.append(node^)
            var slots = List[Int](length=nrf, fill=-1)
            for wf in range(len(n.field_plan)):
                if n.field_slot[wf] < 0:
                    d.skip_value(self.writer, self.writer.field_type(n.w, wf))
                else:
                    var c = self._decode_child(d, n.field_plan[wf], arena)
                    slots[n.field_slot[wf]] = c
            for k in range(len(n.default_slots)):
                var slot = n.default_slots[k]
                var c = _graft_value(arena, n.default_values[k])
                slots[slot] = c
            for k in range(nrf):
                if slots[k] < 0:
                    raise Error(
                        String(
                            "avro.resolve: reader field '",
                            self.reader.field(n.r, k).name,
                            "' has no writer field and no default",
                        )
                    )
            arena[here].kids = slots^
            return here
        if n.action == RA_ERROR:
            raise Error(n.message)
        raise Error("avro.resolve: unreachable plan action")

    def _decode_child(
        self, mut d: Decoder, p: Int, mut arena: List[ValueNode]
    ) raises -> Int:
        return self._decode(d, p, arena)


def _graft_value(mut arena: List[ValueNode], v: Value) -> Int:
    return _graft_from(arena, v.arena[], v.idx)


def _graft_from(mut arena: List[ValueNode], src: List[ValueNode], si: Int) -> Int:
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
        var c = _graft_step(arena, src, src[si].kids[k])
        kids.append(c)
    arena[here].kids = kids^
    return here


def _graft_step(mut arena: List[ValueNode], src: List[ValueNode], si: Int) -> Int:
    return _graft_from(arena, src, si)


# ── building the plan ──────────────────────────────────────────────────────


def _short(name: String) -> String:
    var dot = name.rfind(".")
    if dot < 0:
        return name
    return String(name[byte=dot + 1 :])


def _names_match(w: Schema, wi: Int, r: Schema, ri: Int) -> Bool:
    var wn = w.name(wi)
    var rn = r.name(ri)
    if wn == rn or _short(wn) == _short(rn):
        return True
    for a in r.nodes[ri].aliases:
        if a == wn or _short(a) == _short(wn):
            return True
    return False


def _promotes(wk: Int, rk: Int) -> Bool:
    if wk == INT and (rk == LONG or rk == FLOAT or rk == DOUBLE):
        return True
    if wk == LONG and (rk == FLOAT or rk == DOUBLE):
        return True
    if wk == FLOAT and rk == DOUBLE:
        return True
    if wk == STRING and rk == BYTES:
        return True
    if wk == BYTES and rk == STRING:
        return True
    return False


def _compatible(w: Schema, wi: Int, r: Schema, ri: Int) -> Bool:
    var wk = w.kind(wi)
    var rk = r.kind(ri)
    if wk == rk:
        if wk == RECORD or wk == ENUM or wk == FIXED:
            return _names_match(w, wi, r, ri)
        return True
    return _promotes(wk, rk)


struct _Planner(Copyable, Movable):
    var out: ResolvedReader

    def __init__(out self, var writer: Schema, var reader: Schema):
        self.out = ResolvedReader(writer^, reader^)

    def finish(deinit self) -> ResolvedReader:
        return self.out^

    def _push(mut self, var n: ResolveNode) -> Int:
        self.out.plan.append(n^)
        return len(self.out.plan) - 1

    def build(mut self, wi: Int, ri: Int) raises -> Int:
        ref w = self.out.writer
        ref r = self.out.reader
        var wk = w.kind(wi)
        var rk = r.kind(ri)

        if wk == UNION:
            var here = self._push(ResolveNode.of(RA_UNION_WRITER, wi, ri))
            var kids = List[Int]()
            for b in range(w.num_branches(wi)):
                # A branch the reader cannot take is only an error if the
                # data actually uses it, so plant an RA_ERROR instead of
                # failing the whole plan.
                var c: Int
                try:
                    c = self._build_child(w.branch(wi, b), ri)
                except e:
                    var bad = ResolveNode.of(RA_ERROR, w.branch(wi, b), ri)
                    bad.message = String(e)
                    c = self._push(bad^)
                kids.append(c)
            self.out.plan[here].kids = kids^
            return here

        if rk == UNION:
            var pick = -1
            for b in range(r.num_branches(ri)):
                if _compatible(w, wi, r, r.branch(ri, b)):
                    pick = b
                    break
            if pick < 0:
                raise Error(
                    String(
                        "avro.resolve: no reader union branch accepts a writer ",
                        kind_name(wk),
                    )
                )
            var here = self._push(ResolveNode.of(RA_UNION_READER, wi, ri))
            self.out.plan[here].branch = pick
            var c = self._build_child(wi, r.branch(ri, pick))
            self.out.plan[here].kids.append(c)
            return here

        if wk != rk:
            if not _promotes(wk, rk):
                raise Error(
                    String(
                        "avro.resolve: cannot read a writer ",
                        kind_name(wk),
                        " as a reader ",
                        kind_name(rk),
                    )
                )
            return self._push(ResolveNode.of(RA_PROMOTE, wi, ri))

        if wk == ARRAY:
            var here = self._push(ResolveNode.of(RA_ARRAY, wi, ri))
            var c = self._build_child(w.items(wi), r.items(ri))
            self.out.plan[here].kids.append(c)
            return here

        if wk == MAP:
            var here = self._push(ResolveNode.of(RA_MAP, wi, ri))
            var c = self._build_child(w.values(wi), r.values(ri))
            self.out.plan[here].kids.append(c)
            return here

        if wk == ENUM:
            if not _names_match(w, wi, r, ri):
                raise Error("avro.resolve: enum names do not match")
            var here = self._push(ResolveNode.of(RA_ENUM, wi, ri))
            var wsyms = w.symbols(wi)
            var rsyms = r.symbols(ri)
            var emap = List[Int](capacity=len(wsyms))
            for a in range(len(wsyms)):
                var found = -1
                for b in range(len(rsyms)):
                    if rsyms[b] == wsyms[a]:
                        found = b
                        break
                emap.append(found)
            var fallback = -1
            var dsym = r.nodes[ri].default_symbol
            if dsym:
                for b in range(len(rsyms)):
                    if rsyms[b] == dsym:
                        fallback = b
                        break
            self.out.plan[here].enum_map = emap^
            self.out.plan[here].enum_fallback = fallback
            return here

        if wk == FIXED:
            if not _names_match(w, wi, r, ri) or w.size(wi) != r.size(ri):
                raise Error("avro.resolve: fixed name or size mismatch")
            return self._push(ResolveNode.of(RA_DIRECT, wi, ri))

        if wk == RECORD:
            if not _names_match(w, wi, r, ri):
                raise Error(
                    String(
                        "avro.resolve: record '",
                        w.name(wi),
                        "' does not match reader record '",
                        r.name(ri),
                        "'",
                    )
                )
            var here = self._push(ResolveNode.of(RA_RECORD, wi, ri))
            var nwf = w.num_fields(wi)
            var plans = List[Int](length=nwf, fill=-1)
            var slots = List[Int](length=nwf, fill=-1)
            var matched = List[Bool](length=r.num_fields(ri), fill=False)
            for wf in range(nwf):
                var wname = w.field(wi, wf).name
                var rf = self._reader_field(ri, wname)
                if rf < 0:
                    continue
                matched[rf] = True
                slots[wf] = rf
                var c = self._build_child(
                    w.field_type(wi, wf), r.field_type(ri, rf)
                )
                plans[wf] = c
            var dslots = List[Int]()
            var dvals = List[Value]()
            for rf in range(r.num_fields(ri)):
                if matched[rf]:
                    continue
                var f = r.field(ri, rf)
                if not f.has_default:
                    raise Error(
                        String(
                            "avro.resolve: reader field '",
                            f.name,
                            "' is absent from the writer and has no default",
                        )
                    )
                dslots.append(rf)
                dvals.append(default_value(r, f.type_index, f.default_json))
            self.out.plan[here].field_plan = plans^
            self.out.plan[here].field_slot = slots^
            self.out.plan[here].default_slots = dslots^
            self.out.plan[here].default_values = dvals^
            return here

        return self._push(ResolveNode.of(RA_DIRECT, wi, ri))

    def _reader_field(self, ri: Int, name: String) -> Int:
        ref r = self.out.reader
        var direct = r.field_index(ri, name)
        if direct >= 0:
            return direct
        for k in range(r.num_fields(ri)):
            for a in r.nodes[ri].fields[k].aliases:
                if a == name:
                    return k
        return -1

    def _build_child(mut self, wi: Int, ri: Int) raises -> Int:
        return self.build(wi, ri)


def resolve(writer: Schema, reader: Schema) raises -> ResolvedReader:
    """Build a decoding plan that reads `writer` data as `reader`."""
    var p = _Planner(writer.copy(), reader.copy())
    var root = p.build(writer.root, reader.root)
    p.out.root = root
    return p^.finish()


# ── field defaults (Avro JSON encoding) ────────────────────────────────────


def default_value(schema: Schema, i: Int, default_json: StringSlice) raises -> Value:
    """Decode a field's JSON `default` into a `Value` shaped by `schema`."""
    var doc = parse_json(default_json)
    return _default_from(schema, i, doc, doc.root)


def _default_from(schema: Schema, i: Int, doc: JsonDoc, j: Int) raises -> Value:
    var kind = schema.kind(i)
    if kind == NULL:
        return Value.null()
    if kind == BOOLEAN:
        return Value.boolean(doc.as_bool(j))
    if kind == INT:
        return Value.int(doc.as_int(j))
    if kind == LONG:
        return Value.long(doc.as_int(j))
    if kind == FLOAT:
        return Value.float(Float32(doc.as_float(j)))
    if kind == DOUBLE:
        return Value.double(doc.as_float(j))
    if kind == STRING:
        return Value.string(doc.as_string(j))
    if kind == BYTES or kind == FIXED:
        # Avro spells a bytes/fixed default as a string whose code points are
        # the byte values.
        var raw = _latin1(doc.as_string(j))
        if kind == BYTES:
            return Value.bytes(Span(raw))
        return Value.fixed(Span(raw))
    if kind == ENUM:
        var sym = doc.as_string(j)
        var syms = schema.symbols(i)
        for k in range(len(syms)):
            if syms[k] == sym:
                return Value.enum(k, sym)
        raise Error(String("avro.resolve: default '", sym, "' is not a symbol"))
    if kind == UNION:
        # A union's default matches its first branch.
        var inner = _default_from(schema, schema.branch(i, 0), doc, j)
        return Value.union(0, inner)
    if kind == ARRAY:
        if doc.kind(j) != JSON_ARRAY:
            raise Error("avro.resolve: an array default must be a JSON array")
        var items = List[Value]()
        for k in range(doc.len_of(j)):
            items.append(
                _default_from(schema, schema.items(i), doc, doc.child(j, k))
            )
        return Value.array(items)
    if kind == MAP:
        if doc.kind(j) != JSON_OBJECT:
            raise Error("avro.resolve: a map default must be a JSON object")
        var keys = List[String]()
        var vals = List[Value]()
        for k in range(doc.len_of(j)):
            keys.append(doc.key_at(j, k))
            vals.append(
                _default_from(schema, schema.values(i), doc, doc.child(j, k))
            )
        return Value.map(keys^, vals)
    if kind == RECORD:
        if doc.kind(j) != JSON_OBJECT:
            raise Error("avro.resolve: a record default must be a JSON object")
        var names = List[String]()
        var vals = List[Value]()
        for k in range(schema.num_fields(i)):
            var f = schema.field(i, k)
            names.append(f.name)
            var mj = doc.get(j, f.name)
            if mj >= 0:
                vals.append(_default_from(schema, f.type_index, doc, mj))
            elif f.has_default:
                vals.append(default_value(schema, f.type_index, f.default_json))
            else:
                raise Error(
                    String("avro.resolve: record default is missing '", f.name, "'")
                )
        return Value.record(names^, vals)
    raise Error("avro.resolve: unsupported default")


def _latin1(s: String) -> List[UInt8]:
    """Map a JSON string's code points 0..255 back to raw bytes."""
    var out = List[UInt8]()
    for cp in s.codepoints():
        out.append(UInt8(Int(cp) & 0xFF))
    return out^
