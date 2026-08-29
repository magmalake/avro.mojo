"""Avro schema parsing and printing.

The parsed schema is an arena of `SchemaNode`s with integer child indices.
That is not just a Mojo workaround for self-referential structs: it is also
exactly what Avro's *named type references* want — a record that refers to
itself (`{"type": "record", "name": "Node", "fields": [{"name": "next",
"type": ["null", "Node"]}]}`) is one node index appearing twice.

Everything the JSON carried that Avro does not define is preserved as a raw
property (`field-id`, `element-id`, `key-id`, `value-id` — Apache Iceberg
writes all four), so `to_json()` round-trips and consumers can read them back.
"""

from std.collections import Dict

from avro.json import (
    JSON_ARRAY,
    JSON_INT,
    JSON_OBJECT,
    JSON_STRING,
    JsonDoc,
    parse_json,
    write_json_string,
)

# ── schema kinds ───────────────────────────────────────────────────────────

comptime NULL: Int = 0
comptime BOOLEAN: Int = 1
comptime INT: Int = 2
comptime LONG: Int = 3
comptime FLOAT: Int = 4
comptime DOUBLE: Int = 5
comptime BYTES: Int = 6
comptime STRING: Int = 7
comptime RECORD: Int = 8
comptime ENUM: Int = 9
comptime ARRAY: Int = 10
comptime MAP: Int = 11
comptime UNION: Int = 12
comptime FIXED: Int = 13


def kind_name(kind: Int) -> String:
    if kind == NULL:
        return "null"
    if kind == BOOLEAN:
        return "boolean"
    if kind == INT:
        return "int"
    if kind == LONG:
        return "long"
    if kind == FLOAT:
        return "float"
    if kind == DOUBLE:
        return "double"
    if kind == BYTES:
        return "bytes"
    if kind == STRING:
        return "string"
    if kind == RECORD:
        return "record"
    if kind == ENUM:
        return "enum"
    if kind == ARRAY:
        return "array"
    if kind == MAP:
        return "map"
    if kind == UNION:
        return "union"
    if kind == FIXED:
        return "fixed"
    return "?"


def primitive_kind(name: StringSlice) -> Int:
    """Kind for a primitive type name, or -1 if `name` is not primitive."""
    if name == "null":
        return NULL
    if name == "boolean":
        return BOOLEAN
    if name == "int":
        return INT
    if name == "long":
        return LONG
    if name == "float":
        return FLOAT
    if name == "double":
        return DOUBLE
    if name == "bytes":
        return BYTES
    if name == "string":
        return STRING
    return -1


def is_primitive(kind: Int) -> Bool:
    return kind >= NULL and kind <= STRING


def is_named(kind: Int) -> Bool:
    return kind == RECORD or kind == ENUM or kind == FIXED


# ── properties ─────────────────────────────────────────────────────────────


@fieldwise_init
struct Props(Copyable, Movable, Defaultable, Sized):
    """Ordered extra JSON attributes; values are kept as raw JSON text."""

    var keys: List[String]
    var vals: List[String]

    def __init__(out self):
        self.keys = List[String]()
        self.vals = List[String]()

    def __len__(self) -> Int:
        return len(self.keys)

    def set(mut self, var key: String, var raw_json: String):
        for k in range(len(self.keys)):
            if self.keys[k] == key:
                self.vals[k] = raw_json^
                return
        self.keys.append(key^)
        self.vals.append(raw_json^)

    def get(self, key: StringSlice) -> String:
        """Raw JSON text for `key`, or "" when absent."""
        for k in range(len(self.keys)):
            if self.keys[k] == key:
                return self.vals[k]
        return String()

    def has(self, key: StringSlice) -> Bool:
        for k in range(len(self.keys)):
            if self.keys[k] == key:
                return True
        return False

    def get_int(self, key: StringSlice, default: Int = -1) -> Int:
        """Integer property value, or `default` when absent or not a number."""
        var raw = self.get(key)
        if not raw:
            return default
        try:
            return Int(raw)
        except:
            return default


# ── fields ─────────────────────────────────────────────────────────────────


@fieldwise_init
struct Field(Copyable, Movable):
    """One record field."""

    var name: String
    var doc: String
    var type_index: Int
    """Index of the field's type node in the owning `Schema`'s arena."""
    var has_default: Bool
    var default_json: String
    """Raw JSON text of the field's default, when it has one."""
    var order: String
    var aliases: List[String]
    var props: Props

    @staticmethod
    def make(var name: String, type_index: Int) -> Field:
        return Field(
            name^,
            String(),
            type_index,
            False,
            String(),
            String(),
            List[String](),
            Props(),
        )

    def field_id(self) -> Int:
        """Iceberg's `field-id` attribute, or -1 when the field has none."""
        return self.props.get_int("field-id")


# ── nodes ──────────────────────────────────────────────────────────────────


@fieldwise_init
struct SchemaNode(Copyable, Movable):
    var kind: Int
    var name: String
    """Fullname (namespace-qualified) for record/enum/fixed; "" otherwise."""
    var namespace: String
    var doc: String
    var aliases: List[String]
    var logical_type: String
    var precision: Int
    var scale: Int
    var size: Int
    """Byte width for `fixed`."""
    var symbols: List[String]
    var default_symbol: String
    var fields: List[Field]
    var children: List[Int]
    """Array items / map values (one entry), or union branches."""
    var props: Props

    @staticmethod
    def of(kind: Int) -> SchemaNode:
        return SchemaNode(
            kind,
            String(),
            String(),
            String(),
            List[String](),
            String(),
            -1,
            -1,
            0,
            List[String](),
            String(),
            List[Field](),
            List[Int](),
            Props(),
        )

    def short_name(self) -> String:
        var dot = self.name.rfind(".")
        if dot < 0:
            return self.name
        return String(self.name[byte=dot + 1 :])


# ── schema ─────────────────────────────────────────────────────────────────


struct Schema(Copyable, Movable, Writable):
    """A parsed Avro schema: an arena of nodes plus the root node's index."""

    var nodes: List[SchemaNode]
    var root: Int
    var names: Dict[String, Int]
    """Fullname -> node index, for every named type the schema defines."""

    def __init__(out self):
        self.nodes = List[SchemaNode]()
        self.root = -1
        self.names = Dict[String, Int]()

    def __init__(out self, *, copy: Self):
        self.nodes = copy.nodes.copy()
        self.root = copy.root
        self.names = copy.names.copy()

    def __init__(out self, *, deinit move: Self):
        self.nodes = move.nodes^
        self.root = move.root
        self.names = move.names^

    # ── navigation ─────────────────────────────────────────────────────────

    def kind(self, i: Int) -> Int:
        return self.nodes[i].kind

    def root_kind(self) -> Int:
        return self.nodes[self.root].kind

    def name(self, i: Int) -> String:
        return self.nodes[i].name

    def logical_type(self, i: Int) -> String:
        return self.nodes[i].logical_type

    def size(self, i: Int) -> Int:
        return self.nodes[i].size

    def symbols(self, i: Int) -> List[String]:
        return self.nodes[i].symbols.copy()

    def num_fields(self, i: Int) -> Int:
        return len(self.nodes[i].fields)

    def field(self, i: Int, k: Int) -> Field:
        return self.nodes[i].fields[k].copy()

    def field_index(self, i: Int, name: StringSlice) -> Int:
        """Position of the named field in record node `i`, or -1."""
        ref n = self.nodes[i]
        for k in range(len(n.fields)):
            if n.fields[k].name == name:
                return k
        return -1

    def field_type(self, i: Int, k: Int) -> Int:
        return self.nodes[i].fields[k].type_index

    def items(self, i: Int) -> Int:
        """Element node index of an `array`."""
        return self.nodes[i].children[0]

    def values(self, i: Int) -> Int:
        """Value node index of a `map`."""
        return self.nodes[i].children[0]

    def num_branches(self, i: Int) -> Int:
        return len(self.nodes[i].children)

    def branch(self, i: Int, k: Int) -> Int:
        return self.nodes[i].children[k]

    def props(self, i: Int) -> Props:
        return self.nodes[i].props.copy()

    # ── Iceberg conveniences ───────────────────────────────────────────────

    def is_optional(self, i: Int) -> Bool:
        """True for a two-branch union with a `null` branch — Iceberg's
        spelling of an optional field."""
        ref n = self.nodes[i]
        if n.kind != UNION or len(n.children) != 2:
            return False
        return (
            self.nodes[n.children[0]].kind == NULL
            or self.nodes[n.children[1]].kind == NULL
        )

    def optional_branch(self, i: Int) -> Int:
        """For an optional union, the node index of the non-null branch; -1
        otherwise."""
        if not self.is_optional(i):
            return -1
        ref n = self.nodes[i]
        if self.nodes[n.children[0]].kind == NULL:
            return n.children[1]
        return n.children[0]

    def null_branch_index(self, i: Int) -> Int:
        """Branch *position* of the `null` member of a union, or -1."""
        ref n = self.nodes[i]
        if n.kind != UNION:
            return -1
        for k in range(len(n.children)):
            if self.nodes[n.children[k]].kind == NULL:
                return k
        return -1

    def is_logical_map(self, i: Int) -> Bool:
        """An `array` of two-field records tagged `"logicalType": "map"` —
        how Avro (and Iceberg) spells a map with non-string keys."""
        ref n = self.nodes[i]
        if n.kind != ARRAY or n.logical_type != "map":
            return False
        ref item = self.nodes[n.children[0]]
        return item.kind == RECORD and len(item.fields) == 2

    def element_id(self, i: Int) -> Int:
        return self.nodes[i].props.get_int("element-id")

    def key_id(self, i: Int) -> Int:
        return self.nodes[i].props.get_int("key-id")

    def value_id(self, i: Int) -> Int:
        return self.nodes[i].props.get_int("value-id")

    # ── printing ───────────────────────────────────────────────────────────

    def to_json(self) -> String:
        var seen = List[String]()
        var out = String()
        self._write(self.root, String(), seen, out)
        return out^

    def to_json_at(self, i: Int) -> String:
        var seen = List[String]()
        var out = String()
        self._write(i, String(), seen, out)
        return out^

    def _write(
        self,
        i: Int,
        enclosing_ns: String,
        mut seen: List[String],
        mut out: String,
    ):
        ref n = self.nodes[i]
        if is_primitive(n.kind) and not n.logical_type and len(n.props) == 0:
            out += '"'
            out += kind_name(n.kind)
            out += '"'
            return
        if is_named(n.kind):
            for s in seen:
                if s == n.name:
                    write_json_string(n.name, out)
                    return
            seen.append(n.name)
        if n.kind == UNION:
            out += "["
            for k in range(len(n.children)):
                if k:
                    out += ","
                self._write(n.children[k], enclosing_ns, seen, out)
            out += "]"
            return
        out += '{"type":"'
        out += kind_name(n.kind)
        out += '"'
        var ns = enclosing_ns
        if is_named(n.kind):
            out += ',"name":'
            write_json_string(n.short_name(), out)
            if n.namespace != enclosing_ns:
                out += ',"namespace":'
                write_json_string(n.namespace, out)
            ns = n.namespace
        if n.doc:
            out += ',"doc":'
            write_json_string(n.doc, out)
        if len(n.aliases):
            out += ',"aliases":['
            for k in range(len(n.aliases)):
                if k:
                    out += ","
                write_json_string(n.aliases[k], out)
            out += "]"
        if n.kind == RECORD:
            out += ',"fields":['
            for k in range(len(n.fields)):
                if k:
                    out += ","
                ref f = n.fields[k]
                out += '{"name":'
                write_json_string(f.name, out)
                if f.doc:
                    out += ',"doc":'
                    write_json_string(f.doc, out)
                out += ',"type":'
                self._write(f.type_index, ns, seen, out)
                if f.has_default:
                    out += ',"default":'
                    out += f.default_json
                if f.order:
                    out += ',"order":'
                    write_json_string(f.order, out)
                if len(f.aliases):
                    out += ',"aliases":['
                    for a in range(len(f.aliases)):
                        if a:
                            out += ","
                        write_json_string(f.aliases[a], out)
                    out += "]"
                for a in range(len(f.props)):
                    out += ","
                    write_json_string(f.props.keys[a], out)
                    out += ":"
                    out += f.props.vals[a]
                out += "}"
            out += "]"
        elif n.kind == ENUM:
            out += ',"symbols":['
            for k in range(len(n.symbols)):
                if k:
                    out += ","
                write_json_string(n.symbols[k], out)
            out += "]"
            if n.default_symbol:
                out += ',"default":'
                write_json_string(n.default_symbol, out)
        elif n.kind == FIXED:
            out += ',"size":'
            out += String(n.size)
        elif n.kind == ARRAY:
            out += ',"items":'
            self._write(n.children[0], ns, seen, out)
        elif n.kind == MAP:
            out += ',"values":'
            self._write(n.children[0], ns, seen, out)
        if n.logical_type:
            out += ',"logicalType":'
            write_json_string(n.logical_type, out)
            if n.precision >= 0:
                out += ',"precision":'
                out += String(n.precision)
            if n.scale >= 0:
                out += ',"scale":'
                out += String(n.scale)
        for k in range(len(n.props)):
            out += ","
            write_json_string(n.props.keys[k], out)
            out += ":"
            out += n.props.vals[k]
        out += "}"

    def write_to(self, mut writer: Some[Writer]):
        writer.write(self.to_json())

    # ── parsing canonical form + fingerprint ───────────────────────────────

    def parsing_canonical_form(self) -> String:
        var seen = List[String]()
        var out = String()
        self._canonical(self.root, seen, out)
        return out^

    def _canonical(self, i: Int, mut seen: List[String], mut out: String):
        ref n = self.nodes[i]
        if is_primitive(n.kind):
            out += '"'
            out += kind_name(n.kind)
            out += '"'
            return
        if is_named(n.kind):
            for s in seen:
                if s == n.name:
                    write_json_string(n.name, out)
                    return
            seen.append(n.name)
        if n.kind == UNION:
            out += "["
            for k in range(len(n.children)):
                if k:
                    out += ","
                self._canonical(n.children[k], seen, out)
            out += "]"
            return
        out += "{"
        if is_named(n.kind):
            out += '"name":'
            write_json_string(n.name, out)
            out += ","
        out += '"type":"'
        out += kind_name(n.kind)
        out += '"'
        if n.kind == RECORD:
            out += ',"fields":['
            for k in range(len(n.fields)):
                if k:
                    out += ","
                out += '{"name":'
                write_json_string(n.fields[k].name, out)
                out += ',"type":'
                self._canonical(n.fields[k].type_index, seen, out)
                out += "}"
            out += "]"
        elif n.kind == ENUM:
            out += ',"symbols":['
            for k in range(len(n.symbols)):
                if k:
                    out += ","
                write_json_string(n.symbols[k], out)
            out += "]"
        elif n.kind == FIXED:
            out += ',"size":'
            out += String(n.size)
        elif n.kind == ARRAY:
            out += ',"items":'
            self._canonical(n.children[0], seen, out)
        elif n.kind == MAP:
            out += ',"values":'
            self._canonical(n.children[0], seen, out)
        out += "}"

    def fingerprint(self) -> UInt64:
        """The 64-bit Rabin fingerprint (`CRC-64-AVRO`) of the parsing
        canonical form — Avro's own single-object-encoding schema id."""
        return crc64_avro(self.parsing_canonical_form().as_bytes())


comptime _EMPTY64: UInt64 = 0xC15D213AA4D7A795


def crc64_avro(data: Span[UInt8, _]) -> UInt64:
    """Avro's `CRC-64-AVRO` Rabin fingerprint."""
    var table = List[UInt64](capacity=256)
    for i in range(256):
        var fp = UInt64(i)
        for _j in range(8):
            fp = (fp >> 1) ^ (_EMPTY64 & (0 - (fp & 1)))
        table.append(fp)
    var fp = _EMPTY64
    for b in data:
        fp = (fp >> 8) ^ table[Int((fp ^ UInt64(b)) & 0xFF)]
    return fp


# ── parser ─────────────────────────────────────────────────────────────────

comptime _RECORD_KEYS = "type name namespace doc aliases fields logicalType"
comptime _KNOWN_TYPE_KEYS = (
    "type name namespace doc aliases fields symbols items values size"
    " default logicalType precision scale"
)


def _is_known_key(key: StringSlice) -> Bool:
    var probe = String(" ", key, " ")
    return String(" ", _KNOWN_TYPE_KEYS, " ").find(probe) >= 0


struct _SchemaParser(Copyable, Movable):
    var doc: JsonDoc
    var schema: Schema

    def __init__(out self, var doc: JsonDoc):
        self.doc = doc^
        self.schema = Schema()

    def finish(deinit self) -> Schema:
        return self.schema^

    def _push(mut self, var n: SchemaNode) -> Int:
        self.schema.nodes.append(n^)
        return len(self.schema.nodes) - 1

    def _primitive(mut self, kind: Int) -> Int:
        # Bare primitives are shared: one node per kind is enough and keeps
        # the arena small for wide records.
        for i in range(len(self.schema.nodes)):
            ref n = self.schema.nodes[i]
            if (
                n.kind == kind
                and is_primitive(kind)
                and not n.logical_type
                and len(n.props) == 0
            ):
                return i
        return self._push(SchemaNode.of(kind))

    def parse(mut self, j: Int, enclosing_ns: String) raises -> Int:
        var k = self.doc.kind(j)
        if k == JSON_STRING:
            var name = self.doc.as_string(j)
            var prim = primitive_kind(name)
            if prim >= 0:
                return self._primitive(prim)
            return self._lookup(name, enclosing_ns)
        if k == JSON_ARRAY:
            var node = SchemaNode.of(UNION)
            var idx = self._push(node^)
            var kids = List[Int]()
            for b in range(self.doc.len_of(j)):
                var c = self.parse(self.doc.child(j, b), enclosing_ns)
                kids.append(c)
            self.schema.nodes[idx].children = kids^
            return idx
        if k != JSON_OBJECT:
            raise Error("avro.schema: a schema must be a string, array or object")
        return self._object(j, enclosing_ns)

    def _lookup(mut self, name: String, enclosing_ns: String) raises -> Int:
        if name in self.schema.names:
            return self.schema.names[name]
        if enclosing_ns and name.find(".") < 0:
            var full = String(enclosing_ns, ".", name)
            if full in self.schema.names:
                return self.schema.names[full]
        raise Error(String("avro.schema: unknown type name '", name, "'"))

    def _object(mut self, j: Int, enclosing_ns: String) raises -> Int:
        var tj = self.doc.get(j, "type")
        if tj < 0:
            raise Error("avro.schema: object schema without a 'type'")
        if self.doc.kind(tj) != JSON_STRING:
            # e.g. {"type": {"type": "string"}} — recurse on the inner form.
            return self.parse(tj, enclosing_ns)
        var tname = self.doc.as_string(tj)
        var prim = primitive_kind(tname)
        var kind: Int
        if prim >= 0:
            kind = prim
        elif tname == "record" or tname == "error":
            kind = RECORD
        elif tname == "enum":
            kind = ENUM
        elif tname == "array":
            kind = ARRAY
        elif tname == "map":
            kind = MAP
        elif tname == "fixed":
            kind = FIXED
        else:
            # A named-type reference written in object form.
            return self._lookup(tname, enclosing_ns)

        var node = SchemaNode.of(kind)
        var ns = enclosing_ns
        if is_named(kind):
            var nj = self.doc.get(j, "name")
            if nj < 0:
                raise Error(String("avro.schema: ", tname, " without a 'name'"))
            var raw = self.doc.as_string(nj)
            var dot = raw.rfind(".")
            if dot >= 0:
                ns = String(raw[byte=:dot])
                node.name = raw
            else:
                var nsj = self.doc.get(j, "namespace")
                if nsj >= 0:
                    ns = self.doc.as_string(nsj)
                node.name = String(ns, ".", raw) if ns else raw
            node.namespace = ns
        var dj = self.doc.get(j, "doc")
        if dj >= 0:
            node.doc = self.doc.as_string(dj)
        var aj = self.doc.get(j, "aliases")
        if aj >= 0:
            for a in range(self.doc.len_of(aj)):
                node.aliases.append(self.doc.as_string(self.doc.child(aj, a)))
        var lj = self.doc.get(j, "logicalType")
        if lj >= 0:
            node.logical_type = self.doc.as_string(lj)
        var pj = self.doc.get(j, "precision")
        if pj >= 0:
            node.precision = Int(self.doc.as_int(pj))
        var sj = self.doc.get(j, "scale")
        if sj >= 0:
            node.scale = Int(self.doc.as_int(sj))
        if kind == FIXED:
            var zj = self.doc.get(j, "size")
            if zj < 0:
                raise Error("avro.schema: fixed without a 'size'")
            node.size = Int(self.doc.as_int(zj))
        if kind == ENUM:
            var yj = self.doc.get(j, "symbols")
            if yj < 0:
                raise Error("avro.schema: enum without 'symbols'")
            for a in range(self.doc.len_of(yj)):
                node.symbols.append(self.doc.as_string(self.doc.child(yj, a)))
            var fj = self.doc.get(j, "default")
            if fj >= 0:
                node.default_symbol = self.doc.as_string(fj)
        # Unknown attributes are preserved verbatim (Iceberg's field ids).
        for k in range(self.doc.len_of(j)):
            var key = self.doc.key_at(j, k)
            if not _is_known_key(key):
                node.props.set(key, self.doc.to_json(self.doc.child(j, k)))

        var full = node.name
        var idx = self._push(node^)
        if full:
            self.schema.names[full] = idx

        # Children are parsed after registration so recursive references work.
        if kind == ARRAY:
            var ij = self.doc.get(j, "items")
            if ij < 0:
                raise Error("avro.schema: array without 'items'")
            var child = self.parse(ij, ns)
            self.schema.nodes[idx].children.append(child)
        elif kind == MAP:
            var vj = self.doc.get(j, "values")
            if vj < 0:
                raise Error("avro.schema: map without 'values'")
            var child = self.parse(vj, ns)
            self.schema.nodes[idx].children.append(child)
        elif kind == RECORD:
            var fj = self.doc.get(j, "fields")
            if fj < 0:
                raise Error("avro.schema: record without 'fields'")
            var fields = List[Field]()
            for a in range(self.doc.len_of(fj)):
                fields.append(self._field(self.doc.child(fj, a), ns))
            self.schema.nodes[idx].fields = fields^
        return idx

    def _field(mut self, j: Int, ns: String) raises -> Field:
        var nj = self.doc.get(j, "name")
        if nj < 0:
            raise Error("avro.schema: record field without a 'name'")
        var tj = self.doc.get(j, "type")
        if tj < 0:
            raise Error("avro.schema: record field without a 'type'")
        var f = Field.make(self.doc.as_string(nj), self.parse(tj, ns))
        var dj = self.doc.get(j, "doc")
        if dj >= 0:
            f.doc = self.doc.as_string(dj)
        var vj = self.doc.get(j, "default")
        if vj >= 0:
            f.has_default = True
            f.default_json = self.doc.to_json(vj)
        var oj = self.doc.get(j, "order")
        if oj >= 0:
            f.order = self.doc.as_string(oj)
        var aj = self.doc.get(j, "aliases")
        if aj >= 0:
            for a in range(self.doc.len_of(aj)):
                f.aliases.append(self.doc.as_string(self.doc.child(aj, a)))
        for k in range(self.doc.len_of(j)):
            var key = self.doc.key_at(j, k)
            if (
                key != "name"
                and key != "type"
                and key != "doc"
                and key != "default"
                and key != "order"
                and key != "aliases"
            ):
                f.props.set(key, self.doc.to_json(self.doc.child(j, k)))
        return f^


def parse_schema(text: StringSlice) raises -> Schema:
    """Parse an Avro schema from its JSON text."""
    var doc = parse_json(text)
    var p = _SchemaParser(doc^)
    var root = p.parse(p.doc.root, String())
    p.schema.root = root
    return p^.finish()
