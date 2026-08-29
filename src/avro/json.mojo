"""A small, self-contained JSON reader/writer.

Avro schemas are JSON, and Object Container File metadata carries raw JSON
strings (Iceberg puts its table schema and partition spec there), so
`avro.mojo` needs a JSON parser. It is deliberately in-repo: the library then
has no imports outside the Mojo standard library, and a consumer only needs
`-I ../avro.mojo/src`.

The document is an arena (`List[JsonNode]`) with integer child indices —
Mojo structs cannot contain themselves, and an arena also makes the recursive
schema references Avro allows fall out naturally.
"""

comptime JSON_NULL: Int = 0
comptime JSON_BOOL: Int = 1
comptime JSON_INT: Int = 2
comptime JSON_FLOAT: Int = 3
comptime JSON_STRING: Int = 4
comptime JSON_ARRAY: Int = 5
comptime JSON_OBJECT: Int = 6


@fieldwise_init
struct JsonNode(Copyable, Movable):
    """One JSON value inside a `JsonDoc` arena."""

    var kind: Int
    var b: Bool
    var i: Int64
    var f: Float64
    var s: String
    """String payload for JSON_STRING; unused otherwise."""
    var keys: List[String]
    """Object member names, in document order. Empty for non-objects."""
    var kids: List[Int]
    """Child node indices: array elements, or object member values."""

    @staticmethod
    def scalar(kind: Int) -> JsonNode:
        return JsonNode(kind, False, 0, 0.0, String(), List[String](), List[Int]())


struct JsonDoc(Copyable, Movable, Writable):
    """A parsed JSON document: a flat arena plus the index of the root value."""

    var nodes: List[JsonNode]
    var root: Int

    def __init__(out self):
        self.nodes = List[JsonNode]()
        self.root = -1

    def __init__(out self, *, copy: Self):
        self.nodes = copy.nodes.copy()
        self.root = copy.root

    def __init__(out self, *, deinit move: Self):
        self.nodes = move.nodes^
        self.root = move.root

    # ── accessors ──────────────────────────────────────────────────────────

    def kind(self, i: Int) -> Int:
        return self.nodes[i].kind

    def is_null(self, i: Int) -> Bool:
        return self.nodes[i].kind == JSON_NULL

    def as_bool(self, i: Int) -> Bool:
        return self.nodes[i].b

    def as_int(self, i: Int) -> Int64:
        if self.nodes[i].kind == JSON_FLOAT:
            return Int64(self.nodes[i].f)
        return self.nodes[i].i

    def as_float(self, i: Int) -> Float64:
        if self.nodes[i].kind == JSON_INT:
            return Float64(self.nodes[i].i)
        return self.nodes[i].f

    def as_string(self, i: Int) -> String:
        return self.nodes[i].s

    def len_of(self, i: Int) -> Int:
        return len(self.nodes[i].kids)

    def child(self, i: Int, k: Int) -> Int:
        return self.nodes[i].kids[k]

    def key_at(self, i: Int, k: Int) -> String:
        return self.nodes[i].keys[k]

    def get(self, i: Int, name: StringSlice) -> Int:
        """Object member lookup by name; -1 when absent."""
        ref n = self.nodes[i]
        for k in range(len(n.keys)):
            if n.keys[k] == name:
                return n.kids[k]
        return -1

    def has(self, i: Int, name: StringSlice) -> Bool:
        return self.get(i, name) != -1

    # ── writing ────────────────────────────────────────────────────────────

    def write_node(self, i: Int, mut out: String):
        ref n = self.nodes[i]
        if n.kind == JSON_NULL:
            out += "null"
        elif n.kind == JSON_BOOL:
            out += "true" if n.b else "false"
        elif n.kind == JSON_INT:
            out += String(n.i)
        elif n.kind == JSON_FLOAT:
            out += String(n.f)
        elif n.kind == JSON_STRING:
            write_json_string(n.s, out)
        elif n.kind == JSON_ARRAY:
            out += "["
            for k in range(len(n.kids)):
                if k:
                    out += ","
                self.write_node(n.kids[k], out)
            out += "]"
        else:
            out += "{"
            for k in range(len(n.kids)):
                if k:
                    out += ","
                write_json_string(n.keys[k], out)
                out += ":"
                self.write_node(n.kids[k], out)
            out += "}"

    def to_json(self, i: Int) -> String:
        var out = String()
        self.write_node(i, out)
        return out^

    def write_to(self, mut writer: Some[Writer]):
        writer.write(self.to_json(self.root))


def write_json_string(s: StringSlice, mut out: String):
    """Append `s` to `out` as a quoted, escaped JSON string."""
    out += '"'
    for b in s.as_bytes():
        if b == UInt8(ord('"')):
            out += '\\"'
        elif b == UInt8(ord("\\")):
            out += "\\\\"
        elif b == UInt8(ord("\n")):
            out += "\\n"
        elif b == UInt8(ord("\r")):
            out += "\\r"
        elif b == UInt8(ord("\t")):
            out += "\\t"
        elif b == 8:
            out += "\\b"
        elif b == 12:
            out += "\\f"
        elif b < 0x20:
            out += "\\u00"
            out += _HEX[byte= Int(b >> 4)]
            out += _HEX[byte= Int(b & 0xF)]
        else:
            # Pass every other byte through: the input is already UTF-8 and
            # JSON permits raw UTF-8 in strings.
            out += StringSlice(unsafe_from_utf8=Span(_one_byte(b)))
    out += '"'


comptime _HEX = "0123456789abcdef"


def _one_byte(b: UInt8) -> List[UInt8]:
    var l = List[UInt8](capacity=1)
    l.append(b)
    return l^


# ── parsing ────────────────────────────────────────────────────────────────


struct _JsonParser(Copyable, Movable):
    var src: List[UInt8]
    var pos: Int
    var doc: JsonDoc

    def __init__(out self, text: StringSlice):
        self.src = List[UInt8]()
        self.src.extend(text.as_bytes())
        self.pos = 0
        self.doc = JsonDoc()

    def _fail(self, msg: StringSlice) raises -> Int:
        raise Error(String("avro.json: ", msg, " at byte ", self.pos))

    def _skip_ws(mut self):
        while self.pos < len(self.src):
            var c = self.src[self.pos]
            if c == 0x20 or c == 0x09 or c == 0x0A or c == 0x0D:
                self.pos += 1
            else:
                break

    def _peek(self) -> UInt8:
        if self.pos < len(self.src):
            return self.src[self.pos]
        return 0

    def _expect(mut self, c: UInt8) raises:
        if self.pos >= len(self.src) or self.src[self.pos] != c:
            _ = self._fail(String("expected '", StringSlice(unsafe_from_utf8=Span(_one_byte(c))), "'"))
        self.pos += 1

    def _push(mut self, var node: JsonNode) -> Int:
        self.doc.nodes.append(node^)
        return len(self.doc.nodes) - 1

    def finish(deinit self) -> JsonDoc:
        return self.doc^

    def parse(mut self) raises -> Int:
        self._skip_ws()
        var r = self._value()
        self._skip_ws()
        if self.pos != len(self.src):
            _ = self._fail("trailing data")
        return r

    def _value(mut self) raises -> Int:
        self._skip_ws()
        if self.pos >= len(self.src):
            return self._fail("unexpected end of input")
        var c = self.src[self.pos]
        if c == UInt8(ord("{")):
            return self._object()
        if c == UInt8(ord("[")):
            return self._array()
        if c == UInt8(ord('"')):
            var s = self._string()
            var n = JsonNode.scalar(JSON_STRING)
            n.s = s^
            return self._push(n^)
        if c == UInt8(ord("t")):
            self._lit("true")
            var n = JsonNode.scalar(JSON_BOOL)
            n.b = True
            return self._push(n^)
        if c == UInt8(ord("f")):
            self._lit("false")
            var n = JsonNode.scalar(JSON_BOOL)
            n.b = False
            return self._push(n^)
        if c == UInt8(ord("n")):
            self._lit("null")
            return self._push(JsonNode.scalar(JSON_NULL))
        return self._number()

    def _lit(mut self, word: StringSlice) raises:
        var w = word.as_bytes()
        if self.pos + len(w) > len(self.src):
            _ = self._fail("truncated literal")
        for k in range(len(w)):
            if self.src[self.pos + k] != w[k]:
                _ = self._fail("bad literal")
        self.pos += len(w)

    def _number(mut self) raises -> Int:
        var start = self.pos
        var is_float = False
        if self._peek() == UInt8(ord("-")) or self._peek() == UInt8(ord("+")):
            self.pos += 1
        while self.pos < len(self.src):
            var c = self.src[self.pos]
            if c >= UInt8(ord("0")) and c <= UInt8(ord("9")):
                self.pos += 1
            elif c == UInt8(ord(".")) or c == UInt8(ord("e")) or c == UInt8(ord("E")):
                is_float = True
                self.pos += 1
            elif (c == UInt8(ord("-")) or c == UInt8(ord("+"))) and (
                self.src[self.pos - 1] == UInt8(ord("e")) or self.src[self.pos - 1] == UInt8(ord("E"))
            ):
                self.pos += 1
            else:
                break
        if self.pos == start:
            return self._fail("expected a value")
        var text = String(from_utf8=Span(self.src)[start : self.pos])
        if is_float:
            var n = JsonNode.scalar(JSON_FLOAT)
            n.f = Float64(text)
            return self._push(n^)
        var n = JsonNode.scalar(JSON_INT)
        n.i = Int64(Int(text))
        return self._push(n^)

    def _string(mut self) raises -> String:
        self._expect(UInt8(ord('"')))
        var buf = List[UInt8]()
        while True:
            if self.pos >= len(self.src):
                _ = self._fail("unterminated string")
            var c = self.src[self.pos]
            if c == UInt8(ord('"')):
                self.pos += 1
                break
            if c == UInt8(ord("\\")):
                self.pos += 1
                if self.pos >= len(self.src):
                    _ = self._fail("unterminated escape")
                var e = self.src[self.pos]
                self.pos += 1
                if e == UInt8(ord("n")):
                    buf.append(0x0A)
                elif e == UInt8(ord("t")):
                    buf.append(0x09)
                elif e == UInt8(ord("r")):
                    buf.append(0x0D)
                elif e == UInt8(ord("b")):
                    buf.append(0x08)
                elif e == UInt8(ord("f")):
                    buf.append(0x0C)
                elif e == UInt8(ord("/")):
                    buf.append(UInt8(ord("/")))
                elif e == UInt8(ord('"')):
                    buf.append(UInt8(ord('"')))
                elif e == UInt8(ord("\\")):
                    buf.append(UInt8(ord("\\")))
                elif e == UInt8(ord("u")):
                    var cp = self._hex4()
                    if cp >= 0xD800 and cp <= 0xDBFF and self.pos + 1 < len(self.src):
                        if self.src[self.pos] == UInt8(ord("\\")) and self.src[self.pos + 1] == UInt8(ord("u")):
                            self.pos += 2
                            var lo = self._hex4()
                            if lo >= 0xDC00 and lo <= 0xDFFF:
                                cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00)
                            else:
                                _encode_utf8(cp, buf)
                                cp = lo
                    _encode_utf8(cp, buf)
                else:
                    _ = self._fail("unknown escape")
            else:
                buf.append(c)
                self.pos += 1
        return String(from_utf8=Span(buf))

    def _hex4(mut self) raises -> Int:
        if self.pos + 4 > len(self.src):
            _ = self._fail("truncated \\u escape")
        var v = 0
        for _k in range(4):
            var c = self.src[self.pos]
            self.pos += 1
            if c >= UInt8(ord("0")) and c <= UInt8(ord("9")):
                v = v * 16 + Int(c - UInt8(ord("0")))
            elif c >= UInt8(ord("a")) and c <= UInt8(ord("f")):
                v = v * 16 + Int(c - UInt8(ord("a"))) + 10
            elif c >= UInt8(ord("A")) and c <= UInt8(ord("F")):
                v = v * 16 + Int(c - UInt8(ord("A"))) + 10
            else:
                _ = self._fail("bad hex digit")
        return v

    def _array(mut self) raises -> Int:
        self._expect(UInt8(ord("[")))
        var kids = List[Int]()
        self._skip_ws()
        if self._peek() == UInt8(ord("]")):
            self.pos += 1
        else:
            while True:
                kids.append(self._value())
                self._skip_ws()
                if self._peek() == UInt8(ord(",")):
                    self.pos += 1
                    continue
                self._expect(UInt8(ord("]")))
                break
        var n = JsonNode.scalar(JSON_ARRAY)
        n.kids = kids^
        return self._push(n^)

    def _object(mut self) raises -> Int:
        self._expect(UInt8(ord("{")))
        var keys = List[String]()
        var kids = List[Int]()
        self._skip_ws()
        if self._peek() == UInt8(ord("}")):
            self.pos += 1
        else:
            while True:
                self._skip_ws()
                keys.append(self._string())
                self._skip_ws()
                self._expect(UInt8(ord(":")))
                kids.append(self._value())
                self._skip_ws()
                if self._peek() == UInt8(ord(",")):
                    self.pos += 1
                    continue
                self._expect(UInt8(ord("}")))
                break
        var n = JsonNode.scalar(JSON_OBJECT)
        n.keys = keys^
        n.kids = kids^
        return self._push(n^)


def _encode_utf8(cp: Int, mut buf: List[UInt8]):
    if cp < 0x80:
        buf.append(UInt8(cp))
    elif cp < 0x800:
        buf.append(UInt8(0xC0 | (cp >> 6)))
        buf.append(UInt8(0x80 | (cp & 0x3F)))
    elif cp < 0x10000:
        buf.append(UInt8(0xE0 | (cp >> 12)))
        buf.append(UInt8(0x80 | ((cp >> 6) & 0x3F)))
        buf.append(UInt8(0x80 | (cp & 0x3F)))
    else:
        buf.append(UInt8(0xF0 | (cp >> 18)))
        buf.append(UInt8(0x80 | ((cp >> 12) & 0x3F)))
        buf.append(UInt8(0x80 | ((cp >> 6) & 0x3F)))
        buf.append(UInt8(0x80 | (cp & 0x3F)))


def parse_json(text: StringSlice) raises -> JsonDoc:
    """Parse `text` into a `JsonDoc`."""
    var p = _JsonParser(text)
    var root = p.parse()
    p.doc.root = root
    return p^.finish()
