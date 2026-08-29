"""Raw DEFLATE (RFC 1951) — `inflate` and `deflate`, in pure Mojo.

Avro's `deflate` codec is *raw* DEFLATE: no zlib (RFC 1950) header, no gzip
wrapper, no trailing checksum. Implementing it here rather than binding zlib
keeps `avro.mojo` free of FFI and of any sibling dependency, so a consumer
needs nothing but `-I ../avro.mojo/src`.

`inflate` handles all three block types (stored, fixed-Huffman,
dynamic-Huffman). `deflate` emits fixed-Huffman blocks from a single-slot
hash-chain LZ77 matcher — the same "fast" strategy `snappy.mojo` uses —
which is enough for Avro blocks and keeps the encoder small.
"""

# ── length / distance tables (RFC 1951 §3.2.5) ─────────────────────────────

comptime _LEN_BASE_S: StaticString = (
    "3,4,5,6,7,8,9,10,11,13,15,17,19,23,27,31,35,43,51,59,67,83,99,115,131,"
    "163,195,227,258"
)
comptime _LEN_EXTRA_S: StaticString = (
    "0,0,0,0,0,0,0,0,1,1,1,1,2,2,2,2,3,3,3,3,4,4,4,4,5,5,5,5,0"
)
comptime _DIST_BASE_S: StaticString = (
    "1,2,3,4,5,7,9,13,17,25,33,49,65,97,129,193,257,385,513,769,1025,1537,"
    "2049,3073,4097,6145,8193,12289,16385,24577"
)
comptime _DIST_EXTRA_S: StaticString = (
    "0,0,0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,11,11,12,12,13,13"
)
comptime _CLEN_ORDER_S: StaticString = (
    "16,17,18,0,8,7,9,6,10,5,11,4,12,3,13,2,14,1,15"
)


def _ints(text: StringSlice) raises -> List[Int]:
    var out = List[Int]()
    for part in text.split(","):
        out.append(Int(String(part)))
    return out^


@fieldwise_init
struct _Tables(Copyable, Movable):
    """RFC 1951\'s length and distance tables.

    Mojo cannot materialise a `comptime List[Int]` into runtime code, so the
    tables are rebuilt (from their literal text) once per `inflate`/`deflate`
    call and passed down by reference.
    """

    var len_base: List[Int]
    var len_extra: List[Int]
    var dist_base: List[Int]
    var dist_extra: List[Int]
    var clen_order: List[Int]

    def __init__(out self) raises:
        self.len_base = _ints(_LEN_BASE_S)
        self.len_extra = _ints(_LEN_EXTRA_S)
        self.dist_base = _ints(_DIST_BASE_S)
        self.dist_extra = _ints(_DIST_EXTRA_S)
        self.clen_order = _ints(_CLEN_ORDER_S)


# ── canonical Huffman decoding (the "puff" scheme) ─────────────────────────


struct _Huffman(Copyable, Movable, Defaultable):
    var counts: List[Int]
    """counts[n] = how many symbols have a code of n bits (n = 0..15)."""
    var symbols: List[Int]
    """Symbols ordered by code length, then by symbol value."""

    def __init__(out self):
        self.counts = List[Int]()
        self.symbols = List[Int]()

    def __init__(out self, lengths: List[Int]) raises:
        self.counts = List[Int](length=16, fill=0)
        for i in range(len(lengths)):
            self.counts[lengths[i]] += 1
        if self.counts[0] == len(lengths):
            # An all-zero table is legal (an unused distance tree).
            self.symbols = List[Int]()
            return
        var left = 1
        for n in range(1, 16):
            left <<= 1
            left -= self.counts[n]
            if left < 0:
                raise Error("avro.deflate: over-subscribed Huffman code")
        var offs = List[Int](length=16, fill=0)
        for n in range(1, 15):
            offs[n + 1] = offs[n] + self.counts[n]
        self.symbols = List[Int](length=len(lengths), fill=0)
        for i in range(len(lengths)):
            if lengths[i]:
                self.symbols[offs[lengths[i]]] = i
                offs[lengths[i]] += 1


struct _BitReader[origin: ImmOrigin](Copyable, Movable):
    var data: Span[UInt8, Self.origin]
    var pos: Int
    var bitbuf: UInt32
    var bitcnt: Int

    def __init__(out self, data: Span[UInt8, Self.origin]):
        self.data = data
        self.pos = 0
        self.bitbuf = 0
        self.bitcnt = 0

    def bits(mut self, need: Int) raises -> Int:
        while self.bitcnt < need:
            if self.pos >= len(self.data):
                raise Error("avro.deflate: truncated DEFLATE stream")
            self.bitbuf |= UInt32(self.data[self.pos]) << UInt32(self.bitcnt)
            self.pos += 1
            self.bitcnt += 8
        var v = Int(self.bitbuf & ((UInt32(1) << UInt32(need)) - 1))
        self.bitbuf >>= UInt32(need)
        self.bitcnt -= need
        return v

    def align(mut self):
        self.bitbuf = 0
        self.bitcnt = 0

    def decode(mut self, h: _Huffman) raises -> Int:
        var code = 0
        var first = 0
        var index = 0
        for length in range(1, 16):
            code |= self.bits(1)
            var count = h.counts[length]
            if code - first < count:
                return h.symbols[index + (code - first)]
            index += count
            first = (first + count) << 1
            code <<= 1
        raise Error("avro.deflate: invalid Huffman code")


def _fixed_literal_lengths() -> List[Int]:
    var l = List[Int](length=288, fill=8)
    for i in range(144, 256):
        l[i] = 9
    for i in range(256, 280):
        l[i] = 7
    return l^


def inflate(data: Span[UInt8, _]) raises -> List[UInt8]:
    """Decompress a raw DEFLATE stream."""
    var out = List[UInt8]()
    var t = _Tables()
    var br = _BitReader(data)
    var fixed_lit = _Huffman(_fixed_literal_lengths())
    var fixed_dist = _Huffman(List[Int](length=30, fill=5))
    while True:
        var final = br.bits(1)
        var btype = br.bits(2)
        if btype == 0:
            br.align()
            if br.pos + 4 > len(br.data):
                raise Error("avro.deflate: truncated stored block header")
            var length = Int(br.data[br.pos]) | (Int(br.data[br.pos + 1]) << 8)
            br.pos += 4  # LEN then its one's complement NLEN
            if br.pos + length > len(br.data):
                raise Error("avro.deflate: truncated stored block")
            out.extend(br.data[br.pos : br.pos + length])
            br.pos += length
        elif btype == 1:
            _inflate_block(br, fixed_lit, fixed_dist, t, out)
        elif btype == 2:
            var nlen = br.bits(5) + 257
            var ndist = br.bits(5) + 1
            var ncode = br.bits(4) + 4
            var clen = List[Int](length=19, fill=0)
            for i in range(ncode):
                clen[t.clen_order[i]] = br.bits(3)
            var ch = _Huffman(clen)
            var lengths = List[Int](length=nlen + ndist, fill=0)
            var i = 0
            while i < nlen + ndist:
                var sym = br.decode(ch)
                if sym < 16:
                    lengths[i] = sym
                    i += 1
                elif sym == 16:
                    if i == 0:
                        raise Error("avro.deflate: repeat with no previous length")
                    var prev = lengths[i - 1]
                    var rep = 3 + br.bits(2)
                    for _k in range(rep):
                        lengths[i] = prev
                        i += 1
                elif sym == 17:
                    var rep = 3 + br.bits(3)
                    i += rep
                else:
                    var rep = 11 + br.bits(7)
                    i += rep
            if i > nlen + ndist:
                raise Error("avro.deflate: code-length overflow")
            var lit_lengths = List[Int](capacity=nlen)
            for k in range(nlen):
                lit_lengths.append(lengths[k])
            var dist_lengths = List[Int](capacity=ndist)
            for k in range(ndist):
                dist_lengths.append(lengths[nlen + k])
            var lh = _Huffman(lit_lengths)
            var dh = _Huffman(dist_lengths)
            _inflate_block(br, lh, dh, t, out)
        else:
            raise Error("avro.deflate: reserved block type 3")
        if final:
            break
    return out^


def _inflate_block(
    mut br: _BitReader,
    lit: _Huffman,
    dist: _Huffman,
    t: _Tables,
    mut out: List[UInt8],
) raises:
    while True:
        var sym = br.decode(lit)
        if sym < 256:
            out.append(UInt8(sym))
        elif sym == 256:
            return
        else:
            var lc = sym - 257
            if lc >= 29:
                raise Error("avro.deflate: invalid length symbol")
            var length = t.len_base[lc] + br.bits(t.len_extra[lc])
            var dc = br.decode(dist)
            if dc >= 30:
                raise Error("avro.deflate: invalid distance symbol")
            var d = t.dist_base[dc] + br.bits(t.dist_extra[dc])
            if d > len(out):
                raise Error("avro.deflate: distance runs before the output start")
            var start = len(out) - d
            # Overlapping copies are legal and must be byte-at-a-time.
            for k in range(length):
                out.append(out[start + k])


# ── compression ────────────────────────────────────────────────────────────


struct _BitWriter(Copyable, Movable, Defaultable):
    var out: List[UInt8]
    var bitbuf: UInt32
    var bitcnt: Int

    def __init__(out self):
        self.out = List[UInt8]()
        self.bitbuf = 0
        self.bitcnt = 0

    def put(mut self, value: Int, count: Int):
        """Write `count` low bits of `value`, least-significant first."""
        self.bitbuf |= UInt32(value) << UInt32(self.bitcnt)
        self.bitcnt += count
        while self.bitcnt >= 8:
            self.out.append(UInt8(self.bitbuf & 0xFF))
            self.bitbuf >>= 8
            self.bitcnt -= 8

    def put_code(mut self, code: Int, count: Int):
        """Write a Huffman code: its bits go out most-significant first."""
        var v = 0
        for k in range(count):
            v |= ((code >> (count - 1 - k)) & 1) << k
        self.put(v, count)

    def take(deinit self) -> List[UInt8]:
        return self.out^

    def flush(mut self):
        if self.bitcnt:
            self.out.append(UInt8(self.bitbuf & 0xFF))
            self.bitbuf = 0
            self.bitcnt = 0


def _fixed_lit_code(sym: Int) -> Tuple[Int, Int]:
    """(code, bit count) for a literal/length symbol in a fixed block."""
    if sym < 144:
        return (0x30 + sym, 8)
    if sym < 256:
        return (0x190 + sym - 144, 9)
    if sym < 280:
        return (sym - 256, 7)
    return (0xC0 + sym - 280, 8)


comptime _HASH_BITS = 15
comptime _HASH_SIZE = 1 << _HASH_BITS
comptime _MAX_MATCH = 258
comptime _MIN_MATCH = 3
comptime _MAX_DIST = 32768


def deflate(data: Span[UInt8, _]) raises -> List[UInt8]:
    """Compress to a raw DEFLATE stream (one fixed-Huffman block)."""
    var t = _Tables()
    var w = _BitWriter()
    w.put(1, 1)  # BFINAL
    w.put(1, 2)  # BTYPE = fixed Huffman
    var head = List[Int](length=_HASH_SIZE, fill=-1)
    var prev = List[Int](length=max(len(data), 1), fill=-1)
    var i = 0
    var n = len(data)
    while i < n:
        var best_len = 0
        var best_dist = 0
        if i + _MIN_MATCH <= n:
            var h = _hash3(data, i)
            var cand = head[h]
            var chain = 0
            while cand >= 0 and i - cand <= _MAX_DIST and chain < 16:
                var l = _match_len(data, cand, i, n)
                if l > best_len:
                    best_len = l
                    best_dist = i - cand
                    if l >= _MAX_MATCH:
                        break
                cand = prev[cand]
                chain += 1
            prev[i] = head[h]
            head[h] = i
        if best_len >= _MIN_MATCH:
            var lc = 0
            while lc < 28 and t.len_base[lc + 1] <= best_len:
                lc += 1
            var code_bits = _fixed_lit_code(257 + lc)
            w.put_code(code_bits[0], code_bits[1])
            if t.len_extra[lc]:
                w.put(best_len - t.len_base[lc], t.len_extra[lc])
            var dc = 0
            while dc < 29 and t.dist_base[dc + 1] <= best_dist:
                dc += 1
            w.put_code(dc, 5)
            if t.dist_extra[dc]:
                w.put(best_dist - t.dist_base[dc], t.dist_extra[dc])
            # Index the bytes the match covers so later matches can find them.
            for k in range(1, best_len):
                var p = i + k
                if p + _MIN_MATCH <= n:
                    var hh = _hash3(data, p)
                    prev[p] = head[hh]
                    head[hh] = p
            i += best_len
        else:
            var code_bits = _fixed_lit_code(Int(data[i]))
            w.put_code(code_bits[0], code_bits[1])
            i += 1
    var end = _fixed_lit_code(256)
    w.put_code(end[0], end[1])
    w.flush()
    return w^.take()


def _hash3(data: Span[UInt8, _], i: Int) -> Int:
    var v = (UInt32(data[i]) << 16) | (UInt32(data[i + 1]) << 8) | UInt32(data[i + 2])
    var h = (v * UInt32(0x1E35A7BD)) >> UInt32(32 - _HASH_BITS)
    return Int(h) & (_HASH_SIZE - 1)


def _match_len(data: Span[UInt8, _], a: Int, b: Int, n: Int) -> Int:
    var l = 0
    while b + l < n and l < _MAX_MATCH and data[a + l] == data[b + l]:
        l += 1
    return l
