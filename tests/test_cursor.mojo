"""`RecordCursor` tests — run with `pixi run test-cursor`.

The oracle is the `Value` path: whatever `DataFileReader.next()` decodes, the
cursor has to decode identically, field for field, on the same files. On top
of that there are tests for what only the cursor has — slots, selection,
resolution-at-plan-time and the no-allocation promise.
"""

from std.testing import (
    TestSuite,
    assert_almost_equal,
    assert_equal,
    assert_false,
    assert_raises,
    assert_true,
)

from avro import (
    DataFileReader,
    DefaultCodecs,
    DataFileWriter,
    RecordCursor,
    Value,
    parse_schema,
    resolve,
)
from avro.codec import CodecSet, unknown_codec
from avro.cursor import DecodePlan
from avro.schema import ARRAY, BYTES, DOUBLE, ENUM, FIXED, LONG, MAP, STRING
from avro.value import ArrayBuilder, MapBuilder, RecordBuilder

comptime FIXTURE: StaticString = "tests/fixtures/fastavro_null.avro"
comptime MANIFEST: StaticString = "tests/fixtures/iceberg_manifest.avro"
comptime MANIFEST_LIST: StaticString = "tests/fixtures/iceberg_manifest_list.avro"


def hex_of(data: Span[UInt8, _]) -> String:
    comptime H = "0123456789abcdef"
    var out = String()
    for b in data:
        out += H[byte= Int(b >> 4)]
        out += H[byte= Int(b & 0xF)]
    return out^


# ── the slot table ─────────────────────────────────────────────────────────


def test_plan_paths_are_dotted() raises:
    var c = RecordCursor.open(FIXTURE)
    assert_true(c.plan.has_slot("i64"))
    assert_true(c.plan.has_slot("inner.a"))
    assert_true(c.plan.has_slot("inner.b"))
    assert_true(c.plan.has_slot("inner.b.element"))
    assert_true(c.plan.has_slot("xs"))
    assert_true(c.plan.has_slot("xs.element"))
    assert_true(c.plan.has_slot("props"))
    assert_true(c.plan.has_slot("props.key"))
    assert_true(c.plan.has_slot("props.value"))
    # ["null", "string"] is one slot: the null branch does not take the kind.
    assert_equal(c.plan.slot_kind(c.plan.slot_of("maybe")), STRING)
    # a union that is not a plain optional exposes its branch
    assert_true(c.plan.has_slot("choice.$branch"))
    assert_false(c.plan.has_slot("maybe.$branch"))
    assert_equal(c.plan.slot_kind(c.plan.slot_of("xs")), ARRAY)
    assert_equal(c.plan.slot_kind(c.plan.slot_of("props")), MAP)
    assert_equal(c.plan.slot_depth(c.plan.slot_of("xs.element")), 1)
    assert_equal(c.plan.slot_depth(c.plan.slot_of("i64")), 0)
    with assert_raises():
        _ = c.plan.slot_of("no-such-field")


def test_selection_prunes_the_slot_table() raises:
    var c = RecordCursor.open(FIXTURE, ["i64", "inner.b"])
    assert_true(c.plan.has_slot("i64"))
    assert_true(c.plan.has_slot("inner.b"))
    assert_true(c.plan.has_slot("inner.b.element"))
    assert_false(c.plan.has_slot("inner.a"))
    assert_false(c.plan.has_slot("text"))
    # Unselected fields still have ops — the decoder must step over them.
    assert_true(len(c.plan.ops) > c.plan.num_slots())
    assert_true(c.next())
    assert_equal(c.get_long(c.plan.slot_of("i64")), -2000000000)


# ── cross-checking every type against the Value path ───────────────────────


def _check_against_value(path: StringSlice) raises -> Int:
    """Read `path` twice — cursor and `Value` — and compare every field."""
    var v = DataFileReader.open(path)
    var c = RecordCursor.open(path)
    var s_flag = c.plan.slot_of("flag")
    var s_i32 = c.plan.slot_of("i32")
    var s_i64 = c.plan.slot_of("i64")
    var s_f32 = c.plan.slot_of("f32")
    var s_f64 = c.plan.slot_of("f64")
    var s_blob = c.plan.slot_of("blob")
    var s_text = c.plan.slot_of("text")
    var s_suit = c.plan.slot_of("suit")
    var s_key = c.plan.slot_of("key")
    var s_xs = c.plan.slot_of("xs")
    var s_x = c.plan.slot_of("xs.element")
    var s_props = c.plan.slot_of("props")
    var s_pk = c.plan.slot_of("props.key")
    var s_pv = c.plan.slot_of("props.value")
    var s_maybe = c.plan.slot_of("maybe")
    var s_choice = c.plan.slot_of("choice")
    var s_branch = c.plan.slot_of("choice.$branch")
    var s_inner_a = c.plan.slot_of("inner.a")
    var s_inner_b = c.plan.slot_of("inner.b")
    var s_inner_be = c.plan.slot_of("inner.b.element")
    var s_nothing = c.plan.slot_of("nothing")
    var s_amount = c.plan.slot_of("amount")

    var n = 0
    while c.next():
        assert_true(v.has_next())
        var rec = v.next()
        n += 1
        assert_true(c.is_null(s_nothing))
        assert_equal(c.get_bool(s_flag), rec.field("flag").as_bool())
        assert_equal(c.get_long(s_i32), rec.field("i32").as_long())
        assert_equal(c.get_long(s_i64), rec.field("i64").as_long())
        assert_almost_equal(
            Float64(c.get_float(s_f32)),
            Float64(rec.field("f32").as_float()),
            atol=Float64(1e-9),
        )
        assert_almost_equal(
            c.get_double(s_f64), rec.field("f64").as_double(), atol=Float64(1e-12)
        )
        assert_equal(
            hex_of(c.get_bytes(s_blob)), hex_of(Span(rec.field("blob").as_bytes()))
        )
        assert_equal(String(c.get_str(s_text)), rec.field("text").as_string())
        assert_equal(c.get_symbol(s_suit), rec.field("suit").symbol())
        assert_equal(c.enum_index(s_suit), rec.field("suit").enum_index())
        assert_equal(
            hex_of(c.get_bytes(s_key)), hex_of(Span(rec.field("key").as_bytes()))
        )
        assert_equal(
            hex_of(c.get_bytes(s_amount)),
            hex_of(Span(rec.field("amount").as_bytes())),
        )

        var xs = rec.field("xs")
        assert_equal(c.array_len(s_xs), len(xs))
        for k in range(len(xs)):
            assert_equal(c.get_long(s_x, k), xs.at(k).as_long())

        var props = rec.field("props")
        assert_equal(c.array_len(s_props), len(props))
        for k in range(len(props)):
            assert_equal(String(c.get_str(s_pk, k)), props.key(k))
            assert_equal(c.get_long(s_pv, k), props.at(k).as_long())

        var maybe = rec.field("maybe")
        assert_equal(c.is_null(s_maybe), maybe.is_null())
        if not maybe.is_null():
            assert_equal(String(c.get_str(s_maybe)), maybe.as_string())

        var choice = rec.field_raw("choice")
        assert_equal(c.union_branch(s_branch), choice.union_index())
        var picked = choice.unwrap()
        if choice.union_index() == 0:
            assert_equal(c.get_long(s_choice), picked.as_long())
        elif choice.union_index() == 1:
            assert_equal(String(c.get_str(s_choice)), picked.as_string())
        else:
            # `Value.as_long()` reads a boolean's integer payload, which is
            # always 0 — compare it as the boolean it is.
            assert_equal(c.get_bool(s_choice), picked.as_bool())

        var inner = rec.field("inner")
        assert_equal(c.get_long(s_inner_a), inner.field("a").as_long())
        var b = inner.field("b")
        assert_equal(c.array_len(s_inner_b), len(b))
        for k in range(len(b)):
            assert_equal(String(c.get_str(s_inner_be, k)), b.at(k).as_string())
    assert_false(v.has_next())
    return n


def test_matches_the_value_path_on_every_type() raises:
    assert_equal(_check_against_value(FIXTURE), 240)


def test_matches_the_value_path_with_deflate() raises:
    assert_equal(_check_against_value("tests/fixtures/fastavro_deflate.avro"), 240)


def test_reads_an_iceberg_manifest() raises:
    var v = DataFileReader.open(MANIFEST)
    var c = RecordCursor.open(MANIFEST)
    var s_status = c.plan.slot_of("status")
    var s_path = c.plan.slot_of("data_file.file_path")
    var s_rows = c.plan.slot_of("data_file.record_count")
    var s_lb = c.plan.slot_of("data_file.lower_bounds")
    var s_lbk = c.plan.slot_of("data_file.lower_bounds.element.key")
    var s_lbv = c.plan.slot_of("data_file.lower_bounds.element.value")
    var n = 0
    while c.next():
        var rec = v.next()
        n += 1
        var df = rec.field("data_file")
        assert_equal(c.get_long(s_status), rec.field("status").as_long())
        assert_equal(String(c.get_str(s_path)), df.field("file_path").as_string())
        assert_equal(c.get_long(s_rows), df.field("record_count").as_long())
        var lb = df.field("lower_bounds")
        assert_equal(c.array_len(s_lb), len(lb))
        for k in range(len(lb)):
            var e = lb.at(k)
            assert_equal(c.get_long(s_lbk, k), e.field("key").as_long())
            assert_equal(
                hex_of(c.get_bytes(s_lbv, k)),
                hex_of(Span(e.field("value").as_bytes())),
            )
    assert_equal(n, 2)
    assert_false(v.has_next())


def test_reads_a_manifest_list() raises:
    var c = RecordCursor.open(MANIFEST_LIST)
    var s_path = c.plan.slot_of("manifest_path")
    var s_len = c.plan.slot_of("manifest_length")
    var s_parts = c.plan.slot_of("partitions")
    var s_null = c.plan.slot_of("partitions.element.contains_null")
    var v = DataFileReader.open(MANIFEST_LIST)
    var n = 0
    while c.next():
        var rec = v.next()
        n += 1
        assert_equal(
            String(c.get_str(s_path)), rec.field("manifest_path").as_string()
        )
        assert_equal(c.get_long(s_len), rec.field("manifest_length").as_long())
        var parts = rec.field("partitions")
        assert_equal(c.array_len(s_parts), len(parts))
        for k in range(len(parts)):
            assert_equal(
                c.get_bool(s_null, k),
                parts.at(k).field("contains_null").as_bool(),
            )
    assert_true(n > 0)


# ── the file's own metadata is still reachable ─────────────────────────────


def test_metadata_survives_the_cursor() raises:
    var c = RecordCursor.open(FIXTURE)
    assert_equal(c.reader.metadata_string("written-by"), "fastavro")
    assert_equal(c.reader.codec, "null")
    assert_true(c.reader.schema_json().byte_length() > 100)


# ── no per-record allocation ───────────────────────────────────────────────


def test_a_second_pass_allocates_nothing() raises:
    """Slot buffers only ever grow, so a repeat run that does not move the
    watermark is a run that did not touch the allocator."""
    var data = DataFileReader.open(FIXTURE).data.copy()
    var c = RecordCursor.from_bytes(Span(data))
    var first = 0
    while c.next():
        first += 1
    assert_equal(first, 240)
    var warm = c.slot_watermark()
    assert_true(warm > 0)
    # Point the same cursor — the same slot buffers — at the same bytes
    # again. Every value now overwrites one the first pass left behind.
    c.reader = DataFileReader.from_bytes(Span(data))
    var seen = 0
    while c.next():
        seen += 1
    assert_equal(seen, 240)
    assert_equal(c.slot_watermark(), warm)


def test_fixed_shape_settles_after_one_record() raises:
    """With no arrays or maps in the schema, the buffers are final at once."""
    var text = String(
        '{"type":"record","name":"R","fields":['
        '{"name":"a","type":"long"},'
        '{"name":"b","type":"string"},'
        '{"name":"c","type":["null","double"]}]}'
    )
    var w = DataFileWriter(parse_schema(text), "null")
    for i in range(500):
        var b = RecordBuilder()
        b.add("a", Value.long(Int64(i)))
        b.add("b", Value.string(String("row-", i)))
        if i % 2 == 0:
            b.add("c", Value.union(0, Value.null()))
        else:
            b.add("c", Value.union(1, Value.double(Float64(i))))
        w.append(b^.build())
    var c = RecordCursor.from_bytes(Span(w.bytes()))
    assert_true(c.next())
    var after_first = c.slot_watermark()
    var seen = 1
    while c.next():
        seen += 1
        assert_equal(c.slot_watermark(), after_first)
    assert_equal(seen, 500)


# ── schema resolution, resolved once at plan-build time ────────────────────


def _resolved_cursor(
    writer_json: String, reader_json: String, var data: List[UInt8]
) raises -> RecordCursor[]:
    var rr = resolve(parse_schema(writer_json), parse_schema(reader_json))
    return RecordCursor.resolved(DataFileReader(data^), rr^)


def test_resolution_promotes_and_reorders() raises:
    var writer_json = String(
        '{"type":"record","name":"R","fields":['
        '{"name":"a","type":"int"},'
        '{"name":"gone","type":"string"},'
        '{"name":"b","type":"float"}]}'
    )
    var reader_json = String(
        '{"type":"record","name":"R","fields":['
        '{"name":"b","type":"double"},'
        '{"name":"a","type":"long"},'
        '{"name":"added","type":"string","default":"dflt"},'
        '{"name":"n","type":"int","default":7}]}'
    )
    var w = DataFileWriter(parse_schema(writer_json), "null")
    for i in range(20):
        var b = RecordBuilder()
        b.add("a", Value.int(Int64(i)))
        b.add("gone", Value.string(String("skip-", i)))
        b.add("b", Value.float(Float32(i) * 0.5))
        w.append(b^.build())
    var c = _resolved_cursor(writer_json, reader_json, w.bytes())
    var s_a = c.plan.slot_of("a")
    var s_b = c.plan.slot_of("b")
    var s_added = c.plan.slot_of("added")
    var s_n = c.plan.slot_of("n")
    assert_false(c.plan.has_slot("gone"))
    assert_equal(c.plan.slot_kind(s_a), LONG)
    assert_equal(c.plan.slot_kind(s_b), DOUBLE)
    var i = 0
    while c.next():
        assert_equal(c.get_long(s_a), Int64(i))
        assert_almost_equal(
            c.get_double(s_b), Float64(i) * 0.5, atol=Float64(1e-6)
        )
        assert_equal(c.get_string(s_added), "dflt")
        assert_equal(c.get_long(s_n), 7)
        i += 1
    assert_equal(i, 20)


def test_resolution_maps_enum_symbols_and_narrows_a_union() raises:
    var writer_json = String(
        '{"type":"record","name":"R","fields":['
        '{"name":"s","type":{"type":"enum","name":"E",'
        '"symbols":["A","B","C"]}},'
        '{"name":"u","type":["null","int"]}]}'
    )
    var reader_json = String(
        '{"type":"record","name":"R","fields":['
        '{"name":"s","type":{"type":"enum","name":"E",'
        '"symbols":["C","A"],"default":"A"}},'
        '{"name":"u","type":["null","long"]}]}'
    )
    var w = DataFileWriter(parse_schema(writer_json), "null")
    for i in range(6):
        var b = RecordBuilder()
        b.add("s", Value.enum(i % 3, ["A", "B", "C"][i % 3]))
        if i % 2 == 0:
            b.add("u", Value.union(0, Value.null()))
        else:
            b.add("u", Value.union(1, Value.int(Int64(i))))
        w.append(b^.build())
    var c = _resolved_cursor(writer_json, reader_json, w.bytes())
    var s_s = c.plan.slot_of("s")
    var s_u = c.plan.slot_of("u")
    var expect_symbol: List[String] = ["A", "A", "C", "A", "A", "C"]
    var i = 0
    while c.next():
        assert_equal(c.get_symbol(s_s), expect_symbol[i])
        assert_equal(c.is_null(s_u), i % 2 == 0)
        if i % 2 == 1:
            assert_equal(c.get_long(s_u), Int64(i))
        i += 1
    assert_equal(i, 6)


def test_resolution_refuses_a_container_default() raises:
    var writer_json = String(
        '{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}'
    )
    var reader_json = String(
        '{"type":"record","name":"R","fields":['
        '{"name":"a","type":"int"},'
        '{"name":"xs","type":{"type":"array","items":"int"},"default":[]}]}'
    )
    var rr = resolve(parse_schema(writer_json), parse_schema(reader_json))
    with assert_raises():
        _ = DecodePlan.build_resolved(rr)


# ── errors ─────────────────────────────────────────────────────────────────


def test_a_view_of_a_defaulted_string_is_refused() raises:
    var writer_json = String(
        '{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}'
    )
    var reader_json = String(
        '{"type":"record","name":"R","fields":['
        '{"name":"a","type":"int"},'
        '{"name":"t","type":"string","default":"x"}]}'
    )
    var w = DataFileWriter(parse_schema(writer_json), "null")
    var b = RecordBuilder()
    b.add("a", Value.int(1))
    w.append(b^.build())
    var c = _resolved_cursor(writer_json, reader_json, w.bytes())
    var s_t = c.plan.slot_of("t")
    assert_true(c.next())
    assert_equal(c.get_string(s_t), "x")
    with assert_raises():
        _ = c.get_str(s_t)


def test_a_truncated_block_is_an_error() raises:
    var text = String(
        '{"type":"record","name":"R","fields":[{"name":"a","type":"string"}]}'
    )
    var w = DataFileWriter(parse_schema(text), "null")
    var b = RecordBuilder()
    b.add("a", Value.string("hello"))
    w.append(b^.build())
    var raw = w.bytes()
    # Lie about the string's length so the decoder runs off the end.
    var cut = raw.copy()
    cut[len(cut) - 22] = 0xFE
    var c = RecordCursor.from_bytes(Span(cut))
    with assert_raises():
        while c.next():
            pass


# ── bringing your own codec ────────────────────────────────────────────────


struct StoredDeflate(CodecSet):
    """`null` and `deflate`, with a different `deflate` compressor.

    A `CodecSet` is the seam a consumer substitutes an implementation at —
    an FFI zlib, say. This one is deliberately a *worse* compressor (it emits
    RFC 1951 stored blocks, which compress nothing) precisely so the test can
    tell whose bytes came out, while staying legal raw DEFLATE that the
    built-in `inflate` reads back.
    """

    @staticmethod
    def supports(name: StringSlice) -> Bool:
        return not name or name == "null" or name == "deflate"

    @staticmethod
    def compress(name: StringSlice, data: Span[UInt8, _]) raises -> List[UInt8]:
        if not name or name == "null":
            var copy = List[UInt8](capacity=len(data))
            copy.extend(data)
            return copy^
        if name == "deflate":
            var out = List[UInt8]()
            var at = 0
            while True:
                var n = len(data) - at
                if n > 65535:
                    n = 65535
                var final = 1 if at + n >= len(data) else 0
                out.append(UInt8(final))  # BFINAL, BTYPE=00, then byte-aligned
                out.append(UInt8(n & 0xFF))
                out.append(UInt8((n >> 8) & 0xFF))
                out.append(UInt8((~n) & 0xFF))
                out.append(UInt8(((~n) >> 8) & 0xFF))
                out.extend(data[at : at + n])
                at += n
                if final == 1:
                    break
            return out^
        raise unknown_codec(name)

    @staticmethod
    def decompress(
        name: StringSlice, data: Span[UInt8, _]
    ) raises -> List[UInt8]:
        return DefaultCodecs.decompress(name, data)


def _codec_round_trip_rows(n: Int) raises -> Tuple[String, List[Value]]:
    var text = String(
        '{"type":"record","name":"R","fields":['
        '{"name":"a","type":"long"},{"name":"b","type":"string"}]}'
    )
    var rows = List[Value]()
    for i in range(n):
        var b = RecordBuilder()
        b.add("a", Value.long(Int64(i)))
        b.add("b", Value.string(String("row-", i, "-", i % 7)))
        rows.append(b^.build())
    return (text^, rows^)


def test_a_substituted_codec_is_read_by_the_built_in_one() raises:
    var shape = _codec_round_trip_rows(400)
    var w = DataFileWriter[StoredDeflate](parse_schema(shape[0]), "deflate")
    for i in range(len(shape[1])):
        w.append(shape[1][i])
    var file = w.bytes()

    # Written by the substituted codec, read by the default one.
    var c = RecordCursor.from_bytes(Span(file))
    var s_a = c.plan.slot_of("a")
    var s_b = c.plan.slot_of("b")
    var i = 0
    while c.next():
        assert_equal(c.get_long(s_a), Int64(i))
        assert_equal(String(c.get_str(s_b)), String("row-", i, "-", i % 7))
        i += 1
    assert_equal(i, 400)

    # Stored blocks compress nothing, so the substitution is visible.
    var d = DataFileWriter(parse_schema(shape[0]), "deflate")
    for k in range(len(shape[1])):
        d.append(shape[1][k])
    assert_true(len(d.bytes()) < len(file))


def test_the_built_in_codec_is_read_by_a_substituted_one() raises:
    var shape = _codec_round_trip_rows(400)
    var w = DataFileWriter(parse_schema(shape[0]), "deflate")
    for i in range(len(shape[1])):
        w.append(shape[1][i])
    var file = w.bytes()
    var c = RecordCursor[StoredDeflate].from_bytes(Span(file))
    var s_a = c.plan.slot_of("a")
    var i = 0
    while c.next():
        assert_equal(c.get_long(s_a), Int64(i))
        i += 1
    assert_equal(i, 400)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
