"""The avro.mojo test suite — run with `pixi run test`."""

from std.testing import TestSuite, assert_almost_equal, assert_equal, assert_true

from avro import (
    DataFileReader,
    DataFileWriter,
    Decoder,
    Encoder,
    Schema,
    Value,
    crc32,
    deflate,
    encode_value,
    inflate,
    parse_json,
    parse_schema,
    resolve,
)
from avro.schema import ARRAY, ENUM, LONG, MAP, RECORD, STRING, UNION
from avro.value import ArrayBuilder, MapBuilder, RecordBuilder
from expected import FIXTURE_COUNT, check_everything_record, check_everything_schema


# ── helpers ────────────────────────────────────────────────────────────────


def hex_of(data: Span[UInt8, _]) -> String:
    comptime H = "0123456789abcdef"
    var out = String()
    for b in data:
        out += H[byte= Int(b >> 4)]
        out += H[byte= Int(b & 0xF)]
    return out^


def bytes_of(text: StringSlice) -> List[UInt8]:
    var out = List[UInt8]()
    out.extend(text.as_bytes())
    return out^


def from_hex(text: StringSlice) raises -> List[UInt8]:
    var out = List[UInt8]()
    var bs = text.as_bytes()
    var i = 0
    while i + 1 < len(bs):
        out.append(UInt8(_nib(bs[i]) * 16 + _nib(bs[i + 1])))
        i += 2
    return out^


def _nib(c: UInt8) raises -> Int:
    if c >= 48 and c <= 57:
        return Int(c) - 48
    if c >= 97 and c <= 102:
        return Int(c) - 87
    if c >= 65 and c <= 70:
        return Int(c) - 55
    raise Error("bad hex digit")


# ── JSON ───────────────────────────────────────────────────────────────────


def test_json_round_trip() raises:
    var text = String(
        '{"a":1,"b":[1,true,null,"x"],"c":{"d":-2},"e":"q\\"u\\u263ax"}'
    )
    var doc = parse_json(text)
    # \u escapes come back as raw UTF-8; everything else round-trips verbatim.
    assert_equal(
        doc.to_json(doc.root),
        '{"a":1,"b":[1,true,null,"x"],"c":{"d":-2},"e":"q\\"u\u263ax"}',
    )
    assert_equal(doc.as_int(doc.get(doc.root, "a")), 1)
    assert_equal(doc.len_of(doc.get(doc.root, "b")), 4)
    assert_equal(doc.as_string(doc.get(doc.root, "e")), "q\"u☺x")


def test_json_numbers() raises:
    var doc = parse_json('[0, -17, 2.5, 1e3, -1.25e-2]')
    assert_equal(doc.as_int(doc.child(doc.root, 1)), -17)
    assert_almost_equal(doc.as_float(doc.child(doc.root, 2)), 2.5)
    assert_almost_equal(doc.as_float(doc.child(doc.root, 3)), 1000.0)
    assert_almost_equal(doc.as_float(doc.child(doc.root, 4)), -0.0125)


# ── schemas ────────────────────────────────────────────────────────────────


def test_schema_primitives() raises:
    var names: List[String] = [
        "null",
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "bytes",
        "string",
    ]
    for n in names:
        var s = parse_schema(String('"', n, '"'))
        assert_equal(s.to_json(), String('"', n, '"'))


def test_schema_record_round_trip() raises:
    var text = String(
        '{"type":"record","name":"User","namespace":"org.x","doc":"a user",'
        '"fields":[{"name":"id","type":"long"},'
        '{"name":"name","type":"string","default":"anon"},'
        '{"name":"tags","type":{"type":"array","items":"string"}},'
        '{"name":"props","type":{"type":"map","values":"int"}},'
        '{"name":"kind","type":{"type":"enum","name":"Kind","symbols":["A","B"]}},'
        '{"name":"key","type":{"type":"fixed","name":"K16","size":16}}]}'
    )
    var s = parse_schema(text)
    assert_equal(s.to_json(), text)
    assert_equal(parse_schema(s.to_json()).to_json(), text)
    assert_equal(s.root_kind(), RECORD)
    assert_equal(s.num_fields(s.root), 6)
    assert_equal(s.name(s.root), "org.x.User")
    assert_equal(s.name(s.field_type(s.root, 4)), "org.x.Kind")
    assert_equal(s.size(s.field_type(s.root, 5)), 16)
    assert_true(s.field(s.root, 1).has_default)


def test_schema_recursive() raises:
    var text = String(
        '{"type":"record","name":"Node","fields":['
        '{"name":"v","type":"int"},'
        '{"name":"next","type":["null","Node"],"default":null}]}'
    )
    var s = parse_schema(text)
    assert_equal(s.to_json(), text)
    var next_type = s.field_type(s.root, 1)
    assert_equal(s.kind(next_type), UNION)
    # The self-reference resolves back to the very same arena node.
    assert_equal(s.branch(next_type, 1), s.root)


def test_schema_namespace_inheritance() raises:
    var s = parse_schema(
        '{"type":"record","name":"a.b.Outer","fields":['
        '{"name":"inner","type":{"type":"record","name":"Inner","fields":['
        '{"name":"x","type":"int"}]}},'
        '{"name":"again","type":"Inner"}]}'
    )
    assert_equal(s.name(s.field_type(s.root, 0)), "a.b.Inner")
    assert_equal(s.field_type(s.root, 0), s.field_type(s.root, 1))


def test_schema_logical_types() raises:
    var text = String(
        '{"type":"record","name":"L","fields":['
        '{"name":"d","type":{"type":"bytes","logicalType":"decimal",'
        '"precision":9,"scale":2}},'
        '{"name":"u","type":{"type":"string","logicalType":"uuid"}},'
        '{"name":"day","type":{"type":"int","logicalType":"date"}},'
        '{"name":"tm","type":{"type":"long","logicalType":"timestamp-micros"}},'
        '{"name":"dur","type":{"type":"fixed","name":"D","size":12,'
        '"logicalType":"duration"}}]}'
    )
    var s = parse_schema(text)
    assert_equal(s.to_json(), text)
    assert_equal(s.logical_type(s.field_type(s.root, 0)), "decimal")
    assert_equal(s.nodes[s.field_type(s.root, 0)].precision, 9)
    assert_equal(s.nodes[s.field_type(s.root, 0)].scale, 2)
    assert_equal(s.logical_type(s.field_type(s.root, 2)), "date")
    assert_equal(s.logical_type(s.field_type(s.root, 4)), "duration")


def test_schema_field_ids_preserved() raises:
    var text = String(
        '{"type":"record","name":"m","fields":['
        '{"name":"a","type":"int","field-id":11},'
        '{"name":"b","type":{"type":"array","items":"long","element-id":12},'
        '"field-id":13},'
        '{"name":"c","type":{"type":"map","values":"int","key-id":14,'
        '"value-id":15},"field-id":16}]}'
    )
    var s = parse_schema(text)
    assert_equal(s.to_json(), text)
    assert_equal(s.field(s.root, 0).field_id(), 11)
    assert_equal(s.element_id(s.field_type(s.root, 1)), 12)
    assert_equal(s.key_id(s.field_type(s.root, 2)), 14)
    assert_equal(s.value_id(s.field_type(s.root, 2)), 15)


def test_schema_fingerprint() raises:
    # Cross-checked against fastavro's CRC-64-AVRO fingerprints.
    assert_equal(parse_schema('"string"').fingerprint(), 0x8F014872634503C7)
    assert_equal(parse_schema('"int"').fingerprint(), 0x7275D51A3F395C8F)
    assert_equal(
        parse_schema('{"type":"fixed","name":"md5","size":16}').fingerprint(),
        0x481B34E75CD85D8C,
    )
    var s = parse_schema(
        '{"type":"record","name":"Node","namespace":"a.b","fields":['
        '{"name":"v","type":"int"},'
        '{"name":"next","type":["null","Node"],"default":null}]}'
    )
    assert_equal(
        s.parsing_canonical_form(),
        '{"name":"a.b.Node","type":"record","fields":[{"name":"v","type":"int"},'
        '{"name":"next","type":["null","a.b.Node"]}]}',
    )
    assert_equal(s.fingerprint(), 0xDA6153590A762846)


# ── binary encoding ────────────────────────────────────────────────────────


def test_varint_spec_examples() raises:
    # Avro spec, "binary encoding": the zigzag mapping.
    var cases: List[Tuple[Int64, String]] = [
        (Int64(0), String("00")),
        (Int64(-1), String("01")),
        (Int64(1), String("02")),
        (Int64(-2), String("03")),
        (Int64(2), String("04")),
        (Int64(-64), String("7f")),
        (Int64(64), String("8001")),
        (Int64(8191), String("fe7f")),
        (Int64(8192), String("808001")),
        (Int64(-2147483648), String("ffffffff0f")),
    ]
    for c in cases:
        var e = Encoder()
        e.write_long(c[0])
        assert_equal(hex_of(Span(e.out)), c[1])
        var d = Decoder(Span(e.out))
        assert_equal(d.read_long(), c[0])


def test_primitive_round_trips() raises:
    var e = Encoder()
    e.write_boolean(True)
    e.write_boolean(False)
    e.write_float(3.5)
    e.write_double(-2.25)
    e.write_string("héllo")
    var blob = List[UInt8](capacity=3)
    blob.append(0)
    blob.append(1)
    blob.append(0xFE)
    e.write_bytes(Span(blob))
    var d = Decoder(Span(e.out))
    assert_true(d.read_boolean())
    assert_true(not d.read_boolean())
    assert_equal(d.read_float(), Float32(3.5))
    assert_equal(d.read_double(), Float64(-2.25))
    assert_equal(d.read_string(), "héllo")
    assert_equal(len(d.read_bytes()), 3)
    assert_true(d.at_end())


def test_float_bit_layout() raises:
    # IEEE 754 little-endian, per the spec.
    var e = Encoder()
    e.write_float(1.0)
    assert_equal(hex_of(Span(e.out)), "0000803f")
    var e2 = Encoder()
    e2.write_double(1.0)
    assert_equal(hex_of(Span(e2.out)), "000000000000f03f")


def test_spec_record_example() raises:
    # From the spec: {"a": 27, "b": "foo"} is 36 06 66 6f 6f.
    var s = parse_schema(
        '{"type":"record","name":"test","fields":['
        '{"name":"a","type":"long"},{"name":"b","type":"string"}]}'
    )
    var b = RecordBuilder()
    b.add("a", Value.long(27))
    b.add("b", Value.string("foo"))
    var v = b^.build()
    var raw = encode_value(s, v)
    assert_equal(hex_of(Span(raw)), "3606666f6f")
    var d = Decoder(Span(raw))
    var back = d.read_value(s)
    assert_equal(back.field("a").as_long(), 27)
    assert_equal(back.field("b").as_string(), "foo")
    assert_equal(back.to_json(), '{"a":27,"b":"foo"}')


def test_array_and_map() raises:
    var s = parse_schema('{"type":"array","items":"long"}')
    var items: List[Value] = [Value.long(3), Value.long(27), Value.long(-1)]
    var raw = encode_value(s, Value.array(items))
    # one block of 3, then the terminating zero
    assert_equal(hex_of(Span(raw)), "0606360100")
    var d = Decoder(Span(raw))
    var back = d.read_value(s)
    assert_equal(len(back), 3)
    assert_equal(back.at(1).as_long(), 27)

    var ms = parse_schema('{"type":"map","values":"int"}')
    var mb = MapBuilder()
    mb.add("a", Value.int(1))
    mb.add("b", Value.int(2))
    var mraw = encode_value(ms, mb^.build())
    var md = Decoder(Span(mraw))
    var mback = md.read_value(ms)
    assert_equal(len(mback), 2)
    assert_equal(mback.field("b").as_long(), 2)


def test_array_negative_block_count() raises:
    """A writer may prefix a block with a negative count plus its byte size."""
    var s = parse_schema('{"type":"array","items":"long"}')
    var e = Encoder()
    e.write_long(-2)  # two items, size follows
    e.write_long(2)  # two bytes of payload
    e.write_long(3)
    e.write_long(4)
    e.write_long(0)
    var d = Decoder(Span(e.out))
    var v = d.read_value(s)
    assert_equal(len(v), 2)
    assert_equal(v.at(0).as_long(), 3)
    assert_equal(v.at(1).as_long(), 4)


def test_union_and_enum_and_fixed() raises:
    var s = parse_schema('["null","string"]')
    var raw = encode_value(s, Value.union(1, Value.string("hi")))
    assert_equal(hex_of(Span(raw)), "02046869")
    var d = Decoder(Span(raw))
    var v = d.read_value(s)
    assert_equal(v.union_index(), 1)
    assert_equal(v.unwrap().as_string(), "hi")
    assert_true(not v.is_null())

    var nraw = encode_value(s, Value.union(0, Value.null()))
    assert_equal(hex_of(Span(nraw)), "00")
    var nd = Decoder(Span(nraw))
    assert_true(nd.read_value(s).is_null())

    var es = parse_schema('{"type":"enum","name":"Suit","symbols":["C","D","H","S"]}')
    var eraw = encode_value(es, Value.enum(2, "H"))
    assert_equal(hex_of(Span(eraw)), "04")
    var ed = Decoder(Span(eraw))
    assert_equal(ed.read_value(es).symbol(), "H")

    var fs = parse_schema('{"type":"fixed","name":"F4","size":4}')
    var fraw = encode_value(fs, Value.fixed(Span(bytes_of("abcd"))))
    assert_equal(hex_of(Span(fraw)), "61626364")


def test_nested_round_trip() raises:
    var s = parse_schema(
        '{"type":"record","name":"Outer","fields":['
        '{"name":"items","type":{"type":"array","items":'
        '{"type":"record","name":"Item","fields":['
        '{"name":"id","type":"int"},'
        '{"name":"tag","type":["null","string"],"default":null}]}}}]}'
    )
    var b0 = RecordBuilder()
    b0.add("id", Value.int(1))
    b0.add("tag", Value.union(1, Value.string("x")))
    var i0 = b0^.build()
    var b1 = RecordBuilder()
    b1.add("id", Value.int(2))
    b1.add("tag", Value.union(0, Value.null()))
    var i1 = b1^.build()
    var ab = ArrayBuilder()
    ab.add(i0)
    ab.add(i1)
    var ob = RecordBuilder()
    ob.add("items", ab^.build())
    var v = ob^.build()
    var raw = encode_value(s, v)
    var d = Decoder(Span(raw))
    var back = d.read_value(s)
    assert_equal(back, v)
    assert_equal(len(back.field("items")), 2)
    assert_equal(back.field("items").at(0).field("tag").as_string(), "x")
    assert_true(back.field("items").at(1).field_raw("tag").is_null())


# ── deflate ────────────────────────────────────────────────────────────────


def test_deflate_round_trip() raises:
    var src = List[UInt8]()
    for i in range(50000):
        src.append(UInt8((i * 7 + i // 97) % 251))
    for i in range(20000):
        src.append(UInt8(65 + i % 5))
    var z = deflate(Span(src))
    assert_true(len(z) < len(src) // 2)
    var back = inflate(Span(z))
    assert_equal(len(back), len(src))
    for i in range(len(src)):
        if back[i] != src[i]:
            raise Error(String("deflate round trip differs at ", i))


def test_deflate_edge_cases() raises:
    var empty = List[UInt8]()
    assert_equal(len(inflate(Span(deflate(Span(empty))))), 0)
    var one = bytes_of("a")
    assert_equal(len(inflate(Span(deflate(Span(one))))), 1)
    var run = List[UInt8](length=1000, fill=7)
    var back = inflate(Span(deflate(Span(run))))
    assert_equal(len(back), 1000)
    assert_equal(back[999], UInt8(7))


def test_crc32() raises:
    assert_equal(crc32(bytes_of("123456789")), UInt32(0xCBF43926))
    assert_equal(crc32(Span(List[UInt8]())), UInt32(0))


# ── object container files ─────────────────────────────────────────────────


def sample_schema() raises -> Schema:
    return parse_schema(
        '{"type":"record","name":"Row","fields":['
        '{"name":"id","type":"long"},'
        '{"name":"name","type":"string"},'
        '{"name":"score","type":["null","double"],"default":null}]}'
    )


def sample_row(i: Int) raises -> Value:
    var b = RecordBuilder()
    b.add("id", Value.long(Int64(i)))
    b.add("name", Value.string(String("row-", i)))
    if i % 3 == 0:
        b.add("score", Value.union(0, Value.null()))
    else:
        b.add("score", Value.union(1, Value.double(Float64(i) * 1.5)))
    return b^.build()


def ocf_round_trip(codec: StringSlice, n: Int, sync_interval: Int) raises:
    var w = DataFileWriter(sample_schema(), codec, sync_interval)
    w.set_metadata_string("app", "avro.mojo")
    for i in range(n):
        w.append(sample_row(i))
    var raw = w.bytes()

    var r = DataFileReader.from_bytes(Span(raw))
    assert_equal(r.codec, String(codec) if codec else String("null"))
    assert_equal(r.metadata_string("app"), "avro.mojo")
    assert_equal(r.schema.to_json(), sample_schema().to_json())
    assert_equal(len(r.sync_marker), 16)
    var seen = 0
    while r.has_next():
        if r.next() != sample_row(seen):
            raise Error(String("row ", seen, " did not survive the round trip"))
        seen += 1
    assert_equal(seen, n)
    if sync_interval < 100:
        assert_true(r.count_blocks() > 1)


def test_ocf_null_codec() raises:
    ocf_round_trip("null", 500, 64000)


def test_ocf_deflate_codec() raises:
    ocf_round_trip("deflate", 500, 64000)


def test_ocf_multiple_blocks() raises:
    ocf_round_trip("null", 200, 64)
    ocf_round_trip("deflate", 200, 64)


def test_ocf_empty_file() raises:
    var w = DataFileWriter(sample_schema(), "null")
    var raw = w.bytes()
    var r = DataFileReader.from_bytes(Span(raw))
    assert_true(not r.has_next())
    assert_equal(r.count_blocks(), 0)


def test_ocf_sync_marker_is_checked() raises:
    var w = DataFileWriter(sample_schema(), "null", 64)
    for i in range(50):
        w.append(sample_row(i))
    var raw = w.bytes()
    # Corrupt the very last sync marker.
    raw[len(raw) - 1] = raw[len(raw) - 1] ^ 0xFF
    var r = DataFileReader.from_bytes(Span(raw))
    var failed = False
    try:
        _ = r.read_all()
    except e:
        failed = True
    assert_true(failed)


def test_ocf_rejects_bad_magic() raises:
    var raw = bytes_of("NotAnAvroFileAtAll")
    var failed = False
    try:
        _ = DataFileReader.from_bytes(Span(raw))
    except e:
        failed = True
    assert_true(failed)


def test_ocf_unknown_codec_is_reported() raises:
    var failed = False
    try:
        var w = DataFileWriter(sample_schema(), "snappy")
        _ = w.bytes()
    except e:
        failed = True
    assert_true(failed)


# ── real Iceberg manifests ─────────────────────────────────────────────────


def test_iceberg_manifest_list() raises:
    var r = DataFileReader.open("tests/fixtures/iceberg_manifest_list.avro")
    assert_equal(r.codec, "null")
    assert_equal(r.metadata_string("format-version"), "2")
    assert_true(r.has_metadata("snapshot-id"))
    ref s = r.schema
    assert_equal(s.name(s.root), "manifest_file")
    assert_equal(s.field(s.root, 0).name, "manifest_path")
    assert_equal(s.field(s.root, 0).field_id(), 500)
    assert_equal(s.field(s.root, 1).field_id(), 501)
    # partitions: ["null", {"type": "array", "element-id": 508, ...}]
    var parts = s.field_index(s.root, "partitions")
    var pt = s.field_type(s.root, parts)
    assert_true(s.is_optional(pt))
    var arr = s.optional_branch(pt)
    assert_equal(s.kind(arr), ARRAY)
    assert_equal(s.element_id(arr), 508)
    assert_equal(s.field(arr_items(s, arr), 0).field_id(), 509)

    var recs = r.read_all()
    assert_equal(len(recs), 1)
    ref rec = recs[0]
    assert_equal(rec.field("manifest_length").as_long(), 4412)
    assert_equal(rec.field("added_files_count").as_long(), 2)
    assert_equal(rec.field("added_rows_count").as_long(), 3)
    assert_equal(rec.field("sequence_number").as_long(), 1)
    assert_true(rec.field_raw("key_metadata").is_null())
    var p = rec.field("partitions")
    assert_equal(len(p), 1)
    assert_equal(p.at(0).field("lower_bound").as_string(), "eu")
    assert_equal(p.at(0).field("upper_bound").as_string(), "us")
    assert_true(not p.at(0).field("contains_null").as_bool())


def arr_items(s: Schema, i: Int) -> Int:
    return s.items(i)


def test_iceberg_manifest() raises:
    var r = DataFileReader.open("tests/fixtures/iceberg_manifest.avro")
    assert_equal(r.metadata_string("format-version"), "2")
    assert_true(r.metadata_string("schema").find('"fields"') >= 0)
    assert_true(r.metadata_string("partition-spec").find("region") >= 0)
    ref s = r.schema
    assert_equal(s.name(s.root), "manifest_entry")
    assert_equal(s.field(s.root, 0).name, "status")
    assert_equal(s.field(s.root, 0).field_id(), 0)
    var df = s.field_type(s.root, s.field_index(s.root, "data_file"))
    assert_equal(s.kind(df), RECORD)
    assert_equal(s.name(df), "r2")
    # column_sizes is a map-with-int-keys, spelled as an array of records.
    var cs = s.field_type(df, s.field_index(df, "column_sizes"))
    var inner = s.optional_branch(cs)
    assert_true(s.is_logical_map(inner))
    assert_equal(s.field(s.items(inner), 0).field_id(), 117)
    assert_equal(s.field(s.items(inner), 1).field_id(), 118)

    var recs = r.read_all()
    assert_equal(len(recs), 2)
    ref e0 = recs[0]
    assert_equal(e0.field("status").as_long(), 1)
    assert_equal(e0.field("snapshot_id").as_long(), 4611067932825646045)
    var d0 = e0.field("data_file")
    assert_equal(d0.field("file_format").as_string(), "PARQUET")
    assert_equal(d0.field("record_count").as_long(), 1)
    assert_equal(d0.field("file_size_in_bytes").as_long(), 2256)
    assert_equal(d0.field("partition").field("region").as_string(), "us")
    assert_equal(len(d0.field("column_sizes")), 6)
    assert_equal(len(d0.field("split_offsets")), 1)
    assert_true(d0.field_raw("key_metadata").is_null())


def test_iceberg_manifest_re_encodes_byte_identically() raises:
    """Decode then re-encode every record: the payload must be unchanged."""
    var r = DataFileReader.open("tests/fixtures/iceberg_manifest.avro")
    var schema = r.schema.copy()
    var recs = r.read_all()
    var e = Encoder()
    for rec in recs:
        e.write_value(schema, rec)
    var w = DataFileWriter(schema.copy(), "null")
    for rec in recs:
        w.append(rec)
    var raw = w.bytes()
    var r2 = DataFileReader.from_bytes(Span(raw))
    var back = r2.read_all()
    assert_equal(len(back), len(recs))
    for i in range(len(recs)):
        if back[i] != recs[i]:
            raise Error(String("manifest entry ", i, " changed"))


# ── schema resolution ──────────────────────────────────────────────────────


def test_resolve_field_reordering_and_defaults() raises:
    var writer = parse_schema(
        '{"type":"record","name":"R","fields":['
        '{"name":"a","type":"int"},'
        '{"name":"dropped","type":"string"},'
        '{"name":"b","type":"long"}]}'
    )
    var reader = parse_schema(
        '{"type":"record","name":"R","fields":['
        '{"name":"b","type":"long"},'
        '{"name":"c","type":"string","default":"missing"},'
        '{"name":"a","type":"int"}]}'
    )
    var b = RecordBuilder()
    b.add("a", Value.int(7))
    b.add("dropped", Value.string("gone"))
    b.add("b", Value.long(99))
    var v = b^.build()
    var raw = encode_value(writer, v)
    var plan = resolve(writer, reader)
    var d = Decoder(Span(raw))
    var got = plan.read_value(d)
    assert_equal(got.to_json(), '{"b":99,"c":"missing","a":7}')


def test_resolve_promotions() raises:
    var cases: List[Tuple[String, String]] = [
        (String('"int"'), String('"long"')),
        (String('"int"'), String('"float"')),
        (String('"int"'), String('"double"')),
        (String('"long"'), String('"double"')),
        (String('"float"'), String('"double"')),
    ]
    for c in cases:
        var writer = parse_schema(c[0])
        var reader = parse_schema(c[1])
        var e = Encoder()
        if writer.root_kind() == 4:  # FLOAT
            e.write_float(42.0)
        else:
            e.write_long(42)
        var plan = resolve(writer, reader)
        var d = Decoder(Span(e.out))
        var got = plan.read_value(d)
        assert_equal(got.as_double(), Float64(42.0))
        assert_equal(got.kind(), reader.root_kind())


def test_resolve_string_bytes() raises:
    var writer = parse_schema('"string"')
    var reader = parse_schema('"bytes"')
    var e = Encoder()
    e.write_string("hey")
    var plan = resolve(writer, reader)
    var d = Decoder(Span(e.out))
    assert_equal(len(plan.read_value(d).as_bytes()), 3)

    var plan2 = resolve(reader, writer)
    var d2 = Decoder(Span(e.out))
    assert_equal(plan2.read_value(d2).as_string(), "hey")


def test_resolve_union_selection() raises:
    # Writer union -> reader non-union.
    var writer = parse_schema('["null","int"]')
    var reader = parse_schema('"long"')
    var raw = encode_value(writer, Value.union(1, Value.int(5)))
    var plan = resolve(writer, reader)
    var d = Decoder(Span(raw))
    assert_equal(plan.read_value(d).as_long(), 5)

    # Writer non-union -> reader union.
    var w2 = parse_schema('"int"')
    var r2 = parse_schema('["null","int"]')
    var e = Encoder()
    e.write_long(9)
    var plan2 = resolve(w2, r2)
    var d2 = Decoder(Span(e.out))
    var got = plan2.read_value(d2)
    assert_equal(got.kind(), UNION)
    assert_equal(got.union_index(), 1)
    assert_equal(got.unwrap().as_long(), 9)


def test_resolve_enum_default() raises:
    var writer = parse_schema(
        '{"type":"enum","name":"E","symbols":["A","B","C"]}'
    )
    var reader = parse_schema(
        '{"type":"enum","name":"E","symbols":["A","Z"],"default":"Z"}'
    )
    var raw = encode_value(writer, Value.enum(2, "C"))
    var plan = resolve(writer, reader)
    var d = Decoder(Span(raw))
    assert_equal(plan.read_value(d).symbol(), "Z")


def test_resolve_nested_and_aliases() raises:
    var writer = parse_schema(
        '{"type":"record","name":"Old","fields":['
        '{"name":"xs","type":{"type":"array","items":"int"}},'
        '{"name":"legacy","type":"string"}]}'
    )
    var reader = parse_schema(
        '{"type":"record","name":"New","aliases":["Old"],"fields":['
        '{"name":"xs","type":{"type":"array","items":"long"}},'
        '{"name":"modern","type":"string","aliases":["legacy"]}]}'
    )
    var xs: List[Value] = [Value.int(1), Value.int(2)]
    var b = RecordBuilder()
    b.add("xs", Value.array(xs))
    b.add("legacy", Value.string("ok"))
    var v = b^.build()
    var raw = encode_value(writer, v)
    var plan = resolve(writer, reader)
    var d = Decoder(Span(raw))
    assert_equal(plan.read_value(d).to_json(), '{"xs":[1,2],"modern":"ok"}')


def test_resolve_rejects_impossible() raises:
    var failed = False
    try:
        _ = resolve(parse_schema('"string"'), parse_schema('"int"'))
    except e:
        failed = True
    assert_true(failed)

    var failed2 = False
    try:
        _ = resolve(
            parse_schema('{"type":"record","name":"R","fields":[]}'),
            parse_schema(
                '{"type":"record","name":"R","fields":[{"name":"z","type":"int"}]}'
            ),
        )
    except e:
        failed2 = True
    assert_true(failed2)


# ── files written by Python fastavro ───────────────────────────────────────


def read_fastavro_fixture(codec: StringSlice) raises:
    var r = DataFileReader.open(
        String("tests/fixtures/fastavro_", codec, ".avro")
    )
    assert_equal(r.codec, codec)
    assert_equal(r.metadata_string("written-by"), "fastavro")
    assert_equal(r.metadata_string("note"), "avro.mojo fixture")
    check_everything_schema(r.schema)
    # fastavro's sync_interval=800 forces many small blocks.
    assert_true(r.count_blocks() > 5)
    var seen = 0
    while r.has_next():
        var rec = r.next()
        check_everything_record(rec, seen)
        seen += 1
    assert_equal(seen, FIXTURE_COUNT)


def test_fastavro_null_fixture() raises:
    read_fastavro_fixture("null")


def test_fastavro_deflate_fixture() raises:
    read_fastavro_fixture("deflate")


def test_fastavro_round_trip_through_us() raises:
    """Decode fastavro's file, rewrite it ourselves, decode it again."""
    var r = DataFileReader.open("tests/fixtures/fastavro_deflate.avro")
    var schema = r.schema.copy()
    var recs = r.read_all()
    var w = DataFileWriter(schema.copy(), "deflate", 900)
    for rec in recs:
        w.append(rec)
    var raw = w.bytes()
    var r2 = DataFileReader.from_bytes(Span(raw))
    var seen = 0
    while r2.has_next():
        check_everything_record(r2.next(), seen)
        seen += 1
    assert_equal(seen, FIXTURE_COUNT)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
