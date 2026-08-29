"""The optional-codec test suite — run with `pixi run -e codecs test-codecs`.

Needs the sibling `snappy.mojo` and `zstd.mojo` tins, which the `codecs`
pixi environment installs as git source dependencies.
"""

from std.testing import TestSuite, assert_equal, assert_true

from avro import CodecSet, DataFileReader, DataFileWriter, Schema, Value, parse_schema
from avro.value import RecordBuilder
from avro_codecs import (
    AllCodecs,
    SnappyCodecs,
    ZstdCodecs,
    avro_snappy_compress,
    avro_snappy_decompress,
)
from expected import FIXTURE_COUNT, check_everything_record, check_everything_schema


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


def test_snappy_block_framing() raises:
    """Avro's snappy block is a raw Snappy block plus a big-endian CRC-32."""
    var data = List[UInt8]()
    for i in range(4096):
        data.append(UInt8((i * 11) % 71))
    var packed = avro_snappy_compress(Span(data))
    assert_true(len(packed) < len(data))
    var back = avro_snappy_decompress(Span(packed))
    assert_equal(len(back), len(data))
    for i in range(len(data)):
        if back[i] != data[i]:
            raise Error("snappy round trip differs")
    # A flipped CRC byte must be caught.
    packed[len(packed) - 1] = packed[len(packed) - 1] ^ 0xFF
    var failed = False
    try:
        _ = avro_snappy_decompress(Span(packed))
    except e:
        failed = True
    assert_true(failed)


def ocf_round_trip[C: CodecSet](codec: StringSlice, n: Int) raises:
    var w = DataFileWriter[C](sample_schema(), codec, 700)
    for i in range(n):
        w.append(sample_row(i))
    var raw = w.bytes()
    var r = DataFileReader[C].from_bytes(Span(raw))
    assert_equal(r.codec, codec)
    assert_true(r.count_blocks() > 1)
    var seen = 0
    while r.has_next():
        if r.next() != sample_row(seen):
            raise Error(String("row ", seen, " did not survive the round trip"))
        seen += 1
    assert_equal(seen, n)


def test_snappy_ocf_round_trip() raises:
    ocf_round_trip[SnappyCodecs]("snappy", 400)


def test_zstd_ocf_round_trip() raises:
    ocf_round_trip[ZstdCodecs]("zstandard", 400)


def test_all_codecs_round_trip() raises:
    ocf_round_trip[AllCodecs]("null", 200)
    ocf_round_trip[AllCodecs]("deflate", 200)
    ocf_round_trip[AllCodecs]("snappy", 200)
    ocf_round_trip[AllCodecs]("zstandard", 200)


def read_fastavro_fixture(codec: StringSlice) raises:
    var r = DataFileReader[AllCodecs].open(
        String("tests/fixtures/fastavro_", codec, ".avro")
    )
    assert_equal(r.codec, codec)
    assert_equal(r.metadata_string("written-by"), "fastavro")
    check_everything_schema(r.schema)
    assert_true(r.count_blocks() > 5)
    var seen = 0
    while r.has_next():
        check_everything_record(r.next(), seen)
        seen += 1
    assert_equal(seen, FIXTURE_COUNT)


def test_fastavro_snappy_fixture() raises:
    read_fastavro_fixture("snappy")


def test_fastavro_zstandard_fixture() raises:
    read_fastavro_fixture("zstandard")


def test_fastavro_all_four_fixtures() raises:
    read_fastavro_fixture("null")
    read_fastavro_fixture("deflate")


def test_codec_outside_the_set_is_rejected() raises:
    var failed = False
    try:
        _ = DataFileReader[ZstdCodecs].open("tests/fixtures/fastavro_snappy.avro")
    except e:
        failed = True
    assert_true(failed)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
