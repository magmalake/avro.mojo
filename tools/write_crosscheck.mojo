"""Write one Object Container File per codec, for `tools/crosscheck.py`.

    pixi run -e codecs crosscheck

writes `build/crosscheck/avro_mojo_<codec>.avro` with the same schema and the
same 240 records that `tools/gen_fixtures.py` writes with fastavro, then has
fastavro read them back and compare. It is the other half of the
interoperability check: the fixtures prove we can read Python's files, this
proves Python can read ours.
"""

from std.sys import argv

from avro import DataFileWriter, Schema, Value, parse_schema
from avro.value import ArrayBuilder, MapBuilder, RecordBuilder
from avro_codecs import AllCodecs

comptime N: Int = 240

comptime SCHEMA_JSON: StaticString = (
    '{"type":"record","name":"Everything","namespace":"org.magmalake.avro",'
    '"doc":"one of each Avro type, for the fastavro cross-check","fields":['
    '{"name":"nothing","type":"null"},'
    '{"name":"flag","type":"boolean"},'
    '{"name":"i32","type":"int"},'
    '{"name":"i64","type":"long"},'
    '{"name":"f32","type":"float"},'
    '{"name":"f64","type":"double"},'
    '{"name":"blob","type":"bytes"},'
    '{"name":"text","type":"string"},'
    '{"name":"suit","type":{"type":"enum","name":"Suit",'
    '"symbols":["C","D","H","S"]}},'
    '{"name":"key","type":{"type":"fixed","name":"K4","size":4}},'
    '{"name":"xs","type":{"type":"array","items":"long"}},'
    '{"name":"props","type":{"type":"map","values":"int"}},'
    '{"name":"maybe","type":["null","string"],"default":null},'
    '{"name":"choice","type":["int","string","boolean"]},'
    '{"name":"inner","type":{"type":"record","name":"Inner","fields":['
    '{"name":"a","type":"int"},'
    '{"name":"b","type":{"type":"array","items":"string"}}]}},'
    '{"name":"day","type":{"type":"int","logicalType":"date"}},'
    '{"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"}},'
    '{"name":"amount","type":{"type":"bytes","logicalType":"decimal",'
    '"precision":9,"scale":2}}]}'
)


def make_row(i: Int) raises -> Value:
    var blob = List[UInt8](capacity=4)
    blob.append(UInt8(i % 256))
    blob.append(UInt8((i * 3) % 256))
    blob.append(0)
    blob.append(255)

    var key = List[UInt8](capacity=4)
    key.append(UInt8(i % 256))
    key.append(1)
    key.append(2)
    key.append(3)

    var xs = ArrayBuilder()
    for k in range((i % 3) + 1):
        xs.add(Value.long(Int64(i + k)))

    var props = MapBuilder()
    props.add("a", Value.int(Int64(i)))
    props.add("b", Value.int(Int64(i * 2)))

    var inner_b = ArrayBuilder()
    inner_b.add(Value.string(String("x", i)))
    inner_b.add(Value.string(String("y", i)))
    var inner = RecordBuilder()
    inner.add("a", Value.int(Int64(i * 2)))
    inner.add("b", inner_b^.build())

    var unscaled = Int64(i * 137 - 5000)
    var amount = List[UInt8](capacity=8)
    for k in range(8):
        amount.append(UInt8((unscaled >> Int64(8 * (7 - k))) & 0xFF))

    var suits: List[String] = ["C", "D", "H", "S"]

    var b = RecordBuilder()
    b.add("nothing", Value.null())
    b.add("flag", Value.boolean(i % 2 == 0))
    b.add("i32", Value.int(Int64(i * 7 - 500)))
    b.add("i64", Value.long(Int64(i) * 1000003 - 2000000000))
    b.add("f32", Value.float(Float32(i) * 0.5))
    b.add("f64", Value.double(Float64(i) * -1.25))
    b.add("blob", Value.bytes(Span(blob)))
    b.add("text", Value.string(String("row-", i, "-☺")))
    b.add("suit", Value.enum(i % 4, suits[i % 4]))
    b.add("key", Value.fixed(Span(key)))
    b.add("xs", xs^.build())
    b.add("props", props^.build())
    if i % 3 == 0:
        b.add("maybe", Value.union(0, Value.null()))
    else:
        b.add("maybe", Value.union(1, Value.string(String("yes-", i))))
    if i % 3 == 0:
        b.add("choice", Value.union(0, Value.int(Int64(i))))
    elif i % 3 == 1:
        b.add("choice", Value.union(1, Value.string(String("s", i))))
    else:
        b.add("choice", Value.union(2, Value.boolean(i % 2 == 0)))
    b.add("inner", inner^.build())
    b.add("day", Value.int(Int64(19000 + i)))
    b.add("ts", Value.long(Int64(1700000000000000) + Int64(i) * 1000000))
    b.add("amount", Value.bytes(Span(amount)))
    return b^.build()


def main() raises:
    var args = argv()
    var out_dir = String("build/crosscheck")
    if len(args) > 1:
        out_dir = String(args[1])
    var codecs: List[String] = ["null", "deflate", "snappy", "zstandard"]
    for codec in codecs:
        var w = DataFileWriter[AllCodecs](parse_schema(SCHEMA_JSON), codec, 800)
        w.set_metadata_string("written-by", "avro.mojo")
        w.set_metadata_string("note", "crosscheck")
        for i in range(N):
            w.append(make_row(i))
        var path = String(out_dir, "/avro_mojo_", codec, ".avro")
        w.save(path)
        print(path, len(w.out), "bytes")
