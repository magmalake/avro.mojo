"""Throughput for the pieces that sit in an Iceberg scan's hot path.

    pixi run bench
"""

from std.time import perf_counter_ns

from avro import (
    DataFileReader,
    DataFileWriter,
    Decoder,
    Encoder,
    Schema,
    Value,
    deflate,
    inflate,
    parse_schema,
)
from avro.value import ArrayBuilder, RecordBuilder

comptime N: Int = 100000

comptime SCHEMA_JSON: StaticString = (
    '{"type":"record","name":"Event","fields":['
    '{"name":"id","type":"long"},'
    '{"name":"name","type":"string"},'
    '{"name":"score","type":"double"},'
    '{"name":"flag","type":"boolean"},'
    '{"name":"tags","type":{"type":"array","items":"string"}},'
    '{"name":"note","type":["null","string"],"default":null}]}'
)


def make_row(i: Int) raises -> Value:
    var tags = ArrayBuilder()
    tags.add(Value.string(String("t", i % 17)))
    tags.add(Value.string(String("u", i % 5)))
    var b = RecordBuilder()
    b.add("id", Value.long(Int64(i)))
    b.add("name", Value.string(String("event-", i)))
    b.add("score", Value.double(Float64(i) * 0.125))
    b.add("flag", Value.boolean(i % 2 == 0))
    b.add("tags", tags^.build())
    if i % 4 == 0:
        b.add("note", Value.union(0, Value.null()))
    else:
        b.add("note", Value.union(1, Value.string(String("note-", i))))
    return b^.build()


def mb_per_s(bytes: Int, ns: Int) -> Float64:
    if ns == 0:
        return 0.0
    return (Float64(bytes) / 1048576.0) / (Float64(ns) / 1e9)


def report(label: StringSlice, bytes: Int, ns: Int, rows: Int):
    print(
        label,
        ": ",
        Int(mb_per_s(bytes, ns)),
        " MB/s, ",
        Int(Float64(rows) / (Float64(ns) / 1e9) / 1000.0),
        "k rows/s (",
        ns // 1000000,
        " ms)",
        sep="",
    )


def main() raises:
    var schema = parse_schema(SCHEMA_JSON)
    print("avro.mojo bench —", N, "records of", SCHEMA_JSON.byte_length(), "byte schema")

    # Build the rows once so the encode timing is not dominated by Value
    # construction.
    var rows = List[Value](capacity=N)
    for i in range(N):
        rows.append(make_row(i))

    # ── raw encode / decode, no container ──────────────────────────────────
    var t0 = perf_counter_ns()
    var e = Encoder(capacity=N * 64)
    for i in range(N):
        e.write_value(schema, rows[i])
    var t1 = perf_counter_ns()
    var raw = e^.take()
    report("encode (datum)   ", len(raw), t1 - t0, N)

    var t2 = perf_counter_ns()
    var d = Decoder(Span(raw))
    var checksum: Int64 = 0
    for _i in range(N):
        checksum += d.read_value(schema).at(0).as_long()
    var t3 = perf_counter_ns()
    report("decode (datum)   ", len(raw), t3 - t2, N)
    if checksum != Int64(N) * Int64(N - 1) // 2:
        raise Error("bench: decode checksum mismatch")

    # ── object container files ─────────────────────────────────────────────
    for codec in ["null", "deflate"]:
        var w = DataFileWriter(parse_schema(SCHEMA_JSON), codec)
        var t4 = perf_counter_ns()
        for i in range(N):
            w.append(rows[i])
        var file = w.bytes()
        var t5 = perf_counter_ns()
        report(String("OCF write ", codec, "  "), len(raw), t5 - t4, N)

        var t6 = perf_counter_ns()
        var r = DataFileReader.from_bytes(Span(file))
        var seen = 0
        while r.has_next():
            _ = r.next()
            seen += 1
        var t7 = perf_counter_ns()
        if seen != N:
            raise Error("bench: wrong record count")
        report(String("OCF read  ", codec, "  "), len(raw), t7 - t6, N)
        print(
            "  file: ",
            len(file) // 1024,
            " KiB (",
            Int(100.0 * Float64(len(file)) / Float64(len(raw))),
            "% of the datum stream)",
            sep="",
        )

    # ── the deflate codec on its own ───────────────────────────────────────
    var t8 = perf_counter_ns()
    var z = deflate(Span(raw))
    var t9 = perf_counter_ns()
    report("deflate          ", len(raw), t9 - t8, N)
    var t10 = perf_counter_ns()
    var back = inflate(Span(z))
    var t11 = perf_counter_ns()
    report("inflate          ", len(raw), t11 - t10, N)
    if len(back) != len(raw):
        raise Error("bench: inflate size mismatch")
