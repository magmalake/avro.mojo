"""Throughput for the pieces that sit in an Iceberg scan's hot path.

    pixi run bench             # this file
    pixi run bench-fastavro    # the same files, read by fastavro

The second half is the one that matters: the `Value` path against
`RecordCursor` on two shapes — a five-field record standing in for a
manifest entry, and a real Iceberg manifest replicated up to bench size —
under both the `null` and the `deflate` codec. It leaves the files in
`build/bench/` so `tools/bench_fastavro.py` can read exactly the same bytes.
"""

from std.time import perf_counter_ns

from avro import (
    DataFileReader,
    RecordCursor,
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
from avro import write_file_bytes

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
    print(
        "avro.mojo bench —",
        N,
        "records of",
        SCHEMA_JSON.byte_length(),
        "byte schema",
    )

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

    # ── the Value path against the cursor ──────────────────────────────────
    print()
    print("── Value vs RecordCursor ─────────────────────────────────────────")
    try:
        _ = mkdir_p("build/bench")
    except:
        pass
    bench_shape(
        "manifest-shaped record (5 fields)",
        manifest_shaped_rows(),
        ["a"],
        "build/bench/shape",
    )
    bench_shape(
        "iceberg manifest_entry",
        replicated_manifest(),
        ["status", "data_file.file_path", "data_file.record_count"],
        "build/bench/manifest",
    )


def mkdir_p(path: StringSlice) raises -> Int:
    """`build/bench` — created by writing into it, since std has no mkdir."""
    from std.os import makedirs

    makedirs(String(path), exist_ok=True)
    return 0


comptime SHAPE_JSON: StaticString = (
    '{"type":"record","name":"Entry","fields":['
    '{"name":"a","type":"long"},'
    '{"name":"b","type":"long"},'
    '{"name":"c","type":"string"},'
    '{"name":"d","type":"double"},'
    '{"name":"e","type":["null","long"]}]}'
)


def manifest_shaped_rows() raises -> Tuple[Schema, String, List[Value]]:
    """The probe's shape: two longs, a path-like string, a double, an
    optional long — what a manifest entry costs, without its nesting."""
    var schema = parse_schema(SHAPE_JSON)
    var rows = List[Value](capacity=N)
    var names: List[String] = ["a", "b", "c", "d", "e"]
    for i in range(N):
        var vals = List[Value]()
        vals.append(Value.long(Int64(i)))
        vals.append(Value.long(Int64(i * 7)))
        vals.append(Value.string(String("path/to/data/file-", i, ".parquet")))
        vals.append(Value.double(Float64(i) * 0.5))
        vals.append(Value.union(1, Value.long(Int64(i))))
        rows.append(Value.record(names.copy(), vals^))
    return (schema^, String(SHAPE_JSON), rows^)


def replicated_manifest() raises -> Tuple[Schema, String, List[Value]]:
    """A real Iceberg manifest, its entries repeated up to bench size.

    The schema and the records are the ones a scan actually walks — nested
    `data_file`, four metric maps, partition struct and all — so this is the
    shape the manifest reader in iceberg.mojo pays for."""
    var r = DataFileReader.open("tests/fixtures/iceberg_manifest.avro")
    var json = r.schema_json()
    var schema = r.schema.copy()
    var seed = r.read_all()
    var rows = List[Value](capacity=N)
    for i in range(N):
        rows.append(seed[i % len(seed)].copy())
    return (schema^, json^, rows^)


def bench_shape(
    label: StringSlice,
    var shape: Tuple[Schema, String, List[Value]],
    select: List[String],
    path_stem: StringSlice,
) raises:
    var schema = shape[0].copy()
    var json = shape[1]
    var rows = shape[2].copy()
    print()
    print(label, " — ", len(rows), " records", sep="")
    var logical = 0
    for codec in ["null", "deflate"]:
        var w = DataFileWriter(schema.copy(), codec)
        w.set_schema_json(json)
        for i in range(len(rows)):
            w.append(rows[i])
        var file = w.bytes()
        write_file_bytes(String(path_stem, "-", codec, ".avro"), Span(file))
        if codec == "null":
            logical = len(file)

        var value_ns = best_of[3](file, 0, select)
        var cursor_ns = best_of[3](file, 1, select)
        var pruned_ns = best_of[3](file, 2, select)
        # MB/s is always against the *uncompressed* stream, so the two
        # codecs are on the same scale.
        print("  ", codec, " (", len(file) // 1024, " KiB on disk)", sep="")
        report("    Value          ", logical, value_ns, len(rows))
        report("    cursor         ", logical, cursor_ns, len(rows))
        report("    cursor+select  ", logical, pruned_ns, len(rows))
        print(
            "    speedup: ",
            ratio(value_ns, cursor_ns),
            "x, with a selection ",
            ratio(value_ns, pruned_ns),
            "x",
            sep="",
        )


def ratio(a: Int, b: Int) -> String:
    """`a / b` to two decimals, without pulling in a formatter."""
    if b == 0:
        return String("-")
    var hundredths = (a * 100) // b
    return String(
        hundredths // 100, ".", (hundredths % 100) // 10, hundredths % 10
    )


def best_of[
    reps: Int
](file: List[UInt8], mode: Int, select: List[String]) raises -> Int:
    """Warm best-of-`reps`, so neither side is timed cold."""
    var best = 0
    for r in range(reps):
        var t0 = perf_counter_ns()
        var n = run_once(file, mode, select)
        var t1 = perf_counter_ns()
        if n == 0:
            raise Error("bench: read nothing")
        if r == 0 or t1 - t0 < best:
            best = t1 - t0
    return best


def run_once(file: List[UInt8], mode: Int, select: List[String]) raises -> Int:
    """One full pass, touching enough of every record to defeat the
    optimiser: the `Value` path materialises each record, the cursor fills
    its slots."""
    var seen = 0
    if mode == 0:
        var r = DataFileReader.from_bytes(Span(file))
        while r.has_next():
            _ = r.next().at(0)
            seen += 1
        return seen
    var sel = select.copy() if mode == 2 else List[String]()
    var c = RecordCursor.from_bytes(Span(file), sel)
    while c.next():
        seen += 1
    return seen
