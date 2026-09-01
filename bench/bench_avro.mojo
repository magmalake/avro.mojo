"""Throughput for the pieces that sit in an Iceberg scan's hot path.

    pixi run -e bench bench                  # this file
    pixi run -e bench bench -- --json        # every repetition
    pixi run -e bench bench -- --only bench_manifest_null_cursor
    pixi run bench-fastavro                  # the same files, read by fastavro

The second half is the one that matters: the `Value` path against
`RecordCursor` on two shapes — a five-field record standing in for a
manifest entry, and a real Iceberg manifest replicated up to bench size —
under both the `null` and the `deflate` codec. It leaves the files in
`build/bench/` so `tools/bench_fastavro.py` can read exactly the same bytes.

Setup is expensive here -- 100,000 `Value` records, and for the manifest
shape a fixture read and replicated -- and the harness re-enters a benchmark
body once per phase. Only what is inside `b.iter` is timed, so that cost is
wall-clock only, but it is why this suite runs with three repetitions rather
than five.
"""

from bench import Benchmark, BenchSuite, Metric, keep

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


def _make_row(i: Int) raises -> Value:
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




# ── shapes for the Value/cursor comparison ──────────────────────────────────


comptime SHAPE_JSON: StaticString = (
    '{"type":"record","name":"Entry","fields":['
    '{"name":"a","type":"long"},'
    '{"name":"b","type":"long"},'
    '{"name":"c","type":"string"},'
    '{"name":"d","type":"double"},'
    '{"name":"e","type":["null","long"]}]}'
)


def _mkdir_p(path: StringSlice) raises -> Int:
    """`build/bench` — created by writing into it, since std has no mkdir."""
    from std.os import makedirs

    makedirs(String(path), exist_ok=True)
    return 0


def _manifest_shaped_rows() raises -> Tuple[Schema, String, List[Value]]:
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


def _replicated_manifest() raises -> Tuple[Schema, String, List[Value]]:
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

# ── datum encode / decode ───────────────────────────────────────────────────


def _rows(n: Int) raises -> List[Value]:
    var rows = List[Value](capacity=n)
    for i in range(n):
        rows.append(_make_row(i))
    return rows^


def _encoded(schema: Schema, rows: List[Value]) raises -> List[UInt8]:
    var e = Encoder(capacity=N * 64)
    for i in range(len(rows)):
        e.write_value(schema, rows[i])
    return e^.take()


def bench_encode_datum(mut b: Benchmark) raises:
    var schema = parse_schema(SCHEMA_JSON)
    var rows = _rows(N)
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        var e = Encoder(capacity=N * 64)
        for i in range(N):
            e.write_value(schema, rows[i])
        keep(e^.take())

    b.iter[call]()
    keep(schema)
    keep(rows)


def bench_decode_datum(mut b: Benchmark) raises:
    var schema = parse_schema(SCHEMA_JSON)
    var raw = _encoded(schema, _rows(N))
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        var d = Decoder(Span(raw))
        var checksum = Int64(0)
        for _ in range(N):
            checksum += d.read_value(schema).at(0).as_long()
        keep(checksum)

    b.iter[call]()
    keep(schema)
    keep(raw)


# ── object container files ──────────────────────────────────────────────────


def _ocf(codec: StringSlice, rows: List[Value]) raises -> List[UInt8]:
    var w = DataFileWriter(parse_schema(SCHEMA_JSON), String(codec))
    for i in range(len(rows)):
        w.append(rows[i])
    return w.bytes()


def bench_ocf_write_null(mut b: Benchmark) raises:
    var rows = _rows(N)
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_ocf("null", rows))

    b.iter[call]()
    keep(rows)


def bench_ocf_read_null(mut b: Benchmark) raises:
    var file = _ocf("null", _rows(N))
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        var r = DataFileReader.from_bytes(Span(file))
        var seen = 0
        while r.has_next():
            _ = r.next()
            seen += 1
        keep(seen)

    b.iter[call]()
    keep(file)


def bench_ocf_write_deflate(mut b: Benchmark) raises:
    var rows = _rows(N)
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_ocf("deflate", rows))

    b.iter[call]()
    keep(rows)


def bench_ocf_read_deflate(mut b: Benchmark) raises:
    var file = _ocf("deflate", _rows(N))
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        var r = DataFileReader.from_bytes(Span(file))
        var seen = 0
        while r.has_next():
            _ = r.next()
            seen += 1
        keep(seen)

    b.iter[call]()
    keep(file)


# ── the deflate codec on its own ────────────────────────────────────────────


def bench_deflate(mut b: Benchmark) raises:
    var raw = _encoded(parse_schema(SCHEMA_JSON), _rows(N))
    b.throughput(Metric.bytes(), len(raw))

    @parameter
    def call() raises:
        keep(deflate(Span(raw)))

    b.iter[call]()
    keep(raw)


def bench_inflate(mut b: Benchmark) raises:
    var raw = _encoded(parse_schema(SCHEMA_JSON), _rows(N))
    var z = deflate(Span(raw))
    # Against the uncompressed size, so deflate and inflate share a scale.
    b.throughput(Metric.bytes(), len(raw))

    @parameter
    def call() raises:
        keep(inflate(Span(z)))

    b.iter[call]()
    keep(raw)
    keep(z)


# ── Value against RecordCursor ──────────────────────────────────────────────
#
# The comparison this file exists for. The old bench printed a speedup ratio;
# it is the quotient of two rows in the table now, which also means it comes
# from means over calibrated repetitions rather than a best-of-3.


def _shape_file(
    var shape: Tuple[Schema, String, List[Value]], codec: StringSlice
) raises -> List[UInt8]:
    var schema = shape[0].copy()
    var json = shape[1]
    var rows = shape[2].copy()
    var w = DataFileWriter(schema^, String(codec))
    w.set_schema_json(json)
    for i in range(len(rows)):
        w.append(rows[i])
    return w.bytes()


def _read_values(file: List[UInt8]) raises -> Int:
    var r = DataFileReader.from_bytes(Span(file))
    var seen = 0
    while r.has_next():
        _ = r.next().at(0)
        seen += 1
    return seen


def _read_cursor(file: List[UInt8], var select: List[String]) raises -> Int:
    var c = RecordCursor.from_bytes(Span(file), select^)
    var seen = 0
    while c.next():
        seen += 1
    return seen


def bench_shape5_null_value(mut b: Benchmark) raises:
    """manifest-shaped record (5 fields), null codec, value path."""
    var file = _shape_file(_manifest_shaped_rows(), "null")
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_read_values(file))

    b.iter[call]()
    keep(file)


def bench_shape5_null_cursor(mut b: Benchmark) raises:
    """manifest-shaped record (5 fields), null codec, cursor path."""
    var file = _shape_file(_manifest_shaped_rows(), "null")
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_read_cursor(file, List[String]()))

    b.iter[call]()
    keep(file)


def bench_shape5_null_cursor_select(mut b: Benchmark) raises:
    """manifest-shaped record (5 fields), null codec, cursor+select path."""
    var file = _shape_file(_manifest_shaped_rows(), "null")
    var sel: List[String] = ["a"]
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_read_cursor(file, sel.copy()))

    b.iter[call]()
    keep(sel)
    keep(file)


def bench_shape5_deflate_value(mut b: Benchmark) raises:
    """manifest-shaped record (5 fields), deflate codec, value path."""
    var file = _shape_file(_manifest_shaped_rows(), "deflate")
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_read_values(file))

    b.iter[call]()
    keep(file)


def bench_shape5_deflate_cursor(mut b: Benchmark) raises:
    """manifest-shaped record (5 fields), deflate codec, cursor path."""
    var file = _shape_file(_manifest_shaped_rows(), "deflate")
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_read_cursor(file, List[String]()))

    b.iter[call]()
    keep(file)


def bench_shape5_deflate_cursor_select(mut b: Benchmark) raises:
    """manifest-shaped record (5 fields), deflate codec, cursor+select path."""
    var file = _shape_file(_manifest_shaped_rows(), "deflate")
    var sel: List[String] = ["a"]
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_read_cursor(file, sel.copy()))

    b.iter[call]()
    keep(sel)
    keep(file)


def bench_manifest_null_value(mut b: Benchmark) raises:
    """iceberg manifest_entry, null codec, value path."""
    var file = _shape_file(_replicated_manifest(), "null")
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_read_values(file))

    b.iter[call]()
    keep(file)


def bench_manifest_null_cursor(mut b: Benchmark) raises:
    """iceberg manifest_entry, null codec, cursor path."""
    var file = _shape_file(_replicated_manifest(), "null")
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_read_cursor(file, List[String]()))

    b.iter[call]()
    keep(file)


def bench_manifest_null_cursor_select(mut b: Benchmark) raises:
    """iceberg manifest_entry, null codec, cursor+select path."""
    var file = _shape_file(_replicated_manifest(), "null")
    var sel: List[String] = ["status", "data_file.file_path", "data_file.record_count"]
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_read_cursor(file, sel.copy()))

    b.iter[call]()
    keep(sel)
    keep(file)


def bench_manifest_deflate_value(mut b: Benchmark) raises:
    """iceberg manifest_entry, deflate codec, value path."""
    var file = _shape_file(_replicated_manifest(), "deflate")
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_read_values(file))

    b.iter[call]()
    keep(file)


def bench_manifest_deflate_cursor(mut b: Benchmark) raises:
    """iceberg manifest_entry, deflate codec, cursor path."""
    var file = _shape_file(_replicated_manifest(), "deflate")
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_read_cursor(file, List[String]()))

    b.iter[call]()
    keep(file)


def bench_manifest_deflate_cursor_select(mut b: Benchmark) raises:
    """iceberg manifest_entry, deflate codec, cursor+select path."""
    var file = _shape_file(_replicated_manifest(), "deflate")
    var sel: List[String] = ["status", "data_file.file_path", "data_file.record_count"]
    b.throughput(Metric.elements(), N)

    @parameter
    def call() raises:
        keep(_read_cursor(file, sel.copy()))

    b.iter[call]()
    keep(sel)
    keep(file)


def _print_shape() raises:
    """File sizes, the record-count checks, and the `build/bench/` files that
    `tools/bench_fastavro.py` reads. None of it belongs in a timed region."""
    try:
        _mkdir_p("build/bench")
    except:
        pass

    var schema = parse_schema(SCHEMA_JSON)
    var rows = _rows(N)
    var raw = _encoded(schema, rows)
    var z = deflate(Span(raw))
    if len(inflate(Span(z))) != len(raw):
        raise Error("bench: inflate size mismatch")

    var d = Decoder(Span(raw))
    var checksum = Int64(0)
    for _ in range(N):
        checksum += d.read_value(schema).at(0).as_long()
    if checksum != Int64(N) * Int64(N - 1) // 2:
        raise Error("bench: decode checksum mismatch")

    print(
        "avro.mojo bench —", N, "records of",
        SCHEMA_JSON.byte_length(), "byte schema |",
        "datum stream", len(raw) // 1024, "KiB |",
        "deflated", len(z) // 1024, "KiB",
    )
    for codec in ["null", "deflate"]:
        var file = _ocf(codec, rows)
        if _read_values(file) != N:
            raise Error("bench: wrong record count")
        print(
            "  OCF", codec, ":", len(file) // 1024, "KiB (",
            Int(100.0 * Float64(len(file)) / Float64(len(raw))),
            "% of the datum stream )",
        )

    # The same bytes the shape benchmarks read, on disk for fastavro.
    for codec in ["null", "deflate"]:
        var f5 = _shape_file(_manifest_shaped_rows(), codec)
        write_file_bytes(String("build/bench/shape-", codec, ".avro"), Span(f5))
        var fm = _shape_file(_replicated_manifest(), codec)
        write_file_bytes(
            String("build/bench/manifest-", codec, ".avro"), Span(fm)
        )
        print(
            "  shape", codec, ":", len(f5) // 1024, "KiB | manifest", codec,
            ":", len(fm) // 1024, "KiB",
        )


def main() raises:
    _print_shape()
    BenchSuite.run[__functions_in_module()](num_repetitions=3)
