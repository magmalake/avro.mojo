"""The expected contents of the `fastavro_*.avro` fixtures.

`tools/gen_fixtures.py` writes 240 records with fastavro from a schema that
uses one of every Avro type. The same formulas are repeated here so the tests
check the decoded values against what Python wrote, rather than against a
recording of our own output.
"""

from std.testing import assert_equal, assert_true

from avro import Schema, Value

comptime FIXTURE_COUNT: Int = 240


def check_everything_schema(s: Schema) raises:
    assert_equal(s.name(s.root), "org.magmalake.avro.Everything")
    assert_equal(s.num_fields(s.root), 18)
    assert_equal(s.field(s.root, 0).name, "nothing")
    assert_equal(s.name(s.field_type(s.root, 8)), "org.magmalake.avro.Suit")
    assert_equal(s.size(s.field_type(s.root, 9)), 4)
    assert_equal(s.logical_type(s.field_type(s.root, 15)), "date")
    assert_equal(s.logical_type(s.field_type(s.root, 16)), "timestamp-micros")
    assert_equal(s.logical_type(s.field_type(s.root, 17)), "decimal")
    assert_equal(s.nodes[s.field_type(s.root, 17)].precision, 9)
    assert_equal(s.nodes[s.field_type(s.root, 17)].scale, 2)
    # The schema must survive a print/parse cycle unchanged.
    assert_equal(s.to_json(), Schema(copy=s).to_json())


def _need(ok: Bool, what: StringSlice, i: Int) raises:
    """Cheap per-record check.

    `assert_equal` carries enough machinery that calling it 30 times per
    record for 240 records dominates the suite's runtime, so the hot loop
    raises directly instead.
    """
    if not ok:
        raise Error(String("fixture record ", i, ": ", what, " is wrong"))


def check_everything_record(rec: Value, i: Int) raises:
    _need(rec.field_raw("nothing").is_null(), "nothing", i)
    _need(rec.field("flag").as_bool() == (i % 2 == 0), "flag", i)
    _need(rec.field("i32").as_long() == Int64(i * 7 - 500), "i32", i)
    _need(
        rec.field("i64").as_long() == Int64(i) * 1000003 - 2000000000, "i64", i
    )
    _need(rec.field("f32").as_double() == Float64(i) * 0.5, "f32", i)
    _need(rec.field("f64").as_double() == Float64(i) * -1.25, "f64", i)

    var blob = rec.field("blob").as_bytes()
    _need(len(blob) == 4, "blob length", i)
    _need(Int(blob[0]) == i % 256, "blob[0]", i)
    _need(Int(blob[1]) == (i * 3) % 256, "blob[1]", i)
    _need(Int(blob[2]) == 0 and Int(blob[3]) == 255, "blob tail", i)

    _need(rec.field("text").as_string() == String("row-", i, "-☺"), "text", i)

    var suits: List[String] = ["C", "D", "H", "S"]
    _need(rec.field("suit").symbol() == suits[i % 4], "suit symbol", i)
    _need(rec.field("suit").enum_index() == i % 4, "suit index", i)

    var key = rec.field("key").as_bytes()
    _need(len(key) == 4 and Int(key[0]) == i % 256 and Int(key[3]) == 3, "key", i)

    var xs = rec.field("xs")
    _need(len(xs) == (i % 3) + 1, "xs length", i)
    for k in range(len(xs)):
        _need(xs.at(k).as_long() == Int64(i + k), "xs element", i)

    var props = rec.field("props")
    _need(len(props) == 2 and props.key(0) == "a", "props keys", i)
    _need(props.field("a").as_long() == Int64(i), "props.a", i)
    _need(props.field("b").as_long() == Int64(i * 2), "props.b", i)

    if i % 3 == 0:
        _need(rec.field_raw("maybe").is_null(), "maybe (null)", i)
    else:
        _need(
            rec.field("maybe").as_string() == String("yes-", i), "maybe", i
        )

    var choice = rec.field_raw("choice")
    if i % 3 == 0:
        _need(choice.union_index() == 0, "choice branch", i)
        _need(choice.unwrap().as_long() == Int64(i), "choice int", i)
    elif i % 3 == 1:
        _need(choice.union_index() == 1, "choice branch", i)
        _need(
            choice.unwrap().as_string() == String("s", i), "choice string", i
        )
    else:
        _need(choice.union_index() == 2, "choice branch", i)
        _need(choice.unwrap().as_bool() == (i % 2 == 0), "choice boolean", i)

    var inner = rec.field("inner")
    _need(inner.field("a").as_long() == Int64(i * 2), "inner.a", i)
    var b = inner.field("b")
    _need(len(b) == 2, "inner.b length", i)
    _need(b.at(0).as_string() == String("x", i), "inner.b[0]", i)
    _need(b.at(1).as_string() == String("y", i), "inner.b[1]", i)

    _need(rec.field("day").as_long() == Int64(19000 + i), "day", i)
    _need(
        rec.field("ts").as_long()
        == Int64(1700000000000000) + Int64(i) * 1000000,
        "ts",
        i,
    )

    # decimal(9, 2) as an 8-byte big-endian two's-complement integer
    var amount = rec.field("amount").as_bytes()
    _need(len(amount) == 8, "amount length", i)
    var unscaled: Int64 = 0
    for byte in amount:
        unscaled = (unscaled << 8) | Int64(Int(byte))
    _need(unscaled == Int64(i * 137 - 5000), "amount value", i)
