"""The optional Avro block codecs: `snappy` and `zstandard`.

These live outside the `avro` package on purpose. `avro` itself imports
nothing beyond the Mojo standard library, so a consumer that only needs
`null` and `deflate` — every Apache Iceberg manifest this author has seen —
pays for nothing. Reach for this package when a file's `avro.codec` says
otherwise:

```mojo
from avro import DataFileReader
from avro_codecs import AllCodecs

var r = DataFileReader[AllCodecs].open("data.avro")
```

It needs the sibling magmalake tins on the import path — the pixi `codecs`
environment pulls both in as git source dependencies:

```toml
[feature.codecs.dependencies]
snappy-mojo = { git = "https://github.com/magmalake/snappy.mojo" }
zstd-mojo = { git = "https://github.com/magmalake/zstd.mojo" }
```
"""

from snappy import compress as snappy_compress, decompress as snappy_decompress
from zstd import compress as zstd_compress, decompress as zstd_decompress

from avro.codec import CodecSet, unknown_codec
from avro.crc32 import crc32
from avro.deflate import deflate, inflate


def _copy_of(data: Span[UInt8, _]) -> List[UInt8]:
    var out = List[UInt8](capacity=len(data))
    out.extend(data)
    return out^


# ── snappy ─────────────────────────────────────────────────────────────────
#
# Avro's snappy codec is a raw Snappy block with the big-endian CRC-32 of the
# *uncompressed* data appended — four bytes that are not part of the Snappy
# format itself, so they have to be added and checked here.


def avro_snappy_compress(data: Span[UInt8, _]) raises -> List[UInt8]:
    var out = snappy_compress(data)
    var c = crc32(data)
    out.append(UInt8((c >> 24) & 0xFF))
    out.append(UInt8((c >> 16) & 0xFF))
    out.append(UInt8((c >> 8) & 0xFF))
    out.append(UInt8(c & 0xFF))
    return out^


def avro_snappy_decompress(data: Span[UInt8, _]) raises -> List[UInt8]:
    if len(data) < 4:
        raise Error("avro.snappy: block is too short to hold its CRC-32")
    var body = data[: len(data) - 4]
    var out = snappy_decompress(body)
    var want = (
        (UInt32(data[len(data) - 4]) << 24)
        | (UInt32(data[len(data) - 3]) << 16)
        | (UInt32(data[len(data) - 2]) << 8)
        | UInt32(data[len(data) - 1])
    )
    var got = crc32(Span(out))
    if got != want:
        raise Error(
            String(
                "avro.snappy: CRC-32 mismatch (block says ",
                want,
                ", data hashes to ",
                got,
                ")",
            )
        )
    return out^


# ── codec sets ─────────────────────────────────────────────────────────────


struct SnappyCodecs(CodecSet):
    """`null`, `deflate` and `snappy`."""

    @staticmethod
    def supports(name: StringSlice) -> Bool:
        return not name or name == "null" or name == "deflate" or name == "snappy"

    @staticmethod
    def compress(name: StringSlice, data: Span[UInt8, _]) raises -> List[UInt8]:
        if not name or name == "null":
            return _copy_of(data)
        if name == "deflate":
            return deflate(data)
        if name == "snappy":
            return avro_snappy_compress(data)
        raise unknown_codec(name)

    @staticmethod
    def decompress(name: StringSlice, data: Span[UInt8, _]) raises -> List[UInt8]:
        if not name or name == "null":
            return _copy_of(data)
        if name == "deflate":
            return inflate(data)
        if name == "snappy":
            return avro_snappy_decompress(data)
        raise unknown_codec(name)


struct ZstdCodecs(CodecSet):
    """`null`, `deflate` and `zstandard`."""

    @staticmethod
    def supports(name: StringSlice) -> Bool:
        return (
            not name or name == "null" or name == "deflate" or name == "zstandard"
        )

    @staticmethod
    def compress(name: StringSlice, data: Span[UInt8, _]) raises -> List[UInt8]:
        if not name or name == "null":
            return _copy_of(data)
        if name == "deflate":
            return deflate(data)
        if name == "zstandard":
            return zstd_compress(data)
        raise unknown_codec(name)

    @staticmethod
    def decompress(name: StringSlice, data: Span[UInt8, _]) raises -> List[UInt8]:
        if not name or name == "null":
            return _copy_of(data)
        if name == "deflate":
            return inflate(data)
        if name == "zstandard":
            return zstd_decompress(data)
        raise unknown_codec(name)


struct AllCodecs(CodecSet):
    """Every codec `avro.mojo` implements: null, deflate, snappy, zstandard."""

    @staticmethod
    def supports(name: StringSlice) -> Bool:
        return (
            not name
            or name == "null"
            or name == "deflate"
            or name == "snappy"
            or name == "zstandard"
        )

    @staticmethod
    def compress(name: StringSlice, data: Span[UInt8, _]) raises -> List[UInt8]:
        if name == "snappy":
            return avro_snappy_compress(data)
        if name == "zstandard":
            return zstd_compress(data)
        return SnappyCodecs.compress(name, data)

    @staticmethod
    def decompress(name: StringSlice, data: Span[UInt8, _]) raises -> List[UInt8]:
        if name == "snappy":
            return avro_snappy_decompress(data)
        if name == "zstandard":
            return zstd_decompress(data)
        return SnappyCodecs.decompress(name, data)
