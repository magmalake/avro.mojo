"""Object Container File block codecs.

`DataFileReader` and `DataFileWriter` are parametrised on a `CodecSet`, a
compile-time table of block compressors. The default set — `DefaultCodecs` —
covers `null` and `deflate`, both implemented in this repo, so the core
library has no dependencies at all.

`snappy` and `zstandard` live in `avro.ext_snappy` / `avro.ext_zstd`, which
import the sibling magmalake tins. Nothing in `avro/__init__.mojo` imports
them, so a consumer only pays for a codec it actually asks for:

```mojo
from avro import DataFileReader
from avro.ext_zstd import AllCodecs          # needs -I ../snappy.mojo/src -I ../zstd.mojo/src

var r = DataFileReader[AllCodecs].open("f.avro")
```
"""

from avro.deflate import deflate, inflate


trait CodecSet:
    """A compile-time table of Avro block codecs."""

    @staticmethod
    def supports(name: StringSlice) -> Bool:
        """Whether this set can handle the `avro.codec` value `name`."""
        ...

    @staticmethod
    def compress(name: StringSlice, data: Span[UInt8, _]) raises -> List[UInt8]:
        ...

    @staticmethod
    def decompress(name: StringSlice, data: Span[UInt8, _]) raises -> List[UInt8]:
        ...


def _copy_of(data: Span[UInt8, _]) -> List[UInt8]:
    var out = List[UInt8](capacity=len(data))
    out.extend(data)
    return out^


def unknown_codec(name: StringSlice) -> Error:
    return Error(
        String(
            "avro: unsupported codec '",
            name,
            "' — import avro.ext_snappy / avro.ext_zstd and pass the matching"
            " CodecSet parameter",
        )
    )


struct DefaultCodecs(CodecSet):
    """`null` and `deflate` — no dependencies outside this repo."""

    @staticmethod
    def supports(name: StringSlice) -> Bool:
        return not name or name == "null" or name == "deflate"

    @staticmethod
    def compress(name: StringSlice, data: Span[UInt8, _]) raises -> List[UInt8]:
        if not name or name == "null":
            return _copy_of(data)
        if name == "deflate":
            return deflate(data)
        raise unknown_codec(name)

    @staticmethod
    def decompress(name: StringSlice, data: Span[UInt8, _]) raises -> List[UInt8]:
        if not name or name == "null":
            return _copy_of(data)
        if name == "deflate":
            return inflate(data)
        raise unknown_codec(name)
