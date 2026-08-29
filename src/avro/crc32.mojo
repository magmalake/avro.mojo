"""CRC-32 (IEEE 802.3, the zlib/gzip polynomial).

Avro's `snappy` codec appends the big-endian CRC-32 of the *uncompressed*
block to every compressed block. That is the only place `avro.mojo` needs a
checksum, so it is a small table-driven implementation rather than a
dependency on `hashes.mojo`.
"""


def crc32(data: Span[UInt8, _], seed: UInt32 = 0) -> UInt32:
    var table = List[UInt32](capacity=256)
    for i in range(256):
        var c = UInt32(i)
        for _k in range(8):
            c = (c >> 1) ^ (UInt32(0xEDB88320) & (0 - (c & 1)))
        table.append(c)
    var crc = ~seed
    for b in data:
        crc = (crc >> 8) ^ table[Int((crc ^ UInt32(b)) & 0xFF)]
    return ~crc
