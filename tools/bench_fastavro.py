#!/usr/bin/env python3
"""Read the files `pixi run bench` leaves in build/bench/ with fastavro.

The honesty check: the Mojo numbers only mean something next to a mature
implementation reading the identical bytes. fastavro is the fast Python
reader (a Cython core), so this is not a Python-interpreter strawman.

    pixi run bench            # writes build/bench/*.avro and prints Mojo
    pixi run bench-fastavro   # prints fastavro on the same files
"""
import os
import sys
import time

import fastavro

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.join(os.path.dirname(HERE), "build", "bench")

REPS = 3


def best(path):
    """Warm best-of-three, matching how the Mojo side times itself."""
    with open(path, "rb") as fh:
        data = fh.read()
    import io

    lo = None
    rows = 0
    for _ in range(REPS):
        buf = io.BytesIO(data)
        t0 = time.perf_counter()
        n = 0
        for _rec in fastavro.reader(buf):
            n += 1
        t1 = time.perf_counter()
        lo = t1 - t0 if lo is None else min(lo, t1 - t0)
        rows = n
    return lo, rows, len(data)


def main():
    if not os.path.isdir(BENCH):
        sys.exit("no build/bench — run `pixi run bench` first")
    names = sorted(n for n in os.listdir(BENCH) if n.endswith(".avro"))
    if not names:
        sys.exit("build/bench has no .avro files — run `pixi run bench` first")
    # The uncompressed file of each pair is the size every rate is quoted on.
    logical = {}
    for name in names:
        stem, codec = name[: -len(".avro")].rsplit("-", 1)
        if codec == "null":
            logical[stem] = os.path.getsize(os.path.join(BENCH, name))
    print("fastavro %s — reading the files `pixi run bench` wrote" % fastavro.__version__)
    for name in names:
        stem, codec = name[: -len(".avro")].rsplit("-", 1)
        secs, rows, size = best(os.path.join(BENCH, name))
        mb = logical.get(stem, size) / 1048576.0
        print(
            "  %-24s %-8s %8.1f MB/s, %7.0fk rows/s (%4.0f ms)"
            % (stem, codec, mb / secs, rows / secs / 1000.0, secs * 1000.0)
        )


if __name__ == "__main__":
    main()
