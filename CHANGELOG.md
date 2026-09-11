# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases before 0.4.0 predate this file; their contents are in the commit log.

## [Unreleased]

## [0.4.1] - 2026-09-11

Makes 0.4.0 installable. The package build had no `extra-args` putting the
host prefix on the Mojo import path, which was fine while this library had no
sibling dependencies and broke the moment it gained one:

```
src/avro/codec.mojo:21:6: error: unable to locate module 'deflate'
```

**0.4.0 cannot be installed as a tin and should not be used.** The failure
appears only when a consumer builds the package — this repository's own tasks
compile `deflate.mojo` from a source path, so every test and lint stayed green.


## [0.4.0] - 2026-09-11

Raw DEFLATE moved out of this repository into its own tin,
[deflate.mojo](https://github.com/magmalake/deflate.mojo). It was never
Avro-specific — Avro's `deflate` block codec was just the first thing in
magmalake to need it — and `parquet.mojo` and `iceberg.mojo` were both
resolving and building Apache Avro to get one `inflate`. See
[magmalake/parquet.mojo#38](https://github.com/magmalake/parquet.mojo/issues/38).

The `deflate` block codec is unchanged, and `DefaultCodecs` still needs no
FFI: `deflate-mojo` is pure Mojo too.

### Removed (breaking)

- `avro.deflate` is gone, and `avro` no longer re-exports `deflate` and
  `inflate`. Import them from `deflate` instead:

  ```mojo
  from deflate import deflate, inflate   # was: from avro import deflate, inflate
  ```

### Changed

- New tin dependency `deflate-mojo`. This is the first sibling the core
  library has had; `snappy` and `zstandard` remain optional and confined to
  `avro.ext_snappy` / `avro.ext_zstd`.
- Building from **source paths** needs `-I ../deflate.mojo/src` alongside
  `-I ../avro.mojo/src`.
- The two DEFLATE round-trip tests moved to `deflate.mojo`, where the suite
  now also checks `inflate` against fixtures produced by Python's zlib — one
  per block type — and covers `inflate_at`, which had no test here.
