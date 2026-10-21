# timelake-format

Timelake's table conventions, with no I/O of its own: the table contract stored in `timelake.*` Iceberg properties,
the table format version, time partition math, Iceberg bound decoding, and the codecs of the head object (`head`
feature). With the `write` feature, the write path: preparing rows, writing Parquet with Iceberg statistics,
manifests and table metadata, the rules of a commit and the plan of a maintenance run (as pure functions both
writers call), appends, commits and maintenance over a `Host` the caller implements, and the streaming merge of
compaction. The engine ([`timelake`](https://crates.io/crates/timelake)) and Timelake's WebAssembly reader and
writer share it, so they read and write tables the same way.

Most users want [`timelake`](https://crates.io/crates/timelake) instead. Docs:
[timelake.dev/docs](https://timelake.dev/docs).

Licensed under the Apache License 2.0.
