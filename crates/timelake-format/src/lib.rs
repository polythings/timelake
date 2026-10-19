//! Timelake's on-disk conventions, with no I/O of its own, so the engine (`timelake`) and the
//! WebAssembly reader and writer (`timelake-wasm`) share one definition: the table contract stored
//! in `timelake.*` properties, time partition math, and Iceberg bound decoding. With the `write`
//! feature, also the write path: preparing rows, writing Parquet with Iceberg statistics,
//! manifests and table metadata, and appending and committing over a [`host::Host`] that the
//! caller implements (SPEC §21.10).
//!
//! ```
//! use std::collections::HashMap;
//! use timelake_format::{Chunk, Contract, property};
//!
//! let properties = HashMap::from([
//!     (property::TIME_COLUMN.to_string(), "time".to_string()),
//!     (property::CHUNK.to_string(), "day".to_string()),
//! ]);
//! let contract = Contract::from_properties(&properties)?;
//! assert_eq!(contract.chunk, Some(Chunk::Day));
//!
//! let day = Chunk::Day.value(1_767_225_600_000_000)?; // 2026-01-01T00:00:00Z in µs
//! assert_eq!(Chunk::Day.label(day), "2026-01-01");
//! # Ok::<(), timelake_format::FormatError>(())
//! ```

#![warn(missing_docs)]

use std::fmt;

#[cfg(feature = "write")]
pub mod append;
pub mod bounds;
#[cfg(feature = "write")]
pub mod commit;
pub mod contract;
#[cfg(feature = "write")]
pub mod error;
#[cfg(feature = "write")]
pub mod file;
#[cfg(feature = "head")]
pub mod head;
#[cfg(feature = "write")]
pub mod host;
#[cfg(feature = "write")]
pub mod iceberg;
#[cfg(feature = "write")]
pub mod maintain;
#[cfg(feature = "write")]
pub mod merge;
#[cfg(feature = "write")]
pub mod metadata;
#[cfg(feature = "write")]
pub mod rows;
pub mod settings;
#[cfg(feature = "write")]
pub mod table;
pub mod version;

pub use contract::{Chunk, Contract, TableOptions, format_duration, parse_duration};
pub use version::FormatVersion;

/// Table property keys holding the [`Contract`].
pub mod property {
    /// The timestamp column.
    pub const TIME_COLUMN: &str = "timelake.time-column";
    /// Comma-separated series key columns.
    pub const SERIES_KEY: &str = "timelake.series-key";
    /// Time partition size: `hour`, `day` or `month`.
    pub const CHUNK: &str = "timelake.chunk-interval";
    /// `true` to drop duplicate `(series, time)` rows during compaction.
    pub const DEDUP: &str = "timelake.dedup";
    /// Data retention, e.g. `90d`.
    pub const RETENTION: &str = "timelake.retention";
    /// Target size of compacted files, in bytes.
    pub const TARGET_FILE_SIZE: &str = "timelake.target-file-size-bytes";
    /// Rows per Parquet row group.
    pub const ROW_GROUP_ROWS: &str = "timelake.row-group-rows";
    /// `false` to skip inline maintenance on append.
    pub const AUTO_MAINTAIN: &str = "timelake.auto-maintain";
    /// How long to keep old snapshots, e.g. `1h`.
    pub const SNAPSHOT_RETENTION: &str = "timelake.snapshot-retention";
    /// Snapshots always kept.
    pub const MIN_SNAPSHOTS: &str = "timelake.min-snapshots";
    /// Snapshots kept at most.
    pub const MAX_SNAPSHOTS: &str = "timelake.max-snapshots";
    /// Files per size tier that trigger inline compaction.
    pub const COMPACT_MIN_FILES: &str = "timelake.compact-min-files";
    /// Inline maintenance rewrites a partition whole (sorted, duplicates removed) once the table
    /// holds data newer than the partition's end by this much, or `off`.
    pub const COMPACT_COLD_AFTER: &str = "timelake.compact-cold-after";
    /// The column holding each row's insertion time (`_ingest_ts`), set by the writer; absent for
    /// tables without one. Among rows with the same `(series…, time)` the latest insertion wins.
    pub const INGEST_TIME_COLUMN: &str = "timelake.ingest-time-column";
    /// The Timelake reader removes copies of a `(series…, time)` when reading (default: as `dedup`).
    pub const READ_DEDUP: &str = "timelake.read-dedup";
    /// How the Timelake reader removes copies: `merge` (default) or `keys-first` (SPEC §9.1).
    pub const READ_DEDUP_STRATEGY: &str = "timelake.read-dedup-strategy";
    /// Whether the Timelake reader filters rows while decoding Parquet: `auto` (default), `true` or `false`.
    pub const FILTER_PUSHDOWN: &str = "timelake.filter-pushdown";
    /// The Timelake reader prunes row groups and pages with `IN` lists of more than 20 values.
    pub const IN_LIST_PRUNING: &str = "timelake.in-list-pruning";
    /// `ORDER BY time DESC LIMIT k` reads time partitions newest first and stops early.
    pub const NEWEST_FIRST: &str = "timelake.newest-first";
    /// How the signal aggregates get readings in time order: `auto` (default), `segments` or `merge`.
    pub const SERIES_ORDER: &str = "timelake.series-order";
    /// Windows over each series in time order read the series merged in order: `auto` (default), `true` or `false`.
    pub const WINDOW_SERIES_ORDER: &str = "timelake.window-series-order";
    /// Signal aggregates of the same readings in one query compute one state (default true).
    pub const SHARED_SIGNAL_STATE: &str = "timelake.shared-signal-state";
    /// Format of the head object (`metadata/timelake-head`): `arrow` (default), `json`, another
    /// registered codec, or `none` to not write one.
    pub const HEAD_FORMAT: &str = "timelake.head-format";
    /// Bytes the head may spend on optional sections such as the last-value index.
    pub const HEAD_INLINE_BYTES: &str = "timelake.head-inline-bytes";
    /// No snapshot expires before this time (milliseconds since the epoch). Set by `timelake upgrade`
    /// so the table can be rolled back to its state before the upgrade.
    pub const KEEP_SNAPSHOTS_UNTIL: &str = "timelake.keep-snapshots-until";
    /// The table format version (see [`crate::FORMAT_VERSION`]); absent means 0.1.0.
    pub const FORMAT_VERSION: &str = "timelake.format-version";
    /// On a raw table: its rollups, comma-separated table names (SPEC §11.1).
    pub const ROLLUPS: &str = "timelake.rollups";
    /// `false` to stop the Timelake reader answering queries on this table from its rollups.
    pub const ROLLUP_ROUTING: &str = "timelake.rollup-routing";
    /// On a rollup: the raw table it summarises.
    pub const ROLLUP_SOURCE: &str = "timelake.rollup-source";
    /// On a rollup: the bucket width, e.g. `1m`.
    pub const ROLLUP_BUCKET: &str = "timelake.rollup-bucket";
    /// On a rollup: its aggregates, as JSON (`[{"name": "avg_value", "function": "avg", "column": "value"}]`).
    pub const ROLLUP_AGGREGATES: &str = "timelake.rollup-aggregates";
    /// On a rollup: inline maintenance of the raw table refreshes the rollup once its last refresh
    /// is this old, or `off`.
    pub const ROLLUP_REFRESH_INTERVAL: &str = "timelake.rollup-refresh-interval";
    /// How long the Timelake reader uses a table version without checking for a newer one.
    pub const VERSION_CHECK_INTERVAL: &str = "timelake.version-check-interval";
    /// Rows per Parquet data page in the files written.
    pub const DATA_PAGE_ROWS: &str = "timelake.data-page-rows";
    /// Iceberg's Parquet codec: `zstd` (default), `lz4`, `snappy`, `gzip`, `brotli`, or `uncompressed`.
    pub const COMPRESSION_CODEC: &str = "write.parquet.compression-codec";
    /// Iceberg's Parquet compression level, for `zstd`, `gzip` and `brotli`.
    pub const COMPRESSION_LEVEL: &str = "write.parquet.compression-level";
}

/// Snapshot summary keys: Timelake's own, and the standard Iceberg counters Timelake maintains.
pub mod summary {
    /// Location of the snapshot's last-value index.
    pub const LAST_VALUE_INDEX: &str = "timelake.last-value-index";
    /// The writer-supplied idempotency key of an append.
    pub const BATCH_ID: &str = "timelake.batch-id";
    /// On a rollup: the id of the raw table snapshot the rollup reflects.
    pub const ROLLUP_SOURCE_SNAPSHOT: &str = "timelake.rollup-source-snapshot";
    /// On a rollup: the sequence number of that snapshot. Data files of the raw table with a higher
    /// data sequence number hold rows the rollup doesn't reflect yet.
    pub const ROLLUP_SOURCE_SEQUENCE: &str = "timelake.rollup-source-sequence";

    /// Data files added by the snapshot.
    pub const ADDED_DATA_FILES: &str = "added-data-files";
    /// Rows added by the snapshot.
    pub const ADDED_RECORDS: &str = "added-records";
    /// Bytes added by the snapshot.
    pub const ADDED_FILES_SIZE: &str = "added-files-size";
    /// Data files removed by the snapshot.
    pub const DELETED_DATA_FILES: &str = "deleted-data-files";
    /// Rows removed by the snapshot.
    pub const DELETED_RECORDS: &str = "deleted-records";
    /// Bytes removed by the snapshot.
    pub const REMOVED_FILES_SIZE: &str = "removed-files-size";
    /// Data files in the table after the snapshot.
    pub const TOTAL_DATA_FILES: &str = "total-data-files";
    /// Rows in the table after the snapshot.
    pub const TOTAL_RECORDS: &str = "total-records";
    /// Bytes in the table after the snapshot.
    pub const TOTAL_FILES_SIZE: &str = "total-files-size";
    /// Delete files in the table (always 0: Timelake doesn't write them).
    pub const TOTAL_DELETE_FILES: &str = "total-delete-files";
    /// Position deletes in the table (always 0).
    pub const TOTAL_POSITION_DELETES: &str = "total-position-deletes";
    /// Equality deletes in the table (always 0).
    pub const TOTAL_EQUALITY_DELETES: &str = "total-equality-deletes";
}

/// The table format this version of Timelake writes, recorded in each table as
/// `timelake.format-version`. It reads and writes older formats, and newer ones as far as
/// [`FormatVersion`] allows. `timelake upgrade` brings a table to this format.
///
/// - 0.1.0: tables written by Timelake 0.1 and 0.2 (no property).
/// - 0.2.0: the head object (`metadata/timelake-head`), from Timelake 0.3 (which wrote it as `2`).
/// - 0.3.0: an insertion-time column (`timelake.ingest-time-column`) that writers fill.
pub const FORMAT_VERSION: FormatVersion = FormatVersion::new(0, 3, 0);

/// The name of the insertion-time column of new time-series tables.
pub const INGEST_TIME_COLUMN: &str = "_ingest_ts";

/// Optional warehouse-level index of table names, for stores that cannot list (plain HTTPS).
pub const TABLES_INDEX: &str = "timelake-tables.json";

/// An invalid contract property, duration, or partition value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatError(pub String);

impl FormatError {
    fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FormatError {}

/// `Result` with [`FormatError`].
pub type Result<T> = std::result::Result<T, FormatError>;
