//! The Timelake reader: DataFusion over Timelake tables.
//!
//! What it adds over a generic Iceberg/Parquet scan (spec 21):
//! - **File pruning** on Iceberg column bounds (time range, series key) evaluated with DataFusion's
//!   `PruningPredicate`, then row-group and page pruning inside the (sorted) Parquet files.
//! - **Last-value rewrite:** "newest row per series" queries (`ROW_NUMBER() OVER (PARTITION BY
//!   <series key> ORDER BY <time> DESC) = 1`) and "newest row" queries (`ORDER BY <time> DESC
//!   LIMIT 1` filtered on the series key) are answered from the last-value index instead of a scan.
//! - **Pruning with long `IN` lists:** row groups and pages are skipped by `column IN (…)` filters
//!   of more than 20 values too, which DataFusion's pruning leaves alone.
//! - **Dedup on read:** in tables with dedup on, files that may share a `(series…, time)` are
//!   merged and only the newest write of each key is returned, before compaction removes the rest.
//! - **Rollup routing:** `GROUP BY time_bucket(…)` queries read the buckets a rollup has right from
//!   the rollup, and the rest from the raw table (spec 27).
//! - `time_bucket(interval, ts [, origin])`, an alias of `date_bin`.
//! - Namespaces as SQL schemas: `SELECT … FROM iot.readings`.

mod asof;
mod catalog;
mod changes;
mod dedup;
mod flatline;
mod gapfill;
mod gaps;
mod histogram;
mod in_list;
mod lttb;
mod m4;
mod moments;
mod newest;
mod ohlc;
mod provider;
mod quantile;
mod rewrite;
mod rolling;
mod rollup;
mod route;
mod runs;
mod selection;
mod series_order;
mod session;
mod settings;
mod shared_state;
mod signal;
mod statement;
mod stats;

pub(crate) use rollup::{refresh_session, rollup_frame};
pub use session::Session;
pub use settings::SessionSettings;
pub use statement::{RollupStatement, Statement};

/// Name of the lake's catalog in SQL.
const CATALOG: &str = "timelake";

/// How long a table version the first lookup of a query found is reused by the rest of its
/// planning, whatever `version-check-interval` says, so a query checks each table once.
const PLANNING_REUSE: std::time::Duration = std::time::Duration::from_secs(1);
