//! The Timelake reader (spec 21): DataFusion over Timelake tables, with Timelake's scans, optimizer
//! rules and SQL functions. The [Querying](https://timelake.dev/docs/querying) docs describe what it
//! adds.

mod asof;
mod catalog;
mod changes;
mod dedup;
mod flatline;
mod functions;
mod gapfill;
mod gaps;
mod groups;
mod histogram;
mod in_list;
mod lists;
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
pub(crate) mod time;

pub(crate) use rollup::{refresh_session, rollup_frame};
pub use session::Session;
pub use settings::{SessionSettings, session_key};
pub use statement::{RollupStatement, Statement};

/// Name of the lake's catalog in SQL.
const CATALOG: &str = "timelake";

/// How long a table version the first lookup of a query found is reused by the rest of its
/// planning, whatever `version-check-interval` says, so a query checks each table once.
const PLANNING_REUSE: std::time::Duration = std::time::Duration::from_secs(1);
