//! Timelake: the open time-series lakehouse. Tables are Apache Iceberg tables on any object
//! store, laid out for time series (partitioned by time, sorted by series and time), kept
//! compact by the writer itself, and queried with DataFusion.
//!
//! ```no_run
//! # async fn demo(batch: arrow::array::RecordBatch) -> timelake::Result<()> {
//! use timelake::{Lake, Session, TableOptions};
//!
//! let lake = Lake::open("s3://bucket/warehouse", Default::default())?;
//! let opts = TableOptions {
//!     time_column: Some("time".into()),
//!     series_key: vec!["device_id".into()],
//!     ..Default::default()
//! };
//! let table = lake.create_table("iot.readings", &batch.schema(), opts).await?;
//! let (table, result) = table.append(&[batch], Default::default()).await?;
//!
//! let session = Session::new(lake);
//! let rows = session.sql("SELECT * FROM iot.readings WHERE device_id = 7").await?.collect().await?;
//! let latest = session.latest("iot.readings").await?.collect().await?;
//! # Ok(()) }
//! ```

#![warn(missing_docs)]

mod alter;
mod append;
mod background;
mod block_cache;
mod cache;
mod commit;
mod error;
mod footer;
pub mod head;
mod lake;
mod lvi;
mod maintain;
mod query;
mod rollup;
mod schema;
pub mod storage;
mod table;
#[cfg(test)]
mod testing;
mod upgrade;
mod write;

pub use append::{AppendOptions, AppendResult};
pub use background::flush;
pub use commit::CommitInfo;
pub use error::{Error, Result};
pub use lake::Lake;
pub use maintain::{MaintainOptions, MaintainReport, MaintainSteps};
pub use query::{RollupStatement, Session, SessionSettings, Statement, session_key};
pub use rollup::RollupRefresh;
pub use table::{LiveEntry, Table};
pub use timelake_format::contract::{Chunk, Contract, TableOptions};
pub use timelake_format::name::{DEFAULT_NAMESPACE, TableName};
pub use timelake_format::rows::{align_batch, latest_per_series};
pub use timelake_format::{bounds, contract, format_duration, parse_duration, property, settings, summary};
pub use upgrade::{DEFAULT_KEEP_SNAPSHOTS, UpgradeOptions, UpgradeReport, UpgradeStep};
pub use write::file_partition;
pub use {arrow, datafusion};
