//! What the shared write path needs from where it runs: lakekit's [`Host`] (an object store, a
//! clock, randomness, and a way to wait), and the limits of the place it runs ([`WriteHost`]). The
//! WASM writer implements them over JavaScript (an R2 binding, `fetch`), tests over memory.

pub use lakekit_core::{Host, Listed, MemoryHost, MemoryUpload, Upload, uuid_v4, uuid_v7};

/// A [`Host`] with the limits of where the write path runs.
pub trait WriteHost: Host {
    /// The most bytes a row group being written may hold in memory before it is written out, where
    /// memory is scarce (`None`: row groups end at the table's `row-group-rows`).
    fn max_row_group_bytes(&self) -> Option<usize> {
        None
    }

    /// Rows per Parquet data page in files written here, when the table doesn't set
    /// `data-page-rows`.
    fn data_page_rows(&self) -> usize {
        crate::file::DATA_PAGE_ROWS
    }
}

impl WriteHost for MemoryHost {}
