//! What the shared write path needs from where it runs: a [`Store`] for objects, a [`Clock`] for the
//! time, and a [`Random`] for random numbers (loko's services, through lakekit), and the limits of
//! the place it runs ([`WriteLimits`]). The WASM writer has a store over JavaScript (an R2 binding,
//! `fetch`) and loko's clock and random numbers for JavaScript hosts; tests use memory.

pub use lakekit_core::{Clock, Listed, MemoryStore, MemoryUpload, Random, Services, Store, Upload, uuid_v4, uuid_v7};

/// The limits of where the write path runs.
pub trait WriteLimits {
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

impl WriteLimits for MemoryStore {}
