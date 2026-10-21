//! What the shared write path needs from where it runs: lakekit's [`Store`] for objects and [`Env`]
//! for the clock, randomness, and waiting, and the limits of the place it runs ([`WriteEnv`]). The
//! WASM writer implements them over JavaScript (an R2 binding, `fetch`), tests over memory.

pub use lakekit_core::{Env, Listed, MemoryStore, MemoryUpload, Store, Upload, WithEnv, uuid_v4, uuid_v7};

/// An [`Env`] with the limits of where the write path runs.
pub trait WriteEnv: Env {
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

impl WriteEnv for MemoryStore {}
