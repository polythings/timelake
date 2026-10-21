//! Errors of the write path (feature `write`).

use arrow_schema::ArrowError;
use parquet::errors::ParquetError;

use crate::FormatError;

/// An error writing a table without the engine.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    /// The data or request doesn't fit the table (a missing required column, nulls in the time
    /// column, …).
    #[error("invalid: {0}")]
    Invalid(String),
    /// Another writer changed the table in a way this commit can't build on.
    #[error("conflict: {0}")]
    Conflict(String),
    /// A commit whose outcome can't be told: its metadata may be in the table. Nothing it wrote
    /// was deleted; load the table again to see whether it committed.
    #[error("commit outcome unknown: {0}")]
    CommitUnknown(String),
    /// An object the table needs is missing.
    #[error("not found: {0}")]
    NotFound(String),
    /// The store failed.
    #[error("store: {0}")]
    Store(String),
    /// A file Timelake reads isn't what it expects (JSON metadata, a manifest's fields).
    #[error("unexpected file contents: {0}")]
    Corrupt(String),
    /// An Avro manifest or manifest list that can't be read or written.
    #[error("unexpected file contents: avro: {0}")]
    Avro(#[from] apache_avro::Error),
    /// Table conventions.
    #[error(transparent)]
    Format(#[from] FormatError),
    /// Arrow.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    /// Parquet.
    #[error(transparent)]
    Parquet(#[from] ParquetError),
}

impl WriteError {
    /// An [`WriteError::Invalid`] with `msg`.
    pub fn invalid(msg: impl Into<String>) -> Self {
        Self::Invalid(msg.into())
    }
}

/// Results of the write path.
pub type WriteResult<T> = std::result::Result<T, WriteError>;
