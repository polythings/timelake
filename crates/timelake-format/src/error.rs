//! Errors of the write path (feature `write`).

use std::fmt;

use arrow_schema::ArrowError;
use parquet::errors::ParquetError;

use crate::FormatError;

/// An error writing a table without the engine.
#[derive(Debug)]
pub enum WriteError {
    /// The data or request doesn't fit the table (a missing required column, nulls in the time
    /// column, …).
    Invalid(String),
    /// Another writer changed the table in a way this commit can't build on.
    Conflict(String),
    /// A commit whose outcome can't be told: its metadata may be in the table. Nothing it wrote
    /// was deleted; load the table again to see whether it committed.
    CommitUnknown(String),
    /// An object the table needs is missing.
    NotFound(String),
    /// The store failed.
    Store(String),
    /// A file Timelake reads isn't what it expects (Avro, JSON).
    Corrupt(String),
    /// Table conventions.
    Format(FormatError),
    /// Arrow.
    Arrow(ArrowError),
    /// Parquet.
    Parquet(ParquetError),
}

impl WriteError {
    /// An [`WriteError::Invalid`] with `msg`.
    pub fn invalid(msg: impl Into<String>) -> Self {
        Self::Invalid(msg.into())
    }
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(m) => write!(f, "invalid: {m}"),
            Self::Conflict(m) => write!(f, "conflict: {m}"),
            Self::CommitUnknown(m) => write!(f, "commit outcome unknown: {m}"),
            Self::NotFound(m) => write!(f, "not found: {m}"),
            Self::Store(m) => write!(f, "store: {m}"),
            Self::Corrupt(m) => write!(f, "unexpected file contents: {m}"),
            Self::Format(e) => e.fmt(f),
            Self::Arrow(e) => e.fmt(f),
            Self::Parquet(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for WriteError {}

impl From<FormatError> for WriteError {
    fn from(e: FormatError) -> Self {
        Self::Format(e)
    }
}

/// lakebed's errors keep their meaning: a conflict stays a conflict, an unknown commit outcome stays
/// unknown.
impl From<lakebed_core::Error> for WriteError {
    fn from(e: lakebed_core::Error) -> Self {
        use lakebed_core::Error as E;
        match e {
            E::Invalid(m) | E::Unsupported(m) => Self::Invalid(m),
            E::Conflict(m) => Self::Conflict(m),
            E::CommitUnknown(m) => Self::CommitUnknown(m),
            E::NotFound(m) => Self::NotFound(m),
            E::Store(m) => Self::Store(m),
            E::Corrupt(m) => Self::Corrupt(m),
            other => Self::Store(other.to_string()),
        }
    }
}

impl From<ArrowError> for WriteError {
    fn from(e: ArrowError) -> Self {
        Self::Arrow(e)
    }
}

impl From<ParquetError> for WriteError {
    fn from(e: ParquetError) -> Self {
        Self::Parquet(e)
    }
}

/// Results of the write path.
pub type WriteResult<T> = std::result::Result<T, WriteError>;
