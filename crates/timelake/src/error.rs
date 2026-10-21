use std::convert::Infallible;

use thiserror::Error as ThisError;

/// `Result` with Timelake's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors from Timelake operations.
#[derive(Debug, ThisError)]
#[non_exhaustive]
pub enum Error {
    /// Bad storage or session configuration (unknown option, unsupported URL scheme, …).
    #[error("{0}")]
    Config(String),
    /// The table (or one of its metadata files) doesn't exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// A table with this name already exists.
    #[error("already exists: {0}")]
    AlreadyExists(String),
    /// Invalid input: a table name, schema, option, or data that doesn't fit the table.
    #[error("invalid: {0}")]
    Invalid(String),
    /// Another writer committed first and the operation could not be rebased.
    #[error("commit conflict: {0}")]
    Conflict(String),
    /// A commit whose outcome can't be told: its metadata may be in the table. Nothing it wrote
    /// was deleted; load the table again to see whether it committed.
    #[error("commit outcome unknown: {0}")]
    CommitUnknown(String),
    /// An invalid table contract property, duration, or partition value.
    #[error(transparent)]
    Format(#[from] timelake_format::FormatError),
    /// From Iceberg metadata handling.
    #[error(transparent)]
    Iceberg(#[from] iceberg::Error),
    /// From object storage.
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    /// An object path that isn't valid for the store.
    #[error(transparent)]
    ObjectPath(#[from] object_store::path::Error),
    /// From Arrow compute.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// From reading or writing Parquet.
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    /// From query planning or execution.
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    /// From local file system access.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// From (de)serializing JSON metadata.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl Error {
    pub(crate) fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    pub(crate) fn invalid(msg: impl Into<String>) -> Self {
        Self::Invalid(msg.into())
    }
}

impl From<timelake_format::error::WriteError> for Error {
    fn from(e: timelake_format::error::WriteError) -> Self {
        use timelake_format::error::WriteError as W;
        match e {
            W::Invalid(m) => Self::Invalid(m),
            W::Conflict(m) => Self::Conflict(m),
            W::CommitUnknown(m) => Self::CommitUnknown(m),
            W::NotFound(m) => Self::NotFound(m),
            W::Store(m) | W::Corrupt(m) => Self::Io(std::io::Error::other(m)),
            e @ W::Avro(_) => Self::Io(std::io::Error::other(e)),
            W::Format(e) => Self::Format(e),
            W::Arrow(e) => Self::Arrow(e),
            W::Parquet(e) => Self::Parquet(e),
        }
    }
}

/// Lets APIs that take `impl TryInto<TableName>` also accept a `TableName`.
impl From<Infallible> for Error {
    fn from(e: Infallible) -> Self {
        match e {}
    }
}

impl From<Error> for datafusion::error::DataFusionError {
    fn from(e: Error) -> Self {
        match e {
            Error::DataFusion(e) => e,
            e => Self::External(Box::new(e)),
        }
    }
}
