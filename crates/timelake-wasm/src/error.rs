use arrow_schema::ArrowError;
use parquet::errors::ParquetError;
use timelake_format::FormatError;

/// Errors from reading a table. In JavaScript they surface as `Error` objects with this message.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Input that is well-formed but not what a Timelake table or query allows.
    #[error("{0}")]
    Invalid(String),
    #[error("bad table metadata: {0}")]
    Metadata(#[from] serde_json::Error),
    #[error("bad table contract: {0}")]
    Contract(#[from] FormatError),
    /// A manifest or manifest list (lakekit reads them).
    #[error("{0}")]
    Iceberg(#[from] lakekit_iceberg::Error),
    #[error("Parquet: {0}")]
    Parquet(#[from] ParquetError),
    #[error("Arrow: {0}")]
    Arrow(#[from] ArrowError),
}

impl Error {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}

/// Result with this crate's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;
