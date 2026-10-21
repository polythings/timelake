//! A table's head object (`metadata/timelake-head`, spec 7): its metadata, live files with
//! column bounds, and when they fit, the last-value index and the footers of the newest files. One
//! request instead of the version hint, metadata, manifest list and manifests. Decoded by
//! `timelake_format::head`, as the engine decodes it.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use bytes::Bytes;
use timelake_format::head::built_in;
use timelake_format::head::content::Contents;

use crate::error::{Error, Result};
use crate::meta::{DataFile, TableMeta};

/// A table version as its head describes it.
#[derive(Debug)]
pub struct Head {
    /// The table version (`vN.metadata.json`).
    pub version: u64,
    pub meta: TableMeta,
    pub files: Vec<DataFile>,
    /// The last-value index, if it was inline.
    pub last_values: Option<RecordBatch>,
    /// Footers of data files by path.
    pub footers: HashMap<String, Bytes>,
}

impl Head {
    /// Parses a head written with a built-in codec (`arrow` or `json`).
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let head = Contents::decode(bytes, built_in).map_err(Error::Unreadable)?;
        let meta = TableMeta::parse(&head.metadata_json)?;
        let by_id = meta.fields_by_id();
        let files = head.files.iter().map(|e| DataFile::from_entry(e, &by_id)).collect();
        let footers = head.footers.into_iter().map(|(path, footer)| (path, Bytes::from(footer))).collect();
        Ok(Self { version: head.version, meta, files, last_values: head.last_values, footers })
    }
}
