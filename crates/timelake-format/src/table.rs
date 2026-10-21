//! A table at one version, as the shared write path sees it: its metadata, schema and contract, and
//! (loaded on demand) its live files and last-value index. Loaded from the head when the head is
//! current, otherwise from the metadata chain. Heads are read and written with
//! [`crate::head::content`], as the engine reads and writes them.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;

use crate::error::{WriteError, WriteResult};
use crate::head::content::{Contents, HeadBuilder, footer_order};
use crate::head::{Header, MAX_HEADER, built_in};
use crate::host::Host;
use crate::iceberg::{Content, Entry, ManifestFile, Status, read_manifest, read_manifest_list};
use crate::layout::{head_uri, metadata_prefix, metadata_uri, metadata_version};
use crate::lvi;
use crate::metadata::Metadata;
use crate::rows::align_batch;
use crate::{Contract, summary};

/// A table at one committed version.
#[derive(Clone, Debug)]
pub struct TableState {
    /// The table's root URI.
    pub location: String,
    /// The metadata version (`vN.metadata.json`).
    pub version: u64,
    /// The metadata.
    pub metadata: Metadata,
    /// The current schema as Arrow, with field ids.
    pub schema: SchemaRef,
    /// The Timelake contract.
    pub contract: Contract,
    /// The live files, when known (from the head, or derived by a commit).
    pub files: Option<Vec<Entry>>,
    /// The last-value index, when known (inline in the head, or written by a commit).
    pub last_values: Option<RecordBatch>,
    /// Footers of data files (path → footer bytes), carried from head to head.
    pub footers: BTreeMap<String, Vec<u8>>,
}

impl TableState {
    /// The current version of the table at `location` (its root URI, without a trailing slash).
    pub async fn load<H: Host>(host: &H, location: &str) -> WriteResult<Self> {
        let (head, prefix) = (head_uri(location), metadata_prefix(location));
        let (head, listed) = futures_util::future::join(host.get(&head), host.list(&prefix)).await;
        let newest = listed?.iter().filter_map(|o| metadata_version(&o.uri)).max();
        let Some(newest) = newest else { return Err(WriteError::NotFound(location.to_string())) };
        if let Some(state) = head?.and_then(|bytes| decode_head(location, &bytes).ok())
            && state.version >= newest
        {
            return Ok(state);
        }
        Self::load_version(host, location, newest).await
    }

    /// Version `version` from its metadata file (without the head).
    pub async fn load_version<H: Host>(host: &H, location: &str, version: u64) -> WriteResult<Self> {
        let uri = metadata_uri(location, version);
        let bytes = host.get(&uri).await?.ok_or_else(|| WriteError::NotFound(uri.clone()))?;
        Self::from_metadata(location, version, Metadata::parse(&bytes)?)
    }

    /// Version `version` with `metadata`: an error if the shared writer can't write it.
    pub(crate) fn from_metadata(location: &str, version: u64, metadata: Metadata) -> WriteResult<Self> {
        if metadata.format_version() != 2 {
            return Err(WriteError::invalid("only Iceberg format version 2 tables can be written"));
        }
        let contract = metadata.contract()?;
        contract.check_writable().map_err(|e| WriteError::invalid(format!("{location}: {e}")))?;
        Ok(Self {
            location: location.to_string(),
            version,
            schema: metadata.arrow_schema()?,
            metadata,
            contract,
            files: None,
            last_values: None,
            footers: BTreeMap::new(),
        })
    }

    /// The current snapshot's manifests. A table with delete files (row-level deletes another
    /// engine wrote) is an error: the shared write path would carry them over as data, or
    /// compact deleted rows back in.
    pub async fn manifests<H: Host>(&self, host: &H) -> WriteResult<Vec<ManifestFile>> {
        let Some(list) = self.metadata.manifest_list() else { return Ok(vec![]) };
        let bytes = host.get(list).await?.ok_or_else(|| WriteError::NotFound(list.to_string()))?;
        let manifests = read_manifest_list(&bytes)?;
        if let Some(m) = manifests.iter().find(|m| m.content == Content::Deletes) {
            return Err(WriteError::invalid(format!(
                "{}: the table has delete files ({}), which Timelake doesn't apply; compact them away with the engine \
                 that wrote them first",
                self.location, m.path
            )));
        }
        Ok(manifests)
    }

    /// The entries of `manifest`.
    pub async fn manifest<H: Host>(host: &H, manifest: &ManifestFile) -> WriteResult<Vec<Entry>> {
        let bytes = host.get(&manifest.path).await?.ok_or_else(|| WriteError::NotFound(manifest.path.clone()))?;
        read_manifest(manifest, &bytes)
    }

    /// The live files, read from the manifests unless known.
    pub async fn live_files<H: Host>(&mut self, host: &H) -> WriteResult<&[Entry]> {
        if self.files.is_none() {
            let mut files = vec![];
            for m in self.manifests(host).await? {
                let entries = Self::manifest(host, &m).await?;
                files.extend(entries.into_iter().filter(|e| e.status != Status::Deleted));
            }
            self.files = Some(files);
        }
        Ok(self.files.as_deref().unwrap_or_default())
    }

    /// The last-value index location recorded in the current snapshot.
    pub fn last_value_index(&self) -> Option<String> {
        self.metadata.summary().get(summary::LAST_VALUE_INDEX).cloned()
    }

    /// The last-value index, read unless known.
    pub async fn last_values<H: Host>(&mut self, host: &H) -> WriteResult<Option<RecordBatch>> {
        if self.last_values.is_none()
            && let Some(uri) = self.last_value_index()
        {
            let bytes = host.get(&uri).await?.ok_or_else(|| WriteError::NotFound(uri.clone()))?;
            self.last_values = Some(lvi::decode(Bytes::from(bytes), &self.schema)?);
        }
        Ok(self.last_values.clone())
    }

    /// Writes the head of this version unless the stored one is as new. Two writers racing can
    /// leave the older head; readers notice it is behind the listing and skip it. (The engine
    /// replaces a head with a conditional PUT instead, so it never moves back.)
    pub async fn write_head<H: Host>(&self, host: &H) -> WriteResult<()> {
        let uri = head_uri(&self.location);
        let Some(codec) = self.contract.head_format.as_deref().and_then(built_in) else {
            // Turned off, or a codec the shared writer doesn't have: leave no stale head behind.
            return host.delete(&[uri]).await;
        };
        if let Ok(prefix) = host.get_range(&uri, 0, MAX_HEADER).await
            && Header::parse(&prefix).is_some_and(|h| h.version >= self.version)
        {
            return Ok(());
        }
        let Some(files) = &self.files else { return Ok(()) };
        let budget = self.contract.head_inline_bytes;
        let mut head = HeadBuilder::new(self.version, codec, self.metadata.to_json(), files, budget)?;
        if let Some(index) = &self.last_values {
            head.last_values(index)?;
        }
        // Only the footers this writer has: reading the others would cost a request each.
        for e in footer_order(files) {
            if let Some(footer) = self.footers.get(&e.file.path)
                && !head.footer(&e.file.path, footer)
            {
                break;
            }
        }
        host.put(&uri, head.finish()?).await
    }
}

fn decode_head(location: &str, bytes: &[u8]) -> WriteResult<TableState> {
    let head = Contents::decode(bytes, built_in)?;
    let metadata = Metadata::parse(head.metadata_json.as_bytes())?;
    let mut state = TableState::from_metadata(location, head.version, metadata)?;
    state.files = Some(head.files);
    if let (Some(_), Some(index)) = (state.last_value_index(), &head.last_values) {
        state.last_values = Some(align_batch(index, &state.schema)?);
    }
    state.footers = head.footers.into_iter().collect();
    Ok(state)
}
