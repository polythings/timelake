//! Writing tables from JavaScript (spec 13): the shared write path (`timelake_format::append`)
//! over a [`Host`] whose objects come from a JavaScript store (an R2 binding, memory, or one the
//! application provides). The store gets absolute URIs, as Iceberg metadata records them; the
//! TypeScript client maps them to keys.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Cursor;
use std::rc::Rc;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use js_sys::{Array, Function, Promise, Reflect, Uint8Array};
use serde::{Deserialize, Serialize};
use timelake_format::append::{AppendOptions, append};
use timelake_format::error::{WriteError, WriteResult};
use timelake_format::host::{Host, Listed, Upload};
use timelake_format::maintain::{MaintainOptions, MaintainReport, maintain};
use timelake_format::table::TableState;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{JsFuture, future_to_promise};

/// A [`Host`] over a JavaScript store object (see the TypeScript `Store` interface).
#[derive(Clone, Debug)]
struct JsHost {
    store: JsValue,
}

fn js_err(e: &JsValue) -> WriteError {
    let msg = e.as_string().or_else(|| e.dyn_ref::<js_sys::Error>().map(|e| String::from(e.message())));
    WriteError::Store(msg.unwrap_or_else(|| format!("{e:?}")))
}

impl JsHost {
    async fn call(&self, method: &str, args: &[JsValue]) -> WriteResult<JsValue> {
        let f: Function = Reflect::get(&self.store, &method.into())
            .map_err(|e| js_err(&e))?
            .dyn_into()
            .map_err(|_| WriteError::Store(format!("the store has no {method} method")))?;
        let args: Array = args.iter().collect();
        let value = f.apply(&self.store, &args).map_err(|e| js_err(&e))?;
        JsFuture::from(Promise::resolve(&value)).await.map_err(|e| js_err(&e))
    }
}

fn bytes(v: &JsValue) -> Vec<u8> {
    Uint8Array::new(v).to_vec()
}

fn number(n: u64) -> JsValue {
    #[expect(clippy::cast_precision_loss, reason = "offsets and sizes are far below 2^53")]
    JsValue::from_f64(n as f64)
}

/// A multipart upload of a JavaScript store, or a buffer written in one `put` when the store has
/// none.
#[derive(Debug)]
enum JsUpload {
    Parts { upload: JsValue },
    Buffered { host: JsHost, uri: String, bytes: Vec<u8> },
}

impl Upload for JsUpload {
    async fn put_part(&mut self, part: Vec<u8>) -> WriteResult<()> {
        match self {
            Self::Parts { upload } => {
                JsHost { store: upload.clone() }.call("putPart", &[Uint8Array::from(part.as_slice()).into()]).await?;
                Ok(())
            }
            Self::Buffered { bytes, .. } => {
                bytes.extend(part);
                Ok(())
            }
        }
    }

    async fn complete(self) -> WriteResult<()> {
        match self {
            Self::Parts { upload } => JsHost { store: upload }.call("complete", &[]).await.map(drop),
            Self::Buffered { host, uri, bytes } => host.put(&uri, bytes).await,
        }
    }

    async fn abort(self) {
        if let Self::Parts { upload } = self {
            let _ = JsHost { store: upload }.call("abort", &[]).await;
        }
    }
}

#[derive(Deserialize)]
struct JsListed {
    uri: String,
    size: f64,
}

impl Host for JsHost {
    type Upload = JsUpload;

    async fn get(&self, uri: &str) -> WriteResult<Option<Vec<u8>>> {
        let v = self.call("get", &[uri.into()]).await?;
        Ok((!v.is_null() && !v.is_undefined()).then(|| bytes(&v)))
    }

    async fn get_range(&self, uri: &str, start: u64, len: u64) -> WriteResult<Vec<u8>> {
        Ok(bytes(&self.call("getRange", &[uri.into(), number(start), number(len)]).await?))
    }

    async fn exists(&self, uri: &str) -> WriteResult<bool> {
        Ok(self.call("exists", &[uri.into()]).await?.is_truthy())
    }

    async fn put(&self, uri: &str, bytes: Vec<u8>) -> WriteResult<()> {
        self.call("put", &[uri.into(), Uint8Array::from(bytes.as_slice()).into()]).await.map(drop)
    }

    async fn put_if_absent(&self, uri: &str, bytes: Vec<u8>) -> WriteResult<bool> {
        let v = self.call("putIfAbsent", &[uri.into(), Uint8Array::from(bytes.as_slice()).into()]).await?;
        Ok(v.is_truthy())
    }

    async fn upload(&self, uri: &str) -> WriteResult<JsUpload> {
        let has = Reflect::get(&self.store, &"upload".into()).is_ok_and(|f| f.is_function());
        if !has {
            return Ok(JsUpload::Buffered { host: self.clone(), uri: uri.to_string(), bytes: vec![] });
        }
        let upload = self.call("upload", &[uri.into()]).await?;
        Ok(JsUpload::Parts { upload })
    }

    async fn list(&self, prefix: &str) -> WriteResult<Vec<Listed>> {
        let v = self.call("list", &[prefix.into()]).await?;
        let listed: Vec<JsListed> =
            serde_wasm_bindgen::from_value(v).map_err(|e| WriteError::Store(format!("list: {e}")))?;
        #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "object sizes")]
        Ok(listed.into_iter().map(|o| Listed { uri: o.uri, size: o.size as u64 }).collect())
    }

    async fn delete(&self, uris: &[String]) -> WriteResult<()> {
        if uris.is_empty() {
            return Ok(());
        }
        let list: Array = uris.iter().map(|u| JsValue::from_str(u)).collect();
        self.call("delete", &[list.into()]).await.map(drop)
    }

    fn now_ms(&self) -> i64 {
        #[expect(clippy::cast_possible_truncation, reason = "milliseconds since the epoch")]
        let now = js_sys::Date::now() as i64;
        now
    }

    fn random(&self) -> u64 {
        let mut b = [0u8; 8];
        // getrandom draws from crypto.getRandomValues on wasm32.
        if getrandom::fill(&mut b).is_err() {
            #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "random bits")]
            return (js_sys::Math::random() * 9_007_199_254_740_992.0) as u64;
        }
        u64::from_le_bytes(b)
    }

    fn max_row_group_bytes(&self) -> Option<usize> {
        Some(MAX_ROW_GROUP_BYTES)
    }

    fn data_page_rows(&self) -> usize {
        DATA_PAGE_ROWS
    }

    async fn sleep(&self, ms: u64) {
        let promise = Promise::new(&mut |resolve, _| {
            let global = js_sys::global();
            if let Ok(set_timeout) = Reflect::get(&global, &"setTimeout".into()).and_then(JsCast::dyn_into::<Function>)
            {
                let _ = set_timeout.call2(&global, &resolve, &number(ms));
            } else {
                let _ = resolve.call0(&JsValue::NULL);
            }
        });
        let _ = JsFuture::from(promise).await;
    }
}

/// The most a row group being written holds in memory before it goes out: Workers have 128 MB.
const MAX_ROW_GROUP_BYTES: usize = 16 << 20;

/// Rows per data page in files written from JavaScript, unless the table sets `data-page-rows`:
/// compaction in a Worker holds a page per column of each file it merges.
const DATA_PAGE_ROWS: usize = 8 * 1024;

/// Rows per batch when decoding JSON rows.
const JSON_BATCH_ROWS: usize = 8 * 1024;

/// Bytes inline compaction reads after an append from JavaScript, unless the call says otherwise:
/// keeps an append's CPU time within what a Worker request gets (spec 13).
const DEFAULT_MAX_COMPACT_BYTES: u64 = 64 << 20;

/// Options of an append, from JavaScript.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct JsAppendOptions {
    batch_id: Option<String>,
    #[serde(default)]
    properties: HashMap<String, String>,
    maintain: Option<bool>,
    max_compact_bytes: Option<f64>,
}

/// Options of a maintenance run, from JavaScript.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct JsMaintainOptions {
    compact: Option<bool>,
    retention: Option<bool>,
    expire: Option<bool>,
    max_bytes: Option<f64>,
}

/// What maintenance did, for JavaScript.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsMaintainReport {
    partitions_compacted: usize,
    files_rewritten: usize,
    files_written: usize,
    rows_rewritten: u64,
    bytes_rewritten: u64,
    duplicates_removed: u64,
    more_to_compact: bool,
    partitions_dropped: usize,
    files_dropped: usize,
    snapshots_expired: usize,
    objects_deleted: usize,
    version: u64,
}

impl JsMaintainReport {
    fn new(r: &MaintainReport, version: u64) -> Self {
        Self {
            partitions_compacted: r.partitions_compacted,
            files_rewritten: r.files_rewritten,
            files_written: r.files_written,
            rows_rewritten: r.rows_rewritten,
            bytes_rewritten: r.bytes_rewritten,
            duplicates_removed: r.duplicates_removed,
            more_to_compact: r.more_to_compact,
            partitions_dropped: r.partitions_dropped,
            files_dropped: r.files_dropped,
            snapshots_expired: r.snapshots_expired,
            objects_deleted: r.objects_deleted,
            version,
        }
    }
}

#[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "a byte count from JavaScript")]
fn byte_count(x: f64) -> u64 {
    x.max(0.0) as u64
}

/// What an append did, for JavaScript.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsAppendReport {
    rows: usize,
    files: usize,
    bytes: u64,
    version: u64,
    /// As a string: snapshot ids don't fit a JavaScript number.
    snapshot_id: String,
    attempts: u64,
    duplicate: bool,
    /// Inline maintenance, if it did something; an error message if it failed (the append is
    /// committed either way).
    maintenance: Option<JsMaintainReport>,
    maintenance_error: Option<String>,
}

/// Writes one table: keeps the version it last loaded or committed, so a steady writer doesn't
/// reload the table before each append (a commit that loses to another writer reloads it).
#[wasm_bindgen]
#[derive(Debug)]
pub struct Writer {
    host: JsHost,
    location: String,
    state: Rc<RefCell<Option<TableState>>>,
}

fn to_js_err(e: &WriteError) -> JsValue {
    js_sys::Error::new(&e.to_string()).into()
}

#[wasm_bindgen]
impl Writer {
    /// A writer of the table at `location` (its root URI) through `store`.
    #[wasm_bindgen(constructor)]
    pub fn new(store: JsValue, location: &str) -> Writer {
        Writer { host: JsHost { store }, location: location.trim_end_matches('/').to_string(), state: Rc::default() }
    }

    /// Appends rows given as newline-delimited JSON objects. Timestamps are epoch milliseconds or
    /// ISO strings (millisecond precision; use `appendIpc` for microseconds).
    #[wasm_bindgen(js_name = appendJson)]
    pub fn append_json(&self, rows_json: String, options: JsValue) -> Promise {
        self.run(options, move |schema| rows_from_json(&rows_json, &schema))
    }

    /// Appends rows given as an Arrow IPC stream.
    #[wasm_bindgen(js_name = appendIpc)]
    pub fn append_ipc(&self, ipc: Vec<u8>, options: JsValue) -> Promise {
        self.run(options, move |_| rows_from_ipc(&ipc))
    }

    /// Runs maintenance: retention, compaction (within `maxBytes` read, if given), and snapshot
    /// expiry, as the table's settings say.
    pub fn maintain(&self, options: JsValue) -> Promise {
        let (host, location, state) = (self.host.clone(), self.location.clone(), self.state.clone());
        future_to_promise(async move {
            let o: JsMaintainOptions = if options.is_undefined() || options.is_null() {
                JsMaintainOptions::default()
            } else {
                serde_wasm_bindgen::from_value(options).map_err(|e| JsValue::from_str(&format!("bad options: {e}")))?
            };
            let defaults = MaintainOptions::default();
            let opts = MaintainOptions {
                compact: o.compact.unwrap_or(defaults.compact),
                retention: o.retention.unwrap_or(defaults.retention),
                expire: o.expire.unwrap_or(defaults.expire),
                max_bytes: o.max_bytes.map(byte_count),
                ..defaults
            };
            // Maintenance works on the newest version.
            let table = TableState::load(&host, &location).await.map_err(|e| to_js_err(&e))?;
            let (table, report) = maintain(&host, table, &opts).await.map_err(|e| {
                *state.borrow_mut() = None;
                to_js_err(&e)
            })?;
            let report = JsMaintainReport::new(&report, table.version);
            *state.borrow_mut() = Some(table);
            serde_wasm_bindgen::to_value(&report).map_err(Into::into)
        })
    }

    /// The table's columns (name, type, required), loading the table if needed.
    pub fn schema(&self) -> Promise {
        let (host, location, state) = (self.host.clone(), self.location.clone(), self.state.clone());
        future_to_promise(async move {
            let table = current(&host, &location, &state).await.map_err(|e| to_js_err(&e))?;
            let fields: Vec<(String, String, bool)> = table
                .schema
                .fields()
                .iter()
                .map(|f| (f.name().clone(), f.data_type().to_string(), !f.is_nullable()))
                .collect();
            serde_wasm_bindgen::to_value(&fields).map_err(Into::into)
        })
    }

    fn run(
        &self,
        options: JsValue,
        rows: impl FnOnce(SchemaRef) -> WriteResult<Vec<RecordBatch>> + 'static,
    ) -> Promise {
        let (host, location, state) = (self.host.clone(), self.location.clone(), self.state.clone());
        future_to_promise(async move {
            let options: JsAppendOptions = if options.is_undefined() || options.is_null() {
                JsAppendOptions::default()
            } else {
                serde_wasm_bindgen::from_value(options).map_err(|e| JsValue::from_str(&format!("bad options: {e}")))?
            };
            let table = current(&host, &location, &state).await.map_err(|e| to_js_err(&e))?;
            let batches = rows(table.schema.clone()).map_err(|e| to_js_err(&e))?;
            let opts = AppendOptions {
                batch_id: options.batch_id,
                properties: options.properties,
                maintain: options.maintain,
                max_compact_bytes: Some(options.max_compact_bytes.map_or(DEFAULT_MAX_COMPACT_BYTES, byte_count)),
            };
            let result = append(&host, table, &batches, &opts).await;
            let (table, report) = match result {
                Ok(done) => done,
                Err(e) => {
                    // The cached version may be outdated; load the table again next time.
                    *state.borrow_mut() = None;
                    return Err(to_js_err(&e));
                }
            };
            let version = table.version;
            *state.borrow_mut() = Some(table);
            let (maintenance, maintenance_error) = match &report.maintenance {
                Some(Ok(m)) => (Some(JsMaintainReport::new(m, version)), None),
                Some(Err(e)) => (None, Some(e.clone())),
                None => (None, None),
            };
            let report = JsAppendReport {
                rows: report.rows,
                files: report.files,
                bytes: report.bytes,
                version: report.commit.version,
                snapshot_id: report.commit.snapshot_id.to_string(),
                attempts: report.commit.attempts,
                duplicate: report.commit.duplicate,
                maintenance,
                maintenance_error,
            };
            serde_wasm_bindgen::to_value(&report).map_err(Into::into)
        })
    }
}

/// The table version to write on: the one this writer has, or the current one.
async fn current(host: &JsHost, location: &str, state: &Rc<RefCell<Option<TableState>>>) -> WriteResult<TableState> {
    let cached = state.borrow().clone();
    match cached {
        Some(t) => Ok(t),
        None => TableState::load(host, location).await,
    }
}

/// Rows from newline-delimited JSON objects, decoded with the table schema straight into Arrow
/// (no intermediate JSON values): timestamps as milliseconds (numbers or ISO strings), every
/// column nullable here (the table's own nullability is checked when the rows are aligned), and
/// the insertion time left to the writer.
fn rows_from_json(json: &str, schema: &SchemaRef) -> WriteResult<Vec<RecordBatch>> {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let ty = match f.data_type() {
                DataType::Timestamp(_, tz) => DataType::Timestamp(TimeUnit::Millisecond, tz.clone()),
                t => t.clone(),
            };
            Field::new(f.name(), ty, true)
        })
        .collect();
    let unfit = |e: arrow_schema::ArrowError| WriteError::invalid(format!("rows don't fit the table: {e}"));
    // Strict: a column the table doesn't have is an error, not silently dropped.
    let mut decoder = arrow_json::ReaderBuilder::new(Arc::new(Schema::new(fields)))
        .with_strict_mode(true)
        .with_batch_size(JSON_BATCH_ROWS)
        .build_decoder()
        .map_err(unfit)?;
    let (mut rest, mut out) = (json.as_bytes(), vec![]);
    loop {
        let read = decoder.decode(rest).map_err(unfit)?;
        rest = &rest[read..];
        if let Some(batch) = decoder.flush().map_err(unfit)? {
            out.push(batch);
        }
        if read == 0 {
            return Ok(out);
        }
    }
}

/// Rows from an Arrow IPC stream.
fn rows_from_ipc(ipc: &[u8]) -> WriteResult<Vec<RecordBatch>> {
    let reader = StreamReader::try_new(Cursor::new(ipc), None)?;
    reader.map(|b| b.map_err(WriteError::from)).collect()
}
