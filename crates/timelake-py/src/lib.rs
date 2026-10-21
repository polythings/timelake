//! Python bindings: `timelake.Lake` and `timelake.Table`, Arrow in and out.
//!
//! Every call that does I/O runs on one shared Tokio runtime with the GIL released.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use arrow_array::ffi_stream::ArrowArrayStreamReader;
use arrow_pyarrow::{FromPyArrow, IntoPyArrow};
use pyo3::create_exception;
use pyo3::exceptions::{PyFileExistsError, PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyMapping};
use timelake::arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use timelake::arrow::datatypes::Schema;
use timelake::arrow::error::ArrowError;
use timelake::datafusion::prelude::{DataFrame, SessionConfig};
use timelake::{AppendOptions, Chunk, Error, MaintainSteps, Session, TableOptions, UpgradeOptions};

create_exception!(
    timelake,
    ConflictError,
    PyRuntimeError,
    "Another writer committed first and the operation could not be rebased onto its commit."
);

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("failed to start the Tokio runtime")
    })
}

/// Runs a future on the shared runtime with the GIL released.
fn run<T: Send>(py: Python<'_>, future: impl Future<Output = timelake::Result<T>> + Send) -> PyResult<T> {
    py.detach(|| runtime().block_on(future)).map_err(to_py_err)
}

/// Maps engine errors to the closest built-in Python exception.
fn to_py_err(err: Error) -> PyErr {
    match err {
        Error::NotFound(msg) => PyKeyError::new_err(msg),
        Error::AlreadyExists(msg) => PyFileExistsError::new_err(msg),
        Error::Conflict(msg) => ConflictError::new_err(msg),
        Error::Invalid(msg) | Error::Config(msg) => PyValueError::new_err(msg),
        Error::Format(err) => PyValueError::new_err(err.to_string()),
        Error::Arrow(err) => arrow_err(err),
        err => PyRuntimeError::new_err(err.to_string()),
    }
}

/// Bad input data (a cast that can't be done, a schema that doesn't fit) is a `ValueError`.
fn arrow_err(err: ArrowError) -> PyErr {
    match err {
        ArrowError::CastError(_)
        | ArrowError::SchemaError(_)
        | ArrowError::InvalidArgumentError(_)
        | ArrowError::ParseError(_)
        | ArrowError::ComputeError(_) => PyValueError::new_err(err.to_string()),
        err => PyRuntimeError::new_err(err.to_string()),
    }
}

/// Locks a handle's state. Poisoning is harmless here: the state is only ever replaced whole.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Reads any Arrow-compatible object (pyarrow Table/RecordBatch/RecordBatchReader, polars,
/// anything with `__arrow_c_stream__`) into record batches.
fn read_batches(data: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    if let Ok(batch) = RecordBatch::from_pyarrow_bound(data) {
        return Ok(vec![batch]);
    }
    ArrowArrayStreamReader::from_pyarrow_bound(data)?.collect::<Result<_, _>>().map_err(arrow_err)
}

/// A `dict`, or any other `collections.abc.Mapping`, with string keys; `None` is empty.
fn mapping<T>(map: Option<&Bound<'_, PyMapping>>) -> PyResult<HashMap<String, T>>
where
    for<'a, 'py> (String, T): FromPyObject<'a, 'py, Error = PyErr>,
{
    let Some(map) = map else { return Ok(HashMap::new()) };
    map.items()?.iter().map(|item| item.extract()).collect()
}

/// Reads a pyarrow schema, or the `.schema` of an object that has one, such as a pyarrow Table.
fn read_schema(schema: &Bound<'_, PyAny>) -> PyResult<Schema> {
    Schema::from_pyarrow_bound(schema).or_else(|_| Schema::from_pyarrow_bound(&schema.getattr("schema")?))
}

/// Runs a query and returns its result as a `pyarrow.Table`.
fn collect(py: Python<'_>, query: impl Future<Output = timelake::Result<DataFrame>> + Send) -> PyResult<Py<PyAny>> {
    let (schema, batches) = run(py, async move {
        let df = query.await?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        Ok((schema, df.collect().await?))
    })?;
    let reader: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(batches.into_iter().map(Ok), schema));
    Ok(reader.into_pyarrow(py)?.call_method0("read_all")?.unbind())
}

/// A warehouse of Timelake tables on any object store or local disk.
#[pyclass(frozen, module = "timelake")]
struct Lake {
    inner: timelake::Lake,
    /// Session configuration with the lake's `settings`, for every namespace's session.
    config: SessionConfig,
    /// One query session per namespace, so catalog caches survive between `sql` calls.
    sessions: Mutex<HashMap<String, Session>>,
}

impl Lake {
    fn session(&self, namespace: &str) -> Session {
        lock(&self.sessions)
            .entry(namespace.to_owned())
            .or_insert_with(|| {
                let config = self.config.clone().with_default_catalog_and_schema("timelake", namespace);
                Session::with_config(self.inner.clone(), config)
            })
            .clone()
    }
}

#[pymethods]
impl Lake {
    #[new]
    #[pyo3(signature = (location, storage_options=None, settings=None))]
    fn new(
        location: &str,
        storage_options: Option<&Bound<'_, PyMapping>>,
        settings: Option<&Bound<'_, PyMapping>>,
    ) -> PyResult<Self> {
        let mut config = Session::config();
        for (key, value) in mapping::<String>(settings)? {
            config
                .options_mut()
                .set(&timelake::session_key(&key), &value)
                .map_err(|err| PyValueError::new_err(err.to_string()))?;
        }
        // Opening an object store may spawn tasks, so it needs a runtime context.
        let _guard = runtime().enter();
        let inner = timelake::Lake::open(location, mapping(storage_options)?).map_err(to_py_err)?;
        Ok(Self { inner, config, sessions: Mutex::default() })
    }

    /// The warehouse URL.
    #[getter]
    fn location(&self) -> &str {
        self.inner.location()
    }

    /// Creates a table from a pyarrow schema (or anything with `.schema`).
    #[pyo3(signature = (
        name, schema, time_column=None, series_key=None, chunk=None, retention=None, dedup=None, ingest_time=None,
        properties=None
    ))]
    #[expect(clippy::too_many_arguments, reason = "mirrors the Python keyword arguments")]
    fn create_table(
        &self,
        py: Python<'_>,
        name: String,
        schema: &Bound<'_, PyAny>,
        time_column: Option<String>,
        series_key: Option<Vec<String>>,
        chunk: Option<&str>,
        retention: Option<&str>,
        dedup: Option<bool>,
        ingest_time: Option<bool>,
        properties: Option<&Bound<'_, PyMapping>>,
    ) -> PyResult<Table> {
        let schema = read_schema(schema)?;
        let options = TableOptions {
            time_column,
            series_key: series_key.unwrap_or_default(),
            chunk: chunk.map(str::parse::<Chunk>).transpose().map_err(|err| to_py_err(err.into()))?,
            dedup,
            ingest_time,
            retention: retention.map(timelake::parse_duration).transpose().map_err(|err| to_py_err(err.into()))?,
            properties: mapping(properties)?,
        };
        let lake = self.inner.clone();
        let table = run(py, async move { lake.create_table(&name, &schema, options).await })?;
        Ok(Table::new(table))
    }

    /// Opens an existing table at its latest version.
    fn table(&self, py: Python<'_>, name: String) -> PyResult<Table> {
        let lake = self.inner.clone();
        let table = run(py, async move { lake.table(&name).await })?;
        Ok(Table::new(table))
    }

    /// Names of all tables as "namespace.table".
    fn tables(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let lake = self.inner.clone();
        let names = run(py, async move { lake.all_tables().await })?;
        Ok(names.iter().map(ToString::to_string).collect())
    }

    /// Deletes a table and its files.
    fn drop_table(&self, py: Python<'_>, name: String) -> PyResult<()> {
        let lake = self.inner.clone();
        run(py, async move { lake.drop_table(&name).await })
    }

    /// Runs SQL and returns a pyarrow Table. Unqualified names resolve in `namespace`.
    #[pyo3(signature = (query, namespace="default"))]
    fn sql(&self, py: Python<'_>, query: String, namespace: &str) -> PyResult<Py<PyAny>> {
        let session = self.session(namespace);
        collect(py, async move { session.sql(&query).await })
    }

    /// What the block cache did since the lake was opened, or None with the cache off.
    fn cache_stats(&self) -> Option<CacheSummary> {
        self.inner.storage().cache_stats().map(|s| CacheSummary {
            memory_hits: s.memory_hits,
            disk_hits: s.disk_hits,
            misses: s.misses,
            bytes_fetched: s.bytes_fetched,
        })
    }

    /// The newest row of every series of a table, as a pyarrow Table.
    fn latest(&self, py: Python<'_>, table: String) -> PyResult<Py<PyAny>> {
        let session = self.session(timelake::DEFAULT_NAMESPACE);
        collect(py, async move { session.latest(&table).await })
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        // Python's quoting of the location, as `repr` of a str gives it.
        let location = pyo3::types::PyString::new(py, self.inner.location()).repr()?;
        Ok(format!("Lake({location})"))
    }
}

/// A Timelake table. Methods that commit (append, maintain) advance this handle to the new version.
#[pyclass(frozen, module = "timelake")]
struct Table {
    inner: Mutex<timelake::Table>,
}

impl Table {
    fn new(table: timelake::Table) -> Self {
        Self { inner: Mutex::new(table) }
    }

    /// A snapshot of the handle, so the lock isn't held while a call runs.
    fn get(&self) -> timelake::Table {
        lock(&self.inner).clone()
    }

    /// Advances the handle to `table`, unless it is already on a newer version: calls on one
    /// handle from several threads can finish in any order.
    fn set(&self, table: timelake::Table) {
        let mut inner = lock(&self.inner);
        if inner.version() <= table.version() {
            *inner = table;
        }
    }
}

#[pymethods]
impl Table {
    /// The full name, "namespace.table".
    #[getter]
    fn name(&self) -> String {
        lock(&self.inner).name().to_string()
    }

    /// The table's root URL.
    #[getter]
    fn location(&self) -> String {
        lock(&self.inner).location().to_string()
    }

    /// The version this handle is at.
    #[getter]
    fn version(&self) -> u64 {
        lock(&self.inner).version()
    }

    /// The table's pyarrow schema.
    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let schema = lock(&self.inner).schema();
        Ok(Arc::unwrap_or_clone(schema).into_pyarrow(py)?.unbind())
    }

    /// Appends Arrow data as one commit and returns commit statistics.
    #[pyo3(signature = (data, batch_id=None, maintain=None))]
    fn append<'py>(
        &self,
        py: Python<'py>,
        data: &Bound<'py, PyAny>,
        batch_id: Option<String>,
        maintain: Option<bool>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let batches = read_batches(data)?;
        let table = self.get();
        let options = AppendOptions { batch_id, maintain, ..AppendOptions::default() };
        let (table, result) = run(py, async move { table.append(&batches, options).await })?;
        let summary = AppendSummary {
            rows: result.rows,
            files: result.files,
            bytes: result.bytes,
            version: table.version(),
            commit_ms: result.commit.duration.as_secs_f64() * 1000.0,
            attempts: result.commit.attempts,
            duplicate: result.commit.duplicate,
        };
        self.set(table);
        let dict = summary.into_pyobject(py)?;
        // `maintenance` is only present when inline maintenance ran. A failed one doesn't undo the committed append:
        // report it instead of raising.
        match &result.maintenance {
            Some(Ok(report)) => dict.set_item("maintenance", report_dict(py, report)?)?,
            Some(Err(err)) => dict.set_item("maintenance_error", err.to_string())?,
            None => {}
        }
        Ok(dict)
    }

    /// Compaction (sort + dedup), retention, rollup refresh and snapshot expiry. Returns what was
    /// done.
    #[pyo3(signature = (compact=true, retention=true, expire=true, rollups=true))]
    #[expect(clippy::fn_params_excessive_bools, reason = "Python keyword arguments, each a step to skip")]
    fn maintain<'py>(
        &self,
        py: Python<'py>,
        compact: bool,
        retention: bool,
        expire: bool,
        rollups: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let table = self.get();
        let options = MaintainSteps { compact, retention, rollups, expire }.into();
        let (table, report) = run(py, async move { table.maintain(options).await })?;
        self.set(table);
        report_dict(py, &report)
    }

    /// Brings the table to the current format; returns the steps and how to undo them.
    #[pyo3(signature = (dry_run=false, compact=true, keep_snapshots="7d"))]
    fn upgrade(&self, py: Python<'_>, dry_run: bool, compact: bool, keep_snapshots: &str) -> PyResult<UpgradeSummary> {
        let keep_snapshots = timelake::parse_duration(keep_snapshots).map_err(|err| to_py_err(err.into()))?;
        let options = UpgradeOptions { dry_run, compact, keep_snapshots };
        let table = self.get();
        let (table, report) = run(py, async move { table.upgrade(&options).await })?;
        self.set(table);
        Ok(UpgradeSummary {
            steps: report.steps.iter().map(ToString::to_string).collect(),
            version_before: report.version_before,
            snapshot_before: report.snapshot_before,
            keep_snapshots_until_ms: report.keep_snapshots_until_ms,
        })
    }

    /// Sets table settings and properties; `None` removes one (back to its default).
    fn set_properties(&self, py: Python<'_>, properties: &Bound<'_, PyMapping>) -> PyResult<()> {
        let properties = mapping::<Option<String>>(Some(properties))?;
        let table = self.get();
        let table = run(py, async move {
            let changes: Vec<(&str, Option<&str>)> =
                properties.iter().map(|(k, v)| (k.as_str(), v.as_deref())).collect();
            table.set_properties(&changes).await
        })?;
        self.set(table);
        Ok(())
    }

    /// Makes an earlier snapshot current again.
    fn rollback(&self, py: Python<'_>, snapshot_id: i64) -> PyResult<()> {
        let table = self.get();
        let table = run(py, async move { table.rollback(snapshot_id).await })?;
        self.set(table);
        Ok(())
    }

    /// Reloads the latest committed version.
    fn refresh(&self, py: Python<'_>) -> PyResult<()> {
        let table = self.get();
        let table = run(py, async move { table.refresh().await })?;
        self.set(table);
        Ok(())
    }

    /// Contract, size and layout of the current version, as `timelake info --json` gives them.
    fn info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let table = self.get();
        let info = run(py, async move { table.info().await })?;
        Ok(pythonize::pythonize(py, &info)?)
    }

    fn __repr__(&self) -> String {
        let table = lock(&self.inner);
        format!("Table({}, v{})", table.name(), table.version())
    }
}

/// `Table.append` result (a dict in Python; keys are part of the documented API).
#[derive(IntoPyObject)]
struct AppendSummary {
    rows: usize,
    files: usize,
    bytes: u64,
    version: u64,
    commit_ms: f64,
    attempts: usize,
    duplicate: bool,
}

/// `Table.maintain` result, also `maintenance` in an append result: the report's fields, as
/// `timelake_format` serializes them (`duration` as `seconds`).
fn report_dict<'py>(py: Python<'py>, report: &timelake::MaintainReport) -> PyResult<Bound<'py, PyAny>> {
    Ok(pythonize::pythonize(py, report)?)
}

/// `Lake.cache_stats` result.
#[derive(IntoPyObject)]
struct CacheSummary {
    memory_hits: u64,
    disk_hits: u64,
    misses: u64,
    bytes_fetched: u64,
}

/// `Table.upgrade` result.
#[derive(IntoPyObject)]
struct UpgradeSummary {
    steps: Vec<String>,
    version_before: u64,
    snapshot_before: Option<i64>,
    keep_snapshots_until_ms: Option<i64>,
}

/// Waits for background writes (version hints) to land; registered with `atexit`.
#[pyfunction]
fn flush_background_writes(py: Python<'_>) {
    py.detach(|| runtime().block_on(timelake::flush()));
}

#[pymodule]
fn _timelake(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Lake>()?;
    m.add_class::<Table>()?;
    m.add("ConflictError", m.py().get_type::<ConflictError>())?;
    let flush = wrap_pyfunction!(flush_background_writes, m)?;
    m.py().import("atexit")?.call_method1("register", (flush,))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    /// The stub's default for `Table.upgrade(keep_snapshots=…)` is the engine's. Python signatures need a literal, so
    /// the module repeats it; `tests/test_package.py` checks the module against the stub.
    #[test]
    fn upgrade_keeps_snapshots_as_long_as_the_engine() {
        let stub = include_str!("../python/timelake/_timelake.pyi");
        let default = timelake::format_duration(timelake::DEFAULT_KEEP_SNAPSHOTS);
        assert!(stub.contains(&format!("keep_snapshots: str = \"{default}\"")), "the stub's default isn't {default}");
    }
}
