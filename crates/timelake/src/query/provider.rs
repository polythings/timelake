//! A Timelake table as a DataFusion table: pruned Parquet scans, or the last-value index.

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session as DfSession, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{DFSchema, ScalarValue, Statistics};
use datafusion::common::{JoinType, NullEquality};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::parquet::metadata::{CachedParquetMetaData, DFParquetMetadata};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::cache::cache_manager::CachedFileMetadataEntry;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::union::UnionExec;

use super::dedup::{DedupExec, Keep, RANK, overlapping_groups, series_values};
use super::in_list::{InList, access_plan, long_in_lists};
use super::settings::SessionSettings;
use super::stats::{FileStats, file_statistics};
use crate::property;
use crate::sort::ASCENDING;
use crate::summary;
use crate::table::{LiveEntry, Table};

/// What a [`TimelakeProvider`] reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    /// The table's data files.
    Data,
    /// The last-value index: the newest row of every series.
    Latest,
}

/// A Timelake table at the version loaded during planning.
#[derive(Clone)]
pub(crate) struct TimelakeProvider {
    table: Table,
    kind: Kind,
    /// Rollup routing has dealt with this scan (SPEC §11.1).
    routed: bool,
}

impl TimelakeProvider {
    pub(super) fn data(table: Table) -> Self {
        Self { table, kind: Kind::Data, routed: false }
    }

    pub(super) fn latest(table: Table) -> Self {
        Self { table, kind: Kind::Latest, routed: false }
    }

    /// This scan, marked as dealt with by rollup routing.
    pub(super) fn routed(&self) -> Self {
        Self { routed: true, ..self.clone() }
    }

    pub(super) fn is_routed(&self) -> bool {
        self.routed
    }

    pub(super) fn table(&self) -> &Table {
        &self.table
    }

    pub(super) fn kind(&self) -> Kind {
        self.kind
    }

    /// Plans the scan of `table`, the version this provider was planned with or a newer one with
    /// the same schema.
    async fn scan_version(
        &self,
        table: &Table,
        state: &dyn DfSession,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        match self.kind {
            Kind::Latest => scan_latest(table, projection).await,
            Kind::Data => scan_data(table, state, projection, filters, limit).await,
        }
    }
}

async fn scan_latest(table: &Table, projection: Option<&Vec<usize>>) -> DfResult<Arc<dyn ExecutionPlan>> {
    let uri = table.last_value_index().ok_or_else(|| DataFusionError::Plan("no last-value index".into()))?;
    let batch = crate::lvi::read(table, &uri).await?;
    Ok(MemorySourceConfig::try_new_exec(&[vec![batch]], table.schema(), projection.cloned())?)
}

async fn scan_data(
    table: &Table,
    state: &dyn DfSession,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let schema = table.schema();
    let (files, groups, key_filters) = select_files(table, state, filters).await?;
    let storage = table.lake().storage();
    let url = ObjectStoreUrl::parse(storage.url())?;
    let mut footers: Vec<PartitionedFile> = vec![];
    for f in files.iter().chain(groups.iter().flatten()) {
        footers.push(PartitionedFile::new(
            storage.path(f.data_file.file_path())?.to_string(),
            f.data_file.file_size_in_bytes(),
        ));
    }
    prefetch_footers(table, state, &url, &footers).await?;

    let mut plans = vec![];
    if !files.is_empty() {
        plans.push(scan_files(table, state, &url, files, projection, filters, limit).await?);
    }
    // Keys never repeat across groups (different partitions or key ranges), so one scan serves all.
    let merged: Vec<LiveEntry> = groups.into_iter().flatten().collect();
    if !merged.is_empty() {
        plans.push(match strategy(table, state)? {
            Strategy::Merge => scan_deduplicated(table, state, &url, merged, projection, &key_filters).await?,
            Strategy::KeysFirst => {
                scan_keys_first(table, state, &url, merged, projection, filters, &key_filters).await?
            }
        });
    }
    match plans.len() {
        0 => {
            let projected = match projection {
                Some(p) => Arc::new(schema.project(p)?),
                None => schema.clone(),
            };
            Ok(Arc::new(EmptyExec::new(projected)))
        }
        1 => Ok(plans.swap_remove(0)),
        _ => UnionExec::try_new(plans),
    }
}

/// The files a scan reads: those that can match `filters` and share no key with another file, and
/// the groups of files that may share keys (dedup on), read together and deduplicated; and the
/// filters on key columns only, which can apply before dedup.
///
/// Rows are deduplicated before the filters apply, so files that may share keys are only pruned
/// by filters on the key columns: an older copy of a row must not outlive a newer one that a
/// filter on another column would have pruned.
async fn select_files(
    table: &Table,
    state: &dyn DfSession,
    filters: &[Expr],
) -> DfResult<(Vec<LiveEntry>, Vec<Vec<LiveEntry>>, Vec<Expr>)> {
    let files = table.files().await?;
    let contract = table.contract();
    let keys = contract.sort_columns();
    let session =
        state.config().options().extensions.get::<SessionSettings>().and_then(|s| s.flag(property::READ_DEDUP));
    let read_dedup = contract.dedup && session.unwrap_or(contract.read_dedup);
    if !read_dedup || keys.is_empty() {
        let keep = prune(table, state, &files, filters)?;
        return Ok((files.into_iter().zip(keep).filter_map(|(f, k)| k.then_some(f)).collect(), vec![], vec![]));
    }
    let on_keys: Vec<Expr> =
        filters.iter().filter(|e| e.column_refs().iter().all(|c| keys.contains(&c.name))).cloned().collect();
    let keep = prune(table, state, &files, &on_keys)?;
    let files: Vec<LiveEntry> = files.into_iter().zip(keep).filter_map(|(f, k)| k.then_some(f)).collect();
    let schema = table.schema();
    let key_indices = keys.iter().map(|k| schema.index_of(k)).collect::<Result<Vec<_>, _>>()?;
    let mut overlapping = overlapping_groups(&schema, &key_indices, &files);
    let mut grouped = vec![false; files.len()];
    for &i in overlapping.iter().flatten() {
        grouped[i] = true;
    }
    // A file another engine wrote may be unsorted and repeat keys within itself.
    let sort_order = table_sort_order(table);
    let unsorted = (0..files.len()).filter(|&i| !grouped[i] && files[i].data_file.sort_order_id() != sort_order);
    let unsorted: Vec<usize> = unsorted.collect();
    for &i in &unsorted {
        grouped[i] = true;
        overlapping.push(vec![i]);
    }
    let keep = prune(table, state, &files, filters)?;
    let mut groups = vec![];
    for members in overlapping {
        if members.iter().any(|&i| keep[i]) {
            groups.push(members.iter().map(|&i| files[i].clone()).collect());
        }
    }
    let singles = files.into_iter().enumerate().filter_map(|(i, f)| (keep[i] && !grouped[i]).then_some(f)).collect();
    Ok((singles, groups, on_keys))
}

/// Scans files that share no key with each other: a plain pruned Parquet scan.
async fn scan_files(
    table: &Table,
    state: &dyn DfSession,
    url: &ObjectStoreUrl,
    mut files: Vec<LiveEntry>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let schema = table.schema();
    let storage = table.lake().storage();
    let target = state.config().target_partitions().max(1);
    let n_groups = files.len().min(target);
    let mut groups: Vec<Vec<PartitionedFile>> = vec![vec![]; n_groups];
    let lists = in_lists(table, state, filters);
    // Biggest files first, round-robin: roughly balanced groups.
    files.sort_by_key(|f| std::cmp::Reverse(f.data_file.file_size_in_bytes()));
    let mut total_rows = 0u64;
    for (i, f) in files.iter().enumerate() {
        total_rows += f.data_file.record_count();
        let path = storage.path(f.data_file.file_path())?;
        let pf = PartitionedFile::new(path.to_string(), f.data_file.file_size_in_bytes())
            .with_statistics(Arc::new(file_statistics(&schema, f)));
        groups[i % n_groups].push(with_access_plan(state, &schema, &lists, pf));
    }
    let one_file_per_group = groups.iter().all(|g| g.len() == 1);

    let source = Arc::new(ParquetSource::new(schema.clone()));
    let mut stats = Statistics::new_unknown(&schema);
    stats.num_rows = Precision::Exact(usize::try_from(total_rows).unwrap_or(usize::MAX));
    let mut builder = FileScanConfigBuilder::new(url.clone(), source)
        .with_file_groups(groups.into_iter().map(FileGroup::new).collect())
        .with_statistics(stats)
        .with_projection_indices(projection.cloned())?
        .with_limit(if filters.is_empty() { limit } else { None });
    // Every file is sorted by (series…, time). Declaring it lets DataFusion skip sorts, but it also
    // keeps one partition per file (no byte-range splits), which costs more than it saves when a
    // few large compacted files hold the data. Only declare it when there are enough files.
    let sort_columns = table.contract().sort_columns();
    if n_groups >= target
        && one_file_per_group
        && let Some(ordering) = key_ordering(&schema, &sort_columns)?
    {
        builder = builder.with_output_ordering(vec![ordering]);
    }
    let mut options = state.table_options().parquet.clone();
    let pushdown = filter_pushdown(table, state)?;
    options.global.pushdown_filters = pushdown;
    options.global.reorder_filters = pushdown;
    let format = ParquetFormat::default().with_options(options);
    format.create_physical_plan(state, builder.build()).await
}

/// The long `IN` lists among `filters` that the scan prunes with itself, unless the session's
/// `timelake.in_list_pruning` (else the table's `timelake.in-list-pruning`) turns that off.
fn in_lists(table: &Table, state: &dyn DfSession, filters: &[Expr]) -> Vec<InList> {
    let settings = state.config().options().extensions.get::<SessionSettings>();
    let on = settings.and_then(|s| s.flag(property::IN_LIST_PRUNING)).unwrap_or(table.contract().in_list_pruning);
    if on { long_in_lists(filters) } else { vec![] }
}

/// `file` with the row groups and pages `lists` leave to read as its access plan, from its footer
/// in DataFusion's metadata cache (where the scan's footers were put before planning).
fn with_access_plan(
    state: &dyn DfSession,
    schema: &Schema,
    lists: &[InList],
    file: PartitionedFile,
) -> PartitionedFile {
    if lists.is_empty() {
        return file;
    }
    let cache = state.runtime_env().cache_manager.get_file_metadata_cache();
    let Some(entry) = cache.get(&file.object_meta.location) else { return file };
    let Some(metadata) = entry.file_metadata.as_any().downcast_ref::<CachedParquetMetaData>() else { return file };
    match access_plan(metadata.parquet_metadata(), schema, lists) {
        Some(plan) => file.with_extension(plan),
        None => file,
    }
}

/// The session's `timelake.filter_pushdown`, else the table's `timelake.filter-pushdown`; `auto`
/// is on for local files and off on object storage.
fn filter_pushdown(table: &Table, state: &dyn DfSession) -> DfResult<bool> {
    let settings = state.config().options().extensions.get::<SessionSettings>();
    let session = settings.and_then(|s| s.get(property::FILTER_PUSHDOWN));
    match session.unwrap_or(&table.contract().filter_pushdown).to_ascii_lowercase().as_str() {
        "auto" => Ok(!table.lake().storage().is_remote()),
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(DataFusionError::Plan(format!("unknown filter pushdown {other:?}: auto, true or false"))),
    }
}

/// Scans files that may share keys, strategy merge: each file's rows with its commit rank, split
/// by a hash of the series so every copy of a key lands in the same slice, each slice merged by
/// `(series…, time, rank)` on its own, and the newest write of each key kept. `key_filters` (on
/// key columns only) remove rows before the merge, and prune row groups and pages.
async fn scan_deduplicated(
    table: &Table,
    state: &dyn DfSession,
    url: &ObjectStoreUrl,
    files: Vec<LiveEntry>,
    projection: Option<&Vec<usize>>,
    key_filters: &[Expr],
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let schema = table.schema();
    let keys = table.contract().sort_columns();
    let ranked = Ranked::new(table, files)?;
    let order = table.contract().write_order();
    let (requested, read) = read_columns(&schema, projection, &order)?;
    let plan = ranked.scan(table, state, url, read, key_filters, true).await?;
    let read_schema = plan.schema();
    let key_positions = keys.iter().map(|k| read_schema.index_of(k)).collect::<Result<Vec<_>, _>>()?;
    let ordering = rank_ordering(&read_schema, &order)?;
    let slices = state.config().target_partitions();
    let series = &keys[..keys.len().saturating_sub(usize::from(table.contract().time_column.is_some()))];
    let by_series = series_values(key_filters, series).is_none_or(|n| n >= slices);
    let plan = Arc::new(DedupExec::try_new(plan, key_positions, ordering, slices, by_series, Keep::Latest)?);
    project(plan, requested.len())
}

/// Scans files that may share keys, strategy keys-first. Pass 1 reads only the key columns and
/// the rank, merges them like [`scan_deduplicated`] and lists the older copies. Pass 2 reads the
/// files like any scan, every filter pruning and removing rows, and leaves out the rows on that
/// list: the list was built from all rows, so filters on other columns are safe before it.
async fn scan_keys_first(
    table: &Table,
    state: &dyn DfSession,
    url: &ObjectStoreUrl,
    files: Vec<LiveEntry>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    key_filters: &[Expr],
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let schema = table.schema();
    let keys = table.contract().sort_columns();
    let ranked = Ranked::new(table, files)?;
    let order = table.contract().write_order();
    let key_read: Vec<usize> =
        order.iter().map(|k| schema.index_of(k)).chain([Ok(schema.fields().len())]).collect::<Result<_, _>>()?;
    let pass1 = ranked.scan(table, state, url, key_read, key_filters, true).await?;
    let pass1_schema = pass1.schema();
    let ordering = rank_ordering(&pass1_schema, &order)?;
    let key_positions: Vec<usize> = (0..keys.len()).collect();
    let slices = state.config().target_partitions();
    let older: Arc<dyn ExecutionPlan> =
        Arc::new(DedupExec::try_new(pass1, key_positions, ordering, slices, false, Keep::Older)?);

    let (requested, read) = read_columns(&schema, projection, &keys)?;
    let pass2 = ranked.scan(table, state, url, read, filters, false).await?;
    let pass2_schema = pass2.schema();
    let on = keys.iter().map(String::as_str).chain([RANK]).map(|c| {
        let left = Arc::new(Column::new(c, pass1_schema.index_of(c)?)) as Arc<dyn PhysicalExpr>;
        let right = Arc::new(Column::new(c, pass2_schema.index_of(c)?)) as Arc<dyn PhysicalExpr>;
        Ok((left, right))
    });
    let join = HashJoinExec::try_new(
        older,
        pass2,
        on.collect::<DfResult<_>>()?,
        None,
        &JoinType::RightAnti,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNull,
        false,
    )?;
    project(Arc::new(join), requested.len())
}

/// The requested columns (all without a projection), and those plus the key columns and the rank
/// (at the index after the table's columns): what a deduplicating scan reads.
fn read_columns(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    keys: &[String],
) -> DfResult<(Vec<usize>, Vec<usize>)> {
    let requested: Vec<usize> = projection.cloned().unwrap_or_else(|| (0..schema.fields().len()).collect());
    let mut read = requested.clone();
    for k in keys {
        let i = schema.index_of(k)?;
        if !read.contains(&i) {
            read.push(i);
        }
    }
    read.push(schema.fields().len());
    Ok((requested, read))
}

/// The first `n` columns of `plan`.
fn project(plan: Arc<dyn ExecutionPlan>, n: usize) -> DfResult<Arc<dyn ExecutionPlan>> {
    let schema = plan.schema();
    let output = (0..n).map(|i| {
        let name = schema.field(i).name();
        (Arc::new(Column::new(name, i)) as Arc<dyn PhysicalExpr>, name.clone())
    });
    Ok(Arc::new(ProjectionExec::try_new(output, plan)?))
}

/// `(series…, time, insertion time, rank)` ascending (`order` is the write order), over `schema`.
fn rank_ordering(schema: &SchemaRef, order: &[String]) -> DfResult<LexOrdering> {
    let ranked: Vec<String> = order.iter().cloned().chain([RANK.to_string()]).collect();
    key_ordering(schema, &ranked)?.ok_or_else(|| DataFusionError::Internal("dedup scan without a key".into()))
}

/// Files that may share keys, older commits first, each its own scan group with its commit rank
/// as a constant column (the last of equal keys is then the newest write).
struct Ranked {
    groups: Vec<FileGroup>,
    rows: u64,
    /// Whether every file is in the table's sort order.
    sorted: bool,
}

impl Ranked {
    fn new(table: &Table, mut files: Vec<LiveEntry>) -> DfResult<Self> {
        let storage = table.lake().storage();
        // Older commits first, as compaction orders them.
        files.sort_by(|a, b| {
            (a.sequence_number, a.data_file.file_path()).cmp(&(b.sequence_number, b.data_file.file_path()))
        });
        let sorted = files.iter().all(|f| f.data_file.sort_order_id() == table_sort_order(table));
        let (mut groups, mut rows) = (vec![], 0u64);
        for (rank, f) in files.iter().enumerate() {
            rows += f.data_file.record_count();
            let rank = u32::try_from(rank).map_err(|e| DataFusionError::External(Box::new(e)))?;
            let path = storage.path(f.data_file.file_path())?;
            let pf = PartitionedFile::new(path.to_string(), f.data_file.file_size_in_bytes())
                .with_partition_values(vec![ScalarValue::UInt32(Some(rank))]);
            groups.push(FileGroup::new(vec![pf]));
        }
        Ok(Self { groups, rows, sorted })
    }

    /// A Parquet scan of the files reading `read` (table column indices, the rank after them),
    /// whose rows `filters` remove (and prune row groups and pages); `ordered`: each file's rows
    /// come sorted by `(series…, time, rank)`, sorting them if the files aren't.
    async fn scan(
        &self,
        table: &Table,
        state: &dyn DfSession,
        url: &ObjectStoreUrl,
        read: Vec<usize>,
        filters: &[Expr],
        ordered: bool,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let schema = table.schema();
        // A Timelake file has each key once, so sorted by the key it is also sorted by the key and
        // the insertion time; with the rank (constant per file) that is the merge order.
        let order = table.contract().write_order();
        let rank_field = Arc::new(Field::new(RANK, DataType::UInt32, false));
        let table_schema = TableSchema::new(schema.clone(), vec![rank_field]);
        let with_rank = table_schema.table_schema().clone();
        let mut source = ParquetSource::new(table_schema);
        if let Some(predicate) = conjunction(filters.iter().cloned()) {
            let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
            source = source.with_predicate(state.create_physical_expr(predicate, &df_schema)?);
        }
        let mut stats = Statistics::new_unknown(&with_rank);
        stats.num_rows = Precision::Inexact(usize::try_from(self.rows).unwrap_or(usize::MAX));
        let lists = in_lists(table, state, filters);
        let groups = self
            .groups
            .iter()
            .map(|g| FileGroup::new(g.iter().map(|pf| with_access_plan(state, &schema, &lists, pf.clone())).collect()));
        let mut builder = FileScanConfigBuilder::new(url.clone(), Arc::new(source))
            .with_file_groups(groups.collect())
            .with_statistics(stats)
            .with_projection_indices(Some(read))?;
        if ordered && self.sorted {
            builder = builder.with_output_ordering(vec![rank_ordering(&with_rank, &order)?]);
        }
        // The filters remove rows, not only prune row groups: merging fewer rows saves more than
        // filtering them costs (a join's runtime filters on the key included). It's also required:
        // DataFusion takes a column a predicate fixes (`device = 4`) as constant and leaves it out
        // of the merge order.
        let mut options = state.table_options().parquet.clone();
        options.global.pushdown_filters = true;
        let format = ParquetFormat::default().with_options(options);
        let plan = format.create_physical_plan(state, builder.build()).await?;
        if ordered && !self.sorted {
            let ordering = rank_ordering(&plan.schema(), &order)?;
            return Ok(Arc::new(SortExec::new(ordering, plan).with_preserve_partitioning(true)));
        }
        Ok(plan)
    }
}

/// How copies are removed on read (SPEC §9.1): `merge` or `keys-first`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Strategy {
    Merge,
    KeysFirst,
}

/// The session's `timelake.read_dedup_strategy`, else the table's `timelake.read-dedup-strategy`.
fn strategy(table: &Table, state: &dyn DfSession) -> DfResult<Strategy> {
    let settings = state.config().options().extensions.get::<SessionSettings>();
    let session = settings.and_then(|s| s.get(property::READ_DEDUP_STRATEGY));
    match session.unwrap_or(&table.contract().read_dedup_strategy).to_ascii_lowercase().as_str() {
        "merge" => Ok(Strategy::Merge),
        "keys-first" | "keys_first" => Ok(Strategy::KeysFirst),
        other => Err(DataFusionError::Plan(format!("unknown read dedup strategy {other:?}: merge or keys-first"))),
    }
}

/// The table's sort order id, as data files written by Timelake carry it.
fn table_sort_order(table: &Table) -> Option<i32> {
    i32::try_from(table.metadata().default_sort_order_id()).ok()
}

/// Ascending order by `columns` of `schema` (`None` if `columns` is empty).
fn key_ordering(schema: &SchemaRef, columns: &[String]) -> DfResult<Option<LexOrdering>> {
    let exprs = columns
        .iter()
        .map(|c| Ok(PhysicalSortExpr::new(datafusion::physical_expr::expressions::col(c, schema)?, ASCENDING)))
        .collect::<DfResult<Vec<_>>>()?;
    Ok(LexOrdering::new(exprs))
}

/// Which of `files` can match `filters` on their Iceberg column bounds.
fn prune(table: &Table, state: &dyn DfSession, files: &[LiveEntry], filters: &[Expr]) -> DfResult<Vec<bool>> {
    let Some(predicate) = conjunction(filters.iter().cloned()) else { return Ok(vec![true; files.len()]) };
    let schema = table.schema();
    let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
    let predicate = PruningPredicate::try_new(state.create_physical_expr(predicate, &df_schema)?, schema.clone())?;
    predicate.prune(&FileStats::new(&schema, files))
}

/// Puts each file's footer (and page index) into DataFusion's metadata cache once: from the head
/// when it carried the footer (P2), otherwise read from the store, concurrently. Without this,
/// every scan partition fetches the footer of the file range it reads, which on object storage
/// means dozens of redundant ranged GETs per file.
async fn prefetch_footers(
    table: &Table,
    state: &dyn DfSession,
    url: &ObjectStoreUrl,
    files: &[PartitionedFile],
) -> DfResult<()> {
    let store = state.runtime_env().object_store(url)?;
    let cache = state.runtime_env().cache_manager.get_file_metadata_cache();
    let hint = state.table_options().parquet.global.metadata_size_hint;
    let (base, known) = (table.lake().storage().base_url(), &table.lake().caches().footers);
    let mut fetch = vec![];
    for pf in files {
        let location = &pf.object_meta.location;
        match known.get(&format!("{base}/{location}")) {
            Some(bytes) => {
                let metadata = Arc::new(CachedParquetMetaData::new(crate::footer::decode(&bytes)?));
                cache.put(location, CachedFileMetadataEntry::new(pf.object_meta.clone(), metadata));
            }
            None => fetch.push(pf),
        }
    }
    let fetches = fetch.into_iter().map(|pf| {
        let (store, cache) = (store.clone(), cache.clone());
        async move {
            DFParquetMetadata::new(store.as_ref(), &pf.object_meta)
                .with_metadata_size_hint(hint)
                .with_file_metadata_cache(Some(cache))
                .fetch_metadata()
                .await
        }
    });
    futures::future::try_join_all(fetches).await?;
    Ok(())
}

impl fmt::Debug for TimelakeProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TimelakeProvider({}, v{}, {:?})", self.table.name(), self.table.version(), self.kind)
    }
}

#[async_trait]
impl TableProvider for TimelakeProvider {
    fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn DfSession,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // A version loaded from the head is checked for a newer one while it is planned; an
        // outdated plan is replaced by one of the current version.
        let (stale, plan) =
            tokio::join!(self.table.is_stale(), self.scan_version(&self.table, state, projection, filters, limit));
        if !stale.map_err(|e| DataFusionError::External(Box::new(e)))? {
            return plan;
        }
        let current = self.table.lake().load(self.table.name()).await?;
        if current.schema() != self.table.schema() {
            let name = self.table.name();
            return Err(DataFusionError::Plan(format!("{name} changed while the query was planned; run it again")));
        }
        self.scan_version(&current, state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DfResult<Vec<TableProviderFilterPushDown>> {
        let support = match self.kind {
            Kind::Data => TableProviderFilterPushDown::Inexact,
            Kind::Latest => TableProviderFilterPushDown::Unsupported,
        };
        Ok(vec![support; filters.len()])
    }

    fn statistics(&self) -> Option<Statistics> {
        let rows = self.table.summary_value(summary::TOTAL_RECORDS)?.parse().ok()?;
        let mut s = Statistics::new_unknown(&self.table.schema());
        s.num_rows = Precision::Inexact(rows);
        Some(s)
    }
}
