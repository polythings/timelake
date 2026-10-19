//! A Timelake table as a DataFusion table: pruned Parquet scans, or the last-value index.

use std::collections::BTreeMap;
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
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::union::UnionExec;
use iceberg::spec::{Datum, PrimitiveLiteral};

use super::dedup::{DedupExec, Keep, RANK, overlapping_groups, series_values};
use super::in_list::{InList, SplitInLists, access_plan, long_in_lists};
use super::newest::{Newest, NewestFirstExec};
use super::series_order::Order;
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
    /// The data files for `ORDER BY time DESC LIMIT k`: time partitions newest first, stopping once
    /// older ones can't hold any of the `k` newest rows ([`super::newest`]).
    Newest(Newest),
}

/// A Timelake table at the version loaded during planning.
#[derive(Clone)]
pub(crate) struct TimelakeProvider {
    table: Table,
    kind: Kind,
    /// Rollup routing has dealt with this scan (spec 21).
    routed: bool,
    /// Under a signal aggregate: how each series' rows come in time order ([`super::series_order`]).
    series_order: Option<Order>,
}

impl TimelakeProvider {
    pub(super) fn data(table: Table) -> Self {
        Self { table, kind: Kind::Data, routed: false, series_order: None }
    }

    pub(super) fn latest(table: Table) -> Self {
        Self { table, kind: Kind::Latest, routed: false, series_order: None }
    }

    pub(super) fn newest(table: Table, newest: Newest) -> Self {
        Self { table, kind: Kind::Newest(newest), routed: false, series_order: None }
    }

    /// This scan, marked as dealt with by rollup routing.
    pub(super) fn routed(&self) -> Self {
        Self { routed: true, ..self.clone() }
    }

    /// This scan, reading each series in time order as `order` says.
    pub(super) fn in_series_order(&self, order: Order) -> Self {
        Self { series_order: Some(order), ..self.clone() }
    }

    pub(super) fn reads_in_series_order(&self) -> bool {
        self.series_order.is_some()
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
            Kind::Data => {
                let files = table.files().await?;
                scan_data(table, state, projection, filters, limit, files, self.series_order).await
            }
            Kind::Newest(newest) => scan_newest(table, state, projection, filters, newest).await,
        }
    }
}

async fn scan_latest(table: &Table, projection: Option<&Vec<usize>>) -> DfResult<Arc<dyn ExecutionPlan>> {
    let uri = table.last_value_index().ok_or_else(|| DataFusionError::Plan("no last-value index".into()))?;
    let batch = crate::lvi::read(table, &uri).await?;
    Ok(MemorySourceConfig::try_new_exec(&[vec![batch]], table.schema(), projection.cloned())?)
}

/// A scan of `files`, data files of `table`; with `series_order`, for a signal aggregate.
async fn scan_data(
    table: &Table,
    state: &dyn DfSession,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
    files: Vec<LiveEntry>,
    series_order: Option<Order>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let schema = table.schema();
    let (mut files, groups, key_filters) = select_files(table, state, filters, files)?;
    // Keys never repeat across groups (different partitions or key ranges), so one scan serves all.
    let mut merged: Vec<LiveEntry> = groups.into_iter().flatten().collect();
    // A signal aggregate needs each partition's rows of a series to be a stretch of its readings
    // that no other partition has readings inside. Files read as they are give that in time order,
    // before and after the time of the files merged (to remove copies, or because they overlap or
    // aren't sorted), which are merged with every file in that time.
    let (mut stretches, mut in_order) = (vec![], vec![]);
    let series_order = match series_order {
        Some(Order::MergeOnePartition) => {
            let mut partitions = files.iter().chain(&merged).map(|f| crate::write::file_partition(&f.data_file));
            let first = partitions.next();
            partitions.all(|p| Some(p) == first).then_some(Order::Merge)
        }
        other => other,
    };
    if let Some(order) = series_order {
        let dedup = !merged.is_empty();
        if order == Order::Merge {
            merged.append(&mut files);
        } else if !dedup {
            let merge = if order == Order::Auto { out_of_order(table, &files)? } else { vec![false; files.len()] };
            let (now, alone): (Vec<_>, Vec<_>) = files.into_iter().zip(merge).partition(|(_, m)| *m);
            (merged, files) = (now.into_iter().map(|(f, _)| f).collect(), alone.into_iter().map(|(f, _)| f).collect());
        }
        let (inside, before, after) = around(table, merged, files);
        (merged, files, stretches) = (inside, vec![], vec![before, after]);
        if !dedup {
            in_order = std::mem::take(&mut merged);
        }
    }
    let storage = table.lake().storage();
    let url = ObjectStoreUrl::parse(storage.url())?;
    let mut footers: Vec<PartitionedFile> = vec![];
    for f in files.iter().chain(stretches.iter().flatten()).chain(&merged).chain(&in_order) {
        footers.push(PartitionedFile::new(
            storage.path(f.data_file.file_path())?.to_string(),
            f.data_file.file_size_in_bytes(),
        ));
    }
    prefetch_footers(table, state, &url, &footers).await?;

    let mut plans = vec![];
    if !files.is_empty() {
        plans.push(scan_files(table, state, &url, files, projection, filters, limit, false).await?);
    }
    for stretch in stretches.into_iter().filter(|s| !s.is_empty()) {
        plans.push(scan_files(table, state, &url, stretch, projection, filters, limit, true).await?);
    }
    if !in_order.is_empty() {
        plans.push(scan_in_series_order(table, state, &url, in_order, projection, filters).await?);
    }
    if !merged.is_empty() {
        // Keys-first doesn't read each series in order.
        let strategy = if series_order.is_some() { Strategy::Merge } else { strategy(table, state)? };
        plans.push(match strategy {
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

/// A scan of each time partition, newest first, that stops once the `k` newest rows so far are
/// newer than any row an older partition holds.
async fn scan_newest(
    table: &Table,
    state: &dyn DfSession,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    newest: Newest,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let files = table.files().await?;
    let mut partitions: BTreeMap<Option<i32>, (i64, Vec<LiveEntry>)> = BTreeMap::new();
    for f in files {
        let entry = partitions.entry(crate::write::file_partition(&f.data_file)).or_insert((i64::MIN, vec![]));
        entry.0 = entry.0.max(time_bounds(table, &f).1);
        entry.1.push(f);
    }
    let mut partitions: Vec<(i64, Vec<LiveEntry>)> = partitions.into_values().collect();
    partitions.sort_by_key(|(newest, _)| std::cmp::Reverse(*newest));
    let mut waves = vec![];
    for (last, files) in partitions {
        waves.push((scan_data(table, state, projection, filters, None, files, None).await?, last));
    }
    let time = table.contract().time_column.clone().unwrap_or_default();
    Ok(Arc::new(NewestFirstExec::try_new(waves, newest, &time)?))
}

/// Which of `files` could give a series' rows out of time order: they overlap another on the series
/// and time, or aren't sorted.
fn out_of_order(table: &Table, files: &[LiveEntry]) -> DfResult<Vec<bool>> {
    let sort_order = table_sort_order(table);
    let mut merge: Vec<bool> = files.iter().map(|f| f.data_file.sort_order_id() != sort_order).collect();
    let schema = table.schema();
    let keys = table.contract().sort_columns();
    let key_indices = keys.iter().map(|k| schema.index_of(k)).collect::<Result<Vec<_>, _>>()?;
    for &i in overlapping_groups(&schema, &key_indices, files).iter().flatten() {
        merge[i] = true;
    }
    Ok(merge)
}

/// `merged` and every file of `files` in the time it spans (which grows with each file added),
/// then the files before that time and those after it.
fn around(
    table: &Table,
    mut merged: Vec<LiveEntry>,
    files: Vec<LiveEntry>,
) -> (Vec<LiveEntry>, Vec<LiveEntry>, Vec<LiveEntry>) {
    if merged.is_empty() {
        return (merged, files, vec![]);
    }
    let bounds = |f: &LiveEntry| time_bounds(table, f);
    let (mut lo, mut hi) = merged.iter().map(bounds).fold((i64::MAX, i64::MIN), |(l, h), (a, b)| (l.min(a), h.max(b)));
    let mut rest = files;
    loop {
        let (inside, outside): (Vec<_>, Vec<_>) = rest.into_iter().partition(|f| {
            let (a, b) = bounds(f);
            a <= hi && b >= lo
        });
        rest = outside;
        if inside.is_empty() {
            break;
        }
        for (a, b) in inside.iter().map(bounds) {
            (lo, hi) = (lo.min(a), hi.max(b));
        }
        merged.extend(inside);
    }
    let (before, after) = rest.into_iter().partition(|f| bounds(f).1 < lo);
    (merged, before, after)
}

/// The bounds of `f`'s time column (the widest if its metadata lacks them).
fn time_bounds(table: &Table, f: &LiveEntry) -> (i64, i64) {
    let time = table.contract().time_column.as_ref().and_then(|t| table.metadata().current_schema().field_by_name(t));
    let bound = |bounds: &std::collections::HashMap<i32, Datum>, default| match time
        .and_then(|t| bounds.get(&t.id))
        .map(Datum::literal)
    {
        Some(PrimitiveLiteral::Long(t)) => *t,
        _ => default,
    };
    (bound(f.data_file.lower_bounds(), i64::MIN), bound(f.data_file.upper_bounds(), i64::MAX))
}

/// Every row of `files`, each series in one slice and in `(series…, time)` order: the dedup scan's
/// merge, keeping every copy.
async fn scan_in_series_order(
    table: &Table,
    state: &dyn DfSession,
    url: &ObjectStoreUrl,
    files: Vec<LiveEntry>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let schema = table.schema();
    let order = table.contract().write_order();
    let (requested, read) = read_columns(&schema, projection, &order)?;
    let plan = Ranked::new(table, files)?.scan(table, state, url, read, filters, true).await?;
    let read_schema = plan.schema();
    let keys = table.contract().sort_columns();
    let key_positions = keys.iter().map(|k| read_schema.index_of(k)).collect::<Result<Vec<_>, _>>()?;
    let ordering = rank_ordering(&read_schema, &order)?;
    // Without a series key the whole table is one series: one slice.
    let slices = if table.contract().series_key.is_empty() { 1 } else { state.config().target_partitions() };
    // The dedup operator's split and merge, keeping every row: declared, so the optimizer keeps
    // the order (a bare repartition it would drop and split the files again).
    let merged = Arc::new(DedupExec::try_new(plan, key_positions, ordering, slices, true, Keep::All)?);
    project(merged, requested.len())
}

/// What [`select_files`] picks: the files to scan as they are, the groups of files to
/// deduplicate together, and the filters on key columns only.
type Selection = (Vec<LiveEntry>, Vec<Vec<LiveEntry>>, Vec<Expr>);

/// The files a scan reads: those that can match `filters` and share no key with another file, and
/// the groups of files that may share keys (dedup on), read together and deduplicated; and the
/// filters on key columns only, which can apply before dedup.
///
/// Rows are deduplicated before the filters apply, so files that may share keys are only pruned
/// by filters on the key columns: an older copy of a row must not outlive a newer one that a
/// filter on another column would have pruned.
fn select_files(table: &Table, state: &dyn DfSession, filters: &[Expr], files: Vec<LiveEntry>) -> DfResult<Selection> {
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

/// Scans files that share no key with each other: a plain pruned Parquet scan. With `time_order`,
/// each partition reads a stretch of time, files in time order.
#[expect(clippy::too_many_arguments, reason = "the scan's inputs, all needed")]
async fn scan_files(
    table: &Table,
    state: &dyn DfSession,
    url: &ObjectStoreUrl,
    mut files: Vec<LiveEntry>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
    time_order: bool,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let schema = table.schema();
    let storage = table.lake().storage();
    let target = state.config().target_partitions().max(1);
    let n_groups = files.len().min(target);
    let mut groups: Vec<Vec<PartitionedFile>> = vec![vec![]; n_groups];
    let lists = in_lists(table, state, filters);
    // Biggest files first, round-robin: roughly balanced groups. In time order: consecutive files
    // of about equal size per group (DataFusion's split into byte ranges keeps the order).
    let size = |f: &LiveEntry| f.data_file.file_size_in_bytes();
    let per_group = files.iter().map(size).sum::<u64>().div_ceil(n_groups.max(1) as u64).max(1);
    if time_order {
        files.sort_by_cached_key(|f| time_bounds(table, f));
    } else {
        files.sort_by_key(|f| std::cmp::Reverse(size(f)));
    }
    let (mut total_rows, mut before) = (0u64, 0u64);
    for (i, f) in files.iter().enumerate() {
        total_rows += f.data_file.record_count();
        let path = storage.path(f.data_file.file_path())?;
        let pf = PartitionedFile::new(path.to_string(), size(f)).with_statistics(Arc::new(file_statistics(&schema, f)));
        let group = if time_order {
            usize::try_from(before / per_group).unwrap_or(usize::MAX).min(n_groups - 1)
        } else {
            i % n_groups
        };
        groups[group].push(with_access_plan(state, &schema, &lists, pf));
        before += size(f);
    }
    groups.retain(|g| !g.is_empty());
    let one_file_per_group = groups.iter().all(|g| g.len() == 1);

    let source = Arc::new(ParquetSource::new(schema.clone()));
    let mut stats = Statistics::new_unknown(&schema);
    stats.num_rows = Precision::Exact(usize::try_from(total_rows).unwrap_or(usize::MAX));
    let mut builder = FileScanConfigBuilder::new(url.clone(), source)
        .with_file_groups(groups.into_iter().map(FileGroup::new).collect())
        .with_statistics(stats)
        .with_projection_indices(projection.cloned())?
        .with_limit(if filters.is_empty() { limit } else { None })
        // DataFusion lets an idle partition take files from the others unless told the order matters.
        .with_preserve_order(time_order);
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
    if !pushdown {
        builder = builder.with_expr_adapter(split_adapter(table, state));
    }
    options.global.pushdown_filters = pushdown;
    options.global.reorder_filters = pushdown;
    let format = ParquetFormat::default().with_options(options);
    format.create_physical_plan(state, builder.build()).await
}

/// The session's `timelake.in_list_pruning`, else the table's `timelake.in-list-pruning`.
fn in_list_pruning(table: &Table, state: &dyn DfSession) -> bool {
    let settings = state.config().options().extensions.get::<SessionSettings>();
    settings.and_then(|s| s.flag(property::IN_LIST_PRUNING)).unwrap_or(table.contract().in_list_pruning)
}

/// The long `IN` lists among `filters` that the scan prunes with itself, if the setting is on.
fn in_lists(table: &Table, state: &dyn DfSession, filters: &[Expr]) -> Vec<InList> {
    if in_list_pruning(table, state) { long_in_lists(filters) } else { vec![] }
}

/// The predicate adapter that lets runtime filters' long `IN` lists prune, if the setting is on.
/// Only for scans that don't filter rows while decoding: those would evaluate the added lists on
/// every row too, which cost more than the pruning saved in the industrial benchmark
/// (`daily-stats` 469 → 712 ms, where the filter's bounds already pruned as well).
fn split_adapter(table: &Table, state: &dyn DfSession) -> Option<Arc<dyn PhysicalExprAdapterFactory>> {
    in_list_pruning(table, state).then(|| Arc::new(SplitInLists) as _)
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

/// How copies are removed on read (spec 12): `merge` or `keys-first`.
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
            Kind::Data | Kind::Newest(_) => TableProviderFilterPushDown::Inexact,
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
