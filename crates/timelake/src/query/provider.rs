//! A Timelake table as a DataFusion table: pruned Parquet scans, or the last-value index.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session as DfSession, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{DFSchema, ScalarValue, Statistics};
use datafusion::common::{JoinType, NullEquality};
use datafusion::datasource::DefaultTableSource;
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
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableScan, TableType};
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
use futures::{StreamExt, TryStreamExt};
use parquet::file::metadata::PageIndexPolicy;
use timelake_format::rows::ASCENDING;
use timelake_format::{ReadDedupStrategy, footer};

use super::dedup::{DedupExec, Keep, RANK, Split, SplitBy, overlapping_groups, series_values};
use super::in_list::{InList, SplitInLists, access_plan, long_in_lists};
use super::newest::{Newest, NewestFirstExec};
use super::series_order::Order;
use super::settings::{FILTER_PUSHDOWN, IN_LIST_PRUNING, READ_DEDUP, READ_DEDUP_STRATEGY};
use super::stats::{FileStats, file_statistics};
use crate::summary;
use crate::table::{LiveEntry, Table};

/// The table scan under `plan`, walking down through aliases, projections of columns, and the
/// nodes `through` gives the input of (recording what it passes); `None` if another node is in the
/// way.
pub(super) fn scan_under<'a>(
    plan: &'a LogicalPlan,
    mut through: impl FnMut(&'a LogicalPlan) -> Option<&'a LogicalPlan>,
) -> Option<&'a TableScan> {
    let mut node = plan;
    loop {
        node = match node {
            LogicalPlan::TableScan(scan) => return Some(scan),
            LogicalPlan::SubqueryAlias(alias) => &alias.input,
            LogicalPlan::Projection(p) if p.expr.iter().all(|e| matches!(e, Expr::Column(_))) => &p.input,
            other => through(other)?,
        };
    }
}

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
    /// Rollup routing has dealt with this scan (spec 27).
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

    /// The Timelake table `scan` reads, if it reads one.
    pub(super) fn of_scan(scan: &TableScan) -> Option<&Self> {
        let source = (scan.source.as_ref() as &dyn Any).downcast_ref::<DefaultTableSource>()?;
        (source.table_provider.as_ref() as &dyn Any).downcast_ref()
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
        let scan = Scan::new(table, state, projection, filters)?;
        match self.kind {
            Kind::Latest => scan_latest(table, projection).await,
            Kind::Data => scan.data(&table.files().await?, limit, self.series_order).await,
            Kind::Newest(newest) => scan.newest(newest).await,
        }
    }
}

async fn scan_latest(table: &Table, projection: Option<&Vec<usize>>) -> DfResult<Arc<dyn ExecutionPlan>> {
    let uri = table.last_value_index().ok_or_else(|| DataFusionError::Plan("no last-value index".into()))?;
    let batch = crate::lvi::read(table, &uri).await?;
    Ok(MemorySourceConfig::try_new_exec(&[vec![batch]], table.schema(), projection.cloned())?)
}

/// One scan of a table version: the table, the session planning it, and what the query asks of it,
/// which every part of the plan reads with.
struct Scan<'a> {
    table: &'a Table,
    state: &'a dyn DfSession,
    /// The requested columns (all for `None`).
    projection: Option<&'a Vec<usize>>,
    /// The query's filters.
    filters: &'a [Expr],
    /// Where DataFusion finds the table's store.
    url: ObjectStoreUrl,
}

/// How [`Scan::files`] spreads files over partitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Spread {
    /// Biggest first, round-robin: roughly balanced partitions.
    BySize,
    /// Consecutive files of about equal size per partition, each a stretch of time in time order.
    InTime,
}

/// A scan's files by how they are read.
#[derive(Default)]
struct Reads {
    /// As they are, spread over partitions by size.
    plain: Vec<LiveEntry>,
    /// Stretches of time, each read in time order.
    stretches: Vec<Vec<LiveEntry>>,
    /// Merged in series order, every row kept.
    in_series_order: Vec<LiveEntry>,
    /// Merged in series order, the newest write of each key kept.
    deduplicated: Vec<LiveEntry>,
}

impl Reads {
    /// The reads for a signal aggregate, whose runs join exactly only if no partition has readings
    /// of a series inside another partition's run of it ([`super::series_order`]). The files that
    /// must be merged (`deduplicated`, or overlapping or unsorted ones) are merged with every file
    /// in the time they span; the files before and after that time are read in time order.
    fn in_series_order(
        table: &Table,
        mut plain: Vec<LiveEntry>,
        deduplicated: Vec<LiveEntry>,
        order: Order,
    ) -> DfResult<Self> {
        let dedup = !deduplicated.is_empty();
        let mut merged = deduplicated;
        if order == Order::Merge {
            merged.append(&mut plain);
        } else if !dedup {
            let merge = if order == Order::Auto { out_of_order(table, &plain)? } else { vec![false; plain.len()] };
            let (now, alone): (Vec<_>, Vec<_>) = plain.into_iter().zip(merge).partition(|(_, m)| *m);
            (merged, plain) = (now.into_iter().map(|(f, _)| f).collect(), alone.into_iter().map(|(f, _)| f).collect());
        }
        let (inside, before, after) = around(table, merged, plain);
        let stretches = vec![before, after];
        Ok(if dedup {
            Self { stretches, deduplicated: inside, ..Self::default() }
        } else {
            Self { stretches, in_series_order: inside, ..Self::default() }
        })
    }

    fn all(&self) -> impl Iterator<Item = &LiveEntry> {
        let stretches = self.stretches.iter().flatten();
        self.plain.iter().chain(stretches).chain(&self.in_series_order).chain(&self.deduplicated)
    }
}

impl<'a> Scan<'a> {
    fn new(
        table: &'a Table,
        state: &'a dyn DfSession,
        projection: Option<&'a Vec<usize>>,
        filters: &'a [Expr],
    ) -> DfResult<Self> {
        let url = ObjectStoreUrl::parse(table.lake().storage().url())?;
        Ok(Self { table, state, projection, filters, url })
    }

    /// A scan of `files`, data files of `table`; with `series_order`, for a signal aggregate.
    async fn data(
        &self,
        files: &[LiveEntry],
        limit: Option<usize>,
        series_order: Option<Order>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let Scan { table, projection, .. } = *self;
        let (files, groups, key_filters) = self.select_files(files)?;
        // Keys never repeat across groups (different partitions or key ranges), so one scan serves all.
        let merged: Vec<LiveEntry> = groups.into_iter().flatten().collect();
        let series_order = series_order.and_then(|order| order.for_files(files.iter().chain(&merged)));
        let reads = match series_order {
            Some(order) => Reads::in_series_order(table, files, merged, order)?,
            None => Reads { plain: files, deduplicated: merged, ..Reads::default() },
        };
        let storage = table.lake().storage();
        let footers = reads.all().map(|f| {
            let path = storage.path(f.data_file.file_path())?.to_string();
            Ok(PartitionedFile::new(path, f.data_file.file_size_in_bytes()))
        });
        self.prefetch_footers(&footers.collect::<DfResult<Vec<_>>>()?).await?;
        let mut plans = self.plans(reads, limit, series_order.is_some(), &key_filters).await?;
        match plans.len() {
            0 => {
                let schema = table.schema();
                let projected = match projection {
                    Some(p) => Arc::new(schema.project(p)?),
                    None => schema,
                };
                Ok(Arc::new(EmptyExec::new(projected)))
            }
            1 => Ok(plans.swap_remove(0)),
            _ => UnionExec::try_new(plans),
        }
    }

    /// The scans of `reads`; `in_series_order` for a signal aggregate. `key_filters` are the
    /// filters on key columns only.
    async fn plans(
        &self,
        reads: Reads,
        limit: Option<usize>,
        in_series_order: bool,
        key_filters: &[Expr],
    ) -> DfResult<Vec<Arc<dyn ExecutionPlan>>> {
        let mut plans = vec![];
        if !reads.plain.is_empty() {
            plans.push(self.files(reads.plain, limit, Spread::BySize).await?);
        }
        for stretch in reads.stretches.into_iter().filter(|s| !s.is_empty()) {
            plans.push(self.files(stretch, limit, Spread::InTime).await?);
        }
        if !reads.in_series_order.is_empty() {
            plans.push(self.in_series_order(reads.in_series_order).await?);
        }
        if !reads.deduplicated.is_empty() {
            // Keys-first doesn't read each series in order.
            let strategy = if in_series_order {
                ReadDedupStrategy::Merge
            } else {
                READ_DEDUP_STRATEGY.of(self.state.config().options(), self.table.contract())
            };
            plans.push(match strategy {
                ReadDedupStrategy::Merge => self.deduplicated(reads.deduplicated, key_filters).await?,
                ReadDedupStrategy::KeysFirst => self.keys_first(reads.deduplicated, key_filters).await?,
            });
        }
        Ok(plans)
    }

    /// A scan of each time partition, newest first, that stops once the `k` newest rows so far are
    /// newer than any row an older partition holds.
    async fn newest(&self, newest: Newest) -> DfResult<Arc<dyn ExecutionPlan>> {
        let Scan { table, .. } = *self;
        let files = table.files().await?;
        let mut partitions: BTreeMap<Option<i32>, (i64, Vec<LiveEntry>)> = BTreeMap::new();
        for f in files.iter().cloned() {
            let entry = partitions.entry(crate::write::file_partition(&f.data_file)).or_insert((i64::MIN, vec![]));
            entry.0 = entry.0.max(time_bounds(table, &f).1);
            entry.1.push(f);
        }
        let mut partitions: Vec<(i64, Vec<LiveEntry>)> = partitions.into_values().collect();
        partitions.sort_by_key(|(newest, _)| std::cmp::Reverse(*newest));
        let scans = partitions
            .into_iter()
            .map(|(last, files)| async move { Ok::<_, DataFusionError>((self.data(&files, None, None).await?, last)) });
        let waves = futures::stream::iter(scans).buffered(CONCURRENT_PARTITION_SCANS).try_collect::<Vec<_>>().await?;
        let time = table.contract().time_column.clone().unwrap_or_default();
        Ok(Arc::new(NewestFirstExec::try_new(waves, newest, &time)?))
    }

    /// Every row of `files`, each series in one slice and in `(series…, time)` order: the dedup scan's
    /// merge, keeping every copy.
    async fn in_series_order(&self, files: Vec<LiveEntry>) -> DfResult<Arc<dyn ExecutionPlan>> {
        let Scan { table, state, projection, filters, .. } = *self;
        let schema = table.schema();
        let order = table.contract().write_order();
        let (requested, read) = read_columns(&schema, projection, &order)?;
        let plan = Ranked::new(table, files)?.scan(self, read, filters, true).await?;
        let read_schema = plan.schema();
        let keys = table.contract().sort_columns();
        let key_positions = keys.iter().map(|k| read_schema.index_of(k)).collect::<Result<Vec<_>, _>>()?;
        let ordering = rank_ordering(&read_schema, &order)?;
        // Without a series key the whole table is one series: one slice.
        let slices = if table.contract().series_key.is_empty() { 1 } else { state.config().target_partitions() };
        // The dedup operator's split and merge, keeping every row: declared, so the optimizer keeps
        // the order (a bare repartition it would drop and split the files again).
        let split = Split { slices, by: SplitBy::Series };
        let merged = Arc::new(DedupExec::try_new(plan, key_positions, ordering, split, Keep::All)?);
        project(merged, requested.len())
    }

    /// The files a scan reads: those that can match `filters` and share no key with another file, and
    /// the groups of files that may share keys (dedup on), read together and deduplicated; and the
    /// filters on key columns only, which can apply before dedup.
    ///
    /// Rows are deduplicated before the filters apply, so files that may share keys are only pruned
    /// by filters on the key columns: an older copy of a row must not outlive a newer one that a
    /// filter on another column would have pruned.
    fn select_files(&self, files: &[LiveEntry]) -> DfResult<Selection> {
        let Scan { table, state, filters, .. } = *self;
        let contract = table.contract();
        let keys = contract.sort_columns();
        let read_dedup = contract.dedup && READ_DEDUP.of(state.config().options(), contract);
        if !read_dedup || keys.is_empty() {
            let keep = self.prune(files, filters)?;
            return Ok((kept(files, &keep), vec![], vec![]));
        }
        let on_keys: Vec<Expr> =
            filters.iter().filter(|e| e.column_refs().iter().all(|c| keys.contains(&c.name))).cloned().collect();
        let keep = self.prune(files, &on_keys)?;
        let files = kept(files, &keep);
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
        let keep = self.prune(&files, filters)?;
        let mut groups = vec![];
        for members in overlapping {
            if members.iter().any(|&i| keep[i]) {
                groups.push(members.iter().map(|&i| files[i].clone()).collect());
            }
        }
        let singles =
            files.into_iter().enumerate().filter_map(|(i, f)| (keep[i] && !grouped[i]).then_some(f)).collect();
        Ok((singles, groups, on_keys))
    }

    /// Scans files that share no key with each other: a plain pruned Parquet scan, its partitions
    /// spread as `spread` says.
    async fn files(
        &self,
        files: Vec<LiveEntry>,
        limit: Option<usize>,
        spread: Spread,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let Scan { table, state, projection, filters, ref url, .. } = *self;
        let schema = table.schema();
        let target = state.config().target_partitions().max(1);
        let n_groups = files.len().min(target);
        let total_rows: u64 = files.iter().map(|f| f.data_file.record_count()).sum();
        // Another engine's files may be unsorted.
        let sorted = files.iter().all(|f| f.data_file.sort_order_id() == table_sort_order(table));
        let groups = self.file_groups(files, n_groups, spread)?;
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
            .with_preserve_order(spread == Spread::InTime);
        // Timelake's files are sorted by (series…, time). Declaring it lets DataFusion skip sorts, but it
        // also keeps one partition per file (no byte-range splits), which costs more than it saves when
        // a few large compacted files hold the data. Only declare it when there are enough files.
        let sort_columns = table.contract().sort_columns();
        if sorted
            && n_groups >= target
            && one_file_per_group
            && let Some(ordering) = key_ordering(&schema, &sort_columns)?
        {
            builder = builder.with_output_ordering(vec![ordering]);
        }
        let mut options = state.table_options().parquet.clone();
        let pushdown = self.filter_pushdown();
        if !pushdown {
            builder = builder.with_expr_adapter(self.split_adapter());
        }
        options.global.pushdown_filters = pushdown;
        options.global.reorder_filters = pushdown;
        let format = ParquetFormat::default().with_options(options);
        format.create_physical_plan(state, builder.build()).await
    }

    /// `files` as `n` groups, one per partition. By size: biggest first, round-robin, for roughly
    /// balanced groups. In time: consecutive files of about equal size per group (DataFusion's split
    /// into byte ranges keeps the order).
    fn file_groups(&self, mut files: Vec<LiveEntry>, n: usize, spread: Spread) -> DfResult<Vec<Vec<PartitionedFile>>> {
        let Scan { table, state, filters, .. } = *self;
        let (schema, storage) = (table.schema(), table.lake().storage());
        let lists = self.in_lists(filters);
        let size = |f: &LiveEntry| f.data_file.file_size_in_bytes();
        let per_group = files.iter().map(size).sum::<u64>().div_ceil(n.max(1) as u64).max(1);
        match spread {
            Spread::InTime => files.sort_by_cached_key(|f| time_bounds(table, f)),
            Spread::BySize => files.sort_by_key(|f| std::cmp::Reverse(size(f))),
        }
        let mut groups: Vec<Vec<PartitionedFile>> = vec![vec![]; n];
        let mut before = 0u64;
        for (i, f) in files.iter().enumerate() {
            let path = storage.path(f.data_file.file_path())?;
            let pf =
                PartitionedFile::new(path.to_string(), size(f)).with_statistics(Arc::new(file_statistics(&schema, f)));
            let group = match spread {
                Spread::InTime => usize::try_from(before / per_group).unwrap_or(usize::MAX).min(n - 1),
                Spread::BySize => i % n,
            };
            groups[group].push(with_access_plan(state, &schema, &lists, pf));
            before += size(f);
        }
        groups.retain(|g| !g.is_empty());
        Ok(groups)
    }

    /// Whether the scan prunes with long `IN` lists itself (setting `in-list-pruning`).
    fn in_list_pruning(&self) -> bool {
        IN_LIST_PRUNING.of(self.state.config().options(), self.table.contract())
    }

    /// The long `IN` lists among `filters` that the scan prunes with itself, if the setting is on.
    fn in_lists(&self, filters: &[Expr]) -> Vec<InList> {
        if self.in_list_pruning() { long_in_lists(filters) } else { vec![] }
    }

    /// The predicate adapter that lets runtime filters' long `IN` lists prune, if the setting is on.
    /// Only for scans that don't filter rows while decoding: those would evaluate the added lists on
    /// every row too, which cost more than the pruning saved in the industrial benchmark
    /// (`daily-stats` 469 → 712 ms, where the filter's bounds already pruned as well).
    fn split_adapter(&self) -> Option<Arc<dyn PhysicalExprAdapterFactory>> {
        self.in_list_pruning().then(|| Arc::new(SplitInLists) as _)
    }

    /// Whether the scan filters rows while decoding (setting `filter-pushdown`); `auto` is on for
    /// local files and off on object storage.
    fn filter_pushdown(&self) -> bool {
        let Scan { table, state, .. } = *self;
        FILTER_PUSHDOWN.of(state.config().options(), table.contract()).is_on(|| !table.lake().storage().is_remote())
    }

    /// Scans files that may share keys, strategy merge: each file's rows with its commit rank, split
    /// by a hash of the series so every copy of a key lands in the same slice, each slice merged by
    /// `(series…, time, rank)` on its own, and the newest write of each key kept. `key_filters` (on
    /// key columns only) remove rows before the merge, and prune row groups and pages.
    async fn deduplicated(&self, files: Vec<LiveEntry>, key_filters: &[Expr]) -> DfResult<Arc<dyn ExecutionPlan>> {
        let Scan { table, state, projection, .. } = *self;
        let schema = table.schema();
        let keys = table.contract().sort_columns();
        let ranked = Ranked::new(table, files)?;
        let order = table.contract().write_order();
        let (requested, read) = read_columns(&schema, projection, &order)?;
        let plan = ranked.scan(self, read, key_filters, true).await?;
        let read_schema = plan.schema();
        let key_positions = keys.iter().map(|k| read_schema.index_of(k)).collect::<Result<Vec<_>, _>>()?;
        let ordering = rank_ordering(&read_schema, &order)?;
        let slices = state.config().target_partitions();
        let series = &keys[..keys.len().saturating_sub(usize::from(table.contract().time_column.is_some()))];
        let by =
            if series_values(key_filters, series).is_none_or(|n| n >= slices) { SplitBy::Series } else { SplitBy::Key };
        let plan = Arc::new(DedupExec::try_new(plan, key_positions, ordering, Split { slices, by }, Keep::Latest)?);
        project(plan, requested.len())
    }

    /// Scans files that may share keys, strategy keys-first. Pass 1 reads only the key columns and
    /// the rank, merges them like [`Scan::deduplicated`] and lists the older copies. Pass 2 reads the
    /// files like any scan, every filter pruning and removing rows, and leaves out the rows on that
    /// list: the list was built from all rows, so filters on other columns are safe before it.
    async fn keys_first(&self, files: Vec<LiveEntry>, key_filters: &[Expr]) -> DfResult<Arc<dyn ExecutionPlan>> {
        let Scan { table, state, projection, filters, .. } = *self;
        let schema = table.schema();
        let keys = table.contract().sort_columns();
        let ranked = Ranked::new(table, files)?;
        let order = table.contract().write_order();
        let key_read: Vec<usize> =
            order.iter().map(|k| schema.index_of(k)).chain([Ok(schema.fields().len())]).collect::<Result<_, _>>()?;
        let pass1 = ranked.scan(self, key_read, key_filters, true).await?;
        let pass1_schema = pass1.schema();
        let ordering = rank_ordering(&pass1_schema, &order)?;
        let key_positions: Vec<usize> = (0..keys.len()).collect();
        let slices = state.config().target_partitions();
        let older: Arc<dyn ExecutionPlan> = Arc::new(DedupExec::try_new(
            pass1,
            key_positions,
            ordering,
            Split { slices, by: SplitBy::Key },
            Keep::Older,
        )?);

        let (requested, read) = read_columns(&schema, projection, &keys)?;
        let pass2 = ranked.scan(self, read, filters, false).await?;
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

    /// Which of `files` can match `filters` on their Iceberg column bounds.
    fn prune(&self, files: &[LiveEntry], filters: &[Expr]) -> DfResult<Vec<bool>> {
        let Scan { table, state, .. } = *self;
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
    async fn prefetch_footers(&self, files: &[PartitionedFile]) -> DfResult<()> {
        let Scan { table, state, ref url, .. } = *self;
        let store = state.runtime_env().object_store(url)?;
        let cache = state.runtime_env().cache_manager.get_file_metadata_cache();
        let hint = state.table_options().parquet.global.metadata_size_hint;
        let (base, known) = (table.lake().storage().base_url(), &table.lake().caches().footers);
        let mut fetch = vec![];
        for pf in files {
            let location = &pf.object_meta.location;
            match known.get(&format!("{base}/{location}")) {
                Some(bytes) => {
                    let footer = Arc::new(footer::decode(&bytes, PageIndexPolicy::Optional)?);
                    let metadata = Arc::new(CachedParquetMetaData::new(footer));
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
}

/// The `files` that `keep` says to keep.
fn kept(files: &[LiveEntry], keep: &[bool]) -> Vec<LiveEntry> {
    files.iter().zip(keep).filter(|(_, k)| **k).map(|(f, _)| f.clone()).collect()
}

/// Time partitions a newest-first scan plans at once: each reads the footers of its files.
const CONCURRENT_PARTITION_SCANS: usize = 16;

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

/// The bounds of `f`'s time column (the widest where its manifest entry lacks them).
fn time_bounds(table: &Table, f: &LiveEntry) -> (i64, i64) {
    let (lo, hi) = table.time_field_id().map_or((None, None), |id| f.time_bounds(id));
    (lo.unwrap_or(i64::MIN), hi.unwrap_or(i64::MAX))
}

/// What [`Scan::select_files`] picks: the files to scan as they are, the groups of files to
/// deduplicate together, and the filters on key columns only.
type Selection = (Vec<LiveEntry>, Vec<Vec<LiveEntry>>, Vec<Expr>);

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
        scan: &Scan<'_>,
        read: Vec<usize>,
        filters: &[Expr],
        ordered: bool,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let Scan { table, state, ref url, .. } = *scan;
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
        let lists = scan.in_lists(filters);
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

#[cfg(test)]
mod tests {
    use arrow::array::{AsArray, RecordBatch};
    use arrow::datatypes::Float64Type;
    use datafusion::prelude::SessionConfig;
    use timelake_format::rows::sort_and_split;

    use crate::testing::{T0, append_unsorted, batch, faulty_lake, opts, schema};

    fn values(out: &[RecordBatch]) -> Vec<f64> {
        out.iter().flat_map(|b| b.column(0).as_primitive::<Float64Type>().values().to_vec()).collect()
    }

    /// A file another engine wrote unsorted isn't declared sorted, so `ORDER BY` still sorts it (in
    /// the order a sorted file would be declared in, nulls first).
    #[tokio::test]
    async fn an_unsorted_file_is_sorted_for_order_by() {
        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let day = sort_and_split(&t.schema(), t.contract(), &[batch(&[(T0, 1, 0.0)])], 0).unwrap()[0].0;
        append_unsorted(&t, day, batch(&[(T0 + 5, 2, 3.0), (T0, 1, 1.0), (T0 + 1, 2, 2.0)])).await;
        let session = crate::Session::with_config(lake, SessionConfig::new().with_target_partitions(1));
        session.sql("SET timelake.read_dedup = false").await.unwrap();
        let out = session
            .sql("SELECT value FROM iot.r ORDER BY device NULLS FIRST, time NULLS FIRST")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(values(&out), [1.0, 2.0, 3.0]);
    }
}
