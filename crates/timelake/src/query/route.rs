//! Rollup routing (spec 27): a `GROUP BY time_bucket(…)` query on a raw table, answered from a
//! rollup for the buckets the rollup has right, and from the raw table for the rest. A query over a
//! time range without a bucket (`GROUP BY` keys only) is answered the same way per rollup bucket,
//! in mergeable form, and merged once more.
//!
//! Which buckets a rollup has right takes I/O to find out (its record, the raw table's files), and
//! optimizer rules can't do I/O: [`prepare`] works it out before planning, and [`RollupRewrite`]
//! uses it while optimizing.

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, ScalarValue, TableReference};
use datafusion::datasource::provider_as_source;
use datafusion::error::Result as DfResult;
use datafusion::functions::datetime::date_bin;
use datafusion::functions_aggregate::expr_fn as agg;
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{
    Aggregate, BinaryExpr, Expr, ExprSchemable, Filter, JoinType, LogicalPlan, LogicalPlanBuilder, Operator,
    Projection, TableScan, Union,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::{cast, coalesce, ident, lit};
use timelake_format::rollup::FunctionFamily;

use super::PLANNING_REUSE;
use super::gaps;
use super::provider::{Kind, TimelakeProvider, scan_under};
use super::rollup::{aggregate_of, bucket_width, is_time, unalias};
use super::settings::{READ_DEDUP, ROLLUP_ROUTING, VERSION_CHECK_INTERVAL};
use super::signal::{Method, Output, state_type, udaf};
use super::time;
use crate::TableName;
use crate::lake::Lake;
use crate::rollup::{Definition, Ranges, Reflects, RollupFunction, changed, floor, in_history, layout};
use crate::table::{LiveEntry, Table};

/// Stands for no bound: beyond any time a table holds (about ±73,000 years), with room to add a
/// bucket width without overflow.
const FAR: i64 = i64::MAX / 2;

/// The rollups that may answer queries on one version of a raw table.
#[derive(Debug)]
pub(super) struct Routes {
    version: u64,
    rollups: Vec<Candidate>,
}

#[derive(Debug)]
struct Candidate {
    def: Definition,
    /// The rollup at the version whose record `clean` was worked out from.
    rollup: Table,
    /// The buckets this version of the rollup has right: unchanged in the raw table since its
    /// refresh, and within both tables' retention. Bucket-aligned, half-open.
    clean: Ranges,
    /// How many raw rows each rollup row stands for, on average.
    raw_rows_per_row: u64,
}

/// The fewest raw rows per rollup row for answering from stored signal states. Merging an 80-byte
/// state costs about what reading 25–50 readings does: daily means of the industrial benchmark's
/// tags from a 1-minute rollup (12 readings a row) took 3.6 s against 2.0 s from the readings, a
/// month's from an hourly rollup (515 a row) 0.15 s against 1.5 s.
const SIGNAL_STATE_ROWS: u64 = 32;
/// The same for the smaller gap runs and `m4` points, which cost a few readings each: a month's
/// gaps from the 1-minute rollup 0.24 s against 1.2 s.
const STATE_ROWS: u64 = 4;

/// Routes by raw table, shared by a session and its optimizer rule.
pub(super) type RouteMap = Arc<RwLock<HashMap<TableName, Arc<Routes>>>>;

/// Works out the routes of raw table `name` for the next query with session `options`, or forgets
/// them when it has no rollups to route to. The table version is confirmed current first, so the
/// rule only routes a version that won't be replaced while it is scanned. Rollup `except` is left
/// out (the one being refreshed).
pub(super) async fn prepare(
    lake: &Lake,
    name: &TableName,
    options: &ConfigOptions,
    routes: &RouteMap,
    except: Option<&TableName>,
) {
    let prepared = routes_for(lake, name, options, except).await;
    let mut map = routes.write().unwrap_or_else(PoisonError::into_inner);
    match prepared {
        Some(r) => map.insert(name.clone(), Arc::new(r)),
        None => map.remove(name),
    };
}

async fn routes_for(
    lake: &Lake,
    name: &TableName,
    options: &ConfigOptions,
    except: Option<&TableName>,
) -> Option<Routes> {
    let interval = VERSION_CHECK_INTERVAL.session(options);
    let mut raw = lake.table_recent(name, interval, PLANNING_REUSE).await.ok()?;
    if raw.contract().rollups.is_empty() || !routable(&raw, options) {
        return None;
    }
    if raw.is_stale().await.ok()? {
        raw = lake.load(name).await.ok()?;
    }
    let files = raw.files().await.ok()?;
    let (raw, files) = (&raw, files.as_slice());
    let names = raw.contract().rollups.iter().filter_map(|n| n.parse::<TableName>().ok());
    let candidates = names
        .filter(|n| Some(n) != except)
        .map(|rollup| async move { candidate(lake, raw, files, &rollup, interval).await });
    let mut rollups: Vec<Candidate> = futures::future::join_all(candidates).await.into_iter().flatten().collect();
    // The widest first: fewer rows to read and merge for the queries it answers.
    rollups.sort_by_key(|c| std::cmp::Reverse(c.def.width));
    (!rollups.is_empty()).then_some(Routes { version: raw.version(), rollups })
}

/// Rollup `name` of `raw` (whose live files are `files`) as a candidate for its queries: what it
/// has right of this version of `raw`. `None` if it isn't a rollup of `raw` or reflects a version
/// `raw` went back from.
async fn candidate(
    lake: &Lake,
    raw: &Table,
    files: &[LiveEntry],
    name: &TableName,
    interval: Option<Duration>,
) -> Option<Candidate> {
    let rollup = lake.table_recent(name, interval, PLANNING_REUSE).await.ok()?;
    let def = Definition::of(&rollup).ok()?;
    let reflects = Reflects::of(&rollup)?;
    if def.source != *raw.name() || !in_history(raw, &rollup) {
        return None;
    }
    let dirty = changed(raw, files, reflects.sequence(), def.width)?;
    // Retention drops whole partitions that end before now minus the retention: buckets from
    // there on are complete in a table (and were when the rollup computed them).
    let now = chrono::Utc::now().timestamp_micros();
    let kept = |t: &Table| t.contract().retention_cutoff(now);
    let from = [kept(raw), kept(&rollup)].into_iter().flatten().max();
    let from = from.map_or(-FAR, |t| ceil(t, def.width));
    let clean = complement(&dirty, from, FAR);
    let rollup_files = rollup.files().await.ok()?;
    let raw_rows_per_row = rows(files) / rows(&rollup_files).max(1);
    Some(Candidate { def, rollup: rollup.without_check(), clean, raw_rows_per_row })
}

/// The rows of `files`.
fn rows(files: &[LiveEntry]) -> u64 {
    files.iter().map(|f| f.data_file.record_count()).sum()
}

/// Whether queries on `table` may be routed with session `options`: routing is on for it, and it
/// is read with dedup where it has dedup on, since a rollup counts each reading once.
fn routable(table: &Table, options: &ConfigOptions) -> bool {
    let contract = table.contract();
    ROLLUP_ROUTING.of(options, contract) && (!contract.dedup || READ_DEDUP.of(options, contract))
}

/// The start of the first bucket at or after `t`.
fn ceil(t: i64, width: i64) -> i64 {
    let f = floor(t, width);
    if f == t { f } else { f.saturating_add(width) }
}

/// `[from, to)` minus the (sorted, merged) `ranges`.
fn complement(ranges: &[Range<i64>], from: i64, to: i64) -> Ranges {
    let mut out = vec![];
    let mut start = from;
    for r in ranges {
        if r.start > start {
            out.push(start..r.start.min(to));
        }
        start = start.max(r.end);
    }
    if start < to {
        out.push(start..to);
    }
    out.retain(|r| !r.is_empty());
    out
}

/// The optimizer rule.
#[derive(Debug)]
pub(super) struct RollupRewrite {
    pub(super) routes: RouteMap,
}

impl OptimizerRule for RollupRewrite {
    fn name(&self) -> &'static str {
        "timelake_rollup_routing"
    }

    fn rewrite(&self, plan: LogicalPlan, config: &dyn OptimizerConfig) -> DfResult<Transformed<LogicalPlan>> {
        let options = config.options();
        plan.transform_down(|node| {
            let LogicalPlan::Aggregate(a) = &node else { return Ok(Transformed::no(node)) };
            match self.route(a, &options)? {
                Some(routed) => Ok(Transformed::yes(routed)),
                None => Ok(Transformed::no(node)),
            }
        })
    }
}

/// What a query asks of a raw table, read off its plan.
struct Query<'a> {
    table: Table,
    time: String,
    /// The bucket width, or `None` for a query over its whole time range.
    width: Option<i64>,
    keys: HashSet<&'a str>,
    aggregates: Vec<(RollupFunction, Option<String>)>,
    /// Inclusive bounds on the time column, in microseconds.
    from: i64,
    to: i64,
    /// Filters on grouping columns, which apply to the rollup's rows as they do to the raw rows.
    key_filters: Vec<&'a Expr>,
    /// Semi-joins on grouping columns (`key IN (SELECT …)`, as DataFusion plans it): the right side, and each
    /// pair of equal expressions, the left one of a grouping column (a cast of it, say). They keep the rollup's rows
    /// as they keep the raw rows.
    key_joins: Vec<(&'a LogicalPlan, &'a [(Expr, Expr)])>,
}

impl RollupRewrite {
    /// The routes prepared for this version of `table`, if any.
    fn routes_of(&self, table: &Table) -> Option<Arc<Routes>> {
        let routes = self.routes.read().unwrap_or_else(PoisonError::into_inner).get(table.name()).cloned();
        routes.filter(|r| r.version == table.version())
    }

    fn route(&self, a: &Aggregate, options: &ConfigOptions) -> DfResult<Option<LogicalPlan>> {
        let Some(q) = read_query(a) else { return Ok(None) };
        if !routable(&q.table, options) {
            return Ok(None);
        }
        let Some(width) = q.width else { return self.route_range(a, &q, options) };
        let Some(routes) = self.routes_of(&q.table) else { return Ok(None) };
        for candidate in &routes.rollups {
            let Some(answer) = candidate.answers(&q) else { continue };
            let Some(full) = full_buckets(q.from, q.to, width) else { return Ok(None) };
            // A query bucket comes from the rollup only if all of the rollup's buckets in it do.
            let from_rollup: Ranges = candidate
                .clean
                .iter()
                .map(|r| ceil(r.start, width).max(full.start)..floor(r.end, width).min(full.end))
                .filter(|r| !r.is_empty())
                .collect();
            if from_rollup.is_empty() {
                continue;
            }
            return rewrite(a, &q, candidate, &answer, &from_rollup);
        }
        Ok(None)
    }

    /// A query over a time range without a bucket: its aggregates in mergeable form (states,
    /// sums and counts) per bucket of a rollup's width, routed as a bucketed query, then merged by
    /// the query's keys. The widest rollup that answers goes first.
    fn route_range(&self, a: &Aggregate, q: &Query<'_>, options: &ConfigOptions) -> DfResult<Option<LogicalPlan>> {
        let Some(parts) = a.aggr_expr.iter().map(|e| partial(e, &q.time)).collect::<Option<Vec<_>>>() else {
            return Ok(None);
        };
        let Some(routes) = self.routes_of(&q.table) else { return Ok(None) };
        let mut widths: Vec<i64> = routes.rollups.iter().map(|c| c.def.width).collect();
        widths.sort_unstable_by(|x, y| y.cmp(x));
        widths.dedup();
        let input = Arc::new(with_column(a.input.as_ref().clone(), &q.time)?);
        let (qualifier, _) = input.schema().qualified_field_with_unqualified_name(&q.time)?;
        let time = Expr::Column(Column::new(qualifier.cloned(), &q.time));
        let inputs: Vec<Expr> = parts
            .iter()
            .flat_map(|(exprs, _)| exprs.iter().cloned())
            .enumerate()
            .map(|(i, e)| e.alias(format!("__part{i}")))
            .collect();
        for width in widths {
            let stride = lit(time::micros_interval(width));
            let bucket = date_bin().call(vec![stride, time.clone()]).alias("__bucket");
            let groups = a.group_expr.iter().cloned().chain([bucket]).collect();
            let inner = Aggregate::try_new(input.clone(), groups, inputs.clone())?;
            let Some(routed) = self.route(&inner, options)? else { continue };
            // Merged by the query's keys: the inner query's grouping columns, then the merges.
            let schema = routed.schema().clone();
            let keys = (0..a.group_expr.len()).map(|i| Expr::Column(Column::from(schema.qualified_field(i))));
            let mut next = 0;
            let merges = parts.iter().map(|(exprs, merge)| {
                let columns: Vec<Expr> = (next..next + exprs.len()).map(|i| ident(format!("__part{i}"))).collect();
                next += exprs.len();
                merge.finish(&columns)
            });
            let merged = LogicalPlanBuilder::from(routed).aggregate(keys, merges)?.build()?;
            let merged_schema = merged.schema().clone();
            let columns = merged_schema.columns().into_iter().map(Expr::Column);
            let exprs = as_schema(columns, &merged_schema, &a.schema)?;
            let plan = Projection::try_new_with_schema(exprs, Arc::new(merged), a.schema.clone())?;
            return Ok(Some(LogicalPlan::Projection(plan)));
        }
        Ok(None)
    }
}

/// `plan` rewritten bottom-up by `f`, as `transform_up` does, but for the right side of semi-joins:
/// that subquery only filters the query's rows, its scans and columns aren't the query's.
fn transform_rows(
    plan: LogicalPlan,
    f: &mut impl FnMut(LogicalPlan) -> DfResult<Transformed<LogicalPlan>>,
) -> DfResult<Transformed<LogicalPlan>> {
    let node = match plan {
        LogicalPlan::Join(mut join) if join.join_type == JoinType::LeftSemi => {
            let left = transform_rows(Arc::unwrap_or_clone(join.left), f)?;
            join.left = Arc::new(left.data);
            Transformed::new_transformed(LogicalPlan::Join(join), left.transformed)
        }
        other => other.map_children(|child| transform_rows(child, f))?,
    };
    node.transform_parent(f)
}

/// `plan` (a scan under filters, semi-joins, aliases and projections of columns, as [`read_query`] takes it)
/// with column `name` of the scanned table in its output, where it was projected away.
fn with_column(plan: LogicalPlan, name: &str) -> DfResult<LogicalPlan> {
    if plan.schema().has_column_with_unqualified_name(name) {
        return Ok(plan);
    }
    transform_rows(plan, &mut |node| match node {
        LogicalPlan::TableScan(mut scan) => {
            let Ok(index) = scan.source.schema().index_of(name) else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            if let Some(projection) = scan.projection.as_mut().filter(|p| !p.contains(&index)) {
                projection.push(index);
                let projection = Some(projection.clone());
                let s = TableScan::try_new(scan.table_name, scan.source, projection, scan.filters, scan.fetch)?;
                return Ok(Transformed::yes(LogicalPlan::TableScan(s)));
            }
            Ok(Transformed::no(LogicalPlan::TableScan(scan)))
        }
        LogicalPlan::Projection(p) if !p.schema.has_column_with_unqualified_name(name) => {
            let (qualifier, _) = p.input.schema().qualified_field_with_unqualified_name(name)?;
            let column = Expr::Column(Column::new(qualifier.cloned(), name));
            let exprs = p.expr.into_iter().chain([column]).collect();
            Ok(Transformed::yes(LogicalPlan::Projection(Projection::try_new(exprs, p.input)?)))
        }
        other => other.recompute_schema().map(Transformed::yes),
    })
    .map(|t| t.data)
}

/// How the per-bucket parts of an aggregate merge into its value over a whole range.
enum Merge {
    /// Counts add up, to 0 over no rows.
    Count,
    Sum,
    Min,
    Max,
    /// From each bucket's sum and count: exact, where averages times counts would round.
    Avg,
    /// Points, or a signal's or the gaps' runs, merged by the aggregate.
    State(Box<dyn Fn(Expr) -> Expr>),
}

impl Merge {
    fn finish(&self, parts: &[Expr]) -> Expr {
        let part = || parts[0].clone();
        match self {
            Self::Count => coalesce(vec![agg::sum(part()), lit(0i64)]),
            Self::Sum => agg::sum(part()),
            Self::Min => agg::min(part()),
            Self::Max => agg::max(part()),
            Self::Avg => {
                let sum = |p: &Expr| cast(agg::sum(p.clone()), DataType::Float64);
                sum(&parts[0]) / sum(&parts[1])
            }
            Self::State(merge) => merge(part()),
        }
    }
}

/// An aggregate as parts a rollup can answer per bucket, and how they merge; `None` for one that
/// doesn't merge this way (`first_value`, `last_value`, `DISTINCT`, …).
fn partial(e: &Expr, time: &str) -> Option<(Vec<Expr>, Merge)> {
    let Expr::AggregateFunction(f) = unalias(e) else { return None };
    let params = &f.params;
    if params.distinct || params.filter.is_some() || params.null_treatment.is_some() || !params.order_by.is_empty() {
        return None;
    }
    let whole = Expr::AggregateFunction(f.clone());
    let name = f.func.name();
    Some(match name {
        "count" => (vec![whole], Merge::Count),
        "sum" => (vec![whole], Merge::Sum),
        "min" => (vec![whole], Merge::Min),
        "max" => (vec![whole], Merge::Max),
        "avg" => {
            let [x] = params.args.as_slice() else { return None };
            (vec![agg::sum(x.clone()), agg::count(x.clone())], Merge::Avg)
        }
        "m4" => (vec![whole], Merge::State(Box::new(|p| super::m4::m4_udaf().call(vec![p])))),
        _ => {
            if !matches!(params.args.first(), Some(Expr::Column(c)) if c.name == time) {
                return None;
            }
            if let Some(output) = Output::of(name).filter(|o| *o != Output::State) {
                let method = match params.args.get(2) {
                    Some(Expr::Literal(ScalarValue::Utf8(Some(m)), _)) => Some(lit(m.clone())),
                    Some(_) => return None,
                    None => None,
                };
                let state = udaf(Output::State).call(params.args.clone());
                let merge = move |p| udaf(output).call([p].into_iter().chain(method.clone()).collect());
                (vec![state], Merge::State(Box::new(merge)))
            } else {
                let output = gaps::Output::of(name).filter(|o| *o != gaps::Output::State)?;
                let threshold = params.args.get(1)?.clone();
                let state = gaps::udaf(gaps::Output::State).call(params.args.clone());
                let merge = move |p| gaps::udaf(output).call(vec![p, threshold.clone()]);
                (vec![state], Merge::State(Box::new(merge)))
            }
        }
    })
}

/// How a rollup answers a query.
enum Answer {
    /// Its rows as they are: the query's buckets and keys are the rollup's. The rollup columns
    /// holding the query's aggregates, in order.
    Stored(Vec<String>),
    /// Its rows merged into the query's wider buckets or fewer keys: the aggregates over the
    /// rollup's rows giving the query's, in order.
    Merged(Vec<Expr>),
}

impl Candidate {
    /// How this rollup answers the query, if it can: its buckets must divide the query's, and its
    /// keys include the query's.
    fn answers(&self, q: &Query<'_>) -> Option<Answer> {
        let keys: HashSet<&str> = self.def.keys.iter().map(String::as_str).collect();
        let width = q.width?;
        if width % self.def.width != 0 || !q.keys.is_subset(&keys) || !self.enough_rows_per_row(q) {
            return None;
        }
        // At the rollup's own buckets and keys, each row is a query row.
        if self.def.width == width
            && keys == q.keys
            && let Some(columns) = q.aggregates.iter().map(|a| self.as_stored(a)).collect::<Option<_>>()
        {
            return Some(Answer::Stored(columns));
        }
        q.aggregates.iter().map(|a| self.merged(a, q)).collect::<Option<_>>().map(Answer::Merged)
    }

    /// Whether the rollup holds enough readings per row for its stored states to be cheaper to
    /// merge than the readings are to read, if the query needs them.
    fn enough_rows_per_row(&self, q: &Query<'_>) -> bool {
        let needs = q.aggregates.iter().map(|(f, _)| match f {
            RollupFunction::Signal(..) => SIGNAL_STATE_ROWS,
            RollupFunction::M4 | RollupFunction::Gap(..) => STATE_ROWS,
            _ => 0,
        });
        needs.max().unwrap_or(0) <= self.raw_rows_per_row
    }

    /// The rollup column storing `function` of `column`.
    fn stored(&self, function: RollupFunction, column: Option<&str>) -> Option<String> {
        let a = self.def.aggregates.iter().find(|a| a.function == function && a.column.as_deref() == column)?;
        Some(a.name.clone())
    }

    /// The rollup column storing a signal aggregate's state (its segments) that answers `output`
    /// of `column` by `method`: any of the same column and method (a counter's growth doesn't
    /// depend on the method), once merged. A state of another layout (an older version's) answers
    /// nothing.
    fn signal_state(&self, output: Output, method: Method, column: Option<&str>) -> Option<String> {
        let fits = |m: Method| m == method || !output.takes_method();
        let a =
            self.def.aggregates.iter().find(|a| {
                a.column.as_deref() == column && matches!(a.function, RollupFunction::Signal(_, m) if fits(m))
            })?;
        self.in_layout(&a.name, &state_type())
    }

    /// The rollup column storing the gap aggregates' state for `threshold`, which answers both.
    fn gap_state(&self, threshold: i64) -> Option<String> {
        let a =
            self.def.aggregates.iter().find(|a| matches!(a.function, RollupFunction::Gap(_, t) if t == threshold))?;
        self.in_layout(&a.name, &gaps::state_type())
    }

    /// `column`, if the rollup stores it in the layout of `state`.
    fn in_layout(&self, column: &str, state: &DataType) -> Option<String> {
        let schema = self.rollup.schema();
        let current = schema.field_with_name(column).is_ok_and(|f| layout(f.data_type()) == layout(state));
        current.then(|| column.to_string())
    }

    /// The rollup column holding aggregate `a` as it is, at the rollup's own buckets and keys:
    /// stored values and states. A signal or gap aggregate's value has to be computed from its
    /// state.
    fn as_stored(&self, &(function, ref column): &(RollupFunction, Option<String>)) -> Option<String> {
        let column = column.as_deref();
        match function {
            RollupFunction::Signal(Output::State, method) => self.signal_state(Output::State, method, column),
            RollupFunction::Gap(gaps::Output::State, threshold) => self.gap_state(threshold),
            RollupFunction::Signal(..) | RollupFunction::Gap(..) => None,
            plain => self.stored(plain, column),
        }
    }

    /// Aggregate `a` over the rollup's rows merged into the query's wider buckets or fewer keys.
    fn merged(&self, &(function, ref column): &(RollupFunction, Option<String>), q: &Query<'_>) -> Option<Expr> {
        let stored = |function| self.stored(function, column.as_deref()).map(ident);
        let by_bucket = || vec![ident(&self.def.bucket).sort(true, false)];
        Some(match function {
            // `count(column)` is `count(*)` where the column can't be null.
            RollupFunction::Count => match column {
                Some(c) => agg::sum(stored(function).or_else(|| self.count_of(c, q).map(ident))?),
                None => agg::sum(stored(function)?),
            },
            RollupFunction::Sum => agg::sum(stored(function)?),
            RollupFunction::Min => agg::min(stored(function)?),
            RollupFunction::Max => agg::max(stored(function)?),
            RollupFunction::FirstValue => agg::first_value(stored(function)?, by_bucket()),
            RollupFunction::LastValue => agg::last_value(stored(function)?, by_bucket()),
            // The stored points merge into the wider bucket's.
            RollupFunction::M4 => super::m4::m4_udaf().call(vec![stored(function)?]),
            // A mean of means weighted by the rows each counted.
            RollupFunction::Avg => {
                let n = self.count_of(column.as_deref()?, q)?;
                let total = match stored(RollupFunction::Sum) {
                    Some(sum) => sum,
                    // Rules run after type coercion: the operands' types must match already.
                    None => stored(RollupFunction::Avg)? * cast(ident(n.clone()), DataType::Float64),
                };
                cast(agg::sum(total), DataType::Float64) / cast(agg::sum(ident(n)), DataType::Float64)
            }
            RollupFunction::Signal(output, method) => {
                let mut args = vec![ident(self.signal_state(output, method, column.as_deref())?)];
                if method == Method::Linear {
                    args.push(lit("linear"));
                }
                udaf(output).call(args)
            }
            RollupFunction::Gap(output, threshold) => {
                gaps::udaf(output).call(vec![ident(self.gap_state(threshold)?), gaps::threshold_literal(threshold)])
            }
        })
    }

    /// The rollup column counting a bucket's non-null values of `column`: `count(column)`, or
    /// `count(*)` where the column can't be null.
    fn count_of(&self, column: &str, q: &Query<'_>) -> Option<String> {
        let counts = |c: Option<&str>| {
            self.def
                .aggregates
                .iter()
                .find(|a| a.function == RollupFunction::Count && a.column.as_deref() == c)
                .map(|a| a.name.clone())
        };
        let required = q.table.schema().field_with_name(column).is_ok_and(|f| !f.is_nullable());
        counts(Some(column)).or_else(|| if required { counts(None) } else { None })
    }
}

/// The query's shape, if it is a bucketed aggregate of a Timelake table not routed yet.
fn read_query(a: &Aggregate) -> Option<Query<'_>> {
    let mut filters: Vec<&Expr> = vec![];
    let mut joins = vec![];
    let scan = scan_under(&a.input, |node| match node {
        LogicalPlan::Filter(f) => {
            filters.extend(split_conjunction(&f.predicate));
            Some(&f.input)
        }
        LogicalPlan::Join(j) if j.join_type == JoinType::LeftSemi && j.filter.is_none() => {
            joins.push((j.right.as_ref(), j.on.as_slice()));
            Some(&j.left)
        }
        _ => None,
    })?;
    if scan.fetch.is_some() {
        return None;
    }
    let p = TimelakeProvider::of_scan(scan)?;
    if p.kind() != Kind::Data || p.is_routed() {
        return None;
    }
    let table = p.table().clone();
    let time = table.contract().time_column.clone()?;
    filters.extend(scan.filters.iter());

    let (width, keys) = grouping(&a.group_expr, &time)?;
    let aggregates = a.aggr_expr.iter().map(|e| aggregate_of(e, &time)).collect::<Option<Vec<_>>>()?;
    let on_key = |e: &Expr| {
        let columns = e.column_refs();
        columns.len() == 1 && columns.iter().all(|c| keys.contains(c.name.as_str()))
    };
    if !joins.iter().all(|(_, on)| on.iter().all(|(l, _)| on_key(l))) {
        return None;
    }
    let (from, to, key_filters) = split_filters(filters, &keys, &time)?;
    Some(Query { table, time, width, keys, aggregates, from, to, key_filters, key_joins: joins })
}

/// What a query groups by: a bucket's width (none for the whole time range), and key columns.
fn grouping<'a>(group_expr: &'a [Expr], time: &str) -> Option<(Option<i64>, HashSet<&'a str>)> {
    let mut width = None;
    let mut keys = HashSet::new();
    for g in group_expr {
        match (bucket_width(g, time), unalias(g)) {
            (Some(w), _) if width.is_none() => width = Some(w),
            (None, Expr::Column(c)) => {
                keys.insert(c.name.as_str());
            }
            _ => return None,
        }
    }
    Some((width, keys))
}

/// A query's filters as inclusive bounds (µs) on the time column, and the filters on grouping
/// columns `keys`; `None` if one is neither.
fn split_filters<'a>(filters: Vec<&'a Expr>, keys: &HashSet<&str>, time: &str) -> Option<(i64, i64, Vec<&'a Expr>)> {
    let (mut from, mut to) = (i64::MIN, i64::MAX);
    let mut key_filters = vec![];
    for f in filters {
        let columns = f.column_refs();
        if !columns.is_empty() && columns.iter().all(|c| keys.contains(c.name.as_str())) {
            if !key_filters.contains(&f) {
                key_filters.push(f);
            }
            continue;
        }
        let (lo, hi) = time_bounds(f, time)?;
        (from, to) = (from.max(lo), to.min(hi));
    }
    Some((from, to, key_filters))
}

/// The inclusive bounds (µs) a comparison of the time column with a constant puts on it.
fn time_bounds(e: &Expr, time: &str) -> Option<(i64, i64)> {
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (op, value) = match (is_time(left, time), is_time(right, time)) {
                (true, false) => (*op, nanos(right)?),
                (false, true) => (op.swap()?, nanos(left)?),
                _ => return None,
            };
            let (floor_us, ceil_us) = (value.div_euclid(1000), -(-value).div_euclid(1000));
            let (lo, hi) = match op {
                Operator::Gt => (floor_us + 1, i128::from(i64::MAX)),
                Operator::GtEq => (ceil_us, i128::from(i64::MAX)),
                Operator::Lt => (i128::from(i64::MIN), ceil_us - 1),
                Operator::LtEq => (i128::from(i64::MIN), floor_us),
                Operator::Eq => (ceil_us, floor_us),
                _ => return None,
            };
            Some((clamp(lo), clamp(hi)))
        }
        Expr::Between(b) if !b.negated && is_time(&b.expr, time) => {
            let lo = -(-nanos(&b.low)?).div_euclid(1000);
            let hi = nanos(&b.high)?.div_euclid(1000);
            Some((clamp(lo), clamp(hi)))
        }
        _ => None,
    }
}

fn clamp(v: i128) -> i64 {
    i64::try_from(v).unwrap_or(if v < 0 { i64::MIN } else { i64::MAX })
}

/// A timestamp constant in nanoseconds.
fn nanos(e: &Expr) -> Option<i128> {
    let Expr::Literal(v, _) = e else { return None };
    time::timestamp_nanos(v)
}

/// The buckets entirely within `[from, to]` (inclusive µs), as a half-open range of bucket starts.
fn full_buckets(from: i64, to: i64, width: i64) -> Option<Range<i64>> {
    let first = if from == i64::MIN { -FAR } else { ceil(from, width).max(-FAR) };
    let last = if to == i64::MAX { FAR } else { floor(to.checked_add(1)?, width).min(FAR) };
    (first < last).then_some(first..last)
}

/// `UNION ALL` of the aggregate over the raw rows outside `from_rollup` and the rollup's rows in
/// it, under the aggregate's own schema; `None` if the time column isn't in microseconds.
fn rewrite(
    a: &Aggregate,
    q: &Query<'_>,
    candidate: &Candidate,
    answer: &Answer,
    from_rollup: &[Range<i64>],
) -> DfResult<Option<LogicalPlan>> {
    let Some((raw, tz)) = raw_side(a, q, from_rollup)? else { return Ok(None) };
    let builder = rollup_rows(q, candidate, from_rollup, tz.as_ref())?;
    let from_rollup = rollup_side(a, q, candidate, answer, builder)?;
    let union = LogicalPlan::Union(Union::try_new_with_loose_types(vec![Arc::new(raw), Arc::new(from_rollup)])?);
    // A union's columns have no qualifier: give them the aggregate's back for the plan above.
    let names: Vec<Expr> = union
        .schema()
        .fields()
        .iter()
        .zip(a.schema.iter())
        .map(|(f, (qualifier, original))| {
            Expr::Column(Column::new_unqualified(f.name())).alias_qualified(qualifier.cloned(), original.name())
        })
        .collect();
    Ok(Some(LogicalPlan::Projection(Projection::try_new_with_schema(names, Arc::new(union), a.schema.clone())?)))
}

/// The aggregate over the raw rows outside `from_rollup`, and the time column's time zone; `None`
/// if the time column isn't in microseconds.
fn raw_side(
    a: &Aggregate,
    q: &Query<'_>,
    from_rollup: &[Range<i64>],
) -> DfResult<Option<(LogicalPlan, Option<Arc<str>>)>> {
    let input_schema = a.input.schema();
    let (qualifier, field) = input_schema.qualified_field_with_unqualified_name(&q.time)?;
    let DataType::Timestamp(TimeUnit::Microsecond, tz) = field.data_type() else { return Ok(None) };
    let time = Expr::Column(Column::new(qualifier.cloned(), &q.time));
    let at = |t: i64| time::timestamp_literal(t, tz.clone());
    // Open ends (no time filter on that side) need no bound.
    let (open_from, open_to) = (FAR.saturating_neg(), FAR);
    let outside = from_rollup
        .iter()
        .filter_map(|r| {
            let before = (r.start > open_from).then(|| time.clone().lt(at(r.start)));
            let after = (r.end < open_to).then(|| time.clone().gt_eq(at(r.end)));
            match (before, after) {
                (Some(b), Some(a)) => Some(b.or(a)),
                (b, a) => b.or(a),
            }
        })
        .reduce(Expr::and)
        .unwrap_or_else(|| lit(false));
    let input = mark_routed(a.input.as_ref().clone())?;
    let raw = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(LogicalPlan::Filter(Filter::try_new(outside, Arc::new(input))?)),
        a.group_expr.clone(),
        a.aggr_expr.clone(),
    )?);
    Ok(Some((raw, tz.clone())))
}

/// The aggregate's columns from the rollup's rows that `builder` reads, as `answer` says.
fn rollup_side(
    a: &Aggregate,
    q: &Query<'_>,
    candidate: &Candidate,
    answer: &Answer,
    mut builder: LogicalPlanBuilder,
) -> DfResult<LogicalPlan> {
    // The rollup side's column for each of the aggregate's outputs.
    let outputs: Vec<String> = match answer {
        Answer::Stored(columns) => {
            let groups = a.group_expr.iter().map(|g| match unalias(g) {
                Expr::Column(c) => c.name.clone(),
                _ => candidate.def.bucket.clone(),
            });
            groups.chain(columns.iter().cloned()).collect()
        }
        Answer::Merged(aggregates) => {
            // The query's grouping over the rollup's rows: its bucket of the bucket column.
            let groups = a.group_expr.iter().enumerate().map(|(i, g)| {
                let g = unqualified(unalias(g).clone())?.transform(|e| match e {
                    Expr::Column(c) if c.name == q.time => Ok(Transformed::yes(ident(&candidate.def.bucket))),
                    other => Ok(Transformed::no(other)),
                });
                Ok(g?.data.alias(format!("g{i}")))
            });
            let merged = aggregates.iter().enumerate().map(|(i, e)| e.clone().alias(format!("a{i}")));
            builder = builder.aggregate(groups.collect::<DfResult<Vec<_>>>()?, merged)?;
            let groups = (0..a.group_expr.len()).map(|i| format!("g{i}"));
            groups.chain((0..aggregates.len()).map(|i| format!("a{i}"))).collect()
        }
    };
    let exprs = as_schema(outputs.iter().map(ident), builder.schema(), &a.schema)?;
    builder.project(exprs)?.build()
}

/// `columns` of `input`, each cast to the type of the field of `schema` at its place, and named
/// as that field.
fn as_schema(columns: impl Iterator<Item = Expr>, input: &DFSchema, schema: &DFSchema) -> DfResult<Vec<Expr>> {
    columns
        .zip(schema.iter())
        .map(|(mut e, (qualifier, field))| {
            if e.get_type(input)? != *field.data_type() {
                e = e.cast_to(field.data_type(), input)?;
            }
            Ok(e.alias_qualified(qualifier.cloned(), field.name()))
        })
        .collect()
}

/// The rollup's rows in the buckets `from_rollup`, kept by the query's filters and semi-joins on grouping columns.
/// `tz` is the raw time column's time zone, for a bucket column without one.
fn rollup_rows(
    q: &Query<'_>,
    candidate: &Candidate,
    from_rollup: &[Range<i64>],
    tz: Option<&Arc<str>>,
) -> DfResult<LogicalPlanBuilder> {
    let at = |t: i64, tz: &Option<Arc<str>>| time::timestamp_literal(t, tz.clone());
    let (open_from, open_to) = (FAR.saturating_neg(), FAR);
    let rollup = &candidate.rollup;
    let reference = TableReference::partial(rollup.name().namespace(), rollup.name().name());
    let source = provider_as_source(Arc::new(TimelakeProvider::data(rollup.clone())));
    let bucket_tz = match rollup.schema().field_with_name(&candidate.def.bucket)?.data_type() {
        DataType::Timestamp(_, tz) => tz.clone(),
        _ => tz.cloned(),
    };
    let bucket = ident(&candidate.def.bucket);
    let inside = from_rollup
        .iter()
        .map(|r| {
            let from = (r.start > open_from).then(|| bucket.clone().gt_eq(at(r.start, &bucket_tz)));
            let to = (r.end < open_to).then(|| bucket.clone().lt(at(r.end, &bucket_tz)));
            match (from, to) {
                (Some(f), Some(t)) => f.and(t),
                (f, t) => f.or(t).unwrap_or_else(|| datafusion::prelude::lit(true)),
            }
        })
        .reduce(Expr::or)
        .expect("ranges");
    let mut builder = LogicalPlanBuilder::scan(reference.clone(), source, None)?.filter(inside)?;
    for f in &q.key_filters {
        builder = builder.filter(unqualified((*f).clone())?)?;
    }
    for (right, on) in &q.key_joins {
        // The left expressions over the rollup's columns, qualified so they can't be the right side's.
        let on = on.iter().map(|(l, r)| {
            let l = l.clone().transform(|e| match e {
                Expr::Column(c) => Ok(Transformed::yes(Expr::Column(Column::new(Some(reference.clone()), c.name)))),
                other => Ok(Transformed::no(other)),
            })?;
            Ok(l.data.eq(r.clone()))
        });
        builder = builder.join_on((*right).clone(), JoinType::LeftSemi, on.collect::<DfResult<Vec<_>>>()?)?;
    }
    Ok(builder)
}

/// `plan` with its Timelake scan marked as routed, so the rule leaves it alone from now on; not the
/// scans of a semi-join's subquery, which may still be routed themselves.
fn mark_routed(plan: LogicalPlan) -> DfResult<LogicalPlan> {
    transform_rows(plan, &mut |node| {
        let LogicalPlan::TableScan(scan) = &node else { return Ok(Transformed::no(node)) };
        let Some(p) = TimelakeProvider::of_scan(scan) else { return Ok(Transformed::no(node)) };
        let mut scan = scan.clone();
        scan.source = provider_as_source(Arc::new(p.routed()));
        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
    })
    .map(|t| t.data)
}

/// `e` with its columns unqualified, to apply to the rollup's rows.
fn unqualified(e: Expr) -> DfResult<Expr> {
    e.transform(|e| match e {
        Expr::Column(c) => Ok(Transformed::yes(Expr::Column(Column::new_unqualified(c.name)))),
        other => Ok(Transformed::no(other)),
    })
    .map(|t| t.data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complement_and_full_buckets() {
        assert_eq!(complement(&[10..20, 30..40], 0, 50), vec![0..10, 20..30, 40..50]);
        assert_eq!(complement(std::slice::from_ref(&(0..60)), 0, 60), vec![]);
        assert_eq!(complement(&[], 5, 10), vec![5..10]);
        // [30, 179] holds the 60-wide buckets at 60 and 120 whole; [30, 178] only the one at 60.
        assert_eq!(full_buckets(30, 179, 60), Some(60..180));
        assert_eq!(full_buckets(30, 178, 60), Some(60..120));
        assert_eq!(full_buckets(60, 119, 60), Some(60..120));
        assert_eq!(full_buckets(61, 119, 60), None);
        assert_eq!(ceil(61, 60), 120);
    }

    #[test]
    fn time_comparisons_bound_the_time_column() {
        let t = || ident("time");
        let us = |v: i64| Expr::Literal(ScalarValue::TimestampMicrosecond(Some(v), None), None);
        let ns = |v: i64| Expr::Literal(ScalarValue::TimestampNanosecond(Some(v), None), None);
        assert_eq!(time_bounds(&t().gt_eq(us(5)), "time"), Some((5, i64::MAX)));
        assert_eq!(time_bounds(&t().gt(us(5)), "time"), Some((6, i64::MAX)));
        assert_eq!(time_bounds(&us(5).gt(t()), "time"), Some((i64::MIN, 4)));
        assert_eq!(time_bounds(&t().lt(ns(5_500)), "time"), Some((i64::MIN, 5)));
        assert_eq!(time_bounds(&t().lt_eq(ns(5_500)), "time"), Some((i64::MIN, 5)));
        assert_eq!(time_bounds(&t().gt(ns(5_500)), "time"), Some((6, i64::MAX)));
        assert_eq!(time_bounds(&t().gt_eq(ns(5_500)), "time"), Some((6, i64::MAX)));
        assert_eq!(time_bounds(&t().not_eq(us(5)), "time"), None);
        assert_eq!(time_bounds(&ident("value").gt(us(5)), "time"), None);
    }
}
