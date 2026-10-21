//! Rollups in DataFusion terms (spec 27): a rollup's definition read from a planned `SELECT`,
//! the query that computes its buckets, and the expressions routing matches against it.

use std::ops::Range;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::functions_aggregate::expr_fn as agg;
use datafusion::logical_expr::expr::{AggregateFunction, Sort};
use datafusion::logical_expr::{Aggregate as AggregateNode, Expr, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionContext, ident, lit};

use super::provider::{Kind, TimelakeProvider, scan_under};
use super::session::Session;
use super::signal::{Method, Output, udaf};
use super::time;
use crate::error::{Error, Result};
use crate::lake::Lake;
use crate::rollup::{Aggregate, Definition, RollupFunction};
use crate::table::Table;
use timelake_format::rollup::{FunctionFamily, GapFunction};

/// The width of `date_bin(width, <time>)` (`time_bucket` is its alias) in microseconds: a fixed
/// width and the default origin. `None` for any other expression.
pub(crate) fn bucket_width(e: &Expr, time: &str) -> Option<i64> {
    let Expr::ScalarFunction(f) = unalias(e) else { return None };
    if f.func.name() != "date_bin" {
        return None;
    }
    let (stride, column) = match f.args.as_slice() {
        [stride, column] => (stride, column),
        [stride, column, origin] if is_epoch(origin) => (stride, column),
        _ => return None,
    };
    if !is_time(column, time) {
        return None;
    }
    let Expr::Literal(stride, _) = stride else { return None };
    // Microsecond times have no buckets of fractions of a microsecond.
    if matches!(stride, ScalarValue::IntervalMonthDayNano(Some(v)) if v.nanoseconds % 1000 != 0) {
        return None;
    }
    let width = time::interval_micros(stride)?;
    (width > 0).then_some(width)
}

/// `e` without the aliases around it.
pub(crate) fn unalias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => unalias(&a.expr),
        other => other,
    }
}

/// The time column, or the time column cast to a timestamp at least as fine (no truncation), as
/// DataFusion does for `date_bin` and comparisons with `now()`.
pub(crate) fn is_time(e: &Expr, time: &str) -> bool {
    match e {
        Expr::Column(c) => c.name == time,
        Expr::Cast(c) => {
            matches!(c.field.data_type(), DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _))
                && is_time(&c.expr, time)
        }
        _ => false,
    }
}

fn is_epoch(e: &Expr) -> bool {
    matches!(e, Expr::Literal(v, _) if time::timestamp_nanos(v) == Some(0))
}

/// The function and column of an aggregate a rollup can store (`None` column for `count(*)`).
pub(crate) fn aggregate_of(e: &Expr, time: &str) -> Option<(RollupFunction, Option<String>)> {
    let Expr::AggregateFunction(AggregateFunction { func, params }) = unalias(e) else { return None };
    if params.distinct || params.filter.is_some() || params.null_treatment.is_some() {
        return None;
    }
    let name = func.name();
    // The gap aggregates take `(time, threshold)`: the threshold is part of what is stored.
    if let Some(output) = GapFunction::of(name) {
        return match params.args.as_slice() {
            [Expr::Column(t), Expr::Literal(threshold, _)] if t.name == time && params.order_by.is_empty() => {
                let threshold = super::gaps::threshold_of_literal(threshold)?;
                Some((RollupFunction::Gap(output, threshold), None))
            }
            _ => None,
        };
    }
    // m4 and the signal aggregates take `(time, value)`: they're of `value`.
    let signal = Output::of(name);
    let readings = match (name, signal) {
        ("m4", _) => Some(RollupFunction::M4),
        (_, Some(output)) => Some(RollupFunction::signal(output, Method::ForwardFill)),
        _ => None,
    };
    let (function, column) = match (params.args.as_slice(), readings) {
        ([Expr::Column(c)], None) => (name.parse().ok()?, Some(c.name.clone())),
        ([Expr::Column(t), Expr::Column(c)], Some(f)) if t.name == time => (f, Some(c.name.clone())),
        // A signal aggregate's method is part of what is stored: `integral(time, value, 'linear')`.
        ([Expr::Column(t), Expr::Column(c), Expr::Literal(ScalarValue::Utf8(Some(m)), _)], _) if t.name == time => {
            let output = signal.filter(|o| o.takes_method())?;
            (RollupFunction::signal(output, m.parse().ok()?), Some(c.name.clone()))
        }
        ([Expr::Literal(ScalarValue::Int64(Some(1)), _)], None) if name == "count" => (RollupFunction::Count, None),
        _ => return None,
    };
    let by_time =
        matches!(params.order_by.as_slice(), [Sort { expr: Expr::Column(c), asc: true, .. }] if c.name == time);
    let fits = match function {
        RollupFunction::FirstValue | RollupFunction::LastValue => by_time && column.is_some(),
        _ => params.order_by.is_empty(),
    };
    fits.then_some((function, column))
}

/// The expression computing `a` over the raw table.
fn aggregate_expr(a: &Aggregate, time: &str) -> Result<Expr> {
    let column =
        || a.column.as_deref().map(ident).ok_or_else(|| Error::invalid(format!("{} needs a column", a.function)));
    let by_time = || vec![ident(time).sort(true, false)];
    Ok(match a.function {
        RollupFunction::Count => agg::count(a.column.as_deref().map_or_else(|| lit(1i64), ident)),
        RollupFunction::Sum => agg::sum(column()?),
        RollupFunction::Min => agg::min(column()?),
        RollupFunction::Max => agg::max(column()?),
        RollupFunction::Avg => agg::avg(column()?),
        RollupFunction::FirstValue => agg::first_value(column()?, by_time()),
        RollupFunction::LastValue => agg::last_value(column()?, by_time()),
        RollupFunction::M4 => super::m4::m4_udaf().call(vec![ident(time), column()?]),
        // The state: a bucket's runs, which wider buckets merge.
        RollupFunction::Signal(_, Method::Linear) => {
            udaf(Output::State).call(vec![ident(time), column()?, lit("linear")])
        }
        RollupFunction::Signal(_, Method::ForwardFill) => udaf(Output::State).call(vec![ident(time), column()?]),
        RollupFunction::Gap(_, threshold) => {
            super::gaps::udaf(GapFunction::State).call(vec![ident(time), super::gaps::threshold_literal(threshold)])
        }
    })
}

/// A session for computing `rollup` from `raw`: reads with dedup on, whatever the table says, so
/// each reading counts once, and routes to the raw table's other rollups, so a rollup is computed
/// from a finer one where that one is up to date (hours from minutes) instead of from every row.
pub(crate) async fn refresh_session(
    lake: &Lake,
    raw: &crate::TableName,
    rollup: &crate::TableName,
) -> Result<SessionContext> {
    let mut config = Session::config();
    config.options_mut().set("timelake.read_dedup", "true")?;
    let session = Session::with_config(lake.clone(), config);
    let options = session.context().state().config().options().clone();
    super::route::prepare(lake, raw, &options, &session.routes, Some(rollup)).await;
    Ok(session.context().clone())
}

/// The buckets of rollup `def` computed from `raw`, for the rows in `ranges` (all rows for `None`):
/// the bucket, the keys, then the aggregates.
pub(crate) fn rollup_frame(
    ctx: &SessionContext,
    raw: &Table,
    def: &Definition,
    ranges: Option<&[Range<i64>]>,
) -> Result<DataFrame> {
    let time = raw
        .contract()
        .time_column
        .clone()
        .ok_or_else(|| Error::invalid(format!("{} has no time column", raw.name())))?;
    let DataType::Timestamp(TimeUnit::Microsecond, tz) = raw.schema().field_with_name(&time)?.data_type().clone()
    else {
        return Err(Error::invalid(format!("{}.{time} must be a microsecond timestamp", raw.name())));
    };
    let mut df = ctx.read_table(Arc::new(TimelakeProvider::data(raw.clone())))?;
    if let Some(ranges) = ranges {
        let at = |t: i64| time::timestamp_literal(t, tz.clone());
        let predicate = ranges
            .iter()
            .map(|r| ident(&time).gt_eq(at(r.start)).and(ident(&time).lt(at(r.end))))
            .reduce(Expr::or)
            .unwrap_or_else(|| lit(false));
        df = df.filter(predicate)?;
    }
    let bucket = datafusion::functions::datetime::date_bin()
        .call(vec![lit(time::micros_interval(def.width)), ident(&time)])
        .alias(&def.bucket);
    let group = std::iter::once(bucket).chain(def.keys.iter().map(ident)).collect();
    let aggregates =
        def.aggregates.iter().map(|a| Ok(aggregate_expr(a, &time)?.alias(&a.name))).collect::<Result<Vec<_>>>()?;
    Ok(df.aggregate(group, aggregates)?)
}

/// Output columns of a rollup query: (name, aggregate output index, whether the query named it).
type Outputs = Vec<(String, usize, bool)>;

/// The raw table and the rollup a planned `SELECT … FROM <raw> GROUP BY time_bucket(…), keys…`
/// defines (`name` is the rollup's), with its output columns in the query's order.
pub(crate) fn definition_of(plan: &LogicalPlan, name: &crate::TableName) -> Result<(Table, Definition, Vec<String>)> {
    let fail = |msg: String| Error::invalid(format!("rollup {name}: {msg}"));
    let (outputs, node) = outputs_of(plan).map_err(fail)?;
    let raw = raw_table_of(node).map_err(fail)?;
    let Some(time) = raw.contract().time_column.clone() else { return Err(fail(SHAPE.into())) };
    let (width, keys) = grouping_of(node, &raw, &time).map_err(fail)?;
    let selected = Selected::of(outputs, node, &keys, &time).map_err(fail)?;
    let Some(bucket) = selected.bucket else { return Err(fail("select the time_bucket".into())) };
    if selected.keys.len() != keys.iter().flatten().count() {
        return Err(fail("select every grouping column".into()));
    }
    if selected.aggregates.is_empty() {
        return Err(fail("select at least one aggregate".into()));
    }
    let (keys, aggregates) = (selected.keys, selected.aggregates);
    let def = Definition { source: raw.name().clone(), width, bucket, keys, aggregates };
    Ok((raw, def, selected.order))
}

/// What a rollup query groups by: the bucket width, and for each grouping expression the
/// series-key column it is (`None` for the bucket).
fn grouping_of(
    node: &AggregateNode,
    raw: &Table,
    time: &str,
) -> std::result::Result<(i64, Vec<Option<String>>), String> {
    let series_key = &raw.contract().series_key;
    let mut width = None;
    let mut keys: Vec<Option<String>> = vec![None; node.group_expr.len()];
    for (i, g) in node.group_expr.iter().enumerate() {
        if let Some(w) = bucket_width(g, time) {
            if width.replace(w).is_some() {
                return Err("group by one time_bucket".into());
            }
        } else if let Expr::Column(c) = unalias(g)
            && series_key.contains(&c.name)
        {
            keys[i] = Some(c.name.clone());
        } else {
            let series_key = series_key.join(", ");
            return Err(format!(
                "group by time_bucket(<fixed width>, {time}) and series-key columns ({series_key}), not {g}"
            ));
        }
    }
    let Some(width) = width else { return Err(format!("group by time_bucket(<width>, {time})")) };
    if width % time::SECOND_MICROS != 0 {
        return Err("the bucket width must be whole seconds".into());
    }
    Ok((width, keys))
}

/// The columns a rollup query selects: the bucket's name, the keys, the aggregates, and all of
/// them in the query's order.
struct Selected {
    bucket: Option<String>,
    keys: Vec<String>,
    aggregates: Vec<Aggregate>,
    order: Vec<String>,
}

impl Selected {
    /// The columns of `outputs`, over `node` grouping by `keys` (as [`grouping_of`] gives them).
    fn of(
        outputs: Outputs,
        node: &AggregateNode,
        keys: &[Option<String>],
        time: &str,
    ) -> std::result::Result<Self, String> {
        let mut selected = Self { bucket: None, keys: vec![], aggregates: vec![], order: vec![] };
        for (output, index, named) in outputs {
            let column = match keys.get(index) {
                Some(Some(key)) if *key == output || !named => {
                    selected.keys.push(key.clone());
                    key.clone()
                }
                Some(Some(key)) => return Err(format!("keep the name of key column {key}")),
                Some(None) => {
                    let b = if named { output } else { "bucket".to_string() };
                    selected.bucket = Some(b.clone());
                    b
                }
                None => {
                    let a = stored_aggregate(&node.aggr_expr[index - keys.len()], output, named, time)?;
                    let name = a.name.clone();
                    selected.aggregates.push(a);
                    name
                }
            };
            selected.order.push(column);
        }
        Ok(selected)
    }
}

/// The aggregate a rollup stores for `e`, in column `output` if the query `named` it.
fn stored_aggregate(e: &Expr, output: String, named: bool, time: &str) -> std::result::Result<Aggregate, String> {
    let Some((function, column)) = aggregate_of(e, time) else {
        return Err(format!(
            "{e} isn't an aggregate a rollup stores: count, sum, min, max, avg, first_value / last_value ORDER BY \
             {time}, m4, a signal aggregate or a gap aggregate"
        ));
    };
    // `integral:linear` of `value` is `integral_linear_value`; `gap_count:300s` is `gap_count_300s`.
    let stem = function.to_string().replace(':', "_");
    let default = column.as_ref().map_or_else(|| stem.clone(), |c| format!("{stem}_{c}"));
    Ok(Aggregate { name: if named { output } else { default }, function, column })
}

const SHAPE: &str = "the query must be SELECT … FROM <time-series table> GROUP BY time_bucket(…), <series-key columns>";

/// The aggregate of a rollup query and its outputs: a projection over it, or itself.
fn outputs_of(plan: &LogicalPlan) -> std::result::Result<(Outputs, &AggregateNode), String> {
    match plan {
        LogicalPlan::Projection(p) => {
            let LogicalPlan::Aggregate(a) = p.input.as_ref() else { return Err(SHAPE.into()) };
            let mut outputs = vec![];
            for e in &p.expr {
                // DataFusion names what the query didn't (`count(*)`, `min(t.value)`): those get
                // plain names here.
                let (output, inner) = match e {
                    Expr::Alias(alias) => (alias.name.clone(), unalias(e)),
                    Expr::Column(c) => (c.name.clone(), e),
                    other => (other.schema_name().to_string(), other),
                };
                let named = !output.contains('(');
                let Expr::Column(c) = inner else {
                    return Err("select only the grouping columns and aggregates".into());
                };
                let index = a.schema.index_of_column(c).map_err(|e| e.to_string())?;
                outputs.push((output, index, named));
            }
            Ok((outputs, a))
        }
        LogicalPlan::Aggregate(a) => {
            let outputs = a.schema.fields().iter().enumerate();
            Ok((outputs.map(|(i, f)| (f.name().clone(), i, !f.name().contains('('))).collect(), a))
        }
        _ => Err(SHAPE.into()),
    }
}

/// The Timelake table a rollup query's aggregate reads, whole (no filters).
fn raw_table_of(node: &AggregateNode) -> std::result::Result<Table, String> {
    let mut filtered = false;
    let scan = scan_under(&node.input, |n| {
        filtered |= matches!(n, LogicalPlan::Filter(_));
        None
    });
    let Some(scan) = scan else {
        return Err(if filtered { "WHERE isn't supported in a rollup" } else { SHAPE }.into());
    };
    match TimelakeProvider::of_scan(scan) {
        Some(p) if p.kind() == Kind::Data && scan.filters.is_empty() && scan.fetch.is_none() => Ok(p.table().clone()),
        _ => Err(SHAPE.into()),
    }
}
