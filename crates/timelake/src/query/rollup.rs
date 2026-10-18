//! Rollups in DataFusion terms (SPEC §11.1): a rollup's definition read from a planned `SELECT`,
//! the query that computes its buckets, and the expressions routing matches against it.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, IntervalMonthDayNano, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::datasource::source_as_provider;
use datafusion::functions_aggregate::expr_fn as agg;
use datafusion::logical_expr::expr::{AggregateFunction, Sort};
use datafusion::logical_expr::{Aggregate as AggregateNode, Expr, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionContext, ident, lit};

use super::provider::{Kind, TimelakeProvider};
use super::session::Session;
use crate::error::{Error, Result};
use crate::lake::Lake;
use crate::rollup::{Aggregate, Definition, FUNCTIONS};
use crate::table::Table;

const DAY_MICROS: i64 = 86_400_000_000;
const SECOND_MICROS: i64 = 1_000_000;

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
    let width = match stride {
        Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(v)), _) if v.months == 0 && v.nanoseconds % 1000 == 0 => {
            i64::from(v.days).checked_mul(DAY_MICROS)?.checked_add(v.nanoseconds / 1000)?
        }
        Expr::Literal(ScalarValue::IntervalDayTime(Some(v)), _) => {
            i64::from(v.days).checked_mul(DAY_MICROS)?.checked_add(i64::from(v.milliseconds) * 1000)?
        }
        _ => return None,
    };
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
    use ScalarValue as S;
    matches!(
        e,
        Expr::Literal(
            S::TimestampSecond(Some(0), _)
                | S::TimestampMillisecond(Some(0), _)
                | S::TimestampMicrosecond(Some(0), _)
                | S::TimestampNanosecond(Some(0), _),
            _
        )
    )
}

/// The function and column of an aggregate a rollup can store (`None` column for `count(*)`).
pub(crate) fn aggregate_of(e: &Expr, time: &str) -> Option<(String, Option<String>)> {
    let Expr::AggregateFunction(AggregateFunction { func, params }) = unalias(e) else { return None };
    if params.distinct || params.filter.is_some() || params.null_treatment.is_some() {
        return None;
    }
    let function = func.name();
    let column = match params.args.as_slice() {
        [Expr::Column(c)] if function != "m4" => Some(c.name.clone()),
        // m4(time, value): the points of `value`.
        [Expr::Column(t), Expr::Column(c)] if function == "m4" && t.name == time => Some(c.name.clone()),
        [Expr::Literal(ScalarValue::Int64(Some(1)), _)] if function == "count" => None,
        _ => return None,
    };
    let by_time =
        matches!(params.order_by.as_slice(), [Sort { expr: Expr::Column(c), asc: true, .. }] if c.name == time);
    let fits = match function {
        "first_value" | "last_value" => by_time && column.is_some(),
        f => FUNCTIONS.contains(&f) && params.order_by.is_empty(),
    };
    fits.then(|| (function.to_string(), column))
}

/// The expression computing `a` over the raw table.
fn aggregate_expr(a: &Aggregate, time: &str) -> Result<Expr> {
    let column =
        || a.column.as_deref().map(ident).ok_or_else(|| Error::invalid(format!("{} needs a column", a.function)));
    let by_time = || vec![ident(time).sort(true, false)];
    Ok(match a.function.as_str() {
        "count" => agg::count(a.column.as_deref().map_or_else(|| lit(1i64), ident)),
        "sum" => agg::sum(column()?),
        "min" => agg::min(column()?),
        "max" => agg::max(column()?),
        "avg" => agg::avg(column()?),
        "first_value" => agg::first_value(column()?, by_time()),
        "last_value" => agg::last_value(column()?, by_time()),
        "m4" => super::m4::m4_udaf().call(vec![ident(time), column()?]),
        other => return Err(Error::invalid(format!("a rollup can't store {other}"))),
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
    super::route::prepare(lake, raw, None, &session.routes, Some(rollup)).await;
    Ok(session.context().clone())
}

/// The buckets of rollup `def` computed from `raw`, for the rows in `ranges` (all rows for `None`):
/// the bucket, the keys, then the aggregates.
pub(crate) fn rollup_frame(
    ctx: &SessionContext,
    raw: &Table,
    def: &Definition,
    ranges: Option<&[(i64, i64)]>,
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
        let at = |t: i64| Expr::Literal(ScalarValue::TimestampMicrosecond(Some(t), tz.clone()), None);
        let predicate = ranges
            .iter()
            .map(|&(lo, hi)| ident(&time).gt_eq(at(lo)).and(ident(&time).lt(at(hi))))
            .reduce(Expr::or)
            .unwrap_or_else(|| lit(false));
        df = df.filter(predicate)?;
    }
    let stride =
        ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(0, 0, def.width.saturating_mul(1000))));
    let bucket = datafusion::functions::datetime::date_bin()
        .call(vec![Expr::Literal(stride, None), ident(&time)])
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
    let fail = |msg: &str| Error::invalid(format!("rollup {name}: {msg}"));
    let (outputs, node) = outputs_of(plan).map_err(|msg| fail(&msg))?;
    let raw = raw_table_of(node).map_err(|msg| fail(&msg))?;
    let contract = raw.contract();
    let Some(time) = contract.time_column.clone() else { return Err(fail(SHAPE)) };

    let n_group = node.group_expr.len();
    let mut width = None;
    let mut keys: Vec<Option<String>> = vec![None; n_group];
    for (i, g) in node.group_expr.iter().enumerate() {
        if let Some(w) = bucket_width(g, &time) {
            if width.replace(w).is_some() {
                return Err(fail("group by one time_bucket"));
            }
        } else if let Expr::Column(c) = unalias(g)
            && contract.series_key.contains(&c.name)
        {
            keys[i] = Some(c.name.clone());
        } else {
            return Err(fail(&format!(
                "group by time_bucket(<fixed width>, {time}) and series-key columns ({}), not {g}",
                contract.series_key.join(", ")
            )));
        }
    }
    let Some(width) = width else { return Err(fail(&format!("group by time_bucket(<width>, {time})"))) };
    if width % SECOND_MICROS != 0 {
        return Err(fail("the bucket width must be whole seconds"));
    }

    let (mut bucket, mut key_names, mut aggregates, mut order) = (None, vec![], vec![], vec![]);
    for (output, index, named) in outputs {
        let column = if index < n_group {
            match &keys[index] {
                Some(key) if *key == output || !named => {
                    key_names.push(key.clone());
                    key.clone()
                }
                Some(key) => return Err(fail(&format!("keep the name of key column {key}"))),
                None => {
                    let b = if named { output } else { "bucket".to_string() };
                    bucket = Some(b.clone());
                    b
                }
            }
        } else {
            let e = &node.aggr_expr[index - n_group];
            let Some((function, column)) = aggregate_of(e, &time) else {
                return Err(fail(&format!(
                    "{e} isn't an aggregate a rollup stores: count, sum, min, max, avg, or first_value / last_value \
                     ORDER BY {time}"
                )));
            };
            let default = column.as_ref().map_or_else(|| function.clone(), |c| format!("{function}_{c}"));
            let a = Aggregate { name: if named { output } else { default }, function, column };
            let name = a.name.clone();
            aggregates.push(a);
            name
        };
        order.push(column);
    }
    let Some(bucket) = bucket else { return Err(fail("select the time_bucket")) };
    if key_names.len() != keys.iter().flatten().count() {
        return Err(fail("select every grouping column"));
    }
    if aggregates.is_empty() {
        return Err(fail("select at least one aggregate"));
    }
    let def = Definition { source: raw.name().clone(), width, bucket, keys: key_names, aggregates };
    Ok((raw, def, order))
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
    let mut input = node.input.as_ref();
    let scan = loop {
        match input {
            LogicalPlan::SubqueryAlias(s) => input = &s.input,
            LogicalPlan::Projection(p) if p.expr.iter().all(|e| matches!(e, Expr::Column(_))) => input = &p.input,
            LogicalPlan::TableScan(s) => break s,
            LogicalPlan::Filter(_) => return Err("WHERE isn't supported in a rollup".into()),
            _ => return Err(SHAPE.into()),
        }
    };
    let provider = source_as_provider(&scan.source).map_err(|e| e.to_string())?;
    match (provider.as_ref() as &dyn Any).downcast_ref::<TimelakeProvider>() {
        Some(p) if p.kind() == Kind::Data && scan.filters.is_empty() && scan.fetch.is_none() => Ok(p.table().clone()),
        _ => Err(SHAPE.into()),
    }
}
