//! One state for the signal aggregates of the same readings. `time_weighted_mean`, `integral`,
//! `counter_diff` and `counter_rate` keep the same runs of readings and differ only in what they
//! return from them, so a query with several over one `(time, value)` computes the runs once:
//! `time_weighted_state(time, value)` in the aggregate, and each result from it in a projection
//! (`integral_of_state(state)`). Readings by another method share a state only with the counters,
//! whose growth doesn't depend on the method. Measured on the industrial benchmark, hourly values
//! of 1,000 tags over 30 days: all four 3.47 s each with its own state, 2.11 s sharing one (one
//! alone: 2.03 s).
//!
//! Setting `shared-signal-state` (on by default). Runs after rollup routing, which answers the
//! aggregates themselves from a rollup's stored state.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, ScalarValue};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{Aggregate, LogicalPlan, Projection};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::{Expr, lit};

use super::rollup::unalias;
use super::settings::SessionSettings;
use super::signal::{Method, Output, of_state_udf, udaf};
use crate::property;

/// The optimizer rule.
#[derive(Debug)]
pub(super) struct SharedState;

impl OptimizerRule for SharedState {
    fn name(&self) -> &'static str {
        "timelake_shared_signal_state"
    }

    fn rewrite(&self, plan: LogicalPlan, config: &dyn OptimizerConfig) -> DfResult<Transformed<LogicalPlan>> {
        let options = config.options();
        let settings = options.extensions.get::<SessionSettings>();
        if settings.and_then(|s| s.flag(property::SHARED_SIGNAL_STATE)) == Some(false) {
            return Ok(Transformed::no(plan));
        }
        plan.transform_up(share)
    }
}

/// A signal aggregate of an aggregate node: its position, what it returns, and the readings and
/// method its state is of.
struct Signal {
    position: usize,
    output: Output,
    readings: Vec<Expr>,
    method: Method,
}

/// The signal aggregate `e`, unless it filters, orders or is distinct.
fn signal(position: usize, e: &Expr) -> Option<Signal> {
    let Expr::AggregateFunction(AggregateFunction { func, params }) = unalias(e) else { return None };
    let output = Output::of(func.name()).filter(|o| *o != Output::State)?;
    if params.distinct || params.filter.is_some() || !params.order_by.is_empty() || params.null_treatment.is_some() {
        return None;
    }
    let (readings, method) = match params.args.as_slice() {
        [rest @ .., Expr::Literal(ScalarValue::Utf8(Some(m)), _)] => (rest.to_vec(), Method::parse(m).ok()?),
        args => (args.to_vec(), Method::ForwardFill),
    };
    Some(Signal { position, output, readings, method })
}

/// `node` with the signal aggregates of the same readings computing one state.
fn share(node: LogicalPlan) -> DfResult<Transformed<LogicalPlan>> {
    let LogicalPlan::Aggregate(a) = &node else { return Ok(Transformed::no(node)) };
    if a.group_expr.iter().any(|e| matches!(e, Expr::GroupingSet(_))) {
        return Ok(Transformed::no(node));
    }
    // States: the readings and method of each, and the aggregates it answers. Those that depend on
    // the method first, so the counters join one of theirs.
    let mut signals: Vec<Signal> = a.aggr_expr.iter().enumerate().filter_map(|(i, e)| signal(i, e)).collect();
    signals.sort_by_key(|s| !s.output.takes_method());
    let mut states: Vec<(Vec<Expr>, Method, Vec<Signal>)> = vec![];
    for s in signals {
        let fits = |(readings, method, _): &&mut (Vec<Expr>, Method, Vec<Signal>)| {
            *readings == s.readings && (*method == s.method || !s.output.takes_method())
        };
        match states.iter_mut().find(fits) {
            Some(state) => state.2.push(s),
            None => states.push((s.readings.clone(), s.method, vec![s])),
        }
    }
    states.retain(|(_, _, answers)| answers.len() > 1);
    if states.is_empty() {
        return Ok(Transformed::no(node));
    }
    let shared: HashSet<usize> = states.iter().flat_map(|(_, _, answers)| answers.iter().map(|s| s.position)).collect();
    let kept: Vec<usize> = (0..a.aggr_expr.len()).filter(|i| !shared.contains(i)).collect();
    let mut aggr_expr: Vec<Expr> = kept.iter().map(|&i| a.aggr_expr[i].clone()).collect();
    for (k, (readings, method, _)) in states.iter().enumerate() {
        let mut args = readings.clone();
        if *method == Method::Linear {
            args.push(lit("linear"));
        }
        aggr_expr.push(udaf(Output::State).call(args).alias(format!("__timelake_signal_state_{k}")));
    }
    let inner = Aggregate::try_new(a.input.clone(), a.group_expr.clone(), aggr_expr)?;
    let groups = a.group_expr.len();
    let column = |i: usize| Expr::Column(Column::from(inner.schema.qualified_field(i)));
    let mut from_state: HashMap<usize, Expr> = HashMap::new();
    for (k, (_, method, answers)) in states.iter().enumerate() {
        for s in answers {
            from_state.insert(s.position, of_state_udf(s.output, *method).call(vec![column(groups + kept.len() + k)]));
        }
    }
    // The node's columns as they were: the groups, then each aggregate, kept or from its state.
    let mut exprs: Vec<Expr> = (0..groups).map(column).collect();
    let mut kept_columns = (groups..).map(column);
    for i in 0..a.aggr_expr.len() {
        exprs.push(from_state.remove(&i).unwrap_or_else(|| kept_columns.next().expect("a column per kept aggregate")));
    }
    let exprs = exprs
        .into_iter()
        .zip(a.schema.iter())
        .map(|(e, (qualifier, field))| e.alias_qualified(qualifier.cloned(), field.name()))
        .collect();
    let projection = Projection::try_new(exprs, Arc::new(LogicalPlan::Aggregate(inner)))?;
    Ok(Transformed::yes(LogicalPlan::Projection(projection)))
}

#[cfg(test)]
mod tests {
    use crate::testing::{T0, batch, faulty_lake, opts, schema};
    use crate::{AppendOptions, Session};

    async fn answer(s: &Session, sql: &str) -> String {
        let rows = s.sql(sql).await.unwrap().collect().await.unwrap();
        arrow::util::pretty::pretty_format_batches(&rows).unwrap().to_string()
    }

    /// The four aggregates of one column by both methods, with others: two states (one per
    /// method, the counters with the first), and the answers they give without sharing.
    #[tokio::test]
    async fn signal_aggregates_share_a_state() {
        let (lake, _) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let rows: Vec<(i64, i32, f64)> =
            (0..600).map(|i| (T0 + i * 7_000_000, i32::try_from(i % 3).unwrap(), ((i * 37) % 101) as f64)).collect();
        t.append(&[batch(&rows)], AppendOptions::default()).await.unwrap();
        let sql = "SELECT device, time_bucket(INTERVAL '10 minutes', time) AS b, max(value) AS m,
                          time_weighted_mean(time, value) AS mean, integral(time, value, 'linear') AS area,
                          counter_diff(time, value) AS grew, time_weighted_mean(time, value, 'linear') AS lin,
                          counter_rate(time, value) AS rate, integral(time, value) AS ff_area, count(*) AS n
                   FROM iot.r GROUP BY 1, 2 ORDER BY 1, 2";
        let shared = Session::new(lake.clone());
        let plan = answer(&shared, &format!("EXPLAIN {sql}")).await;
        let states = plan.lines().find(|l| l.contains("Aggregate:")).unwrap().matches("time_weighted_state(").count();
        assert_eq!(states, 2, "{plan}");
        assert!(plan.contains("counter_rate_of_state("), "{plan}");
        let alone = Session::new(lake);
        alone.sql("SET timelake.shared_signal_state = false").await.unwrap();
        assert!(!answer(&alone, &format!("EXPLAIN {sql}")).await.contains("time_weighted_state("));
        assert_eq!(answer(&shared, sql).await, answer(&alone, sql).await);
    }
}
