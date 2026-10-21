//! Selection aggregates (spec 28): a group's value at its first or last time, and the latest
//! value at or before a time.
//!
//! - `first(time, value)` and `last(time, value)`: the value of the row with the earliest or
//!   latest time.
//! - `value_at(time, value, at)` (`asof`): the value of the latest row at or before `at`.
//!
//! Each is a name for an ordered `first_value` or `last_value` (with a filter for `value_at`),
//! which the optimizer puts in its place before planning, so rollups store and answer them like
//! any ordered `first_value` and `last_value`.

use arrow::datatypes::DataType;
use datafusion::common::plan_err;
use datafusion::error::Result as DfResult;
use datafusion::functions_aggregate::first_last::{first_value_udaf, last_value_udaf};
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::function::{AccumulatorArgs, AggregateFunctionSimplification};
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility};

/// Which selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    First,
    Last,
    ValueAt,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Selection {
    kind: Kind,
    signature: Signature,
    aliases: Vec<String>,
}

/// The selection aggregates, to register.
pub(super) fn udafs() -> Vec<AggregateUDF> {
    [Kind::First, Kind::Last, Kind::ValueAt].into_iter().map(|kind| AggregateUDF::from(Selection::new(kind))).collect()
}

impl Selection {
    fn new(kind: Kind) -> Self {
        let signature = Signature::user_defined(Volatility::Immutable);
        let aliases = if kind == Kind::ValueAt { vec!["asof".to_owned()] } else { vec![] };
        Self { kind, signature, aliases }
    }

    /// How the function is called, for error messages.
    fn usage(&self) -> &'static str {
        match self.kind {
            Kind::First => "first(time, value)",
            Kind::Last => "last(time, value)",
            Kind::ValueAt => "value_at(time, value, at)",
        }
    }
}

impl AggregateUDFImpl for Selection {
    fn name(&self) -> &str {
        match self.kind {
            Kind::First => "first",
            Kind::Last => "last",
            Kind::ValueAt => "value_at",
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        // `at` compares with the time column, so it takes its type.
        match (self.kind, arg_types) {
            (Kind::First | Kind::Last, [time @ DataType::Timestamp(..), value]) => {
                Ok(vec![time.clone(), value.clone()])
            }
            (Kind::ValueAt, [time @ DataType::Timestamp(..), value, _]) => {
                Ok(vec![time.clone(), value.clone(), time.clone()])
            }
            _ => plan_err!("{} takes the time column first", self.usage()),
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> DfResult<DataType> {
        let [DataType::Timestamp(..), value, ..] = arg_types else {
            return plan_err!("{} takes the time column first", self.usage());
        };
        Ok(value.clone())
    }

    fn accumulator(&self, _args: AccumulatorArgs<'_>) -> DfResult<Box<dyn Accumulator>> {
        plan_err!("{} is rewritten before planning: run it through the optimizer", self.usage())
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        let kind = self.kind;
        Some(Box::new(move |f: AggregateFunction, _| {
            let mut args = f.params.args.into_iter();
            let (Some(time), Some(value)) = (args.next(), args.next()) else {
                return plan_err!("selection aggregates take the time and the value");
            };
            let order_by = vec![time.clone().sort(true, false)];
            let (udaf, filter) = match kind {
                Kind::First => (first_value_udaf(), f.params.filter.map(|e| *e)),
                Kind::Last => (last_value_udaf(), f.params.filter.map(|e| *e)),
                Kind::ValueAt => {
                    let Some(at) = args.next() else {
                        return plan_err!("value_at takes the time, the value and a time");
                    };
                    let until = time.lt_eq(at);
                    (last_value_udaf(), Some(f.params.filter.map_or(until.clone(), |e| e.and(until))))
                }
            };
            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                udaf,
                vec![value],
                false,
                filter.map(Box::new),
                order_by,
                None,
            )))
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::Session;
    use crate::testing::faulty_lake;

    async fn answer(sql: &str) -> crate::Result<String> {
        let s = Session::new(faulty_lake().0);
        let rows = s.sql(sql).await?.collect().await?;
        Ok(arrow::util::pretty::pretty_format_batches(&rows).unwrap().to_string())
    }

    #[tokio::test]
    async fn values_at_first_last_and_a_time() {
        // Rows out of time order; the value at 11:30 is the 11:00 one, before the first row there is none.
        let got = answer(
            "SELECT d, first(t, x) AS f, last(t, x) AS l, value_at(t, x, TIMESTAMP '2026-01-01 11:30:00') AS at,
                    asof(t, x, '2026-01-01 09:00:00') AS early, last(t, x) FILTER (WHERE x < 30) AS l30
             FROM (SELECT d, CAST(t AS TIMESTAMP) AS t, x FROM (VALUES
                 (1, '2026-01-01 12:00:00', 30.0), (1, '2026-01-01 10:00:00', 10.0),
                 (1, '2026-01-01 11:00:00', 20.0), (2, '2026-01-01 10:30:00', 5.0)) AS v(d, t, x))
             GROUP BY d ORDER BY d",
        )
        .await
        .unwrap();
        assert!(got.contains("| 1 | 10.0 | 30.0 | 20.0 |       | 20.0 |"), "{got}");
        assert!(got.contains("| 2 | 5.0  | 5.0  | 5.0  |       | 5.0  |"), "{got}");
        assert!(answer("SELECT first(x, t) FROM (VALUES (1.0, 2.0)) AS v(x, t)").await.is_err());
    }
}
