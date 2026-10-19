//! Gap filling (spec 24): `time_buckets`, a table of every bucket in a range to join readings
//! against, so empty buckets get a row, and window functions that fill the gaps:
//! - `forward_fill(x)` (pandas' `ffill`): the last value that isn't null, at or before the row;
//! - `backward_fill(x)` (`bfill`): the next value that isn't null, at or after the row;
//! - `interpolate(x [, t])`: a straight line between the values that aren't null before and
//!   after the row, by row position or, given `t`, by time (pandas' `linear` and `time`). Rows
//!   before the first value or after the last stay null.
//!
//! ```sql
//! SELECT b.bucket, forward_fill(h.temp) OVER (ORDER BY b.bucket) AS temp
//! FROM time_buckets(INTERVAL '1 hour', now() - INTERVAL '1 day', now()) b
//! LEFT JOIN (SELECT time_bucket(INTERVAL '1 hour', time) AS bucket, avg(temp) AS temp
//!            FROM iot.readings WHERE device = 'pump-7' GROUP BY 1) h ON h.bucket = b.bucket
//! ```

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, TimestampMicrosecondArray, UInt32Array};
use arrow::compute::{cast, take};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Schema, TimeUnit};
use datafusion::catalog::{MemTable, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{ScalarValue, exec_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::simplify::SimplifyContextBuilder;
use datafusion::logical_expr::{PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDF, WindowUDFImpl};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::prelude::Expr;

/// The most buckets `time_buckets` makes: a guard against a width typed in the wrong unit.
const MAX_BUCKETS: i64 = 10_000_000;

/// `time_buckets(width, start, end [, origin])`: one row per bucket, column `bucket`, from the
/// bucket holding `start` (aligned like `time_bucket`, to the Unix epoch or `origin`) to the last
/// one starting before `end`.
#[derive(Debug)]
pub(super) struct TimeBuckets;

impl TableFunctionImpl for TimeBuckets {
    fn call_with_args(&self, args: TableFunctionArgs<'_, '_>) -> DfResult<Arc<dyn TableProvider>> {
        let session = args.session();
        let context = SimplifyContextBuilder::default()
            .with_query_execution_start_time(session.execution_props().query_execution_start_time)
            .with_config_options(Arc::new(session.config_options().clone()))
            .build();
        let simplifier = ExprSimplifier::new(context);
        let values = args
            .exprs()
            .iter()
            .map(|e| match simplifier.simplify(e.clone())? {
                Expr::Literal(v, _) => Ok(v),
                other => plan_err!("time_buckets takes constants, not {other}"),
            })
            .collect::<DfResult<Vec<_>>>()?;
        let (width, start, end, origin) = match values.as_slice() {
            [w, s, e] => (micros_of_interval(w)?, micros_of_time(s)?, micros_of_time(e)?, 0),
            [w, s, e, o] => (micros_of_interval(w)?, micros_of_time(s)?, micros_of_time(e)?, micros_of_time(o)?),
            _ => return plan_err!("time_buckets takes (width, start, end [, origin])"),
        };
        if width <= 0 {
            return plan_err!("time_buckets needs a positive width");
        }
        let first = origin + (start - origin).div_euclid(width) * width;
        let count = if end > first { (end - first + width - 1) / width } else { 0 };
        if count > MAX_BUCKETS {
            return plan_err!("time_buckets would make {count} buckets, more than {MAX_BUCKETS}");
        }
        let buckets =
            TimestampMicrosecondArray::from_iter_values((0..count).map(|i| first + i * width)).with_timezone("+00:00");
        let schema = Arc::new(Schema::new(vec![Field::new("bucket", buckets.data_type().clone(), false)]));
        let batch = arrow::array::RecordBatch::try_new(schema.clone(), vec![Arc::new(buckets)])?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

fn micros_of_interval(v: &ScalarValue) -> DfResult<i64> {
    match v {
        ScalarValue::IntervalMonthDayNano(Some(i)) if i.months == 0 => {
            Ok(i64::from(i.days) * 86_400_000_000 + i.nanoseconds / 1_000)
        }
        ScalarValue::IntervalDayTime(Some(i)) => {
            Ok(i64::from(i.days) * 86_400_000_000 + i64::from(i.milliseconds) * 1_000)
        }
        ScalarValue::DurationMicrosecond(Some(d)) => Ok(*d),
        other => plan_err!("time_buckets needs a fixed width (seconds to days, not months), not {other}"),
    }
}

fn micros_of_time(v: &ScalarValue) -> DfResult<i64> {
    Ok(match v {
        ScalarValue::TimestampSecond(Some(t), _) => t * 1_000_000,
        ScalarValue::TimestampMillisecond(Some(t), _) => t * 1_000,
        ScalarValue::TimestampMicrosecond(Some(t), _) => *t,
        ScalarValue::TimestampNanosecond(Some(t), _) => t.div_euclid(1_000),
        other => return plan_err!("time_buckets needs timestamps, not {other}"),
    })
}

/// How a gap is filled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Fill {
    Forward,
    Backward,
    Interpolate,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FillFunction {
    fill: Fill,
    aliases: Vec<String>,
    signature: Signature,
}

/// The window functions: `forward_fill`, `backward_fill`, `interpolate`.
pub(super) fn window_udfs() -> Vec<WindowUDF> {
    let any = |n| Signature::any(n, Volatility::Immutable);
    vec![
        FillFunction { fill: Fill::Forward, aliases: vec!["ffill".into()], signature: any(1) },
        FillFunction { fill: Fill::Backward, aliases: vec!["bfill".into()], signature: any(1) },
        FillFunction {
            fill: Fill::Interpolate,
            aliases: vec![],
            signature: Signature::one_of(vec![TypeSignature::Any(1), TypeSignature::Any(2)], Volatility::Immutable),
        },
    ]
    .into_iter()
    .map(WindowUDF::from)
    .collect()
}

impl WindowUDFImpl for FillFunction {
    fn name(&self) -> &str {
        match self.fill {
            Fill::Forward => "forward_fill",
            Fill::Backward => "backward_fill",
            Fill::Interpolate => "interpolate",
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(&self, _args: PartitionEvaluatorArgs<'_>) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(FillEvaluator(self.fill)))
    }

    fn field(&self, args: WindowUDFFieldArgs<'_>) -> DfResult<FieldRef> {
        let data_type = match (self.fill, args.input_fields().first()) {
            (Fill::Interpolate, _) => DataType::Float64,
            (_, Some(f)) => f.data_type().clone(),
            (_, None) => return plan_err!("{} takes a column", self.name()),
        };
        Ok(Arc::new(Field::new(args.name(), data_type, true)))
    }
}

#[derive(Debug)]
struct FillEvaluator(Fill);

impl PartitionEvaluator for FillEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> DfResult<ArrayRef> {
        let Some(x) = values.first() else { return exec_err!("a fill takes a column") };
        match self.0 {
            Fill::Forward => Ok(take(x, &fill_indices(x, (0..num_rows).collect::<Vec<_>>()), None)?),
            Fill::Backward => Ok(take(x, &fill_indices(x, (0..num_rows).rev().collect::<Vec<_>>()), None)?),
            Fill::Interpolate => interpolate(x, values.get(1)),
        }
    }
}

/// For each row, in `order`, the index of the last row so far whose value isn't null.
fn fill_indices(x: &ArrayRef, order: Vec<usize>) -> UInt32Array {
    let mut indices = vec![None; x.len()];
    let mut last = None;
    for i in order {
        if x.is_valid(i) {
            last = u32::try_from(i).ok();
        }
        indices[i] = last;
    }
    UInt32Array::from(indices)
}

/// `x` interpolated linearly by `t` (cast to numbers), else by row position.
fn interpolate(x: &ArrayRef, t: Option<&ArrayRef>) -> DfResult<ArrayRef> {
    let x = cast(x, &DataType::Float64)?;
    let x = x.as_primitive::<Float64Type>();
    let t: Vec<f64> = match t {
        Some(t) => {
            let t = match t.data_type() {
                DataType::Timestamp(_, _) => {
                    cast(&cast(t, &DataType::Timestamp(TimeUnit::Microsecond, None))?, &DataType::Int64)?
                }
                _ => t.clone(),
            };
            let t = cast(&t, &DataType::Float64)?;
            let t = t.as_primitive::<Float64Type>();
            if t.null_count() > 0 {
                return exec_err!("interpolate needs a time on every row");
            }
            t.values().to_vec()
        }
        None => (0..x.len()).map(|i| i as f64).collect(),
    };
    let known: Vec<usize> = (0..x.len()).filter(|&i| x.is_valid(i)).collect();
    let mut out: Vec<Option<f64>> = vec![None; x.len()];
    for &i in &known {
        out[i] = Some(x.value(i));
    }
    for pair in known.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        let (ta, tb, xa, xb) = (t[a], t[b], x.value(a), x.value(b));
        for (i, slot) in out.iter_mut().enumerate().take(b).skip(a + 1) {
            // Rows at one time take the earlier value.
            *slot = Some(if tb > ta { xa + (xb - xa) * (t[i] - ta) / (tb - ta) } else { xa });
        }
    }
    Ok(Arc::new(Float64Array::from(out)))
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
    async fn buckets_cover_the_range() {
        // From the bucket holding the start to the last starting before the end.
        let got = answer(
            "SELECT bucket FROM time_buckets(INTERVAL '6 hours', TIMESTAMP '2026-01-01 05:00:00',
                                              TIMESTAMP '2026-01-01 18:00:00')",
        )
        .await
        .unwrap();
        for b in ["2026-01-01T00:00:00Z", "2026-01-01T06:00:00Z", "2026-01-01T12:00:00Z"] {
            assert!(got.contains(b), "{got}");
        }
        assert_eq!(got.matches("2026-01-01T").count(), 3, "{got}");
        // An origin shifts the buckets; expressions are folded.
        let got = answer(
            "SELECT count(*) AS n, min(bucket) AS first FROM time_buckets(INTERVAL '1 day',
                 TIMESTAMP '2026-01-01 05:00:00', TIMESTAMP '2026-01-01 05:00:00' + INTERVAL '3 days',
                 TIMESTAMP '2000-01-01 06:00:00')",
        )
        .await
        .unwrap();
        assert!(got.contains("| 4 | 2025-12-31T06:00:00Z |"), "{got}");
        // A day back from now: the partial hour it starts in, then 24 more (24 if now is on the hour).
        let got = answer("SELECT count(*) AS n FROM time_buckets(INTERVAL '1 hour', now() - INTERVAL '1 day', now())")
            .await
            .unwrap();
        assert!(got.contains("| 25 ") || got.contains("| 24 "), "{got}");
        for bad in [
            "SELECT * FROM time_buckets(INTERVAL '1 month', TIMESTAMP '2026-01-01', TIMESTAMP '2026-03-01')",
            "SELECT * FROM time_buckets(INTERVAL '1 second', TIMESTAMP '2000-01-01', TIMESTAMP '2026-01-01')",
        ] {
            assert!(answer(bad).await.is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn gaps_are_filled() {
        let got = answer(
            "SELECT d, t, forward_fill(x) OVER w AS ff, bfill(x) OVER w AS bf, interpolate(x) OVER w AS rows,
                    interpolate(x, t) OVER w AS by_time
             FROM (VALUES (1, 0, NULL), (1, 1, 10.0), (1, 2, NULL), (1, 5, 40.0), (1, 6, NULL),
                          (2, 0, 7.0), (2, 3, NULL)) AS v(d, t, x)
             WINDOW w AS (PARTITION BY d ORDER BY t) ORDER BY d, t",
        )
        .await
        .unwrap();
        let rows: Vec<&str> = got.lines().filter(|l| l.starts_with("| 1") || l.starts_with("| 2")).collect();
        let expected = [
            "| 1 | 0 |      | 10.0 |      |         |",
            "| 1 | 1 | 10.0 | 10.0 | 10.0 | 10.0    |",
            "| 1 | 2 | 10.0 | 40.0 | 25.0 | 17.5    |",
            "| 1 | 5 | 40.0 | 40.0 | 40.0 | 40.0    |",
            "| 1 | 6 | 40.0 |      |      |         |",
            "| 2 | 0 | 7.0  | 7.0  | 7.0  | 7.0     |",
            "| 2 | 3 | 7.0  |      |      |         |",
        ];
        assert_eq!(rows, expected, "{got}");
    }
}
