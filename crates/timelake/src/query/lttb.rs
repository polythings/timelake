//! `lttb(time, value, n)` (spec 28): a group's readings downsampled to `n` points that keep its
//! shape on a chart, by Largest-Triangle-Three-Buckets (Steinarsson, 2013): the first and last
//! readings, and from each of `n - 2` buckets of readings between them the one that makes the
//! largest triangle with the point chosen before it and the next bucket's average. A list of
//! `{time, value}` in time order, as `m4` returns; groups of `n` readings or fewer come back whole.
//!
//! Unlike `m4`, whose points from several buckets merge, LTTB needs a group's readings together,
//! so it keeps them until the end; use it on a chart's series, not as rollup state.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int64Type, TimeUnit, TimestampMicrosecondType};
use datafusion::common::{ScalarValue, exec_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility};

use super::functions::literal_arg;
use super::m4::{points, points_list};

/// The `lttb` aggregate function.
pub(super) fn udaf() -> AggregateUDF {
    AggregateUDF::from(Lttb { signature: Signature::user_defined(Volatility::Immutable) })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Lttb {
    signature: Signature,
}

fn time_zone(arg_types: &[DataType]) -> DfResult<Option<Arc<str>>> {
    let Some(DataType::Timestamp(_, tz)) = arg_types.first() else { return plan_err!("lttb takes (time, value, n)") };
    Ok(tz.clone())
}

impl AggregateUDFImpl for Lttb {
    fn name(&self) -> &'static str {
        "lttb"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        match arg_types {
            [DataType::Timestamp(_, tz), value, n] if value.is_numeric() && n.is_integer() => {
                Ok(vec![DataType::Timestamp(TimeUnit::Microsecond, tz.clone()), DataType::Float64, DataType::Int64])
            }
            _ => plan_err!("lttb takes (time, value, n)"),
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(points(time_zone(arg_types)?))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> DfResult<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(format_state_name(args.name, "points"), args.return_type().clone(), true))])
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn Accumulator>> {
        let Some(&ScalarValue::Int64(Some(n))) = literal_arg(args.exprs, 2) else {
            return plan_err!("lttb(time, value, n) takes n as a whole number");
        };
        let Ok(n @ 2..) = usize::try_from(n) else { return plan_err!("lttb needs n of 2 or more") };
        let types: Vec<DataType> = args.expr_fields.iter().map(|f| f.data_type().clone()).collect();
        Ok(Box::new(LttbAccumulator { n, tz: time_zone(&types)?, points: vec![] }))
    }
}

#[derive(Debug)]
struct LttbAccumulator {
    n: usize,
    tz: Option<Arc<str>>,
    points: Vec<(i64, f64)>,
}

/// The `n` points of `points` (sorted by time) that LTTB keeps.
#[expect(clippy::cast_precision_loss, reason = "times and bucket sizes as doubles for areas")]
fn downsample(points: &[(i64, f64)], n: usize) -> Vec<(i64, f64)> {
    let len = points.len();
    if len <= n || n < 3 {
        return if len <= n { points.to_vec() } else { vec![points[0], points[len - 1]] };
    }
    let every = (len - 2) as f64 / (n - 2) as f64;
    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "bucket edges within 1..len-1")]
    let edge = |i: usize| (1 + (i as f64 * every) as usize).min(len - 1);
    let mut out = Vec::with_capacity(n);
    out.push(points[0]);
    let mut chosen = points[0];
    for bucket in 0..n - 2 {
        let (start, end) = (edge(bucket), edge(bucket + 1));
        // The next bucket's average (the last point, for the last bucket).
        let next = &points[end..edge(bucket + 2).max(end + 1).min(len)];
        let (ax, ay) = next.iter().fold((0.0, 0.0), |(x, y), p| (x + p.0 as f64, y + p.1));
        let (ax, ay) = (ax / next.len() as f64, ay / next.len() as f64);
        let (cx, cy) = (chosen.0 as f64, chosen.1);
        let area = |p: &(i64, f64)| ((cx - ax) * (p.1 - cy) - (cx - p.0 as f64) * (ay - cy)).abs();
        let best = points[start..end.max(start + 1)]
            .iter()
            .max_by(|a, b| area(a).total_cmp(&area(b)))
            .copied()
            .unwrap_or(points[start]);
        out.push(best);
        chosen = best;
    }
    out.push(points[len - 1]);
    out
}

impl LttbAccumulator {
    fn list(&self, points: &[(i64, f64)]) -> ScalarValue {
        let (times, values) = points.iter().copied().unzip();
        ScalarValue::List(Arc::new(points_list(times, values, [points.len()], self.tz.clone())))
    }
}

impl Accumulator for LttbAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let times = values[0].as_primitive::<TimestampMicrosecondType>();
        let v = cast(&values[1], &DataType::Float64)?;
        for (t, v) in times.iter().zip(v.as_primitive::<Float64Type>()) {
            if let (Some(t), Some(v)) = (t, v) {
                self.points.push((t, v));
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let Some(lists) = states.first() else { return exec_err!("lttb merges lists of points") };
        let lists = lists.as_list::<i32>();
        for row in 0..lists.len() {
            if lists.is_null(row) {
                continue;
            }
            let points = lists.value(row);
            let points = points.as_struct();
            let times = cast(points.column(0), &DataType::Int64)?;
            let times = times.as_primitive::<Int64Type>();
            let values = points.column(1).as_primitive::<Float64Type>();
            self.points.extend(times.values().iter().copied().zip(values.values().iter().copied()));
        }
        Ok(())
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![self.list(&self.points)])
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        self.points.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
        Ok(self.list(&downsample(&self.points, self.n)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.points.capacity() * size_of::<(i64, f64)>()
    }
}

#[cfg(test)]
mod tests {
    use super::downsample;

    #[test]
    fn keeps_the_ends_and_the_peaks() {
        // A flat line with one spike: 3 points keep both ends and the spike.
        let mut points: Vec<(i64, f64)> = (0..100).map(|t| (t, 0.0)).collect();
        points[37].1 = 50.0;
        assert_eq!(downsample(&points, 3), [(0, 0.0), (37, 50.0), (99, 0.0)]);
        let kept = downsample(&points, 10);
        assert_eq!(kept.len(), 10);
        assert!(kept.contains(&(37, 50.0)) && kept.first() == Some(&(0, 0.0)) && kept.last() == Some(&(99, 0.0)));
        assert!(kept.windows(2).all(|w| w[0].0 < w[1].0), "{kept:?}");
        assert_eq!(downsample(&points[..5], 10).len(), 5);
    }
}
