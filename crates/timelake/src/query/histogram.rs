//! `histogram(x, low, high, bins)` (spec 28): how many of a group's values fall in each of `bins`
//! equal-width bins from `low` to `high`, as numpy's `histogram` with a range: a list of counts,
//! each bin including its lower edge, the last its upper edge too. Values outside the range and
//! nulls aren't counted. Counts add up, so partial histograms merge exactly.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int64Type};
use datafusion::common::{ScalarValue, exec_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility};

use super::functions::literal_arg;

/// The `histogram` aggregate function.
pub(super) fn udaf() -> AggregateUDF {
    AggregateUDF::from(Histogram { signature: Signature::user_defined(Volatility::Immutable) })
}

fn counts_type() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Histogram {
    signature: Signature,
}

/// The literal at `i` of a call, as a double.
fn literal(args: &AccumulatorArgs<'_>, i: usize) -> Option<f64> {
    let value = literal_arg(args.exprs, i)?.cast_to(&DataType::Float64).ok()?;
    if let ScalarValue::Float64(v) = value { v } else { None }
}

impl AggregateUDFImpl for Histogram {
    fn name(&self) -> &'static str {
        "histogram"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        match arg_types {
            [x, low, high, bins] if [x, low, high].iter().all(|t| t.is_numeric()) && bins.is_integer() => {
                Ok(vec![DataType::Float64, DataType::Float64, DataType::Float64, DataType::Int64])
            }
            _ => plan_err!("histogram takes (x, low, high, bins)"),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(counts_type())
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> DfResult<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(format_state_name(args.name, "counts"), counts_type(), true))])
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn Accumulator>> {
        let (Some(low), Some(high), Some(bins)) = (literal(&args, 1), literal(&args, 2), literal(&args, 3)) else {
            return plan_err!("histogram(x, low, high, bins) takes low, high and bins as numbers");
        };
        #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "checked to be 1 to 10,000")]
        let bins = bins as usize;
        if low.partial_cmp(&high) != Some(std::cmp::Ordering::Less) || !(1..=10_000).contains(&bins) {
            return plan_err!("histogram needs low below high and from 1 to 10,000 bins");
        }
        Ok(Box::new(HistogramAccumulator { low, high, counts: vec![0; bins] }))
    }
}

#[derive(Debug)]
struct HistogramAccumulator {
    low: f64,
    high: f64,
    counts: Vec<i64>,
}

impl HistogramAccumulator {
    /// The bin of `x`, if it is in the range.
    fn bin(&self, x: f64) -> Option<usize> {
        if !(self.low..=self.high).contains(&x) {
            return None;
        }
        let bins = self.counts.len();
        #[expect(clippy::cast_precision_loss, reason = "at most 10,000 bins")]
        let at = (x - self.low) / (self.high - self.low) * bins as f64;
        #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "0 <= at <= bins")]
        Some((at as usize).min(bins - 1))
    }
}

impl Accumulator for HistogramAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let x = cast(&values[0], &DataType::Float64)?;
        for x in x.as_primitive::<Float64Type>().iter().flatten() {
            if let Some(b) = self.bin(x) {
                self.counts[b] += 1;
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let Some(lists) = states.first() else { return exec_err!("histogram merges lists of counts") };
        let lists = lists.as_list::<i32>();
        for row in 0..lists.len() {
            if lists.is_null(row) {
                continue;
            }
            let counts = lists.value(row);
            for (c, n) in self.counts.iter_mut().zip(counts.as_primitive::<Int64Type>().values()) {
                *c += n;
            }
        }
        Ok(())
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let counts = Arc::new(Int64Array::from(self.counts.clone()));
        let field = Arc::new(Field::new_list_field(DataType::Int64, true));
        let list = ListArray::new(field, OffsetBuffer::from_lengths([counts.len()]), counts, None);
        Ok(ScalarValue::List(Arc::new(list)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.counts.capacity() * size_of::<i64>()
    }
}

#[cfg(test)]
mod tests {
    use crate::Session;
    use crate::testing::faulty_lake;

    #[tokio::test]
    async fn counts_per_bin() {
        let s = Session::new(faulty_lake().0);
        // numpy.histogram([0, 1, 2.5, 4.9, 5, 7, 10, 11, -1], bins=4, range=(0, 10)) counts [2, 2, 2, 1].
        let sql = "SELECT histogram(x, 0, 10, 4) AS h
                   FROM (VALUES (0.0), (1.0), (2.5), (4.9), (5.0), (7.0), (10.0), (11.0), (-1.0), (NULL)) AS v(x)";
        let rows = s.sql(sql).await.unwrap().collect().await.unwrap();
        let got = arrow::util::pretty::pretty_format_batches(&rows).unwrap().to_string();
        assert!(got.contains("[2, 2, 2, 1]"), "{got}");
        let bad = "SELECT histogram(x, 5, 1, 4) FROM (VALUES (1.0)) AS v(x)";
        let err = match s.sql(bad).await {
            Ok(df) => df.collect().await.unwrap_err().to_string(),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("low below high"), "{err}");
    }
}
