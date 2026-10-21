//! Quantiles (spec 28):
//! - `quantile(x, q)`: the exact `q` quantile, interpolated linearly between the two values around
//!   it, as pandas' `quantile` and numpy's default; `median(x)` is `quantile(x, 0.5)`;
//! - `approx_quantile(x, q)`: an estimate from a t-digest, in bounded memory (DataFusion's
//!   `approx_percentile_cont`).
//!
//! The exact quantile keeps a group's values and selects the one or two it needs (no full sort).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{ScalarValue, exec_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility};

use super::functions::literal_arg;

/// `quantile`, and `approx_quantile` as a name for `approx_percentile_cont`, to register.
pub(super) fn udafs() -> Vec<AggregateUDF> {
    vec![
        AggregateUDF::from(Quantile { signature: Signature::user_defined(Volatility::Immutable) }),
        approx_percentile_cont_udaf().as_ref().clone().with_aliases(["approx_quantile"]),
    ]
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Quantile {
    signature: Signature,
}

/// The quantile a call asks for: its second argument, a number literal from 0 to 1.
fn q_of(args: &AccumulatorArgs<'_>) -> DfResult<f64> {
    let q = literal_arg(args.exprs, 1);
    match q.map(|v| v.cast_to(&DataType::Float64)) {
        Some(Ok(ScalarValue::Float64(Some(q)))) if (0.0..=1.0).contains(&q) => Ok(q),
        _ => plan_err!("quantile(x, q) takes q as a number from 0 to 1"),
    }
}

impl AggregateUDFImpl for Quantile {
    fn name(&self) -> &'static str {
        "quantile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        match arg_types {
            [x, q] if x.is_numeric() && q.is_numeric() => Ok(vec![DataType::Float64, DataType::Float64]),
            _ => plan_err!("quantile takes (x, q)"),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> DfResult<Vec<FieldRef>> {
        let list = DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true)));
        Ok(vec![Arc::new(Field::new(format_state_name(args.name, "values"), list, true))])
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(QuantileAccumulator { q: q_of(&args)?, values: vec![] }))
    }
}

#[derive(Debug)]
struct QuantileAccumulator {
    q: f64,
    values: Vec<f64>,
}

/// The `q` quantile of `values` (reordered), interpolated linearly; `None` for no values.
fn quantile(values: &mut [f64], q: f64) -> Option<f64> {
    let n = values.len();
    let last = n.checked_sub(1)?;
    #[expect(clippy::cast_precision_loss, reason = "a group's value count is far below 2^53")]
    let h = q * last as f64;
    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "0 <= h <= n - 1")]
    let lo = h.floor() as usize;
    let (_, &mut below, above) = values.select_nth_unstable_by(lo, f64::total_cmp);
    #[expect(clippy::cast_precision_loss, reason = "as above")]
    let fraction = h - lo as f64;
    if fraction == 0.0 || above.is_empty() {
        return Some(below);
    }
    // The next value up is the smallest of those above.
    let next = above.iter().copied().min_by(f64::total_cmp)?;
    Some(below + fraction * (next - below))
}

impl Accumulator for QuantileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let x = cast(&values[0], &DataType::Float64)?;
        self.values.extend(x.as_primitive::<Float64Type>().iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let Some(lists) = states.first() else { return exec_err!("quantile merges lists of values") };
        let lists = lists.as_list::<i32>();
        for row in 0..lists.len() {
            if lists.is_valid(row) {
                self.values.extend(lists.value(row).as_primitive::<Float64Type>().iter().flatten());
            }
        }
        Ok(())
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let values = Arc::new(Float64Array::from(std::mem::take(&mut self.values)));
        let field = Arc::new(Field::new_list_field(DataType::Float64, true));
        let list = ListArray::new(field, OffsetBuffer::from_lengths([values.len()]), values, None);
        Ok(vec![ScalarValue::List(Arc::new(list))])
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(ScalarValue::Float64(quantile(&mut self.values, self.q)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.capacity() * size_of::<f64>()
    }
}

#[cfg(test)]
mod tests {
    use super::quantile;

    #[test]
    fn like_pandas() {
        // pandas.Series([8, 1, 4, 2]).quantile(q) for q in 0, 0.5, 0.95, 1.
        for (q, want) in [(0.0, 1.0), (0.5, 3.0), (0.95, 7.4), (1.0, 8.0), (0.25, 1.75)] {
            let got = quantile(&mut [8.0, 1.0, 4.0, 2.0], q).unwrap();
            assert!((got - want).abs() < 1e-12, "q {q}: {got}, want {want}");
        }
        assert_eq!(quantile(&mut [], 0.5), None);
        assert_eq!(quantile(&mut [5.0], 0.9), Some(5.0));
    }
}
