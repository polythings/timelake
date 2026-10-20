//! Higher moments (spec 28): `skewness(x)` (`skew`) and `kurtosis(x)` (`kurt`), as pandas defines
//! them for a sample: the adjusted Fisher–Pearson skewness, and the excess kurtosis with its
//! small-sample correction. Null below 3 and 4 values; 0 where every value is the same.
//!
//! The state is the count, mean and sums of the second to fourth powers of the differences from
//! the mean, which merge exactly across partitions (Pébay's formulas), so the result doesn't
//! depend on how rows are split.

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Float64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{ScalarValue, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility};

/// Which moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Moment {
    Skewness,
    Kurtosis,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MomentAggregate {
    moment: Moment,
    signature: Signature,
    aliases: Vec<String>,
}

/// `skewness` and `kurtosis`, to register.
pub(super) fn udafs() -> Vec<AggregateUDF> {
    [(Moment::Skewness, "skew"), (Moment::Kurtosis, "kurt")]
        .into_iter()
        .map(|(moment, alias)| {
            AggregateUDF::from(MomentAggregate {
                moment,
                signature: Signature::numeric(1, Volatility::Immutable),
                aliases: vec![alias.to_owned()],
            })
        })
        .collect()
}

impl AggregateUDFImpl for MomentAggregate {
    fn name(&self) -> &str {
        match self.moment {
            Moment::Skewness => "skewness",
            Moment::Kurtosis => "kurtosis",
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DfResult<DataType> {
        if arg_types.len() != 1 {
            return plan_err!("{} takes one column", self.name());
        }
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> DfResult<Vec<FieldRef>> {
        Ok(["n", "mean", "m2", "m3", "m4"]
            .iter()
            .map(|part| Arc::new(Field::new(format_state_name(args.name, part), DataType::Float64, true)))
            .collect())
    }

    fn accumulator(&self, _args: AccumulatorArgs<'_>) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(MomentAccumulator { moment: self.moment, m: Moments::default() }))
    }
}

/// A set of values' count, mean, and sums of the 2nd to 4th powers of their differences from it.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
struct Moments {
    n: f64,
    mean: f64,
    m2: f64,
    m3: f64,
    m4: f64,
}

impl Moments {
    /// The moments of both sets together.
    fn merge(self, b: Self) -> Self {
        let a = self;
        if a.n == 0.0 {
            return b;
        }
        if b.n == 0.0 {
            return a;
        }
        let n = a.n + b.n;
        let d = b.mean - a.mean;
        let (d2, d3, d4) = (d * d, d * d * d, d * d * d * d);
        let ab = a.n * b.n;
        Self {
            n,
            mean: a.mean + d * b.n / n,
            m2: a.m2 + b.m2 + d2 * ab / n,
            m3: a.m3 + b.m3 + d3 * ab * (a.n - b.n) / (n * n) + 3.0 * d * (a.n * b.m2 - b.n * a.m2) / n,
            m4: a.m4
                + b.m4
                + d4 * ab * (a.n * a.n - ab + b.n * b.n) / (n * n * n)
                + 6.0 * d2 * (a.n * a.n * b.m2 + b.n * b.n * a.m2) / (n * n)
                + 4.0 * d * (a.n * b.m3 - b.n * a.m3) / n,
        }
    }

    fn of(x: f64) -> Self {
        Self { n: 1.0, mean: x, ..Self::default() }
    }

    /// pandas' `skew`: the adjusted Fisher–Pearson coefficient.
    fn skewness(self) -> Option<f64> {
        let n = self.n;
        if n < 3.0 {
            return None;
        }
        if self.m2 == 0.0 {
            return Some(0.0);
        }
        let g1 = (self.m3 / n) / (self.m2 / n).powf(1.5);
        Some((n * (n - 1.0)).sqrt() / (n - 2.0) * g1)
    }

    /// pandas' `kurt`: the excess kurtosis with the small-sample correction.
    fn kurtosis(self) -> Option<f64> {
        let n = self.n;
        if n < 4.0 {
            return None;
        }
        if self.m2 == 0.0 {
            return Some(0.0);
        }
        let g2 = n * self.m4 / (self.m2 * self.m2) - 3.0;
        Some(((n + 1.0) * g2 + 6.0) * (n - 1.0) / ((n - 2.0) * (n - 3.0)))
    }
}

#[derive(Debug)]
struct MomentAccumulator {
    moment: Moment,
    m: Moments,
}

impl Accumulator for MomentAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let x = cast(&values[0], &DataType::Float64)?;
        for x in x.as_primitive::<Float64Type>().iter().flatten() {
            self.m = self.m.merge(Moments::of(x));
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let parts: Vec<&Float64Array> = states.iter().map(AsArray::as_primitive::<Float64Type>).collect();
        for i in 0..states[0].len() {
            let at = |p: usize| parts[p].value(i);
            self.m = self.m.merge(Moments { n: at(0), mean: at(1), m2: at(2), m3: at(3), m4: at(4) });
        }
        Ok(())
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let m = self.m;
        Ok([m.n, m.mean, m.m2, m.m3, m.m4].map(|v| ScalarValue::Float64(Some(v))).to_vec())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(ScalarValue::Float64(match self.moment {
            Moment::Skewness => self.m.skewness(),
            Moment::Kurtosis => self.m.kurtosis(),
        }))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn moments(xs: &[f64]) -> Moments {
        xs.iter().fold(Moments::default(), |m, &x| m.merge(Moments::of(x)))
    }

    #[test]
    fn match_pandas() {
        // pandas.Series([1, 2, 4, 8, 9]).skew(), .kurt()
        let m = moments(&[1.0, 2.0, 4.0, 8.0, 9.0]);
        assert!((m.skewness().unwrap() - 0.271_768_758_235_262_53).abs() < 1e-12, "{:?}", m.skewness());
        assert!((m.kurtosis().unwrap() - (-2.680_265_360_530_720_3)).abs() < 1e-12, "{:?}", m.kurtosis());
        assert_eq!(moments(&[1.0, 2.0]).skewness(), None);
        assert_eq!(moments(&[3.0; 5]).kurtosis(), Some(0.0));
    }

    #[test]
    fn merging_parts_is_merging_all() {
        let xs: Vec<f64> = (0..50).map(|i| f64::from(i * i % 17) + 0.5 * f64::from(i)).collect();
        let whole = moments(&xs);
        let parts = moments(&xs[..13]).merge(moments(&xs[13..31])).merge(moments(&xs[31..]));
        for (a, b) in [(whole.skewness(), parts.skewness()), (whole.kurtosis(), parts.kurtosis())] {
            assert!((a.unwrap() - b.unwrap()).abs() < 1e-12, "{a:?} {b:?}");
        }
    }
}
