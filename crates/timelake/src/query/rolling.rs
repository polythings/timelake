//! Rolling statistics and anomalies (spec 28), as window functions over the window's order:
//! - `rolling_mean(x, n)` and `rolling_std(x, n)`: the mean and sample standard deviation of the
//!   last `n` rows, this one included, as pandas' `rolling(n).mean()` and `.std()`: null until the
//!   last `n` rows hold `n` values;
//! - `zscore(x)`: how many sample standard deviations the row is from the mean of the whole window
//!   partition, `(x - mean) / std`; null where every value is the same.
//!
//! The rolling functions add each row to running sums and take the one `n` rows back out
//! (Welford's update both ways), so they cost the same at any `n`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{ScalarValue, exec_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{PartitionEvaluator, Signature, Volatility, WindowUDF, WindowUDFImpl};
use datafusion::physical_expr::expressions::Literal;

/// Which statistic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Stat {
    RollingMean,
    RollingStd,
    Zscore,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct RollingFunction {
    stat: Stat,
    signature: Signature,
}

/// `rolling_mean`, `rolling_std` and `zscore`, to register.
pub(super) fn window_udfs() -> Vec<WindowUDF> {
    [(Stat::RollingMean, 2), (Stat::RollingStd, 2), (Stat::Zscore, 1)]
        .into_iter()
        .map(|(stat, args)| {
            WindowUDF::from(RollingFunction { stat, signature: Signature::any(args, Volatility::Immutable) })
        })
        .collect()
}

impl WindowUDFImpl for RollingFunction {
    fn name(&self) -> &str {
        match self.stat {
            Stat::RollingMean => "rolling_mean",
            Stat::RollingStd => "rolling_std",
            Stat::Zscore => "zscore",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(&self, args: PartitionEvaluatorArgs<'_>) -> DfResult<Box<dyn PartitionEvaluator>> {
        let rows = match (self.stat, args.input_exprs()) {
            (Stat::Zscore, _) => 0,
            (_, [_, n]) => match n.downcast_ref::<Literal>().map(Literal::value) {
                Some(&ScalarValue::Int64(Some(n))) if n > 0 => usize::try_from(n).unwrap_or(usize::MAX),
                _ => return plan_err!("{}(x, n) takes a positive whole number of rows", self.name()),
            },
            _ => return plan_err!("{}(x, n) takes a column and a number of rows", self.name()),
        };
        Ok(Box::new(RollingEvaluator { stat: self.stat, rows }))
    }

    fn field(&self, args: WindowUDFFieldArgs<'_>) -> DfResult<FieldRef> {
        if !args.input_fields().first().is_some_and(|f| f.data_type().is_numeric()) {
            return plan_err!("{} takes a numeric column", self.name());
        }
        Ok(Arc::new(Field::new(args.name(), DataType::Float64, true)))
    }
}

#[derive(Debug)]
struct RollingEvaluator {
    stat: Stat,
    /// The window's rows, for the rolling statistics.
    rows: usize,
}

/// A running count, mean and sum of squared differences from it.
#[derive(Debug, Default, Clone, Copy)]
struct Running {
    n: f64,
    mean: f64,
    m2: f64,
}

impl Running {
    fn add(&mut self, x: f64) {
        self.n += 1.0;
        let d = x - self.mean;
        self.mean += d / self.n;
        self.m2 += d * (x - self.mean);
    }

    fn remove(&mut self, x: f64) {
        if self.n <= 1.0 {
            *self = Self::default();
            return;
        }
        let d = x - self.mean;
        self.n -= 1.0;
        self.mean -= d / self.n;
        self.m2 -= d * (x - self.mean);
    }

    /// The sample standard deviation.
    fn std(&self) -> Option<f64> {
        (self.n > 1.0).then(|| (self.m2.max(0.0) / (self.n - 1.0)).sqrt())
    }
}

impl PartitionEvaluator for RollingEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> DfResult<ArrayRef> {
        let Some(x) = values.first() else { return exec_err!("{:?} takes a column", self.stat) };
        let x = cast(x, &DataType::Float64)?;
        let x = x.as_primitive::<Float64Type>();
        let value = |i: usize| x.is_valid(i).then(|| x.value(i));
        let out: Float64Array = if self.stat == Stat::Zscore {
            let mut all = Running::default();
            (0..num_rows).filter_map(value).for_each(|v| all.add(v));
            let std = all.std().filter(|s| *s > 0.0);
            (0..num_rows).map(|i| Some((value(i)? - all.mean) / std?)).collect()
        } else {
            let mut window = Running::default();
            (0..num_rows)
                .map(|i| {
                    if let Some(v) = value(i) {
                        window.add(v);
                    }
                    if let Some(old) = i.checked_sub(self.rows).and_then(value) {
                        window.remove(old);
                    }
                    #[expect(clippy::cast_precision_loss, reason = "a window of rows is far below 2^53")]
                    let full = window.n >= self.rows as f64;
                    match self.stat {
                        Stat::RollingMean => full.then_some(window.mean),
                        _ => window.std().filter(|_| full),
                    }
                })
                .collect()
        };
        Ok(Arc::new(out))
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
    async fn like_pandas() {
        // pandas.Series([1, 2, 4, 8, None, 9]).rolling(3).mean() / .std(); zscore over the series.
        let got = answer(
            "SELECT t, round(rolling_mean(x, 3) OVER w, 6) AS m, round(rolling_std(x, 3) OVER w, 6) AS s,
                    round(zscore(x) OVER w, 6) AS z
             FROM (VALUES (1, 1.0), (2, 2.0), (3, 4.0), (4, 8.0), (5, NULL), (6, 9.0)) AS v(t, x)
             WINDOW w AS (ORDER BY t) ORDER BY t",
        )
        .await
        .unwrap();
        let rows: Vec<&str> = got.lines().filter(|l| l.starts_with("| ")).skip(1).collect();
        assert_eq!(
            rows,
            [
                "| 1 |          |          | -1.066306 |",
                "| 2 |          |          | -0.785699 |",
                "| 3 | 2.333333 | 1.527525 | -0.224485 |",
                "| 4 | 4.666667 | 3.05505  | 0.897942  |",
                "| 5 |          |          |           |",
                "| 6 |          |          | 1.178548  |",
            ],
            "{got}"
        );
        assert!(answer("SELECT rolling_mean(x, 0) OVER () FROM (VALUES (1.0)) AS v(x)").await.is_err());
    }
}
