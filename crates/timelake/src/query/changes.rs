//! Changes between consecutive rows (spec 28), as window functions over the window's order:
//! - `diff(x)`: the change from the row before (pandas' `diff`);
//! - `percent_change(x)` (`pct_change`): that change as a fraction of the row before; null where it
//!   was 0;
//! - `rate(time, x)`: that change per second since the row before.
//!
//! The first row, and a row after a null, get null.
//!
//! ```sql
//! SELECT time, diff(energy) OVER w AS used, rate(time, energy) OVER w AS kw
//! FROM iot.meters WINDOW w AS (PARTITION BY meter ORDER BY time)
//! ```

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{exec_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{PartitionEvaluator, Signature, Volatility, WindowUDF, WindowUDFImpl};

use super::time;

/// Which change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Change {
    Diff,
    Percent,
    Rate,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ChangeFunction {
    change: Change,
    signature: Signature,
    aliases: Vec<String>,
}

/// `diff`, `percent_change` and `rate`, to register.
pub(super) fn window_udfs() -> Vec<WindowUDF> {
    [(Change::Diff, 1, None), (Change::Percent, 1, Some("pct_change")), (Change::Rate, 2, None)]
        .into_iter()
        .map(|(change, args, alias)| {
            WindowUDF::from(ChangeFunction {
                change,
                signature: Signature::any(args, Volatility::Immutable),
                aliases: alias.map(str::to_owned).into_iter().collect(),
            })
        })
        .collect()
}

impl WindowUDFImpl for ChangeFunction {
    fn name(&self) -> &str {
        match self.change {
            Change::Diff => "diff",
            Change::Percent => "percent_change",
            Change::Rate => "rate",
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(&self, _args: PartitionEvaluatorArgs<'_>) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(ChangeEvaluator(self.change)))
    }

    fn field(&self, args: WindowUDFFieldArgs<'_>) -> DfResult<FieldRef> {
        let types: Vec<&DataType> = args.input_fields().iter().map(|f| f.data_type()).collect();
        let numeric = |t: &DataType| t.is_numeric();
        let fits = match (self.change, types.as_slice()) {
            (Change::Diff | Change::Percent, [x]) | (Change::Rate, [DataType::Timestamp(..), x]) => numeric(x),
            _ => false,
        };
        if !fits {
            let usage = if self.change == Change::Rate { "rate(time, value)" } else { "a numeric column" };
            return plan_err!("{} takes {usage}", self.name());
        }
        Ok(Arc::new(Field::new(args.name(), DataType::Float64, true)))
    }
}

#[derive(Debug)]
struct ChangeEvaluator(Change);

impl PartitionEvaluator for ChangeEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> DfResult<ArrayRef> {
        let (time, x) = match (self.0, values) {
            (Change::Rate, [t, x]) => (Some(time::seconds_array(t)?), x),
            (_, [x]) => (None, x),
            _ => return exec_err!("a change takes its column"),
        };
        let x = cast(x, &DataType::Float64)?;
        let x = x.as_primitive::<Float64Type>();
        let out: Float64Array = (0..num_rows)
            .map(|i| {
                let i_prev = i.checked_sub(1)?;
                if x.is_null(i) || x.is_null(i_prev) {
                    return None;
                }
                let (now, before) = (x.value(i), x.value(i_prev));
                match self.0 {
                    Change::Diff => Some(now - before),
                    Change::Percent => (before != 0.0).then(|| now / before - 1.0),
                    Change::Rate => {
                        let t = time.as_ref()?;
                        let dt = t.get(i).copied().flatten()? - t.get(i_prev).copied().flatten()?;
                        (dt != 0.0).then(|| (now - before) / dt)
                    }
                }
            })
            .collect();
        Ok(Arc::new(out))
    }
}

#[cfg(test)]
mod tests {
    use crate::testing::answer_on_empty_lake;

    #[tokio::test]
    async fn changes_from_the_row_before() {
        let got = answer_on_empty_lake(
            "SELECT d, diff(x) OVER w AS diff, pct_change(x) OVER w AS pct, rate(t, x) OVER w AS rate
             FROM (SELECT d, CAST(t AS TIMESTAMP) AS t, x FROM (VALUES
                 (1, '2026-01-01 00:00:10', 20.0), (1, '2026-01-01 00:00:00', 10.0),
                 (1, '2026-01-01 00:00:20', NULL), (1, '2026-01-01 00:00:40', 15.0),
                 (2, '2026-01-01 00:00:00', 0.0), (2, '2026-01-01 00:01:00', 3.0)) AS v(d, t, x))
             WINDOW w AS (PARTITION BY d ORDER BY t) ORDER BY d, t",
        )
        .await
        .unwrap();
        let rows: Vec<&str> = got.lines().filter(|l| l.starts_with("| 1") || l.starts_with("| 2")).collect();
        assert_eq!(
            rows,
            [
                "| 1 |      |     |      |",
                "| 1 | 10.0 | 1.0 | 1.0  |",
                "| 1 |      |     |      |",
                "| 1 |      |     |      |",
                "| 2 |      |     |      |",
                "| 2 | 3.0  |     | 0.05 |",
            ],
            "{got}"
        );
        assert!(answer_on_empty_lake("SELECT rate(x, x) OVER () FROM (VALUES (1.0)) AS v(x)").await.is_err());
    }
}
