//! `ohlc(time, price)` (spec 28): a group's open, high, low and close, as pandas' `ohlc`, in one
//! struct `{open, high, low, close}`: the price at the earliest time, the highest, the lowest, and
//! the price at the latest time. Volume is a `sum`. Nulls are skipped; a group without prices gives
//! null.
//!
//! ```sql
//! SELECT minute, bar['open'] AS open, bar['high'] AS high, bar['low'] AS low, bar['close'] AS close, volume
//! FROM (SELECT time_bucket(INTERVAL '1 minute', time) AS minute, ohlc(time, price) AS bar, sum(size) AS volume
//!       FROM market.trades WHERE symbol = 'ABC' GROUP BY 1)
//! ORDER BY minute
//! ```
//!
//! Ties at the earliest or latest time keep the smaller open and the larger close, wherever the rows
//! arrive, so the result doesn't depend on partitioning.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, StructArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Float64Type, Int64Type, TimeUnit};
use datafusion::common::{ScalarValue, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility};

/// The `ohlc` aggregate function.
pub(super) fn udaf() -> AggregateUDF {
    AggregateUDF::from(Ohlc { signature: Signature::user_defined(Volatility::Immutable) })
}

/// The result's fields.
fn fields() -> Fields {
    ["open", "high", "low", "close"].into_iter().map(|n| Field::new(n, DataType::Float64, true)).collect()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Ohlc {
    signature: Signature,
}

impl AggregateUDFImpl for Ohlc {
    fn name(&self) -> &'static str {
        "ohlc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        match arg_types {
            [DataType::Timestamp(_, tz), price] if price.is_numeric() => {
                Ok(vec![DataType::Timestamp(TimeUnit::Microsecond, tz.clone()), DataType::Float64])
            }
            _ => plan_err!("ohlc takes (time, price)"),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Struct(fields()))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> DfResult<Vec<FieldRef>> {
        let field = |part, data_type| Arc::new(Field::new(format_state_name(args.name, part), data_type, true));
        Ok(vec![
            field("open_time", DataType::Int64),
            field("open", DataType::Float64),
            field("high", DataType::Float64),
            field("low", DataType::Float64),
            field("close_time", DataType::Int64),
            field("close", DataType::Float64),
        ])
    }

    fn accumulator(&self, _args: AccumulatorArgs<'_>) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(Bar::default()))
    }
}

/// A group's bar so far: `None` before its first price.
#[derive(Debug, Default)]
struct Bar(Option<Prices>);

#[derive(Debug, Clone, Copy)]
struct Prices {
    open_time: i64,
    open: f64,
    high: f64,
    low: f64,
    close_time: i64,
    close: f64,
}

impl Bar {
    fn add(&mut self, p: Prices) {
        let Some(b) = &mut self.0 else {
            self.0 = Some(p);
            return;
        };
        if (p.open_time, p.open) < (b.open_time, b.open) {
            (b.open_time, b.open) = (p.open_time, p.open);
        }
        if (p.close_time, p.close) > (b.close_time, b.close) {
            (b.close_time, b.close) = (p.close_time, p.close);
        }
        b.high = b.high.max(p.high);
        b.low = b.low.min(p.low);
    }
}

impl Accumulator for Bar {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let times = cast(&values[0], &DataType::Int64)?;
        let prices = cast(&values[1], &DataType::Float64)?;
        for (t, p) in times.as_primitive::<Int64Type>().iter().zip(prices.as_primitive::<Float64Type>()) {
            if let (Some(t), Some(p)) = (t, p) {
                self.add(Prices { open_time: t, open: p, high: p, low: p, close_time: t, close: p });
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let int = |i: usize| states[i].as_primitive::<Int64Type>();
        let float = |i: usize| states[i].as_primitive::<Float64Type>();
        let (open_time, open, high, low, close_time, close) = (int(0), float(1), float(2), float(3), int(4), float(5));
        for i in 0..states[0].len() {
            if open_time.is_valid(i) {
                self.add(Prices {
                    open_time: open_time.value(i),
                    open: open.value(i),
                    high: high.value(i),
                    low: low.value(i),
                    close_time: close_time.value(i),
                    close: close.value(i),
                });
            }
        }
        Ok(())
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let b = self.0;
        Ok(vec![
            ScalarValue::Int64(b.map(|b| b.open_time)),
            ScalarValue::Float64(b.map(|b| b.open)),
            ScalarValue::Float64(b.map(|b| b.high)),
            ScalarValue::Float64(b.map(|b| b.low)),
            ScalarValue::Int64(b.map(|b| b.close_time)),
            ScalarValue::Float64(b.map(|b| b.close)),
        ])
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let b = self.0;
        let column = |f: fn(&Prices) -> f64| Arc::new(Float64Array::from(vec![b.as_ref().map(f)])) as ArrayRef;
        let columns = vec![column(|b| b.open), column(|b| b.high), column(|b| b.low), column(|b| b.close)];
        let nulls = b.is_none().then(|| vec![false].into());
        Ok(ScalarValue::Struct(Arc::new(StructArray::new(fields(), columns, nulls))))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::Session;
    use crate::testing::{answer, faulty_lake};

    #[tokio::test]
    async fn open_high_low_close() {
        let s = Session::new(faulty_lake().0);
        let sql = "SELECT s, b['open'] AS o, b['high'] AS h, b['low'] AS l, b['close'] AS c FROM (
                   SELECT s, ohlc(t, p) AS b FROM (SELECT s, CAST(t AS TIMESTAMP) AS t, p FROM (VALUES
                       ('a', '2026-01-01 00:00:03', 10.0), ('a', '2026-01-01 00:00:01', 12.0),
                       ('a', '2026-01-01 00:00:02', 9.0), ('a', '2026-01-01 00:00:04', 11.0),
                       ('b', '2026-01-01 00:00:01', NULL)) AS v(s, t, p))
                   GROUP BY s) ORDER BY s";
        let got = answer(&s, sql).await.unwrap();
        assert!(got.contains("| a | 12.0 | 12.0 | 9.0 | 11.0 |"), "{got}");
        assert!(got.contains("| b |      |      |     |      |"), "{got}");
    }
}
