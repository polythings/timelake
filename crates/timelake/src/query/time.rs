//! Time in the query layer: Timelake's times are microseconds since the epoch, and SQL brings
//! intervals and timestamps in other units. Conversions are checked: `None` where a value doesn't
//! fit.

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Int64Type, IntervalMonthDayNano, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::Expr;

pub(crate) const SECOND_MICROS: i64 = 1_000_000;
pub(crate) const HOUR_MICROS: i64 = 3_600 * SECOND_MICROS;
pub(crate) const DAY_MICROS: i64 = 24 * HOUR_MICROS;
const MICRO_NANOS: i64 = 1_000;

/// An interval without months in microseconds, nanoseconds rounded toward zero (a day is 24 hours,
/// as `date_bin` takes it); `None` for any other value, or one that overflows.
pub(crate) fn interval_micros(v: &ScalarValue) -> Option<i64> {
    let (days, micros) = match v {
        ScalarValue::IntervalMonthDayNano(Some(i)) if i.months == 0 => (i.days, i.nanoseconds / MICRO_NANOS),
        ScalarValue::IntervalDayTime(Some(i)) => (i.days, i64::from(i.milliseconds) * 1_000),
        _ => return None,
    };
    i64::from(days).checked_mul(DAY_MICROS)?.checked_add(micros)
}

/// `micros` as an interval literal's value, in whole days and the rest, so no length of time in
/// microseconds overflows its nanoseconds.
pub(crate) fn micros_interval(micros: i64) -> ScalarValue {
    let (days, rest) = (micros.div_euclid(DAY_MICROS), micros.rem_euclid(DAY_MICROS));
    let days = i32::try_from(days).expect("i64 microseconds are fewer than i32::MAX days");
    ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(0, days, rest * MICRO_NANOS)))
}

/// A timestamp value in nanoseconds since the epoch, whatever its unit; `None` for any other value.
pub(crate) fn timestamp_nanos(v: &ScalarValue) -> Option<i128> {
    Some(match v {
        ScalarValue::TimestampSecond(Some(t), _) => i128::from(*t) * 1_000_000_000,
        ScalarValue::TimestampMillisecond(Some(t), _) => i128::from(*t) * 1_000_000,
        ScalarValue::TimestampMicrosecond(Some(t), _) => i128::from(*t) * i128::from(MICRO_NANOS),
        ScalarValue::TimestampNanosecond(Some(t), _) => i128::from(*t),
        _ => return None,
    })
}

/// A timestamp value in microseconds since the epoch, nanoseconds rounded down; `None` for any
/// other value, or one outside the microsecond range.
pub(crate) fn timestamp_micros(v: &ScalarValue) -> Option<i64> {
    i64::try_from(timestamp_nanos(v)?.div_euclid(i128::from(MICRO_NANOS))).ok()
}

/// A timestamp literal of `micros` in time zone `tz`, as a time column of that zone compares with.
pub(crate) fn timestamp_literal(micros: i64, tz: Option<Arc<str>>) -> Expr {
    Expr::Literal(ScalarValue::TimestampMicrosecond(Some(micros), tz), None)
}

/// A column of timestamps or dates as microseconds since the epoch (`Int64`).
pub(crate) fn micros_array(times: &ArrayRef) -> DfResult<ArrayRef> {
    let micros = cast(times, &DataType::Timestamp(TimeUnit::Microsecond, None))?;
    Ok(cast(&micros, &DataType::Int64)?)
}

/// A column of timestamps or dates as seconds since the epoch.
pub(crate) fn seconds_array(times: &ArrayRef) -> DfResult<Vec<Option<f64>>> {
    let micros = micros_array(times)?;
    Ok(micros.as_primitive::<Int64Type>().iter().map(|t| t.map(seconds)).collect())
}

/// `micros` in seconds.
#[expect(clippy::cast_precision_loss, reason = "microseconds since the epoch fit a double's 53 bits until 2255")]
pub(crate) fn seconds(micros: i64) -> f64 {
    micros as f64 / 1e6
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::IntervalDayTime;

    use super::*;

    #[test]
    fn intervals_round_trip_through_microseconds() {
        for micros in [0, 1, SECOND_MICROS, DAY_MICROS + 5, 400 * DAY_MICROS, i64::MAX / 2] {
            assert_eq!(interval_micros(&micros_interval(micros)), Some(micros), "{micros}");
        }
        let day_time = ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(1, 1_500)));
        assert_eq!(interval_micros(&day_time), Some(DAY_MICROS + 1_500_000));
        let months = ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(1, 0, 0)));
        assert_eq!(interval_micros(&months), None);
        let huge = ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(0, i32::MAX, 0)));
        assert_eq!(interval_micros(&huge), None);
    }

    #[test]
    fn timestamps_in_any_unit() {
        let s = ScalarValue::TimestampSecond(Some(2), None);
        assert_eq!((timestamp_nanos(&s), timestamp_micros(&s)), (Some(2_000_000_000), Some(2 * SECOND_MICROS)));
        assert_eq!(timestamp_micros(&ScalarValue::TimestampNanosecond(Some(-1), None)), Some(-1));
        assert_eq!(timestamp_micros(&ScalarValue::TimestampSecond(Some(i64::MAX), None)), None);
        assert_eq!(timestamp_micros(&ScalarValue::Int64(Some(1))), None);
        assert_eq!(seconds(1_500_000), 1.5);
    }
}
