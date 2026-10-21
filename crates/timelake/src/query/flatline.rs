//! Flatlines in a series' readings (spec 28), a stuck sensor's sign, over each group's readings:
//! - `flatline_count(time, value, threshold)`: how many times the value stayed exactly the same
//!   for longer than `threshold` (an interval);
//! - `flatline_seconds(time, value, threshold)`: how long those stretches lasted in all.
//!
//! A stretch is the readings in a row with the same value; it lasts from its first reading to the
//! next reading with another value, or to its own last reading at the end of the group. Nulls are
//! skipped.
//!
//! The state is a list of runs ([`super::runs`]), each with its stretches so far. A run's first
//! and last stretches stay open until runs are joined or the result is taken, since the run before
//! or after may hold the same value.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Float64Type, Int64Type, IntervalUnit, TimeUnit, TimestampMicrosecondType,
};
use datafusion::common::{exec_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, Signature, Volatility};

use super::functions::literal_arg;
use super::gaps::threshold_of_literal;
use super::groups::{AllGroups, OneGroup};
use super::lists::{list_items, list_of_structs, list_type};
use super::runs::{Input, Run, RunAggregate, Runs};
use super::time;
use timelake_format::rollup::FunctionFamily;

/// What a flatline aggregate returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Output {
    Count,
    Seconds,
}

impl FunctionFamily for Output {
    const ALL: &'static [Self] = &[Self::Count, Self::Seconds];

    fn name(self) -> &'static str {
        match self {
            Self::Count => "flatline_count",
            Self::Seconds => "flatline_seconds",
        }
    }
}

/// The aggregate function for `output`.
pub(crate) fn udaf(output: Output) -> AggregateUDF {
    AggregateUDF::from(FlatlineAggregate { output, signature: Signature::user_defined(Volatility::Immutable) })
}

/// The type of one run.
fn run_fields() -> Fields {
    let int = |name| Field::new(name, DataType::Int64, true);
    let float = |name| Field::new(name, DataType::Float64, false);
    Fields::from(vec![
        int("first"),
        int("last"),
        float("first_value"),
        float("last_value"),
        int("lead_end"),
        int("tail_start"),
        int("count"),
        int("micros"),
    ])
}

/// The state: a list of runs.
fn state_type() -> DataType {
    list_type(run_fields())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FlatlineAggregate {
    output: Output,
    signature: Signature,
}

/// The threshold a call names (its last argument, an interval literal), in microseconds.
fn threshold_of(args: &AccumulatorArgs<'_>) -> DfResult<i64> {
    let literal = literal_arg(args.exprs, args.exprs.len().saturating_sub(1));
    let Some(threshold) = literal.and_then(threshold_of_literal) else {
        return plan_err!("the threshold must be an interval literal without months, such as INTERVAL '1 hour'");
    };
    Ok(threshold)
}

impl AggregateUDFImpl for FlatlineAggregate {
    fn name(&self) -> &'static str {
        self.output.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        let interval = DataType::Interval(IntervalUnit::MonthDayNano);
        match arg_types {
            [DataType::List(_), DataType::Interval(_)] => Ok(vec![state_type(), interval]),
            [DataType::Timestamp(_, tz), value, DataType::Interval(_)] if value.is_numeric() => {
                Ok(vec![DataType::Timestamp(TimeUnit::Microsecond, tz.clone()), DataType::Float64, interval])
            }
            _ => plan_err!("{} takes (time, value, threshold)", self.name()),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(match self.output {
            Output::Count => DataType::Int64,
            Output::Seconds => DataType::Float64,
        })
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> DfResult<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(format_state_name(args.name, "runs"), state_type(), true))])
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(OneGroup::new(Flatlines { output: self.output, threshold: threshold_of(&args)? })))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs<'_>) -> bool {
        true
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(AllGroups::new(Flatlines { output: self.output, threshold: threshold_of(&args)? })))
    }
}

/// Readings in time order, as stretches of one value: the first and last stretches open, the
/// ones between them counted.
#[derive(Clone, Copy, Debug, PartialEq)]
struct FlatRun {
    first: i64,
    last: i64,
    first_value: f64,
    last_value: f64,
    /// Where the first stretch ends (the first reading of another value), if the value changed.
    lead_end: Option<i64>,
    /// Where the last stretch starts.
    tail_start: i64,
    /// The stretches between the first and the last that lasted longer than the threshold.
    count: i64,
    micros: i64,
}

impl FlatRun {
    /// Counts a closed stretch from `start` to `end`.
    fn pass(&mut self, start: i64, end: i64, threshold: i64) {
        if end - start > threshold {
            self.count += 1;
            self.micros += end - start;
        }
    }

    /// The last stretch, ending at `end`, closes: the first one too if the value never changed.
    fn close_tail(&mut self, end: i64, threshold: i64) {
        if self.lead_end.is_none() {
            self.lead_end = Some(end);
        } else {
            self.pass(self.tail_start, end, threshold);
        }
    }

    /// The count and duration over the whole run: its first and last stretches closed by its ends.
    fn finished(mut self, threshold: i64) -> (i64, i64) {
        match self.lead_end {
            None => self.pass(self.first, self.last, threshold),
            Some(end) => {
                self.pass(self.first, end, threshold);
                self.pass(self.tail_start, self.last, threshold);
            }
        }
        (self.count, self.micros)
    }
}

impl Run for FlatRun {
    type Reading = (i64, f64);
    type How = i64;

    fn new((t, v): (i64, f64)) -> Self {
        Self { first: t, last: t, first_value: v, last_value: v, lead_end: None, tail_start: t, count: 0, micros: 0 }
    }

    fn time((t, _): &(i64, f64)) -> i64 {
        *t
    }

    fn first_time(&self) -> i64 {
        self.first
    }

    fn last_time(&self) -> i64 {
        self.last
    }

    fn push(&mut self, (t, v): (i64, f64), threshold: i64) {
        #[expect(clippy::float_cmp, reason = "a flatline is the same value exactly")]
        if v != self.last_value {
            self.close_tail(t, threshold);
            self.tail_start = t;
            self.last_value = v;
        }
        self.last = t;
    }

    fn join(mut self, next: FlatRun, threshold: i64) -> FlatRun {
        let mut out = FlatRun {
            last: next.last,
            last_value: next.last_value,
            count: self.count + next.count,
            micros: self.micros + next.micros,
            ..self
        };
        #[expect(clippy::float_cmp, reason = "a flatline is the same value exactly")]
        let same = self.last_value == next.first_value;
        if same {
            // One stretch across the boundary, from this run's last stretch through the next's first.
            match next.lead_end {
                None => out.tail_start = self.tail_start,
                Some(end) => {
                    self.close_tail(end, threshold);
                    (out.lead_end, out.count, out.micros) =
                        (self.lead_end, self.count + next.count, self.micros + next.micros);
                    out.tail_start = next.tail_start;
                }
            }
        } else {
            self.close_tail(next.first, threshold);
            let mut next = next;
            if let Some(end) = next.lead_end {
                next.pass(next.first, end, threshold);
            }
            (out.lead_end, out.count, out.micros) = (self.lead_end, self.count + next.count, self.micros + next.micros);
            out.tail_start = next.tail_start;
        }
        out
    }
}

/// A flatline aggregate's output by its threshold, as a run aggregate.
#[derive(Debug)]
struct Flatlines {
    output: Output,
    /// In microseconds.
    threshold: i64,
}

impl RunAggregate for Flatlines {
    type Run = FlatRun;

    fn how(&self) -> i64 {
        self.threshold
    }

    /// Readings `(time, value, threshold)` or states `(runs [, threshold])`.
    fn inputs(&self, columns: &[ArrayRef], mut add: impl FnMut(usize, Input<FlatRun>)) -> DfResult<()> {
        match columns {
            [lists] | [lists, _] if matches!(lists.data_type(), DataType::List(_)) => for_each_run(lists, |row, r| {
                add(row, Input::Run(r));
            }),
            [times, values, _] => {
                let times = times.as_primitive::<TimestampMicrosecondType>();
                let values = cast(values, &DataType::Float64)?;
                let values = values.as_primitive::<Float64Type>();
                for row in 0..times.len() {
                    if times.is_valid(row) && values.is_valid(row) {
                        add(row, Input::Reading((times.value(row), values.value(row))));
                    }
                }
                Ok(())
            }
            _ => exec_err!("expected (time, value, threshold) or runs"),
        }
    }

    fn states<'a>(&self, groups: impl Iterator<Item = &'a Runs<FlatRun>>) -> ArrayRef {
        let (mut ints, mut floats): ([Vec<Option<i64>>; 6], [Vec<f64>; 2]) = Default::default();
        let mut lengths = vec![];
        for runs in groups {
            lengths.push(runs.0.len());
            for r in &runs.0 {
                let values =
                    [Some(r.first), Some(r.last), r.lead_end, Some(r.tail_start), Some(r.count), Some(r.micros)];
                for (c, v) in ints.iter_mut().zip(values) {
                    c.push(v);
                }
                floats[0].push(r.first_value);
                floats[1].push(r.last_value);
            }
        }
        let [first, last, lead_end, tail_start, count, micros] =
            ints.map(|c| Arc::new(Int64Array::from(c)) as ArrayRef);
        let [first_value, last_value] = floats.map(|c| Arc::new(Float64Array::from(c)) as ArrayRef);
        let columns = vec![first, last, first_value, last_value, lead_end, tail_start, count, micros];
        Arc::new(list_of_structs(run_fields(), columns, lengths))
    }

    fn results<'a>(&self, groups: impl Iterator<Item = &'a Runs<FlatRun>>) -> DfResult<ArrayRef> {
        let joined: Vec<Option<FlatRun>> = groups.map(|r| r.joined(self.threshold)).collect::<DfResult<_>>()?;
        let finished = joined.iter().map(|r| r.map(|r| r.finished(self.threshold)));
        Ok(match self.output {
            Output::Count => Arc::new(Int64Array::from_iter(finished.map(|r| r.map(|(n, _)| n)))),
            Output::Seconds => Arc::new(Float64Array::from_iter(finished.map(|r| r.map(|(_, us)| time::seconds(us))))),
        })
    }
}

/// Calls `add(row, run)` for each run in `lists`, a column of states.
fn for_each_run(lists: &ArrayRef, mut add: impl FnMut(usize, FlatRun)) -> DfResult<()> {
    let Some((runs, items)) = list_items(lists) else { return exec_err!("expected lists of runs") };
    let int = |i: usize| runs.column(i).as_primitive::<Int64Type>();
    let float = |i: usize| runs.column(i).as_primitive::<Float64Type>();
    let (first, last, first_value, last_value) = (int(0), int(1), float(2), float(3));
    let (lead_end, tail_start, count, micros) = (int(4), int(5), int(6), int(7));
    for (row, i) in items {
        add(
            row,
            FlatRun {
                first: first.value(i),
                last: last.value(i),
                first_value: first_value.value(i),
                last_value: last_value.value(i),
                lead_end: lead_end.is_valid(i).then(|| lead_end.value(i)),
                tail_start: tail_start.value(i),
                count: count.value(i),
                micros: micros.value(i),
            },
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::array::TimestampMicrosecondArray;
    use datafusion::common::ScalarValue;

    use super::*;
    use crate::testing::answer;

    const THRESHOLD: i64 = 10;

    fn group(output: Output) -> OneGroup<Flatlines> {
        OneGroup::new(Flatlines { output, threshold: THRESHOLD })
    }

    fn update(acc: &mut OneGroup<Flatlines>, readings: &[(i64, f64)]) {
        let times: ArrayRef = Arc::new(TimestampMicrosecondArray::from_iter_values(readings.iter().map(|r| r.0)));
        let values: ArrayRef = Arc::new(Float64Array::from_iter_values(readings.iter().map(|r| r.1)));
        let threshold: ArrayRef = Arc::new(Int64Array::from(vec![THRESHOLD; readings.len()]));
        acc.update_batch(&[times, values, threshold]).unwrap();
    }

    /// Straight from the definition: each stretch of one value, to the next value's first reading
    /// or its own last reading at the end.
    #[expect(clippy::float_cmp, reason = "a flatline is the same value exactly")]
    fn reference(readings: &[(i64, f64)]) -> (i64, i64) {
        let (mut count, mut micros, mut start) = (0, 0, 0);
        for i in 1..=readings.len() {
            let end = match readings.get(i) {
                Some(&(t, v)) if v != readings[start].1 => t,
                Some(_) => continue,
                None => readings[i - 1].0,
            };
            if end - readings[start].0 > THRESHOLD {
                count += 1;
                micros += end - readings[start].0;
            }
            start = i;
        }
        (count, micros)
    }

    #[test]
    #[expect(clippy::cast_precision_loss, reason = "test values are small")]
    fn split_and_merged_like_whole() {
        let mut seed = 7u64;
        let mut rand = |n: u64| {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            (seed >> 33) % n
        };
        for case in 0..500 {
            let mut t = 0i64;
            let readings: Vec<(i64, f64)> = (0..=rand(40))
                .map(|_| {
                    t += i64::try_from(1 + rand(8)).unwrap();
                    (t, rand(3) as f64)
                })
                .collect();
            let mut parts: Vec<&[(i64, f64)]> = vec![];
            let mut rest = readings.as_slice();
            while !rest.is_empty() {
                let k = 1 + usize::try_from(rand(rest.len() as u64)).unwrap();
                parts.push(&rest[..k]);
                rest = &rest[k..];
            }
            let shift = usize::try_from(rand(parts.len() as u64)).unwrap();
            parts.rotate_left(shift);
            let mut merged = [group(Output::Count), group(Output::Seconds)];
            for part in &parts {
                let mut acc = group(Output::Count);
                update(&mut acc, part);
                let ScalarValue::List(state) = acc.state().unwrap().remove(0) else { panic!("a list") };
                let threshold: ArrayRef = Arc::new(Int64Array::from(vec![THRESHOLD]));
                for m in &mut merged {
                    m.merge_batch(&[state.clone(), threshold.clone()]).unwrap();
                }
            }
            let (count, micros) = reference(&readings);
            assert_eq!(merged[0].evaluate().unwrap(), ScalarValue::Int64(Some(count)), "case {case}: {readings:?}");
            assert_eq!(merged[1].evaluate().unwrap(), ScalarValue::Float64(Some(time::seconds(micros))), "case {case}");
        }
    }

    #[tokio::test]
    async fn stuck_sensors() {
        let s = crate::Session::new(crate::testing::faulty_lake().0);
        let sql = "SELECT tag, flatline_count(t, v, INTERVAL '1 minute') AS n, flatline_seconds(t, v, INTERVAL '1 minute') AS s
                   FROM (VALUES ('a', TIMESTAMP '2026-01-01 00:00:00', 1.0), ('a', TIMESTAMP '2026-01-01 00:00:30', 2.0),
                                ('a', TIMESTAMP '2026-01-01 00:01:00', 2.0), ('a', TIMESTAMP '2026-01-01 00:03:00', 2.0),
                                ('a', TIMESTAMP '2026-01-01 00:03:30', 3.0), ('b', TIMESTAMP '2026-01-01 00:00:00', 5.0),
                                ('b', TIMESTAMP '2026-01-01 00:01:00', 6.0)) AS r(tag, t, v)
                   GROUP BY tag ORDER BY tag";
        let got = answer(&s, sql).await.unwrap();
        // a held 2.0 from 0:30 until 3.0 came at 3:30; b's minute isn't longer than the threshold.
        assert!(got.contains("| a   | 1 | 180.0 |") && got.contains("| b   | 0 | 0.0   |"), "{got}");
    }
}
