//! Gaps in a series' readings, over each group's first to last reading:
//! - `gap_count(time, threshold)`: how many times the readings stopped for longer than
//!   `threshold` (an interval: a sensor or gateway down);
//! - `gap_seconds(time, threshold)`: how long those gaps lasted in all.
//!
//! A gap is the time from a reading to the next, when longer than the threshold. A silence before
//! a group's first reading or after its last isn't one (join `time_buckets` to see those).
//!
//! Both share one state, a list of runs ([`super::runs`]): each with its first and last reading's
//! time and its gaps so far. Runs that follow each other join exactly: the time between them is a
//! gap when longer than the threshold. `gap_state(time, threshold)` returns the state, and each
//! aggregate also takes a state and the threshold, and merges it.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array, ListArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Int64Type, IntervalMonthDayNano, IntervalUnit, TimeUnit,
    TimestampMicrosecondType,
};
use datafusion::common::{ScalarValue, exec_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, Signature, Volatility};
use datafusion::physical_expr::expressions::Literal;
use datafusion::prelude::{Expr, lit};

use super::rollup::{DAY_MICROS, day_time_micros};
use super::runs::{AllGroups, Input, OneGroup, Run, RunAggregate, Runs};

/// What a gap aggregate returns from a group's joined run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Output {
    Count,
    Seconds,
    /// The state itself, as a rollup stores it.
    State,
}

impl Output {
    pub(crate) const ALL: [Output; 3] = [Output::Count, Output::Seconds, Output::State];

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Count => "gap_count",
            Self::Seconds => "gap_seconds",
            Self::State => "gap_state",
        }
    }

    /// The aggregate called `name`, if it is one of these.
    pub(crate) fn of(name: &str) -> Option<Output> {
        Self::ALL.into_iter().find(|o| o.name() == name)
    }
}

/// The aggregate function for `output`.
pub(crate) fn udaf(output: Output) -> AggregateUDF {
    AggregateUDF::from(GapAggregate { output, signature: Signature::user_defined(Volatility::Immutable) })
}

/// A gap aggregate as a rollup names it, with its threshold: `gap_count:300s` (whole seconds) or
/// `gap_count:1500us`.
pub(crate) fn rollup_name(output: Output, threshold: i64) -> String {
    match threshold % 1_000_000 {
        0 => format!("{}:{}s", output.name(), threshold / 1_000_000),
        _ => format!("{}:{threshold}us", output.name()),
    }
}

/// The gap aggregate and threshold (µs) a rollup's function names.
pub(crate) fn of_rollup(function: &str) -> Option<(Output, i64)> {
    let (name, threshold) = function.split_once(':')?;
    let output = Output::of(name)?;
    let threshold = match threshold.strip_suffix("us") {
        Some(us) => us.parse().ok()?,
        None => threshold.strip_suffix('s')?.parse::<i64>().ok()?.checked_mul(1_000_000)?,
    };
    Some((output, threshold))
}

/// A threshold (µs) as the interval literal the aggregates take.
pub(crate) fn threshold_literal(threshold: i64) -> Expr {
    // Whole days apart, so the nanoseconds can't overflow.
    let (days, micros) = (threshold.div_euclid(DAY_MICROS), threshold.rem_euclid(DAY_MICROS));
    let days = i32::try_from(days).expect("i64 microseconds are fewer than i32::MAX days");
    lit(ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(0, days, micros * 1_000))))
}

/// The threshold (µs) of an interval literal without months, as the aggregates take it.
pub(crate) fn threshold_of_literal(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::IntervalMonthDayNano(Some(i)) if i.months == 0 && i.days >= 0 && i.nanoseconds >= 0 => {
            day_time_micros(i.days, i.nanoseconds / 1_000)
        }
        _ => None,
    }
}

/// The type of one run.
fn run_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("first_time", DataType::Int64, false),
        Field::new("last_time", DataType::Int64, false),
        Field::new("gaps", DataType::Int64, false),
        Field::new("gap_micros", DataType::Int64, false),
    ]))
}

/// The state: a list of runs.
pub(crate) fn state_type() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(run_type(), true)))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct GapAggregate {
    output: Output,
    signature: Signature,
}

/// The threshold a call names (its last argument, an interval literal of days and smaller units),
/// in microseconds.
fn threshold_of(args: &AccumulatorArgs<'_>) -> DfResult<i64> {
    let literal = args.exprs.get(1).and_then(|e| e.downcast_ref::<Literal>()).map(Literal::value);
    let Some(threshold) = literal.and_then(threshold_of_literal) else {
        return plan_err!("the threshold must be an interval literal without months, such as INTERVAL '5 minutes'");
    };
    Ok(threshold)
}

impl AggregateUDFImpl for GapAggregate {
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
            [DataType::Timestamp(_, tz), DataType::Interval(_)] => {
                Ok(vec![DataType::Timestamp(TimeUnit::Microsecond, tz.clone()), interval])
            }
            _ => plan_err!("{} takes (time, threshold), or a state and the threshold", self.name()),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(match self.output {
            Output::Count => DataType::Int64,
            Output::Seconds => DataType::Float64,
            Output::State => state_type(),
        })
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> DfResult<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(format_state_name(args.name, "runs"), state_type(), true))])
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(OneGroup::new(Gaps { output: self.output, threshold: threshold_of(&args)? })))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs<'_>) -> bool {
        true
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(AllGroups::new(Gaps { output: self.output, threshold: threshold_of(&args)? })))
    }
}

/// A run of reading times, with its gaps longer than the threshold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GapRun {
    first: i64,
    last: i64,
    gaps: i64,
    /// How long the gaps lasted in all.
    micros: i64,
}

impl GapRun {
    /// Counts the time `span` from one reading to the next if it is a gap.
    fn pass(&mut self, span: i64, threshold: i64) {
        if span > threshold {
            self.gaps += 1;
            self.micros += span;
        }
    }
}

impl Run for GapRun {
    type Reading = i64;
    type How = i64;

    fn new(t: i64) -> Self {
        Self { first: t, last: t, gaps: 0, micros: 0 }
    }

    fn time(t: &i64) -> i64 {
        *t
    }

    fn first_time(&self) -> i64 {
        self.first
    }

    fn last_time(&self) -> i64 {
        self.last
    }

    fn push(&mut self, t: i64, threshold: i64) {
        self.pass(t - self.last, threshold);
        self.last = t;
    }

    fn join(mut self, next: GapRun, threshold: i64) -> GapRun {
        self.pass(next.first - self.last, threshold);
        GapRun { first: self.first, last: next.last, gaps: self.gaps + next.gaps, micros: self.micros + next.micros }
    }
}

/// A gap aggregate's output by its threshold, as a run aggregate.
#[derive(Debug)]
struct Gaps {
    output: Output,
    /// In microseconds.
    threshold: i64,
}

/// Calls `add(row, run)` for each run in `lists`, a column of states.
fn for_each_run(lists: &ArrayRef, mut add: impl FnMut(usize, GapRun)) -> DfResult<()> {
    let lists = lists.as_list::<i32>();
    let Some(runs) = lists.values().as_any().downcast_ref::<StructArray>() else {
        return exec_err!("expected lists of runs");
    };
    let column = |i: usize| runs.column(i).as_primitive::<Int64Type>();
    let (first, last, gaps, micros) = (column(0), column(1), column(2), column(3));
    let offsets = lists.value_offsets();
    for row in 0..lists.len() {
        if lists.is_null(row) {
            continue;
        }
        for i in offsets[row]..offsets[row + 1] {
            let i = usize::try_from(i).unwrap_or(usize::MAX);
            let run =
                GapRun { first: first.value(i), last: last.value(i), gaps: gaps.value(i), micros: micros.value(i) };
            add(row, run);
        }
    }
    Ok(())
}

impl RunAggregate for Gaps {
    type Run = GapRun;

    fn how(&self) -> i64 {
        self.threshold
    }

    /// Readings `(time [, threshold])` or states `(runs [, threshold])`: the threshold is known.
    fn inputs(&self, columns: &[ArrayRef], mut add: impl FnMut(usize, Input<GapRun>)) -> DfResult<()> {
        match columns {
            [lists] | [lists, _] if matches!(lists.data_type(), DataType::List(_)) => {
                for_each_run(lists, |row, r| add(row, Input::Run(r)))
            }
            [times] | [times, _] => {
                let times = times.as_primitive::<TimestampMicrosecondType>();
                for row in 0..times.len() {
                    if times.is_valid(row) {
                        add(row, Input::Reading(times.value(row)));
                    }
                }
                Ok(())
            }
            _ => exec_err!("expected (time, threshold) or runs"),
        }
    }

    fn states<'a>(&self, groups: impl Iterator<Item = &'a Runs<GapRun>>) -> ArrayRef {
        let mut columns: [Vec<i64>; 4] = Default::default();
        let mut lengths = vec![];
        for runs in groups {
            lengths.push(runs.0.len());
            for r in &runs.0 {
                for (c, v) in columns.iter_mut().zip([r.first, r.last, r.gaps, r.micros]) {
                    c.push(v);
                }
            }
        }
        let DataType::Struct(fields) = run_type() else { unreachable!("a struct") };
        let columns = columns.map(|c| Arc::new(Int64Array::from(c)) as ArrayRef).to_vec();
        let runs = StructArray::new(fields, columns, None);
        let field = Arc::new(Field::new_list_field(run_type(), true));
        Arc::new(ListArray::new(field, OffsetBuffer::from_lengths(lengths), Arc::new(runs), None))
    }

    /// The state, joined, or each group's gaps.
    fn results<'a>(&self, groups: impl Iterator<Item = &'a Runs<GapRun>>) -> DfResult<ArrayRef> {
        let joined: Vec<Option<GapRun>> = groups.map(|r| r.joined(self.threshold)).collect::<DfResult<_>>()?;
        Ok(match self.output {
            Output::Count => Arc::new(Int64Array::from_iter(joined.iter().map(|r| r.map(|r| r.gaps)))),
            Output::Seconds => {
                Arc::new(Float64Array::from_iter(joined.iter().map(|r| r.map(|r| r.micros as f64 / 1e6))))
            }
            Output::State => {
                let runs: Vec<Runs<GapRun>> = joined.into_iter().map(|r| Runs(r.into_iter().collect())).collect();
                self.states(runs.iter())
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::TimestampMicrosecondArray;

    use super::*;

    const THRESHOLD: i64 = 10;

    fn group(output: Output) -> OneGroup<Gaps> {
        OneGroup::new(Gaps { output, threshold: THRESHOLD })
    }

    fn update(acc: &mut OneGroup<Gaps>, times: &[i64]) {
        let times: ArrayRef = Arc::new(TimestampMicrosecondArray::from(times.to_vec()));
        let threshold: ArrayRef = Arc::new(Int64Array::from(vec![THRESHOLD; times.len()]));
        acc.update_batch(&[times, threshold]).unwrap();
    }

    fn value(acc: &mut OneGroup<Gaps>) -> ScalarValue {
        acc.evaluate().unwrap()
    }

    /// Straight from the definition: the sorted times' steps longer than the threshold.
    fn reference(times: &[i64]) -> (i64, i64) {
        let mut times = times.to_vec();
        times.sort_unstable();
        let gaps: Vec<i64> = times.windows(2).map(|w| w[1] - w[0]).filter(|&d| d > THRESHOLD).collect();
        (i64::try_from(gaps.len()).unwrap(), gaps.iter().sum())
    }

    #[test]
    fn rollup_names() {
        let names = [(Output::Count, 300_000_000, "gap_count:300s"), (Output::Seconds, 1_500, "gap_seconds:1500us")];
        for (output, threshold, name) in names {
            assert_eq!(rollup_name(output, threshold), name);
            assert_eq!(of_rollup(name), Some((output, threshold)));
        }
        assert_eq!(of_rollup("gap_state:300s"), Some((Output::State, 300_000_000)));
        assert_eq!(of_rollup("gap_count"), None);
    }

    #[test]
    fn known_values() {
        let times = [0, 5, 20, 20, 30, 45];
        let mut count = group(Output::Count);
        update(&mut count, &times);
        // 5 → 20 and 30 → 45 are longer than 10; 20 → 30 isn't.
        assert_eq!(value(&mut count), ScalarValue::Int64(Some(2)));
        let mut seconds = group(Output::Seconds);
        update(&mut seconds, &times);
        assert_eq!(value(&mut seconds), ScalarValue::Float64(Some(30e-6)));
        // One reading: no gaps. None: null.
        let mut one = group(Output::Count);
        update(&mut one, &[7]);
        assert_eq!(value(&mut one), ScalarValue::Int64(Some(0)));
        assert_eq!(value(&mut group(Output::Count)), ScalarValue::Int64(None));
    }

    /// Times split into runs contiguous in time (sharing boundary times), merged through states in
    /// any order, give the gaps of all times.
    #[test]
    fn merged_runs_give_the_gaps_of_all_readings() {
        let mut seed = 11u64;
        let mut rand = |n: u64| {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            (seed >> 33) % n
        };
        for case in 0..300 {
            let mut t = 0i64;
            let times: Vec<i64> = (0..=rand(40))
                .map(|_| {
                    t += i64::try_from(rand(25)).unwrap();
                    t
                })
                .collect();
            let mut runs: Vec<&[i64]> = vec![];
            let mut rest = times.as_slice();
            while !rest.is_empty() {
                let k = 1 + usize::try_from(rand(rest.len() as u64)).unwrap();
                runs.push(&rest[..k]);
                rest = &rest[k..];
            }
            let shift = usize::try_from(rand(runs.len() as u64)).unwrap();
            runs.rotate_left(shift);
            let mut merged = [group(Output::Count), group(Output::Seconds)];
            for run in &runs {
                let mut part = group(Output::State);
                update(&mut part, run);
                let ScalarValue::List(state) = part.state().unwrap().remove(0) else { panic!("a list") };
                let threshold: ArrayRef = Arc::new(Int64Array::from(vec![THRESHOLD]));
                for m in &mut merged {
                    m.merge_batch(&[state.clone(), threshold.clone()]).unwrap();
                }
            }
            let (gaps, micros) = reference(&times);
            assert_eq!(value(&mut merged[0]), ScalarValue::Int64(Some(gaps)), "case {case}");
            assert_eq!(value(&mut merged[1]), ScalarValue::Float64(Some(micros as f64 / 1e6)), "case {case}");
        }
    }

    #[tokio::test]
    async fn gaps_of_each_series() {
        let s = crate::Session::new(crate::testing::faulty_lake().0);
        let sql = "SELECT tag, gap_count(t, INTERVAL '1 minute') AS gaps, gap_seconds(t, INTERVAL '1 minute') AS secs
                   FROM (VALUES ('a', TIMESTAMP '2026-01-01 00:00:00'), ('a', TIMESTAMP '2026-01-01 00:00:30'),
                                ('a', TIMESTAMP '2026-01-01 00:05:30'), ('a', TIMESTAMP '2026-01-01 00:06:00'),
                                ('b', TIMESTAMP '2026-01-01 00:00:00'), ('b', TIMESTAMP '2026-01-01 00:01:00'))
                        AS r(tag, t)
                   GROUP BY tag ORDER BY tag";
        let rows = s.sql(sql).await.unwrap().collect().await.unwrap();
        let got = arrow::util::pretty::pretty_format_batches(&rows).unwrap().to_string();
        // a stopped from 0:30 to 5:30; b's minute is not longer than the threshold.
        assert!(got.contains("| a   | 1    | 300.0 |") && got.contains("| b   | 0    | 0.0   |"), "{got}");
        let months = "SELECT gap_count(t, INTERVAL '1 month') FROM (VALUES (TIMESTAMP '2026-01-01')) AS r(t)";
        let err = match s.sql(months).await {
            Ok(df) => df.collect().await.unwrap_err().to_string(),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("without months"), "{err}");
        let huge = "SELECT gap_count(t, INTERVAL '200000000 days') FROM (VALUES (TIMESTAMP '2026-01-01')) AS r(t)";
        let err = match s.sql(huge).await {
            Ok(df) => df.collect().await.unwrap_err().to_string(),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("threshold"), "{err}");
    }

    /// A threshold as a literal and back, up to the largest the microseconds hold.
    #[test]
    fn thresholds_round_trip() {
        for threshold in [0, 1_500, 300_000_000, i64::MAX] {
            let Expr::Literal(value, _) = threshold_literal(threshold) else { panic!("a literal") };
            assert_eq!(threshold_of_literal(&value), Some(threshold));
        }
    }
}
