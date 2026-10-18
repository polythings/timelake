//! `time_weighted_mean(time, value [, method])`: the mean of a signal over time, from irregular
//! readings, over each group's first to last reading. With `forward_fill` (the default; pandas'
//! `ffill` too) a reading holds until the next; with `linear` the signal moves in a straight line
//! between readings. Readings at the same time count as one, with the largest value.
//!
//! The state is a list of segments: runs of a group's readings in time order, each with its first
//! and last reading, the time from the first to the next and into the last, and the area under
//! the signal. Segments that follow each other in time join exactly (the gap between them adds
//! its area), so partial results from different partitions, and a rollup's buckets, merge into
//! the mean of all readings. Segments that overlap in time don't: their readings would have to be
//! interleaved, so the mean is reported as an error, and scans that could give such runs merge
//! their files in series order first (setting `time-weighted-mean`, [`super::series_order`]).
//!
//! `time_weighted_state(…)` takes the same arguments and returns the state; `time_weighted_mean`
//! and `time_weighted_state` of a list of segments merge it. Rollups store the state.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int64Array, ListArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Float64Type, Int64Type, TimeUnit, TimestampMicrosecondType};
use datafusion::common::{ScalarValue, exec_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;

/// The `time_weighted_mean` aggregate: the mean.
pub(crate) fn time_weighted_mean_udaf() -> AggregateUDF {
    AggregateUDF::from(TimeWeighted {
        name: "time_weighted_mean",
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

/// The `time_weighted_state` aggregate: the state, as a rollup stores it.
pub(crate) fn time_weighted_state_udaf() -> AggregateUDF {
    AggregateUDF::from(TimeWeighted {
        name: "time_weighted_state",
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

/// How the signal moves between readings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Method {
    ForwardFill,
    Linear,
}

impl Method {
    pub(crate) fn parse(s: &str) -> DfResult<Self> {
        match s.to_ascii_lowercase().as_str() {
            "forward_fill" | "ffill" => Ok(Self::ForwardFill),
            "linear" => Ok(Self::Linear),
            other => plan_err!("time_weighted_mean: method must be 'forward_fill' or 'linear', not {other:?}"),
        }
    }

    /// The area between a reading of value `a` and the next, of `b`, `dt` later.
    fn area(self, a: f64, b: f64, dt: i64) -> f64 {
        let dt = dt as f64;
        match self {
            Self::ForwardFill => a * dt,
            Self::Linear => f64::midpoint(a, b) * dt,
        }
    }
}

/// The type of one segment.
fn segment_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("first_time", DataType::Int64, false),
        Field::new("first_value", DataType::Float64, false),
        Field::new("first_span", DataType::Int64, false),
        Field::new("last_time", DataType::Int64, false),
        Field::new("last_value", DataType::Float64, false),
        Field::new("last_span", DataType::Int64, false),
        Field::new("area", DataType::Float64, false),
    ]))
}

/// The state: a list of segments.
fn state_type() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(segment_type(), true)))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TimeWeighted {
    name: &'static str,
    signature: Signature,
}

impl TimeWeighted {
    fn returns_state(&self) -> bool {
        self.name == "time_weighted_state"
    }
}

/// The method a call names (its last argument, a string literal), else `forward_fill`.
fn method_of(args: &AccumulatorArgs<'_>) -> DfResult<Method> {
    // After the readings `(time, value)`, or after a state.
    let at = if args.expr_fields.first().is_some_and(is_list) { 1 } else { 2 };
    let Some(method) = args.exprs.get(at) else { return Ok(Method::ForwardFill) };
    if let Some(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s))) =
        method.downcast_ref::<Literal>().map(Literal::value)
    {
        Method::parse(s)
    } else {
        plan_err!("time_weighted_mean: the method must be a string literal")
    }
}

fn is_list(field: &FieldRef) -> bool {
    matches!(field.data_type(), DataType::List(_))
}

impl AggregateUDFImpl for TimeWeighted {
    fn name(&self) -> &'static str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        let text = |t: &DataType| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View);
        match arg_types {
            [DataType::List(_)] => Ok(vec![state_type()]),
            [DataType::List(_), m] if text(m) => Ok(vec![state_type(), DataType::Utf8]),
            [DataType::Timestamp(_, tz), v] if v.is_numeric() => {
                Ok(vec![DataType::Timestamp(TimeUnit::Microsecond, tz.clone()), DataType::Float64])
            }
            [DataType::Timestamp(_, tz), v, m] if v.is_numeric() && text(m) => {
                Ok(vec![DataType::Timestamp(TimeUnit::Microsecond, tz.clone()), DataType::Float64, DataType::Utf8])
            }
            _ => plan_err!("{} takes (time, value [, method]), or the state other calls return [, method]", self.name),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(if self.returns_state() { state_type() } else { DataType::Float64 })
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> DfResult<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(format_state_name(args.name, "segments"), state_type(), true))])
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(OneGroup { runs: Runs::default(), method: method_of(&args)?, state: self.returns_state() }))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs<'_>) -> bool {
        true
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(AllGroups { groups: vec![], method: method_of(&args)?, state: self.returns_state() }))
    }
}

/// A run of readings in time order.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Segment {
    first: (i64, f64),
    /// From the first reading to the next (0 while the segment has one time).
    first_span: i64,
    last: (i64, f64),
    /// From the reading before the last to the last (0 while the segment has one time).
    last_span: i64,
    area: f64,
}

impl Segment {
    fn new(p: (i64, f64)) -> Self {
        Self { first: p, first_span: 0, last: p, last_span: 0, area: 0.0 }
    }

    /// Adds reading `p`, at or after the last.
    fn push(&mut self, p: (i64, f64), method: Method) {
        if p.0 == self.last.0 {
            self.raise_last(p.1, method);
        } else {
            let span = p.0 - self.last.0;
            self.area += method.area(self.last.1, p.1, span);
            if self.first_span == 0 {
                self.first_span = span;
            }
            self.last_span = span;
            self.last = p;
        }
    }

    /// The last reading's value becomes the larger of it and `v` (readings at one time count as
    /// one, with the largest value), and the area into it follows.
    fn raise_last(&mut self, v: f64, method: Method) {
        let max = self.last.1.max(v);
        if method == Method::Linear {
            self.area += (max - self.last.1) / 2.0 * self.last_span as f64;
        }
        if self.first.0 == self.last.0 {
            self.first.1 = max;
        }
        self.last.1 = max;
    }

    /// The first reading's value becomes the larger of it and `v`, and the area out of it follows.
    fn raise_first(&mut self, v: f64, method: Method) {
        let max = self.first.1.max(v);
        let gain = max - self.first.1;
        self.area += match method {
            Method::ForwardFill => gain * self.first_span as f64,
            Method::Linear => gain / 2.0 * self.first_span as f64,
        };
        if self.first.0 == self.last.0 {
            self.last.1 = max;
        }
        self.first.1 = max;
    }

    /// `self` followed by `next`, which starts at or after `self`'s last reading.
    fn join(mut self, mut next: Segment, method: Method) -> Segment {
        if next.first.0 == self.last.0 {
            let v = self.last.1.max(next.first.1);
            self.raise_last(v, method);
            next.raise_first(v, method);
            let first_span = if self.first_span > 0 { self.first_span } else { next.first_span };
            let last_span = if next.last_span > 0 { next.last_span } else { self.last_span };
            let first = if self.first.0 == next.last.0 { (self.first.0, v) } else { self.first };
            return Segment { first, first_span, last: next.last, last_span, area: self.area + next.area };
        }
        let gap = next.first.0 - self.last.0;
        Segment {
            first: self.first,
            first_span: if self.first_span > 0 { self.first_span } else { gap },
            last: next.last,
            last_span: if next.last_span > 0 { next.last_span } else { gap },
            area: self.area + method.area(self.last.1, next.first.1, gap) + next.area,
        }
    }
}

/// A group's segments so far, in arrival order: the run being extended is the last.
#[derive(Clone, Debug, Default)]
struct Runs(Vec<Segment>);

impl Runs {
    fn add(&mut self, p: (i64, f64), method: Method) {
        match self.0.last_mut() {
            Some(run) if p.0 >= run.last.0 => run.push(p, method),
            _ => self.0.push(Segment::new(p)),
        }
    }

    /// All segments joined in time order, or an error if two overlap.
    fn joined(&self, method: Method) -> DfResult<Option<Segment>> {
        let mut runs = self.0.clone();
        runs.sort_by(|a, b| a.first.0.cmp(&b.first.0).then(a.last.0.cmp(&b.last.0)));
        let mut out: Option<Segment> = None;
        for run in runs {
            out = Some(match out {
                None => run,
                Some(prev) if run.first.0 < prev.last.0 => {
                    return exec_err!(
                        "time_weighted_mean: a group's readings came in runs that overlap in time. Group by the series key \
                         (each series is one signal); if you do, the files were written out of order: SET \
                         timelake.time_weighted_mean = 'merge' reads each series in order"
                    );
                }
                Some(prev) => prev.join(run, method),
            });
        }
        Ok(out)
    }

    fn mean(&self, method: Method) -> DfResult<Option<f64>> {
        Ok(self.joined(method)?.map(|s| {
            let span = s.last.0 - s.first.0;
            if span == 0 { s.first.1 } else { s.area / span as f64 }
        }))
    }
}

/// Calls `add(row, segment)` for each segment in `lists`, a column of states.
fn for_each_segment(lists: &ArrayRef, mut add: impl FnMut(usize, Segment)) -> DfResult<()> {
    let lists = lists.as_list::<i32>();
    let Some(segments) = lists.values().as_any().downcast_ref::<StructArray>() else {
        return exec_err!("time_weighted_mean merges lists of segments");
    };
    let i64s = |i: usize| segments.column(i).as_primitive::<Int64Type>();
    let f64s = |i: usize| segments.column(i).as_primitive::<Float64Type>();
    let (first_t, first_v, first_span, last_t, last_v, last_span, area) =
        (i64s(0), f64s(1), i64s(2), i64s(3), f64s(4), i64s(5), f64s(6));
    for row in 0..lists.len() {
        if lists.is_null(row) {
            continue;
        }
        let offsets = lists.value_offsets();
        for i in offsets[row]..offsets[row + 1] {
            let i = usize::try_from(i).unwrap_or(usize::MAX);
            add(
                row,
                Segment {
                    first: (first_t.value(i), first_v.value(i)),
                    first_span: first_span.value(i),
                    last: (last_t.value(i), last_v.value(i)),
                    last_span: last_span.value(i),
                    area: area.value(i),
                },
            );
        }
    }
    Ok(())
}

/// Calls `add(row, reading)` for the readings of `(time, value)` columns, skipping nulls.
fn for_each_reading(times: &ArrayRef, values: &ArrayRef, mut add: impl FnMut(usize, (i64, f64))) {
    let times = times.as_primitive::<TimestampMicrosecondType>();
    let values = values.as_primitive::<Float64Type>();
    for row in 0..times.len() {
        if times.is_valid(row) && values.is_valid(row) {
            add(row, (times.value(row), values.value(row)));
        }
    }
}

/// The groups' segments as a state column: each group's runs as they came (joining them here
/// could hide a run of another partition between them).
fn states<'a>(groups: impl Iterator<Item = &'a Runs>) -> ListArray {
    let mut columns: [Vec<i64>; 4] = Default::default();
    let mut values: [Vec<f64>; 3] = Default::default();
    let mut lengths = vec![];
    for runs in groups {
        lengths.push(runs.0.len());
        for s in &runs.0 {
            columns[0].push(s.first.0);
            values[0].push(s.first.1);
            columns[1].push(s.first_span);
            columns[2].push(s.last.0);
            values[1].push(s.last.1);
            columns[3].push(s.last_span);
            values[2].push(s.area);
        }
    }
    let [first_t, first_span, last_t, last_span] = columns.map(|c| Arc::new(Int64Array::from(c)) as ArrayRef);
    let [first_v, last_v, area] = values.map(|v| Arc::new(Float64Array::from(v)) as ArrayRef);
    let DataType::Struct(fields) = segment_type() else { unreachable!("a struct") };
    let segments = StructArray::new(fields, vec![first_t, first_v, first_span, last_t, last_v, last_span, area], None);
    let field = Arc::new(Field::new_list_field(segment_type(), true));
    ListArray::new(field, OffsetBuffer::from_lengths(lengths), Arc::new(segments), None)
}

/// Calls `add` for each row's input: readings `(time, value [, method])` or states
/// `(segments [, method])`.
fn add_input(columns: &[ArrayRef], mut add: impl FnMut(usize, AddInput)) -> DfResult<()> {
    match columns {
        [lists] | [lists, _] if matches!(lists.data_type(), DataType::List(_)) => {
            for_each_segment(lists, |row, s| add(row, AddInput::Segment(s)))
        }
        [times, values] | [times, values, _] => {
            for_each_reading(times, values, |row, p| add(row, AddInput::Reading(p)));
            Ok(())
        }
        _ => exec_err!("time_weighted_mean takes (time, value [, method]) or segments"),
    }
}

/// One row's input.
#[derive(Clone, Copy)]
enum AddInput {
    Reading((i64, f64)),
    Segment(Segment),
}

impl Runs {
    fn take(&mut self, input: AddInput, method: Method) {
        match input {
            AddInput::Reading(p) => self.add(p, method),
            AddInput::Segment(s) => self.0.push(s),
        }
    }
}

/// One group (DataFusion's fallback, e.g. without `GROUP BY`).
#[derive(Debug)]
struct OneGroup {
    runs: Runs,
    method: Method,
    /// Return the state (`time_weighted_state`), not the mean.
    state: bool,
}

impl Accumulator for OneGroup {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let method = self.method;
        add_input(values, |_, input| self.runs.take(input, method))
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::List(Arc::new(states(std::iter::once(&self.runs))))])
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        if self.state {
            let joined = Runs(self.runs.joined(self.method)?.into_iter().collect());
            return Ok(ScalarValue::List(Arc::new(states(std::iter::once(&joined)))));
        }
        Ok(ScalarValue::Float64(self.runs.mean(self.method)?))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.runs.0.capacity() * size_of::<Segment>()
    }
}

/// All groups at once, as DataFusion's grouped aggregation uses.
#[derive(Debug)]
struct AllGroups {
    groups: Vec<Runs>,
    method: Method,
    state: bool,
}

impl AllGroups {
    fn add(&mut self, columns: &[ArrayRef], groups: &[usize], filter: Option<&BooleanArray>, n: usize) -> DfResult<()> {
        self.groups.resize_with(n, Runs::default);
        let (all, method) = (&mut self.groups, self.method);
        add_input(columns, |row, input| {
            if filter.is_none_or(|f| f.is_valid(row) && f.value(row)) {
                all[groups[row]].take(input, method);
            }
        })
    }
}

impl GroupsAccumulator for AllGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DfResult<()> {
        self.add(values, group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DfResult<()> {
        self.add(values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DfResult<ArrayRef> {
        let taken = emit_to.take_needed(&mut self.groups);
        if self.state {
            let joined: Vec<Runs> = taken
                .iter()
                .map(|r| Ok(Runs(r.joined(self.method)?.into_iter().collect())))
                .collect::<DfResult<_>>()?;
            return Ok(Arc::new(states(joined.iter())));
        }
        let means: Vec<Option<f64>> = taken.iter().map(|r| r.mean(self.method)).collect::<DfResult<_>>()?;
        Ok(Arc::new(Float64Array::from(means)))
    }

    fn state(&mut self, emit_to: EmitTo) -> DfResult<Vec<ArrayRef>> {
        let taken = emit_to.take_needed(&mut self.groups);
        Ok(vec![Arc::new(states(taken.iter()))])
    }

    fn size(&self) -> usize {
        self.groups.iter().map(|r| r.0.capacity() * size_of::<Segment>()).sum::<usize>()
            + self.groups.capacity() * size_of::<Runs>()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::TimestampMicrosecondArray;

    use super::*;

    fn group(method: Method) -> OneGroup {
        OneGroup { runs: Runs::default(), method, state: false }
    }

    fn update(acc: &mut OneGroup, rows: &[(i64, f64)]) {
        let times: ArrayRef = Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>()));
        let values: ArrayRef = Arc::new(Float64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>()));
        acc.update_batch(&[times, values]).unwrap();
    }

    fn mean(acc: &mut OneGroup) -> Option<f64> {
        match acc.evaluate().unwrap() {
            ScalarValue::Float64(v) => v,
            other => panic!("{other:?}"),
        }
    }

    /// The mean straight from the definition: readings sorted, one per time with the largest value.
    fn reference(rows: &[(i64, f64)], method: Method) -> Option<f64> {
        let mut rows = rows.to_vec();
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
        let mut points: Vec<(i64, f64)> = vec![];
        for p in rows {
            match points.last_mut() {
                Some(last) if last.0 == p.0 => last.1 = last.1.max(p.1),
                _ => points.push(p),
            }
        }
        let (first, last) = (points.first()?, points.last()?);
        if first.0 == last.0 {
            return Some(first.1);
        }
        let area: f64 = points.windows(2).map(|w| method.area(w[0].1, w[1].1, w[1].0 - w[0].0)).sum();
        Some(area / (last.0 - first.0) as f64)
    }

    #[test]
    fn forward_fill_and_linear() {
        let rows = [(0, 1.0), (10, 3.0), (20, 0.0)];
        let mut ffill = group(Method::ForwardFill);
        update(&mut ffill, &rows);
        assert_eq!(mean(&mut ffill), Some(2.0)); // (1 × 10 + 3 × 10) / 20
        let mut linear = group(Method::Linear);
        update(&mut linear, &rows);
        assert_eq!(mean(&mut linear), Some(1.75)); // (2 × 10 + 1.5 × 10) / 20
        // One time: its largest value. No readings: null.
        let mut one = group(Method::Linear);
        update(&mut one, &[(5, 1.0), (5, 4.0), (5, 2.0)]);
        assert_eq!(mean(&mut one), Some(4.0));
        assert_eq!(mean(&mut group(Method::ForwardFill)), None);
    }

    /// Readings split into runs contiguous in time (sharing boundary times, with repeats), merged
    /// through states in any order, give the mean of all readings.
    #[test]
    fn merged_runs_give_the_mean_of_all_readings() {
        let mut seed = 7u64;
        let mut rand = |n: u64| {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            (seed >> 33) % n
        };
        for case in 0..300 {
            let n = 1 + rand(40);
            let mut t = 0i64;
            let rows: Vec<(i64, f64)> = (0..n)
                .map(|_| {
                    t += i64::try_from(rand(3)).unwrap(); // repeats when 0
                    (t, rand(50) as f64 - 10.0)
                })
                .collect();
            for method in [Method::ForwardFill, Method::Linear] {
                // Cut into runs; a run may start with the time the previous ended at.
                let mut runs: Vec<Vec<(i64, f64)>> = vec![];
                let mut rest = rows.as_slice();
                while !rest.is_empty() {
                    let k = 1 + usize::try_from(rand(rest.len() as u64)).unwrap();
                    runs.push(rest[..k].to_vec());
                    rest = &rest[k..];
                }
                let shift = usize::try_from(rand(runs.len() as u64)).unwrap();
                runs.rotate_left(shift);
                let mut merged = group(method);
                for run in &runs {
                    let mut part = group(method);
                    update(&mut part, run);
                    let ScalarValue::List(state) = part.state().unwrap().remove(0) else { panic!("a list") };
                    merged.merge_batch(&[state]).unwrap();
                }
                let (got, want) = (mean(&mut merged).unwrap(), reference(&rows, method).unwrap());
                assert!((got - want).abs() <= 1e-9 * want.abs().max(1.0), "case {case} {method:?}: {got} vs {want}");
            }
        }
    }

    #[test]
    fn overlapping_runs_are_an_error() {
        let mut a = group(Method::ForwardFill);
        update(&mut a, &[(0, 1.0), (10, 2.0)]);
        let ScalarValue::List(state) = a.state().unwrap().remove(0) else { panic!("a list") };
        let mut b = group(Method::ForwardFill);
        update(&mut b, &[(5, 1.0), (15, 2.0)]);
        b.merge_batch(&[state]).unwrap();
        assert!(b.evaluate().unwrap_err().to_string().contains("overlap in time"));
    }
}
