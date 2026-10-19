//! Aggregates over a signal's readings in time order, from irregular readings, over each group's
//! first to last reading:
//! - `time_weighted_mean(time, value [, method])`: the mean, each reading weighted by how long it
//!   held. With `forward_fill` (the default; pandas' `ffill` too) a reading holds until the next;
//!   with `linear` the signal moves in a straight line between readings.
//! - `integral(time, value [, method])`: the area under the signal, in value × seconds.
//! - `counter_diff(time, value)`: how much a counter grew; a drop is a reset to zero, so the
//!   reading after it counts whole.
//! - `counter_rate(time, value)`: that growth per second.
//!
//! A condition is a value of 1 while true and 0 while false: `integral(time, value > limit)` is
//! the seconds spent above the limit, `time_weighted_mean(time, state = 'running')` the share of
//! time running.
//!
//! Readings at the same time count as one, with the largest value.
//!
//! All share one state, a list of segments: runs of a group's readings in time order
//! ([`super::runs`]), each with its first and last reading, the values of the readings next to
//! them, the times between, the area under the signal and the counter's growth. Segments that
//! follow each other in time join exactly (the gap between them adds its area and growth).
//!
//! `time_weighted_state(time, value [, method])` returns the state, and each aggregate also takes
//! a state (`[, method]` for the mean and the integral) and merges it. Rollups store the state.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array, ListArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Float64Type, Int64Type, TimeUnit, TimestampMicrosecondType};
use datafusion::common::{ScalarValue, exec_err, plan_datafusion_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, GroupsAccumulator, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;

use super::runs::{AllGroups, Input, OneGroup, Run, RunAggregate, Runs};

/// What an aggregate returns from a group's joined segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Output {
    Mean,
    Integral,
    CounterDiff,
    CounterRate,
    /// The state itself, as a rollup stores it.
    State,
}

impl Output {
    pub(crate) const ALL: [Output; 5] =
        [Output::Mean, Output::Integral, Output::CounterDiff, Output::CounterRate, Output::State];

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Mean => "time_weighted_mean",
            Self::Integral => "integral",
            Self::CounterDiff => "counter_diff",
            Self::CounterRate => "counter_rate",
            Self::State => "time_weighted_state",
        }
    }

    /// The aggregate called `name`, if it is one of these.
    pub(crate) fn of(name: &str) -> Option<Output> {
        Self::ALL.into_iter().find(|o| o.name() == name)
    }

    /// Whether the result depends on how the signal moves between readings.
    pub(crate) fn takes_method(self) -> bool {
        matches!(self, Self::Mean | Self::Integral | Self::State)
    }
}

/// The aggregate function for `output`.
pub(crate) fn udaf(output: Output) -> AggregateUDF {
    AggregateUDF::from(SignalAggregate { output, signature: Signature::user_defined(Volatility::Immutable) })
}

/// The scalar function giving what `output` returns for a column of states (`integral_of_state`),
/// so that several signal aggregates of the same readings compute one state ([`super::shared_state`]).
pub(crate) fn of_state_udf(output: Output, method: Method) -> ScalarUDF {
    let name = format!("{}_of_state", output.name());
    ScalarUDF::from(OfState { name, output, method, signature: Signature::any(1, Volatility::Immutable) })
}

/// How the signal moves between readings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Method {
    ForwardFill,
    Linear,
}

impl Method {
    pub(crate) fn parse(s: &str) -> DfResult<Self> {
        match s.to_ascii_lowercase().as_str() {
            "forward_fill" | "ffill" => Ok(Self::ForwardFill),
            "linear" => Ok(Self::Linear),
            other => plan_err!("the method must be 'forward_fill' or 'linear', not {other:?}"),
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

/// A signal aggregate as a rollup names it: `name`, or `name:linear` for the linear method.
pub(crate) fn of_rollup(function: &str) -> Option<(Output, Method)> {
    let (name, method) = match function.split_once(':') {
        Some((name, "linear")) => (name, Method::Linear),
        Some(_) => return None,
        None => (function, Method::ForwardFill),
    };
    Output::of(name).filter(|o| *o != Output::State).map(|o| (o, method))
}

/// The rollup name of `output` by `method`.
pub(crate) fn rollup_name(output: Output, method: Method) -> String {
    match method {
        Method::Linear if output.takes_method() => format!("{}:linear", output.name()),
        _ => output.name().to_string(),
    }
}

/// How much a counter grew from a reading of `a` to the next, of `b`: a drop is a reset to zero.
fn growth(a: f64, b: f64) -> f64 {
    if b >= a { b - a } else { b }
}

/// The type of one segment.
fn segment_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("first_time", DataType::Int64, false),
        Field::new("first_value", DataType::Float64, false),
        Field::new("first_span", DataType::Int64, false),
        Field::new("second_value", DataType::Float64, true),
        Field::new("last_time", DataType::Int64, false),
        Field::new("last_value", DataType::Float64, false),
        Field::new("last_span", DataType::Int64, false),
        Field::new("penultimate_value", DataType::Float64, true),
        Field::new("area", DataType::Float64, false),
        Field::new("growth", DataType::Float64, false),
    ]))
}

/// The state: a list of segments.
pub(crate) fn state_type() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(segment_type(), true)))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SignalAggregate {
    output: Output,
    signature: Signature,
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
        plan_err!("the method must be a string literal")
    }
}

fn is_list(field: &FieldRef) -> bool {
    matches!(field.data_type(), DataType::List(_))
}

impl AggregateUDFImpl for SignalAggregate {
    fn name(&self) -> &'static str {
        self.output.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        let text = |t: &DataType| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View);
        // A condition counts as 1 while true: `integral(time, value > limit)` is the seconds above it.
        let value = |t: &DataType| t.is_numeric() || *t == DataType::Boolean;
        let method = self.output.takes_method();
        match arg_types {
            [DataType::List(_)] => Ok(vec![state_type()]),
            [DataType::List(_), m] if method && text(m) => Ok(vec![state_type(), DataType::Utf8]),
            [DataType::Timestamp(_, tz), v] if value(v) => {
                Ok(vec![DataType::Timestamp(TimeUnit::Microsecond, tz.clone()), DataType::Float64])
            }
            [DataType::Timestamp(_, tz), v, m] if method && value(v) && text(m) => {
                Ok(vec![DataType::Timestamp(TimeUnit::Microsecond, tz.clone()), DataType::Float64, DataType::Utf8])
            }
            _ if method => plan_err!("{} takes (time, value [, method]), or a state [, method]", self.name()),
            _ => plan_err!("{} takes (time, value), or a state", self.name()),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(if self.output == Output::State { state_type() } else { DataType::Float64 })
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> DfResult<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(format_state_name(args.name, "segments"), state_type(), true))])
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(OneGroup::new(Signal { output: self.output, method: method_of(&args)? })))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs<'_>) -> bool {
        true
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(AllGroups::new(Signal { output: self.output, method: method_of(&args)? })))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct OfState {
    name: String,
    output: Output,
    method: Method,
    signature: Signature,
}

impl ScalarUDFImpl for OfState {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let [state] =
            ColumnarValue::values_to_arrays(&args.args)?.try_into().map_err(|_| plan_datafusion_err!("one state"))?;
        let mut groups = vec![Runs::default(); state.len()];
        for_each_segment(&state, |row, s| groups[row].0.push(s))?;
        Ok(ColumnarValue::Array(Signal { output: self.output, method: self.method }.results(groups.iter())?))
    }
}

/// A run of readings in time order.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Segment {
    first: (i64, f64),
    /// From the first reading to the next (0 while the segment has one time).
    first_span: i64,
    /// The value of the reading after the first (meaningless while the segment has one time).
    second: f64,
    last: (i64, f64),
    /// From the reading before the last to the last (0 while the segment has one time).
    last_span: i64,
    /// The value of the reading before the last (meaningless while the segment has one time).
    penultimate: f64,
    area: f64,
    growth: f64,
}

impl Run for Segment {
    type Reading = (i64, f64);
    type How = Method;

    fn new(p: (i64, f64)) -> Self {
        Self { first: p, first_span: 0, second: p.1, last: p, last_span: 0, penultimate: p.1, area: 0.0, growth: 0.0 }
    }

    fn time(p: &(i64, f64)) -> i64 {
        p.0
    }

    fn first_time(&self) -> i64 {
        self.first.0
    }

    fn last_time(&self) -> i64 {
        self.last.0
    }

    fn push(&mut self, p: (i64, f64), method: Method) {
        if p.0 == self.last.0 {
            self.raise_last(p.1, method);
            return;
        }
        let span = p.0 - self.last.0;
        self.area += method.area(self.last.1, p.1, span);
        self.growth += growth(self.last.1, p.1);
        if self.one_time() {
            self.first_span = span;
            self.second = p.1;
        }
        self.last_span = span;
        self.penultimate = self.last.1;
        self.last = p;
    }

    fn join(mut self, mut next: Segment, method: Method) -> Segment {
        if next.first.0 == self.last.0 {
            let v = self.last.1.max(next.first.1);
            self.raise_last(v, method);
            next.raise_first(v, method);
            return Segment {
                first: self.first,
                first_span: if self.one_time() { next.first_span } else { self.first_span },
                second: if self.one_time() { next.second } else { self.second },
                last: next.last,
                last_span: if next.one_time() { self.last_span } else { next.last_span },
                penultimate: if next.one_time() { self.penultimate } else { next.penultimate },
                area: self.area + next.area,
                growth: self.growth + next.growth,
            };
        }
        let gap = next.first.0 - self.last.0;
        Segment {
            first: self.first,
            first_span: if self.one_time() { gap } else { self.first_span },
            second: if self.one_time() { next.first.1 } else { self.second },
            last: next.last,
            last_span: if next.one_time() { gap } else { next.last_span },
            penultimate: if next.one_time() { self.last.1 } else { next.penultimate },
            area: self.area + method.area(self.last.1, next.first.1, gap) + next.area,
            growth: self.growth + growth(self.last.1, next.first.1) + next.growth,
        }
    }
}

impl Segment {
    fn one_time(&self) -> bool {
        self.first.0 == self.last.0
    }

    /// The last reading's value becomes the larger of it and `v` (readings at one time count as
    /// one, with the largest value), and what led into it follows.
    fn raise_last(&mut self, v: f64, method: Method) {
        let (old, new) = (self.last.1, self.last.1.max(v));
        if self.one_time() {
            self.first.1 = new;
        } else {
            if method == Method::Linear {
                self.area += (new - old) / 2.0 * self.last_span as f64;
            }
            self.growth += growth(self.penultimate, new) - growth(self.penultimate, old);
            // With two times, the last is also the second.
            if self.first.0 + self.first_span == self.last.0 {
                self.second = new;
            }
        }
        self.last.1 = new;
    }

    /// The first reading's value becomes the larger of it and `v`, and what follows it too.
    fn raise_first(&mut self, v: f64, method: Method) {
        let (old, new) = (self.first.1, self.first.1.max(v));
        if self.one_time() {
            self.last.1 = new;
        } else {
            let gain = new - old;
            self.area += match method {
                Method::ForwardFill => gain * self.first_span as f64,
                Method::Linear => gain / 2.0 * self.first_span as f64,
            };
            self.growth += growth(new, self.second) - growth(old, self.second);
            // With two times, the first is also the one before the last.
            if self.last.0 - self.last_span == self.first.0 {
                self.penultimate = new;
            }
        }
        self.first.1 = new;
    }

    /// What `output` returns for this segment.
    fn value(&self, output: Output) -> Option<f64> {
        let span = self.last.0 - self.first.0;
        let seconds = span as f64 / 1e6;
        match output {
            Output::Mean if span == 0 => Some(self.first.1),
            Output::Mean => Some(self.area / span as f64),
            Output::Integral => Some(self.area / 1e6),
            Output::CounterDiff => Some(self.growth),
            Output::CounterRate => (span > 0).then(|| self.growth / seconds),
            Output::State => None,
        }
    }
}

/// Calls `add(row, segment)` for each segment in `lists`, a column of states.
fn for_each_segment(lists: &ArrayRef, mut add: impl FnMut(usize, Segment)) -> DfResult<()> {
    let lists = lists.as_list::<i32>();
    let Some(segments) = lists.values().as_any().downcast_ref::<StructArray>() else {
        return exec_err!("expected lists of segments");
    };
    let i64s = |i: usize| segments.column(i).as_primitive::<Int64Type>();
    let f64s = |i: usize| segments.column(i).as_primitive::<Float64Type>();
    let (first_t, first_v, first_span, second) = (i64s(0), f64s(1), i64s(2), f64s(3));
    let (last_t, last_v, last_span, penultimate) = (i64s(4), f64s(5), i64s(6), f64s(7));
    let (area, grown) = (f64s(8), f64s(9));
    let offsets = lists.value_offsets();
    for row in 0..lists.len() {
        if lists.is_null(row) {
            continue;
        }
        for i in offsets[row]..offsets[row + 1] {
            let i = usize::try_from(i).unwrap_or(usize::MAX);
            add(
                row,
                Segment {
                    first: (first_t.value(i), first_v.value(i)),
                    first_span: first_span.value(i),
                    second: second.value(i),
                    last: (last_t.value(i), last_v.value(i)),
                    last_span: last_span.value(i),
                    penultimate: penultimate.value(i),
                    area: area.value(i),
                    growth: grown.value(i),
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
fn states<'a>(groups: impl Iterator<Item = &'a Runs<Segment>>) -> ListArray {
    let (mut times, mut spans): ([Vec<i64>; 2], [Vec<i64>; 2]) = Default::default();
    let (mut values, mut optionals): ([Vec<f64>; 4], [Vec<Option<f64>>; 2]) = Default::default();
    let mut lengths = vec![];
    for runs in groups {
        lengths.push(runs.0.len());
        for s in &runs.0 {
            times[0].push(s.first.0);
            times[1].push(s.last.0);
            spans[0].push(s.first_span);
            spans[1].push(s.last_span);
            values[0].push(s.first.1);
            values[1].push(s.last.1);
            values[2].push(s.area);
            values[3].push(s.growth);
            // Null while the segment has one time.
            let known = |v: f64| (!s.one_time()).then_some(v);
            optionals[0].push(known(s.second));
            optionals[1].push(known(s.penultimate));
        }
    }
    let [first_t, last_t] = times.map(|c| Arc::new(Int64Array::from(c)) as ArrayRef);
    let [first_span, last_span] = spans.map(|c| Arc::new(Int64Array::from(c)) as ArrayRef);
    let [first_v, last_v, area, grown] = values.map(|v| Arc::new(Float64Array::from(v)) as ArrayRef);
    let [second, penultimate] = optionals.map(|v| Arc::new(Float64Array::from(v)) as ArrayRef);
    let DataType::Struct(fields) = segment_type() else { unreachable!("a struct") };
    let columns = vec![first_t, first_v, first_span, second, last_t, last_v, last_span, penultimate, area, grown];
    let segments = StructArray::new(fields, columns, None);
    let field = Arc::new(Field::new_list_field(segment_type(), true));
    ListArray::new(field, OffsetBuffer::from_lengths(lengths), Arc::new(segments), None)
}

/// A signal aggregate's output by its method, as a run aggregate.
#[derive(Debug)]
struct Signal {
    output: Output,
    method: Method,
}

impl RunAggregate for Signal {
    type Run = Segment;

    fn how(&self) -> Method {
        self.method
    }

    /// Readings `(time, value [, method])` or states `(segments [, method])`.
    fn inputs(&self, columns: &[ArrayRef], mut add: impl FnMut(usize, Input<Segment>)) -> DfResult<()> {
        match columns {
            [lists] | [lists, _] if matches!(lists.data_type(), DataType::List(_)) => {
                for_each_segment(lists, |row, s| add(row, Input::Run(s)))
            }
            [times, values] | [times, values, _] => {
                for_each_reading(times, values, |row, p| add(row, Input::Reading(p)));
                Ok(())
            }
            _ => exec_err!("expected (time, value [, method]) or segments"),
        }
    }

    fn states<'a>(&self, groups: impl Iterator<Item = &'a Runs<Segment>>) -> ArrayRef {
        Arc::new(states(groups))
    }

    /// The state, joined, or each group's value.
    fn results<'a>(&self, groups: impl Iterator<Item = &'a Runs<Segment>>) -> DfResult<ArrayRef> {
        if self.output == Output::State {
            let joined: Vec<Runs<Segment>> =
                groups.map(|r| Ok(Runs(r.joined(self.method)?.into_iter().collect()))).collect::<DfResult<_>>()?;
            return Ok(Arc::new(states(joined.iter())));
        }
        let values: Vec<Option<f64>> =
            groups.map(|r| Ok(r.joined(self.method)?.and_then(|s| s.value(self.output)))).collect::<DfResult<_>>()?;
        Ok(Arc::new(Float64Array::from(values)))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::TimestampMicrosecondArray;

    use super::*;

    fn group(output: Output, method: Method) -> OneGroup<Signal> {
        OneGroup::new(Signal { output, method })
    }

    fn update(acc: &mut OneGroup<Signal>, rows: &[(i64, f64)]) {
        let times: ArrayRef = Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>()));
        let values: ArrayRef = Arc::new(Float64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>()));
        acc.update_batch(&[times, values]).unwrap();
    }

    fn value(acc: &mut OneGroup<Signal>) -> Option<f64> {
        match acc.evaluate().unwrap() {
            ScalarValue::Float64(v) => v,
            other => panic!("{other:?}"),
        }
    }

    /// Straight from the definitions: readings sorted, one per time with the largest value.
    fn reference(rows: &[(i64, f64)], output: Output, method: Method) -> Option<f64> {
        let mut rows = rows.to_vec();
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
        let mut points: Vec<(i64, f64)> = vec![];
        for p in rows {
            match points.last_mut() {
                Some(last) if last.0 == p.0 => last.1 = last.1.max(p.1),
                _ => points.push(p),
            }
        }
        let (first, last) = (*points.first()?, *points.last()?);
        let span = last.0 - first.0;
        let area: f64 = points.windows(2).map(|w| method.area(w[0].1, w[1].1, w[1].0 - w[0].0)).sum();
        let grown: f64 = points.windows(2).map(|w| growth(w[0].1, w[1].1)).sum();
        match output {
            Output::Mean if span == 0 => Some(first.1),
            Output::Mean => Some(area / span as f64),
            Output::Integral => Some(area / 1e6),
            Output::CounterDiff => Some(grown),
            Output::CounterRate => (span > 0).then(|| grown / (span as f64 / 1e6)),
            Output::State => None,
        }
    }

    #[test]
    fn known_values() {
        let rows = [(0, 1.0), (10, 3.0), (20, 0.0)];
        let one = |output, method| {
            let mut acc = group(output, method);
            update(&mut acc, &rows);
            value(&mut acc)
        };
        assert_eq!(one(Output::Mean, Method::ForwardFill), Some(2.0)); // (1 × 10 + 3 × 10) / 20
        assert_eq!(one(Output::Mean, Method::Linear), Some(1.75)); // (2 × 10 + 1.5 × 10) / 20
        assert_eq!(one(Output::Integral, Method::ForwardFill), Some(40e-6)); // value × µs → value × s
        // The counter grows 2, then resets: the 0 after it counts whole.
        assert_eq!(one(Output::CounterDiff, Method::ForwardFill), Some(2.0));
        assert_eq!(one(Output::CounterRate, Method::ForwardFill), Some(2.0 / 20e-6));
        // One time: its largest value; a counter didn't grow, and has no rate. No readings: null.
        let mut single = group(Output::Mean, Method::Linear);
        update(&mut single, &[(5, 1.0), (5, 4.0), (5, 2.0)]);
        assert_eq!(value(&mut single), Some(4.0));
        let mut rate = group(Output::CounterRate, Method::ForwardFill);
        update(&mut rate, &[(5, 1.0)]);
        assert_eq!(value(&mut rate), None);
        assert_eq!(value(&mut group(Output::Mean, Method::ForwardFill)), None);
    }

    /// Readings split into runs contiguous in time (sharing boundary times, with repeats), merged
    /// through states in any order, give each output's value over all readings.
    #[test]
    fn merged_runs_give_the_value_of_all_readings() {
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
            let mut runs: Vec<Vec<(i64, f64)>> = vec![];
            let mut rest = rows.as_slice();
            while !rest.is_empty() {
                let k = 1 + usize::try_from(rand(rest.len() as u64)).unwrap();
                runs.push(rest[..k].to_vec());
                rest = &rest[k..];
            }
            let shift = usize::try_from(rand(runs.len() as u64)).unwrap();
            runs.rotate_left(shift);
            for output in [Output::Mean, Output::Integral, Output::CounterDiff, Output::CounterRate] {
                for method in [Method::ForwardFill, Method::Linear] {
                    let mut merged = group(output, method);
                    for run in &runs {
                        let mut part = group(output, method);
                        update(&mut part, run);
                        let ScalarValue::List(state) = part.state().unwrap().remove(0) else { panic!("a list") };
                        merged.merge_batch(&[state]).unwrap();
                    }
                    let (got, want) = (value(&mut merged), reference(&rows, output, method));
                    let close = match (got, want) {
                        (Some(g), Some(w)) => (g - w).abs() <= 1e-9 * w.abs().max(1.0),
                        (g, w) => g == w,
                    };
                    assert!(close, "case {case} {output:?} {method:?}: {got:?} vs {want:?}");
                }
            }
        }
    }

    #[test]
    fn overlapping_runs_are_an_error() {
        let mut a = group(Output::Mean, Method::ForwardFill);
        update(&mut a, &[(0, 1.0), (10, 2.0)]);
        let ScalarValue::List(state) = a.state().unwrap().remove(0) else { panic!("a list") };
        let mut b = group(Output::CounterDiff, Method::ForwardFill);
        update(&mut b, &[(5, 1.0), (15, 2.0)]);
        b.merge_batch(&[state]).unwrap();
        assert!(b.evaluate().unwrap_err().to_string().contains("overlap in time"));
    }

    /// Conditions count as 1 while true: seconds above a limit, and the share of time above it.
    #[tokio::test]
    async fn conditions_are_ones_while_true() {
        let s = crate::Session::new(crate::testing::faulty_lake().0);
        let sql = "SELECT integral(t, v > 10) AS above, time_weighted_mean(t, v > 10) AS share
                   FROM (VALUES (TIMESTAMP '2026-01-01 00:00:00', 5.0), (TIMESTAMP '2026-01-01 00:00:10', 15.0),
                                (TIMESTAMP '2026-01-01 00:00:40', 12.0), (TIMESTAMP '2026-01-01 00:01:00', 3.0),
                                (TIMESTAMP '2026-01-01 00:01:40', 20.0)) AS r(t, v)";
        let rows = s.sql(sql).await.unwrap().collect().await.unwrap();
        let got = arrow::util::pretty::pretty_format_batches(&rows).unwrap().to_string();
        // Above from 0:10 to 1:00 of 0:00 to 1:40: 50 of 100 seconds.
        assert!(got.contains("| 50.0  | 0.5   |"), "{got}");
    }
}
