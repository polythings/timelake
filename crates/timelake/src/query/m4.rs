//! `m4(time, value)`: the first, smallest, largest and last points of a group, such as a
//! `time_bucket`, as a list of `{time, value}` in time order without repeats. A line drawn
//! through the points of each bucket keeps every bucket's extremes and its ends, from at most
//! four points a bucket (after the time-window `M4` function of Apache `IoTDB`).
//!
//! The points of several groups are a state that merges: the M4 of the groups' points together
//! is the M4 of their rows together. So `m4(points)` merges stored lists, which lets a rollup
//! store the points and answer wider buckets from them.
//!
//! Ties are broken the same way wherever rows arrive: the first point is the earliest (the
//! smaller value among equal times), the last the latest (the larger value), the smallest the
//! earliest among equal values, and the largest too.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, ListArray, StructArray, TimestampMicrosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Float64Type, TimeUnit, TimestampMicrosecondType};
use datafusion::common::{ScalarValue, exec_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};

/// The `m4` aggregate function.
pub(crate) fn m4_udaf() -> AggregateUDF {
    AggregateUDF::from(M4 { signature: Signature::user_defined(Volatility::Immutable) })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct M4 {
    signature: Signature,
}

/// The type of one point: `{time, value}`.
fn point(tz: Option<Arc<str>>) -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("time", DataType::Timestamp(TimeUnit::Microsecond, tz), true),
        Field::new("value", DataType::Float64, true),
    ]))
}

/// The type `m4` returns: a list of points.
pub(super) fn points(tz: Option<Arc<str>>) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(point(tz), true)))
}

/// The time zone of the points a call works on: its time argument's, or its points'.
fn time_zone(arg_types: &[DataType]) -> DfResult<Option<Arc<str>>> {
    let time = match arg_types {
        [time, _] => Some(time),
        [DataType::List(item)] => {
            if let DataType::Struct(fields) = item.data_type() {
                fields.find("time").map(|(_, f)| f.data_type())
            } else {
                None
            }
        }
        _ => None,
    };
    if let Some(DataType::Timestamp(_, tz)) = time {
        Ok(tz.clone())
    } else {
        plan_err!("m4 takes (time, value), or the lists of {{time, value}} other m4 calls return")
    }
}

fn args_time_zone(args: &AccumulatorArgs<'_>) -> DfResult<Option<Arc<str>>> {
    let types: Vec<DataType> = args.expr_fields.iter().map(|f| f.data_type().clone()).collect();
    time_zone(&types)
}

impl AggregateUDFImpl for M4 {
    fn name(&self) -> &'static str {
        "m4"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        let tz = time_zone(arg_types)?;
        match arg_types {
            [_, value] if value.is_numeric() => {
                Ok(vec![DataType::Timestamp(TimeUnit::Microsecond, tz), DataType::Float64])
            }
            [_, other] => plan_err!("m4 needs a numeric value, not {other}"),
            _ => Ok(vec![points(tz)]),
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(points(time_zone(arg_types)?))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> DfResult<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(format_state_name(args.name, "points"), args.return_type().clone(), true))])
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(M4Accumulator { tz: args_time_zone(&args)?, extremes: Extremes::default() }))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs<'_>) -> bool {
        true
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(M4Groups { tz: args_time_zone(&args)?, groups: vec![] }))
    }
}

/// A point: time in microseconds, and value.
type Point = (i64, f64);

/// A group's first, last, smallest and largest points so far.
#[derive(Clone, Copy, Debug, Default)]
struct Extremes {
    first: Option<Point>,
    last: Option<Point>,
    min: Option<Point>,
    max: Option<Point>,
}

impl Extremes {
    fn add(&mut self, p: Point) {
        let by_time = |a: &Point, b: &Point| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1));
        let by_value = |a: &Point, b: &Point| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0));
        if self.first.is_none_or(|f| by_time(&p, &f) == Ordering::Less) {
            self.first = Some(p);
        }
        if self.last.is_none_or(|l| by_time(&p, &l) == Ordering::Greater) {
            self.last = Some(p);
        }
        if self.min.is_none_or(|m| by_value(&p, &m) == Ordering::Less) {
            self.min = Some(p);
        }
        // The largest value, the earliest among equals.
        if self.max.is_none_or(|m| p.1.total_cmp(&m.1).then(m.0.cmp(&p.0)) == Ordering::Greater) {
            self.max = Some(p);
        }
    }

    /// The points in time order, without repeats.
    fn points(&self) -> Vec<Point> {
        let mut points: Vec<Point> = [self.first, self.min, self.max, self.last].into_iter().flatten().collect();
        points.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
        points.dedup_by(|a, b| a.0 == b.0 && a.1.total_cmp(&b.1) == Ordering::Equal);
        points
    }
}

/// Calls `add(row, point)` for each point of the input rows, skipping nulls: the rows are
/// `(time, value)` columns, or lists of points to merge.
fn for_each_point(columns: &[ArrayRef], mut add: impl FnMut(usize, Point)) -> DfResult<()> {
    match columns {
        [times, values] => {
            let times = times.as_primitive::<TimestampMicrosecondType>();
            let values = values.as_primitive::<Float64Type>();
            for row in 0..times.len() {
                if times.is_valid(row) && values.is_valid(row) {
                    add(row, (times.value(row), values.value(row)));
                }
            }
        }
        [lists] => {
            let lists = lists.as_list::<i32>();
            let Some(points) = lists.values().as_any().downcast_ref::<StructArray>() else {
                return exec_err!("m4 merges lists of {{time, value}}");
            };
            let times = points.column(0).as_primitive::<TimestampMicrosecondType>();
            let values = points.column(1).as_primitive::<Float64Type>();
            for row in 0..lists.len() {
                if lists.is_null(row) {
                    continue;
                }
                for i in lists.value_offsets()[row]..lists.value_offsets()[row + 1] {
                    let i = usize::try_from(i).unwrap_or(usize::MAX);
                    if points.is_valid(i) && times.is_valid(i) && values.is_valid(i) {
                        add(row, (times.value(i), values.value(i)));
                    }
                }
            }
        }
        _ => return exec_err!("m4 takes (time, value), or the points of other m4 calls"),
    }
    Ok(())
}

/// The points of `groups`, one list each.
fn lists<'a>(groups: impl Iterator<Item = &'a Extremes>, tz: Option<Arc<str>>) -> ListArray {
    let (mut times, mut values, mut lengths) = (vec![], vec![], vec![]);
    for group in groups {
        let points = group.points();
        lengths.push(points.len());
        for (t, v) in points {
            times.push(t);
            values.push(v);
        }
    }
    let DataType::Struct(fields) = point(tz.clone()) else { unreachable!("a struct") };
    let times = TimestampMicrosecondArray::from(times).with_timezone_opt(tz.clone());
    let structs =
        StructArray::new(fields, vec![Arc::new(times) as ArrayRef, Arc::new(Float64Array::from(values))], None);
    let field = Arc::new(Field::new_list_field(point(tz), true));
    ListArray::new(field, OffsetBuffer::from_lengths(lengths), Arc::new(structs), None)
}

/// One group at a time (DataFusion's fallback, e.g. without `GROUP BY`).
#[derive(Debug, Default)]
struct M4Accumulator {
    tz: Option<Arc<str>>,
    extremes: Extremes,
}

impl M4Accumulator {
    fn points(&self) -> ScalarValue {
        ScalarValue::List(Arc::new(lists(std::iter::once(&self.extremes), self.tz.clone())))
    }
}

impl Accumulator for M4Accumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        for_each_point(values, |_, p| self.extremes.add(p))
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        for_each_point(states, |_, p| self.extremes.add(p))
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![self.points()])
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(self.points())
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

/// All groups at once, as DataFusion's grouped aggregation uses.
#[derive(Debug)]
struct M4Groups {
    tz: Option<Arc<str>>,
    groups: Vec<Extremes>,
}

impl M4Groups {
    fn add(&mut self, columns: &[ArrayRef], groups: &[usize], filter: Option<&BooleanArray>, n: usize) -> DfResult<()> {
        self.groups.resize(n, Extremes::default());
        let all = &mut self.groups;
        for_each_point(columns, |row, p| {
            if filter.is_none_or(|f| f.is_valid(row) && f.value(row)) {
                all[groups[row]].add(p);
            }
        })
    }
}

impl GroupsAccumulator for M4Groups {
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
        Ok(Arc::new(lists(taken.iter(), self.tz.clone())))
    }

    fn state(&mut self, emit_to: EmitTo) -> DfResult<Vec<ArrayRef>> {
        Ok(vec![self.evaluate(emit_to)?])
    }

    fn size(&self) -> usize {
        self.groups.capacity() * size_of::<Extremes>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn points_of(acc: &mut M4Accumulator) -> Vec<Point> {
        let ScalarValue::List(list) = acc.evaluate().unwrap() else { panic!("a list") };
        let points = list.value(0);
        let points = points.as_struct();
        let times = points.column(0).as_primitive::<TimestampMicrosecondType>();
        let values = points.column(1).as_primitive::<Float64Type>();
        (0..points.len()).map(|i| (times.value(i), values.value(i))).collect()
    }

    fn update(acc: &mut M4Accumulator, rows: &[(Option<i64>, Option<f64>)]) {
        let times: ArrayRef = Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>()));
        let values: ArrayRef = Arc::new(Float64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>()));
        acc.update_batch(&[times, values]).unwrap();
    }

    #[test]
    fn first_min_max_last_in_time_order() {
        let mut acc = M4Accumulator::default();
        update(&mut acc, &[(Some(3), Some(5.0)), (Some(1), Some(2.0)), (Some(2), Some(9.0)), (Some(4), Some(1.0))]);
        assert_eq!(points_of(&mut acc), [(1, 2.0), (2, 9.0), (4, 1.0)], "the last point is also the smallest");
        // One row is all four points, listed once; nulls are skipped.
        let mut one = M4Accumulator::default();
        update(&mut one, &[(Some(7), Some(1.5)), (None, Some(0.0)), (Some(8), None)]);
        assert_eq!(points_of(&mut one), [(7, 1.5)]);
    }

    #[test]
    fn ties_are_broken_the_same_in_any_order() {
        let rows = [
            (Some(5), Some(3.0)),
            (Some(1), Some(3.0)),
            (Some(1), Some(1.0)),
            (Some(9), Some(3.0)),
            (Some(9), Some(4.0)),
        ];
        let mut forward = M4Accumulator::default();
        update(&mut forward, &rows);
        let mut backward = M4Accumulator::default();
        update(&mut backward, &rows.iter().rev().copied().collect::<Vec<_>>());
        // First: earliest, smaller value; smallest: earliest; largest: earliest; last: latest, larger.
        assert_eq!(points_of(&mut forward), [(1, 1.0), (9, 4.0)]);
        assert_eq!(points_of(&mut forward), points_of(&mut backward));
    }

    #[test]
    fn merged_points_are_the_points_of_all_rows() {
        let rows: Vec<(Option<i64>, Option<f64>)> = (0..100).map(|i| (Some(i), Some(((i * 37) % 23) as f64))).collect();
        let mut all = M4Accumulator::default();
        update(&mut all, &rows);
        let mut merged = M4Accumulator::default();
        for part in rows.chunks(7) {
            let mut acc = M4Accumulator::default();
            update(&mut acc, part);
            let ScalarValue::List(list) = acc.state().unwrap().remove(0) else { panic!("a list") };
            merged.merge_batch(&[list]).unwrap();
        }
        assert_eq!(points_of(&mut merged), points_of(&mut all));
    }

    /// All groups at once give each group's points, with a filter, and through merged states.
    #[test]
    fn groups_match_one_group_at_a_time() {
        let rows: Vec<(Option<i64>, Option<f64>)> = (0..60).map(|i| (Some(i), Some(((i * 13) % 17) as f64))).collect();
        let groups: Vec<usize> = (0..60).map(|i| i % 3).collect();
        let filter = BooleanArray::from((0..60).map(|i| i % 5 != 0).collect::<Vec<_>>());
        let times: ArrayRef = Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>()));
        let values: ArrayRef = Arc::new(Float64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>()));
        let mut all = M4Groups { tz: None, groups: vec![] };
        all.update_batch(&[times.clone(), values.clone()], &groups, Some(&filter), 3).unwrap();
        let mut merged = M4Groups { tz: None, groups: vec![] };
        merged.merge_batch(&all.state(EmitTo::All).unwrap(), &[0, 1, 2], None, 3).unwrap();
        let lists = merged.evaluate(EmitTo::All).unwrap();
        for g in 0..3 {
            let mut one = M4Accumulator::default();
            let kept: Vec<_> = (0..60).filter(|&i| groups[i] == g && i % 5 != 0).map(|i| rows[i]).collect();
            update(&mut one, &kept);
            let ScalarValue::List(expected) = one.evaluate().unwrap() else { panic!("a list") };
            assert_eq!(lists.as_list::<i32>().value(g).to_data(), expected.value(0).to_data(), "group {g}");
        }
    }
}
