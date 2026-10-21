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

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, ListArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Float64Type, TimeUnit, TimestampMicrosecondType};
use datafusion::common::{exec_err, plan_datafusion_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, Signature, Volatility};

use super::functions::arg_types;
use super::groups::{AllGroups, Fold, GroupState, OneGroup};
use super::lists::{list_items, list_of_structs, list_type};

/// The `m4` aggregate function.
pub(crate) fn m4_udaf() -> AggregateUDF {
    AggregateUDF::from(M4 { signature: Signature::user_defined(Volatility::Immutable) })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct M4 {
    signature: Signature,
}

/// The fields of one point: `{time, value}`.
fn point_fields(tz: Option<Arc<str>>) -> Fields {
    Fields::from(vec![
        Field::new("time", DataType::Timestamp(TimeUnit::Microsecond, tz), true),
        Field::new("value", DataType::Float64, true),
    ])
}

/// The type `m4` returns: a list of points.
pub(super) fn points(tz: Option<Arc<str>>) -> DataType {
    list_type(point_fields(tz))
}

/// `points`, one list of `(time, value)` per row of `lengths`, as `m4` returns them.
pub(super) fn points_list(
    times: Vec<i64>,
    values: Vec<f64>,
    lengths: impl IntoIterator<Item = usize>,
    tz: Option<Arc<str>>,
) -> ListArray {
    let times = TimestampMicrosecondArray::from(times).with_timezone_opt(tz.clone());
    let columns = vec![Arc::new(times) as ArrayRef, Arc::new(Float64Array::from(values))];
    list_of_structs(point_fields(tz), columns, lengths)
}

/// The time zone of a timestamp type, or of the times in a list of points; `None` for another
/// type.
pub(super) fn time_zone_of(t: &DataType) -> Option<&Option<Arc<str>>> {
    match t {
        DataType::Timestamp(_, tz) => Some(tz),
        DataType::List(item) => match item.data_type() {
            DataType::Struct(fields) => time_zone_of(fields.find("time")?.1.data_type()),
            _ => None,
        },
        _ => None,
    }
}

/// The time zone of the points a call works on: its time argument's, or its points'.
fn time_zone(arg_types: &[DataType]) -> DfResult<Option<Arc<str>>> {
    let tz = match arg_types {
        [time @ DataType::Timestamp(..), _] | [time @ DataType::List(_)] => time_zone_of(time),
        _ => None,
    };
    tz.cloned().ok_or_else(|| {
        plan_datafusion_err!("m4 takes (time, value), or the lists of {{time, value}} other m4 calls return")
    })
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
        Ok(Box::new(OneGroup::new(M4Points { tz: time_zone(&arg_types(&args))? })))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs<'_>) -> bool {
        true
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> DfResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(AllGroups::new(M4Points { tz: time_zone(&arg_types(&args))? })))
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
            let Some((points, items)) = list_items(lists) else {
                return exec_err!("m4 merges lists of {{time, value}}");
            };
            let times = points.column(0).as_primitive::<TimestampMicrosecondType>();
            let values = points.column(1).as_primitive::<Float64Type>();
            for (row, i) in items {
                if points.is_valid(i) && times.is_valid(i) && values.is_valid(i) {
                    add(row, (times.value(i), values.value(i)));
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
    points_list(times, values, lengths, tz)
}

impl GroupState for Extremes {}

/// `m4` as a fold: points, from readings or other calls' points, into each group's extremes.
#[derive(Debug, Default)]
struct M4Points {
    tz: Option<Arc<str>>,
}

impl Fold for M4Points {
    type Group = Extremes;
    type Item = Point;

    fn items(&self, columns: &[ArrayRef], add: impl FnMut(usize, Point)) -> DfResult<()> {
        for_each_point(columns, add)
    }

    fn fold(&self, group: &mut Extremes, point: Point) {
        group.add(point);
    }

    /// The state is the result: the points.
    fn states<'a>(&self, groups: impl Iterator<Item = &'a Extremes>) -> ArrayRef {
        Arc::new(lists(groups, self.tz.clone()))
    }

    fn results<'a>(&self, groups: impl Iterator<Item = &'a Extremes>) -> DfResult<ArrayRef> {
        Ok(self.states(groups))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::BooleanArray;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Accumulator, EmitTo, GroupsAccumulator};

    use super::*;

    fn group() -> OneGroup<M4Points> {
        OneGroup::new(M4Points::default())
    }

    fn points_of(acc: &mut OneGroup<M4Points>) -> Vec<Point> {
        let ScalarValue::List(list) = acc.evaluate().unwrap() else { panic!("a list") };
        let points = list.value(0);
        let points = points.as_struct();
        let times = points.column(0).as_primitive::<TimestampMicrosecondType>();
        let values = points.column(1).as_primitive::<Float64Type>();
        (0..points.len()).map(|i| (times.value(i), values.value(i))).collect()
    }

    fn update(acc: &mut OneGroup<M4Points>, rows: &[(Option<i64>, Option<f64>)]) {
        let times: ArrayRef = Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>()));
        let values: ArrayRef = Arc::new(Float64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>()));
        acc.update_batch(&[times, values]).unwrap();
    }

    #[test]
    fn first_min_max_last_in_time_order() {
        let mut acc = group();
        update(&mut acc, &[(Some(3), Some(5.0)), (Some(1), Some(2.0)), (Some(2), Some(9.0)), (Some(4), Some(1.0))]);
        assert_eq!(points_of(&mut acc), [(1, 2.0), (2, 9.0), (4, 1.0)], "the last point is also the smallest");
        // One row is all four points, listed once; nulls are skipped.
        let mut one = group();
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
        let mut forward = group();
        update(&mut forward, &rows);
        let mut backward = group();
        update(&mut backward, &rows.iter().rev().copied().collect::<Vec<_>>());
        // First: earliest, smaller value; smallest: earliest; largest: earliest; last: latest, larger.
        assert_eq!(points_of(&mut forward), [(1, 1.0), (9, 4.0)]);
        assert_eq!(points_of(&mut forward), points_of(&mut backward));
    }

    #[test]
    fn merged_points_are_the_points_of_all_rows() {
        let rows: Vec<(Option<i64>, Option<f64>)> = (0..100).map(|i| (Some(i), Some(((i * 37) % 23) as f64))).collect();
        let mut all = group();
        update(&mut all, &rows);
        let mut merged = group();
        for part in rows.chunks(7) {
            let mut acc = group();
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
        let mut all = AllGroups::new(M4Points::default());
        all.update_batch(&[times.clone(), values.clone()], &groups, Some(&filter), 3).unwrap();
        let mut merged = AllGroups::new(M4Points::default());
        merged.merge_batch(&all.state(EmitTo::All).unwrap(), &[0, 1, 2], 3).unwrap();
        let lists = merged.evaluate(EmitTo::All).unwrap();
        for g in 0..3 {
            let mut one = group();
            let kept: Vec<_> = (0..60).filter(|&i| groups[i] == g && i % 5 != 0).map(|i| rows[i]).collect();
            update(&mut one, &kept);
            let ScalarValue::List(expected) = one.evaluate().unwrap() else { panic!("a list") };
            assert_eq!(lists.as_list::<i32>().value(g).to_data(), expected.value(0).to_data(), "group {g}");
        }
    }

    /// Rows turned into states of their own and merged give what updating with the rows gives, as
    /// DataFusion merges them when partial aggregation doesn't reduce the rows.
    #[test]
    fn rows_converted_to_states_merge_as_updates() {
        let times: ArrayRef = Arc::new(TimestampMicrosecondArray::from((0..40).map(Some).collect::<Vec<_>>()));
        let values: ArrayRef =
            Arc::new(Float64Array::from((0..40).map(|i| Some(f64::from((i * 7) % 11))).collect::<Vec<_>>()));
        let groups: Vec<usize> = (0..40).map(|i| i % 4).collect();
        let filter = BooleanArray::from((0..40).map(|i| i % 3 != 0).collect::<Vec<_>>());
        let mut updated = AllGroups::new(M4Points::default());
        updated.update_batch(&[times.clone(), values.clone()], &groups, Some(&filter), 4).unwrap();
        let states = AllGroups::new(M4Points::default()).convert_to_state(&[times, values], Some(&filter)).unwrap();
        let mut merged = AllGroups::new(M4Points::default());
        merged.merge_batch(&states, &groups, 4).unwrap();
        assert_eq!(merged.evaluate(EmitTo::All).unwrap().to_data(), updated.evaluate(EmitTo::All).unwrap().to_data());
    }
}
