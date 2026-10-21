//! Aggregate states as list columns of structs: each group's items (runs, segments, points) in
//! one row.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, ListArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{ArrowNativeType, DataType, Field, Fields};

/// The list type of `fields` structs.
pub(super) fn list_type(fields: Fields) -> DataType {
    DataType::List(item_field(fields))
}

fn item_field(fields: Fields) -> Arc<Field> {
    Arc::new(Field::new_list_field(DataType::Struct(fields), true))
}

/// A list column of `fields` structs: `columns` hold the items, one value each, and `lengths` how
/// many items each row has.
pub(super) fn list_of_structs(
    fields: Fields,
    columns: Vec<ArrayRef>,
    lengths: impl IntoIterator<Item = usize>,
) -> ListArray {
    let item = item_field(fields.clone());
    let structs = StructArray::new(fields, columns, None);
    ListArray::new(item, OffsetBuffer::from_lengths(lengths), Arc::new(structs), None)
}

/// The items of `lists`, a list column of structs, and `(row, item)` for each item of each row
/// that isn't null; `None` if `lists` isn't one.
pub(super) fn list_items(lists: &ArrayRef) -> Option<(&StructArray, impl Iterator<Item = (usize, usize)> + '_)> {
    let lists = lists.as_list_opt::<i32>()?;
    let items = lists.values().as_struct_opt()?;
    let offsets = lists.value_offsets();
    let rows = (0..lists.len()).filter(|&row| lists.is_valid(row));
    let each = rows.flat_map(move |row| (offsets[row].as_usize()..offsets[row + 1].as_usize()).map(move |i| (row, i)));
    Some((items, each))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, Int64Builder, ListBuilder, StructBuilder};
    use arrow::datatypes::Int64Type;

    use super::*;

    #[test]
    fn lists_of_structs_round_trip() {
        let fields = Fields::from(vec![Field::new("x", DataType::Int64, false)]);
        let column = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let lists: ArrayRef = Arc::new(list_of_structs(fields.clone(), vec![column], [2, 0, 1]));
        assert_eq!(lists.data_type(), &list_type(fields.clone()));
        let (items, each) = list_items(&lists).unwrap();
        let x = items.column(0).as_primitive::<Int64Type>();
        let seen: Vec<(usize, i64)> = each.map(|(row, i)| (row, x.value(i))).collect();
        assert_eq!(seen, [(0, 1), (0, 2), (2, 3)]);

        // Null rows have no items.
        let mut builder = ListBuilder::new(StructBuilder::new(fields, vec![Box::new(Int64Builder::new())]));
        builder.append_null();
        let nulls: ArrayRef = Arc::new(builder.finish());
        assert_eq!(list_items(&nulls).unwrap().1.count(), 0);
        assert!(list_items(&(Arc::new(Int64Array::from(vec![1])) as ArrayRef)).is_none());
    }
}
