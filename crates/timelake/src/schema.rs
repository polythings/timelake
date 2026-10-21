//! Mapping Arrow schemas onto what a table can store, on create. Batches are fitted to a table's
//! schema on write by `timelake_format::rows::align_batch`.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::error::{Error, Result};

/// The time zone timestamps with any zone are stored in.
pub(crate) const UTC: &str = timelake_format::rows::UTC;

/// Maps Arrow types to ones with a lossless Iceberg equivalent: any timestamp → microseconds,
/// small ints → int, dictionaries → their value type, large/view strings → strings. The time
/// column is made NOT NULL, and a text time column (CSV, JSON) becomes a UTC timestamp.
pub(crate) fn normalize_schema(schema: &Schema, time_column: Option<&str>) -> Result<Schema> {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let is_time = Some(f.name().as_str()) == time_column;
            let data_type = match normalize_type(f.data_type())? {
                DataType::Utf8 if is_time => DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
                dt => dt,
            };
            Ok(Field::new(f.name(), data_type, f.is_nullable() && !is_time))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

fn normalize_type(dt: &DataType) -> Result<DataType> {
    Ok(match dt {
        DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16 => DataType::Int32,
        DataType::UInt32 => DataType::Int64,
        DataType::Float16 => DataType::Float32,
        DataType::Timestamp(_, tz) => DataType::Timestamp(TimeUnit::Microsecond, tz.as_ref().map(|_| UTC.into())),
        DataType::Date64 => DataType::Date32,
        DataType::Dictionary(_, v) => normalize_type(v)?,
        DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        DataType::UInt64 => return Err(Error::invalid("UInt64 columns are not supported by Iceberg; cast to Int64")),
        other => other.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_types_iceberg_cannot_store() {
        let schema = Schema::new(vec![
            Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, Some("Europe/Kyiv".into())), true),
            Field::new("local", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("small", DataType::UInt8, false),
            Field::new("u32", DataType::UInt32, true),
            Field::new("tag", DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::LargeUtf8)), true),
            Field::new("day", DataType::Date64, true),
        ]);
        let n = normalize_schema(&schema, Some("t")).unwrap();
        let types: Vec<_> = n.fields().iter().map(|f| f.data_type().clone()).collect();
        assert_eq!(
            types,
            [
                DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Int32,
                DataType::Int64,
                DataType::Utf8,
                DataType::Date32,
            ]
        );
        // The time column is required; other columns keep their nullability.
        assert!(!n.field(0).is_nullable() && !n.field(2).is_nullable() && n.field(3).is_nullable());
    }

    #[test]
    fn text_time_column_becomes_a_timestamp() {
        let schema = Schema::new(vec![Field::new("time", DataType::Utf8, true), Field::new("s", DataType::Utf8, true)]);
        let n = normalize_schema(&schema, Some("time")).unwrap();
        assert_eq!(n.field(0).data_type(), &DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())));
        assert_eq!(n.field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn rejects_uint64() {
        let schema = Schema::new(vec![Field::new("n", DataType::UInt64, true)]);
        assert!(normalize_schema(&schema, None).is_err());
    }
}
