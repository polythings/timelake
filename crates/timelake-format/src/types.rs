//! Iceberg's column types, as table metadata names them (`long`, `timestamptz`, `decimal(10,2)`).
//! <https://iceberg.apache.org/spec/#schemas-and-data-types>

use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

/// A column's Iceberg type.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum IcebergType {
    /// `boolean`.
    Boolean,
    /// `int`: 32-bit.
    Int,
    /// `long`: 64-bit.
    Long,
    /// `float`: 32-bit.
    Float,
    /// `double`: 64-bit.
    Double,
    /// `decimal(P,S)`.
    Decimal {
        /// Digits in all.
        precision: u8,
        /// Digits after the point.
        scale: i8,
    },
    /// `date`: days since the epoch.
    Date,
    /// `time`: microseconds since midnight.
    Time,
    /// `timestamp`: microseconds, without a time zone.
    Timestamp,
    /// `timestamptz`: microseconds, UTC.
    Timestamptz,
    /// `timestamp_ns`: nanoseconds, without a time zone.
    TimestampNs,
    /// `timestamptz_ns`: nanoseconds, UTC.
    TimestamptzNs,
    /// `string`.
    String,
    /// `uuid`.
    Uuid,
    /// `fixed[L]`: `L` bytes.
    Fixed(u64),
    /// `binary`.
    Binary,
    /// A struct, list or map.
    Nested,
    /// A type this version doesn't know (a newer Iceberg's), by its name.
    Other(String),
}

impl IcebergType {
    const NAMED: [Self; 14] = [
        Self::Boolean,
        Self::Int,
        Self::Long,
        Self::Float,
        Self::Double,
        Self::Date,
        Self::Time,
        Self::Timestamp,
        Self::Timestamptz,
        Self::TimestampNs,
        Self::TimestamptzNs,
        Self::String,
        Self::Uuid,
        Self::Binary,
    ];

    /// The name of a type without parameters.
    fn name(&self) -> Option<&'static str> {
        Some(match self {
            Self::Boolean => "boolean",
            Self::Int => "int",
            Self::Long => "long",
            Self::Float => "float",
            Self::Double => "double",
            Self::Date => "date",
            Self::Time => "time",
            Self::Timestamp => "timestamp",
            Self::Timestamptz => "timestamptz",
            Self::TimestampNs => "timestamp_ns",
            Self::TimestamptzNs => "timestamptz_ns",
            Self::String => "string",
            Self::Uuid => "uuid",
            Self::Binary => "binary",
            Self::Decimal { .. } | Self::Fixed(_) | Self::Nested | Self::Other(_) => return None,
        })
    }

    /// Whether values are times in nanoseconds.
    pub fn is_nanos(&self) -> bool {
        matches!(self, Self::TimestampNs | Self::TimestamptzNs)
    }
}

impl IcebergType {
    /// The primitive type named `name`; names this version doesn't know are [`IcebergType::Other`].
    pub fn named(name: &str) -> Self {
        if let Some(t) = Self::NAMED.into_iter().find(|t| t.name() == Some(name)) {
            return t;
        }
        let fixed = name.strip_prefix("fixed[").and_then(|s| s.strip_suffix(']')).and_then(|n| n.parse().ok());
        if let Some(length) = fixed {
            return Self::Fixed(length);
        }
        let decimal = name.strip_prefix("decimal(").and_then(|s| s.strip_suffix(')')).and_then(|ps| {
            let (p, s) = ps.split_once(',')?;
            Some(Self::Decimal { precision: p.trim().parse().ok()?, scale: s.trim().parse().ok()? })
        });
        decimal.unwrap_or_else(|| Self::Other(name.to_string()))
    }
}

/// As [`IcebergType::named`].
impl FromStr for IcebergType {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Infallible> {
        Ok(Self::named(s))
    }
}

/// The name as Iceberg writes it; [`IcebergType::Nested`] is `nested`.
impl fmt::Display for IcebergType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decimal { precision, scale } => write!(f, "decimal({precision},{scale})"),
            Self::Fixed(length) => write!(f, "fixed[{length}]"),
            Self::Nested => f.write_str("nested"),
            Self::Other(name) => f.write_str(name),
            named => f.write_str(named.name().unwrap_or_default()),
        }
    }
}

/// As its name, as JavaScript sees a column's type.
impl serde::Serialize for IcebergType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn types_round_trip_by_name() {
        for name in ["boolean", "long", "timestamptz_ns", "binary", "decimal(10,2)", "fixed[16]", "variant"] {
            let t: IcebergType = name.parse().unwrap();
            assert_eq!(t.to_string(), name);
        }
        let t: IcebergType = "decimal(38, -2)".parse().unwrap();
        assert_eq!(t, IcebergType::Decimal { precision: 38, scale: -2 });
        assert_eq!("geometry".parse(), Ok(IcebergType::Other("geometry".into())));
        assert!("timestamp_ns".parse::<IcebergType>().unwrap().is_nanos());
    }
}
