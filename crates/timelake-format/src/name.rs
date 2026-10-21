//! Table names: `namespace.table`, or `table` in the [`DEFAULT_NAMESPACE`]. The engine, the
//! WebAssembly reader and writer, and the TypeScript client (through the WASM) parse them here.

use std::fmt;
use std::str::FromStr;

use crate::{FormatError, Result};

/// Namespace of table names given without one.
pub const DEFAULT_NAMESPACE: &str = "default";

/// A validated table name: `namespace.table`, or just `table` in the [`DEFAULT_NAMESPACE`].
/// Both parts use `[A-Za-z0-9_-]`, so they are safe as path segments.
///
/// ```
/// use timelake_format::name::TableName;
///
/// let name: TableName = "iot.readings".parse()?;
/// assert_eq!((name.namespace(), name.name()), ("iot", "readings"));
/// assert_eq!("readings".parse::<TableName>()?.to_string(), "default.readings");
/// # Ok::<(), timelake_format::FormatError>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableName {
    namespace: String,
    name: String,
}

impl TableName {
    /// A name from its two parts.
    pub fn new(namespace: &str, name: &str) -> Result<Self> {
        if !valid_part(namespace) || !valid_part(name) {
            return Err(invalid(&format!("{namespace:?}.{name:?}")));
        }
        Ok(Self { namespace: namespace.to_string(), name: name.to_string() })
    }

    /// The namespace (a directory under the warehouse, and a schema in SQL).
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The table name within its namespace.
    pub fn name(&self) -> &str {
        &self.name
    }
}

fn valid_part(p: &str) -> bool {
    !p.is_empty() && p.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn invalid(name: &str) -> FormatError {
    FormatError::new(format!("table name {name} must be [namespace.]table using [A-Za-z0-9_-]"))
}

impl FromStr for TableName {
    type Err = FormatError;

    fn from_str(s: &str) -> Result<Self> {
        let (namespace, name) = s.split_once('.').unwrap_or((DEFAULT_NAMESPACE, s));
        Self::new(namespace, name).map_err(|_| invalid(&format!("{s:?}")))
    }
}

impl TryFrom<&str> for TableName {
    type Error = FormatError;

    fn try_from(s: &str) -> Result<Self> {
        s.parse()
    }
}

impl TryFrom<String> for TableName {
    type Error = FormatError;

    fn try_from(s: String) -> Result<Self> {
        s.parse()
    }
}

impl TryFrom<&String> for TableName {
    type Error = FormatError;

    fn try_from(s: &String) -> Result<Self> {
        s.parse()
    }
}

impl From<&TableName> for TableName {
    fn from(name: &TableName) -> Self {
        name.clone()
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_qualified_and_default_names() {
        let n: TableName = "iot.readings".parse().unwrap();
        assert_eq!((n.namespace(), n.name()), ("iot", "readings"));
        let n: TableName = "my-table_2".parse().unwrap();
        assert_eq!((n.namespace(), n.name()), (DEFAULT_NAMESPACE, "my-table_2"));
        assert_eq!(n.to_string().parse::<TableName>().unwrap(), n);
    }

    #[test]
    fn rejects_invalid_names() {
        for bad in ["", ".", "iot.", ".readings", "a.b.c", "iot/readings", "../x", "a b", "naïve", "iot.read\"ings"] {
            let err = bad.parse::<TableName>().unwrap_err();
            assert!(err.to_string().contains("must be [namespace.]table"), "{bad:?}: {err}");
        }
        assert!(TableName::new("iot", "a.b").is_err());
    }

    #[test]
    fn converts_from_strings_and_names() {
        let n = TableName::try_from("iot.r").unwrap();
        assert_eq!(TableName::try_from(String::from("iot.r")).unwrap(), n);
        assert_eq!(TableName::from(&n), n);
    }
}
