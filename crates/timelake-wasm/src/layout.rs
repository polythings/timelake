//! Table names, durations and object paths as `timelake-format` defines them, for the TypeScript
//! client: one definition for the engine, the WASM reader and writer, and the client.

use serde::Serialize;
use timelake_format::name::TableName;
use timelake_format::{duration_micros, footer, layout, parse_duration};
use wasm_bindgen::prelude::*;

use crate::error::{Error, Result};
use crate::{safe_u64, to_js};

/// The largest integer a JavaScript number holds exactly.
const MAX_SAFE_INTEGER: i64 = (1 << 53) - 1;

#[derive(Serialize)]
struct NameParts<'a> {
    namespace: &'a str,
    name: &'a str,
}

fn table_name(name: &str) -> Result<TableName> {
    Ok(name.parse()?)
}

/// `[namespace.]table` as `{ namespace, name }`; a name without a namespace is in `default`.
/// Throws for names outside `[A-Za-z0-9_-]`.
#[wasm_bindgen(js_name = parseTableName)]
pub fn parse_table_name(name: &str) -> Result<JsValue, JsValue> {
    let name = table_name(name)?;
    to_js(&NameParts { namespace: name.namespace(), name: name.name() })
}

/// Where table `name` lives in the warehouse at `warehouse` (no trailing slash).
#[wasm_bindgen(js_name = tableLocation)]
pub fn table_location(warehouse: &str, name: &str) -> Result<String, JsValue> {
    Ok(layout::table_location(warehouse, &table_name(name)?))
}

/// The warehouse's table index, for stores that can't list.
#[wasm_bindgen(js_name = tablesIndexUri)]
pub fn tables_index_uri(warehouse: &str) -> String {
    layout::tables_index_uri(warehouse)
}

/// The metadata file of table version `version`.
#[wasm_bindgen(js_name = metadataUri)]
pub fn metadata_uri(location: &str, version: f64) -> Result<String, JsValue> {
    Ok(layout::metadata_uri(location, safe_u64(version, "table version")?))
}

/// The table's version hint.
#[wasm_bindgen(js_name = hintUri)]
pub fn hint_uri(location: &str) -> String {
    layout::hint_uri(location)
}

/// The table's head object.
#[wasm_bindgen(js_name = headUri)]
pub fn head_uri(location: &str) -> String {
    layout::head_uri(location)
}

/// The version of a `…/v<N>.metadata.json` URI; undefined for other URIs.
#[wasm_bindgen(js_name = metadataVersion)]
pub fn metadata_version(uri: &str) -> Option<f64> {
    #[expect(clippy::cast_precision_loss, reason = "versions are far below 2^53")]
    layout::metadata_version(uri).map(|v| v as f64)
}

/// Microseconds of a duration such as `10m`, `1h` or `3 hours`, as table settings take them.
#[wasm_bindgen(js_name = parseDuration)]
pub fn parse_duration_micros(duration: &str) -> Result<f64, JsValue> {
    #[expect(clippy::cast_precision_loss, reason = "at most 2^53 - 1")]
    Ok(micros_of(duration)? as f64)
}

/// [`parse_duration_micros`], within what a JavaScript number holds exactly.
fn micros_of(duration: &str) -> Result<i64> {
    let micros = duration_micros(parse_duration(duration)?);
    if micros > MAX_SAFE_INTEGER {
        return Err(Error::invalid(format!("duration {duration:?} is too long")));
    }
    Ok(micros)
}

/// Bytes to read from the end of a data file first: the whole footer of typical files.
#[wasm_bindgen(js_name = footerPrefetchBytes)]
pub fn footer_prefetch_bytes() -> f64 {
    #[expect(clippy::cast_precision_loss, reason = "64 KiB")]
    let bytes = footer::PREFETCH as f64;
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_fit_javascript_numbers() {
        assert_eq!(micros_of(" 3 hours ").unwrap(), 3 * 3_600_000_000);
        assert_eq!(micros_of("285y").unwrap(), 285 * 365 * 86_400_000_000);
        // 300 years of microseconds is past 2^53.
        assert!(micros_of("300y").unwrap_err().to_string().contains("too long"));
        assert!(micros_of("1.5h").is_err());
    }

    #[test]
    fn names_and_paths() {
        assert_eq!(table_location("s3://b/lake", "readings").unwrap(), "s3://b/lake/default/readings");
        assert!(table_name("a.b.c").is_err());
        assert_eq!(metadata_version("s3://b/t/metadata/v7.metadata.json"), Some(7.0));
        assert_eq!(metadata_version("s3://b/t/metadata/timelake-head"), None);
    }
}
