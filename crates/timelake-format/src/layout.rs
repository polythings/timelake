//! Where a table's objects live: the Hadoop-catalog layout other Iceberg engines read (`DuckDB`'s
//! `iceberg_scan('<table dir>')`, Spark's `HadoopCatalog`), and the names Timelake gives the files
//! it writes. The engine, the WebAssembly reader and writer, and the TypeScript client (through
//! the WASM) all build paths here.
//!
//! ```text
//! <warehouse>/timelake-tables.json                                  table index, for stores that can't list
//! <warehouse>/<namespace>/<table>/metadata/v<N>.metadata.json       the commit point, written if absent
//! <warehouse>/<namespace>/<table>/metadata/version-hint.text        "<N>", best effort
//! <warehouse>/<namespace>/<table>/metadata/timelake-head            version N for readers (spec 7)
//! <warehouse>/<namespace>/<table>/metadata/snap-<id>-<uuid>.avro    manifest list of snapshot <id>
//! <warehouse>/<namespace>/<table>/metadata/<uuid>-<kind>-m.avro     manifest
//! <warehouse>/<namespace>/<table>/metadata/lvi-<id>-<uuid>.parquet  last-value index of snapshot <id>
//! <warehouse>/<namespace>/<table>/data/<time>_<chunk>=<label>/<uuid>.parquet
//! ```

use std::fmt::{Display, Write as _};

use crate::name::TableName;
use crate::{Chunk, Contract, TABLES_INDEX};

/// Name of the version hint in a table's `metadata/` directory.
const HINT_FILE: &str = "version-hint.text";
/// Name of the head object in a table's `metadata/` directory.
pub const HEAD_FILE: &str = "timelake-head";

/// `<warehouse>/<namespace>/<table>`, for a warehouse URI without a trailing slash.
pub fn table_location(warehouse: &str, name: &TableName) -> String {
    format!("{warehouse}/{}/{}", name.namespace(), name.name())
}

/// `<warehouse>/timelake-tables.json`.
pub fn tables_index_uri(warehouse: &str) -> String {
    format!("{warehouse}/{TABLES_INDEX}")
}

/// `<location>/metadata`.
pub fn metadata_dir(location: &str) -> String {
    format!("{location}/metadata")
}

/// `<location>/metadata/v<version>.metadata.json`.
pub fn metadata_uri(location: &str, version: u64) -> String {
    format!("{location}/metadata/v{version}.metadata.json")
}

/// The prefix every metadata file's URI starts with: list it to find the versions.
pub fn metadata_prefix(location: &str) -> String {
    format!("{location}/metadata/v")
}

/// The version of a `…/v<N>.metadata.json` URI.
pub fn metadata_version(uri: &str) -> Option<u64> {
    uri.rsplit('/').next()?.strip_prefix('v')?.strip_suffix(".metadata.json")?.parse().ok()
}

/// `<location>/metadata/version-hint.text`.
pub fn hint_uri(location: &str) -> String {
    format!("{location}/metadata/{HINT_FILE}")
}

/// `<location>/metadata/timelake-head`.
pub fn head_uri(location: &str) -> String {
    format!("{location}/metadata/{HEAD_FILE}")
}

/// A new manifest list of snapshot `snapshot_id`; `unique` (a UUID) keeps retries apart.
pub fn manifest_list_uri(location: &str, snapshot_id: i64, unique: impl Display) -> String {
    format!("{location}/metadata/snap-{snapshot_id}-{unique}.avro")
}

/// A new manifest; `kind` says what it holds (`added`, `merged`, `deleted`, …).
pub fn manifest_uri(location: &str, unique: impl Display, kind: &str) -> String {
    format!("{location}/metadata/{unique}-{kind}-m.avro")
}

/// A new last-value index of snapshot `snapshot_id`.
pub fn last_value_index_uri(location: &str, snapshot_id: i64, unique: impl Display) -> String {
    format!("{location}/metadata/lvi-{snapshot_id}-{unique}.parquet")
}

/// The directory of a partition's data files: `<location>/data/<time>_<chunk>=<label>`
/// (`time_day=2026-01-01`), or `<location>/data` for unpartitioned tables.
pub fn partition_dir(location: &str, contract: &Contract, partition: Option<i32>) -> String {
    match (partition, &contract.time_column, contract.chunk) {
        (Some(v), Some(time_column), Some(chunk)) => {
            format!("{location}/data/{time_column}_{chunk}={}", chunk.label(v))
        }
        _ => format!("{location}/data"),
    }
}

/// A new data file in `dir` (see [`partition_dir`]); `unique` is a time-ordered UUID (v7).
pub fn data_file_uri(dir: &str, unique: impl Display) -> String {
    format!("{dir}/{unique}.parquet")
}

/// The name of a time-series table's partition field, `<time>_<chunk>` as a valid Avro name:
/// manifests record it as an Avro field, which readers reject otherwise.
pub fn partition_field_name(time_column: &str, chunk: Chunk) -> String {
    avro_name(&format!("{time_column}_{chunk}"))
}

/// `name` as a valid Avro name, by the rule Iceberg's Avro schemas use: a leading digit gets a
/// `_` before it, and any other character outside `[A-Za-z0-9_]` becomes `_x` and its code in
/// uppercase hex (`event-time` → `event_x2Dtime`). Valid names are unchanged.
pub fn avro_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for (i, c) in name.chars().enumerate() {
        match c {
            'A'..='Z' | 'a'..='z' | '_' => out.push(c),
            '0'..='9' if i > 0 => out.push(c),
            '0'..='9' => {
                out.push('_');
                out.push(c);
            }
            _ => write!(out, "_x{:X}", u32::from(c)).expect("writing to a String can't fail"),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::property;

    #[test]
    fn table_paths() {
        let name: TableName = "iot.readings".parse().unwrap();
        let t = table_location("s3://b/lake", &name);
        assert_eq!(t, "s3://b/lake/iot/readings");
        assert_eq!(tables_index_uri("s3://b/lake"), "s3://b/lake/timelake-tables.json");
        assert_eq!(metadata_uri(&t, 12), "s3://b/lake/iot/readings/metadata/v12.metadata.json");
        assert!(metadata_uri(&t, 12).starts_with(&metadata_prefix(&t)));
        assert_eq!(hint_uri(&t), "s3://b/lake/iot/readings/metadata/version-hint.text");
        assert_eq!(head_uri(&t), "s3://b/lake/iot/readings/metadata/timelake-head");
        assert_eq!(manifest_list_uri(&t, 7, "u"), "s3://b/lake/iot/readings/metadata/snap-7-u.avro");
        assert_eq!(manifest_uri(&t, "u", "added"), "s3://b/lake/iot/readings/metadata/u-added-m.avro");
        assert_eq!(last_value_index_uri(&t, 7, "u"), "s3://b/lake/iot/readings/metadata/lvi-7-u.parquet");
    }

    #[test]
    fn metadata_versions() {
        assert_eq!(metadata_version("s3://b/t/metadata/v12.metadata.json"), Some(12));
        assert_eq!(metadata_version("v3.metadata.json"), Some(3));
        for other in ["s3://b/t/metadata/version-hint.text", "s3://b/t/metadata/vx.metadata.json", "v1.metadata"] {
            assert_eq!(metadata_version(other), None, "{other}");
        }
    }

    #[test]
    fn data_paths() {
        let contract = Contract::from_properties(&HashMap::from([
            (property::TIME_COLUMN.to_string(), "time".to_string()),
            (property::CHUNK.to_string(), "day".to_string()),
        ]))
        .unwrap();
        assert_eq!(partition_dir("s3://b/t", &contract, Some(20_454)), "s3://b/t/data/time_day=2026-01-01");
        assert_eq!(partition_dir("s3://b/t", &contract, None), "s3://b/t/data");
        let plain = Contract::from_properties(&HashMap::new()).unwrap();
        assert_eq!(partition_dir("s3://b/t", &plain, Some(1)), "s3://b/t/data");
        assert_eq!(data_file_uri("s3://b/t/data", "u"), "s3://b/t/data/u.parquet");
        assert_eq!(partition_field_name("event-time", Chunk::Hour), "event_x2Dtime_hour");
    }

    #[test]
    fn avro_names() {
        assert_eq!(avro_name("time_day"), "time_day");
        assert_eq!(avro_name("event-time_day"), "event_x2Dtime_day");
        assert_eq!(avro_name("1st"), "_1st");
        assert_eq!(avro_name("a b\"é"), "a_x20b_x22_xE9");
    }
}
