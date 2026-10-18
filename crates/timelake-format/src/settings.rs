//! The registry of Timelake's settings (SPEC §21.9): every switch is declared once here, with its
//! type, default, the levels it may be set at, and the version that added it. Table properties,
//! SQL `SET`, the CLI's `timelake settings` and the docs all come from [`SETTINGS`].
//!
//! A setting has one name in kebab-case (`read-dedup`). As a table property it is
//! `timelake.read-dedup`; in SQL, where names are identifiers, `timelake.read_dedup`. Both
//! spellings find the same setting.
//!
//! ```
//! use timelake_format::settings::{self, Level};
//!
//! let s = settings::find("timelake.read_dedup").unwrap();
//! assert_eq!(s.property(), "timelake.read-dedup");
//! assert!(s.allows(Level::Session));
//! assert!(s.check("maybe").is_err());
//! ```

use std::fmt;

use crate::{Chunk, FormatError, FormatVersion, Result, parse_duration};

/// The prefix of Timelake's table properties and session settings.
pub const PREFIX: &str = "timelake.";

/// Where a setting can be set. The more specific level wins: a call over a session over a table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    /// Only when the table is created, through the table options (`time_column`, `chunk`, …).
    Create,
    /// As a table property, at creation or later (`timelake alter`, `Table::set_properties`).
    Table,
    /// For one SQL session: `SET timelake.<name> = …`, or `--set` on the CLI.
    Session,
    /// For one call, through its options (`AppendOptions`, `MaintainOptions`).
    Call,
}

impl Level {
    /// The level's name, as the docs and `timelake settings` show it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Table => "table",
            Self::Session => "session",
            Self::Call => "call",
        }
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A setting's value when nothing sets it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefaultValue {
    /// This value.
    Value(&'static str),
    /// Unset, with what that means (`forever`, `none`).
    Unset(&'static str),
    /// Follows from other settings, as described.
    Derived(&'static str),
}

impl fmt::Display for DefaultValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(s) | Self::Unset(s) | Self::Derived(s) => f.write_str(s),
        }
    }
}

/// One setting.
#[derive(Debug)]
pub struct Setting {
    /// The name in kebab-case, without the `timelake.` prefix.
    pub name: &'static str,
    /// The kind of value, as the docs show it (`boolean`, `duration`, …).
    pub kind: &'static str,
    /// The value when nothing sets it.
    pub default: DefaultValue,
    /// Where it can be set. Empty for properties Timelake manages itself.
    pub levels: &'static [Level],
    /// The Timelake version that added it.
    pub since: &'static str,
    /// What it does, for the docs.
    pub description: &'static str,
    check: fn(&str) -> Result<()>,
}

impl Setting {
    /// The table property: `timelake.read-dedup`.
    pub fn property(&self) -> String {
        format!("{PREFIX}{}", self.name)
    }

    /// The name in SQL: `timelake.read_dedup`.
    pub fn sql_name(&self) -> String {
        format!("{PREFIX}{}", self.name.replace('-', "_"))
    }

    /// Whether the setting can be set at `level`.
    pub fn allows(&self, level: Level) -> bool {
        self.levels.contains(&level)
    }

    /// Errors when `value` isn't a valid value of this setting.
    pub fn check(&self, value: &str) -> Result<()> {
        (self.check)(value.trim()).map_err(|e| FormatError::new(format!("{}: {e}", self.property())))
    }
}

/// Finds a setting by name: `read-dedup`, `read_dedup`, `timelake.read-dedup` or
/// `timelake.read_dedup`, in any case.
pub fn find(name: &str) -> Option<&'static Setting> {
    let name = name.trim().to_ascii_lowercase().replace('_', "-");
    let name = name.strip_prefix(PREFIX).unwrap_or(&name);
    SETTINGS.iter().find(|s| s.name == name)
}

/// Checks Timelake's properties among `properties` for `level` ([`Level::Table`] when creating
/// or altering a table) and returns them with every Timelake key in its canonical spelling. Keys
/// outside the `timelake.` prefix (Iceberg's own) pass through.
pub fn check_properties<'a>(
    properties: impl IntoIterator<Item = (&'a String, &'a String)>,
    level: Level,
) -> Result<Vec<(String, String)>> {
    let mut out = vec![];
    for (key, value) in properties {
        if !key.trim().to_ascii_lowercase().starts_with(PREFIX) {
            out.push((key.clone(), value.clone()));
            continue;
        }
        let setting = find(key).ok_or_else(|| unknown(key))?;
        if !setting.allows(level) {
            return Err(not_settable(setting, level));
        }
        setting.check(value)?;
        out.push((setting.property(), value.trim().to_string()));
    }
    Ok(out)
}

/// The error for a name that isn't a setting.
pub fn unknown(name: &str) -> FormatError {
    FormatError::new(format!("unknown setting {name:?} (`timelake settings` lists them)"))
}

/// The error for a setting set where it can't be: says where it can be.
pub fn not_settable(setting: &Setting, level: Level) -> FormatError {
    let name = setting.property();
    let msg = if setting.levels.is_empty() {
        format!("{name} is set by Timelake itself, not by hand")
    } else if setting.levels == [Level::Create] {
        let options = "time_column, series_key, chunk, dedup, ingest_time";
        format!("{name} is fixed when the table is created, by its options ({options})")
    } else {
        let levels: Vec<&str> = setting.levels.iter().map(|l| l.as_str()).collect();
        format!("{name} can't be set at the {level} level, only at: {}", levels.join(", "))
    };
    FormatError::new(msg)
}

/// Every setting, in the order the docs list them.
pub static SETTINGS: &[Setting] = &[
    Setting {
        name: "time-column",
        kind: "column",
        default: DefaultValue::Unset("none"),
        levels: &[Level::Create],
        since: "0.1.0",
        description: "The time column. Its presence makes the table a time-series table.",
        check: column,
    },
    Setting {
        name: "series-key",
        kind: "columns",
        default: DefaultValue::Unset("none"),
        levels: &[Level::Create],
        since: "0.1.0",
        description: "Comma-separated columns that identify a series.",
        check: columns,
    },
    Setting {
        name: "chunk-interval",
        kind: "hour, day, or month",
        default: DefaultValue::Derived("day for time-series tables"),
        levels: &[Level::Create],
        since: "0.1.0",
        description: "Partition size: how much time one partition covers.",
        check: chunk,
    },
    Setting {
        name: "dedup",
        kind: "boolean",
        default: DefaultValue::Derived("true with a series key, else false"),
        levels: &[Level::Create],
        since: "0.1.0",
        description: "Rows with the same (series, time) count once; the latest write wins. Applied within an append, \
                      by the Timelake reader, and by compaction. Fixed at creation: files written with it off may \
                      hold copies, which the reader would then miss.",
        check: boolean,
    },
    Setting {
        name: "ingest-time-column",
        kind: "column",
        default: DefaultValue::Derived("_ingest_ts for time-series tables"),
        levels: &[Level::Create],
        since: "0.5.0",
        description: "The column holding each row's insertion time (`ingest_time=False` or `--no-ingest-time` leaves \
                      it out). Each append fills it with the time the append started, unless the input brings its \
                      own values. Among rows with the same (series, time), the latest insertion wins, then the \
                      later commit.",
        check: column,
    },
    Setting {
        name: "retention",
        kind: "duration",
        default: DefaultValue::Unset("forever"),
        levels: &[Level::Create, Level::Table],
        since: "0.1.0",
        description: "Maintenance drops partitions that end before now minus this duration, e.g. 90d.",
        check: duration,
    },
    Setting {
        name: "auto-maintain",
        kind: "boolean",
        default: DefaultValue::Value("true"),
        levels: &[Level::Table, Level::Call],
        since: "0.1.0",
        description: "Run size-tiered compaction, retention, and snapshot expiry inline after appends. One append \
                      can override it (`maintain=` in Python, `--no-maintain` on the CLI).",
        check: boolean,
    },
    Setting {
        name: "compact-min-files",
        kind: "count",
        default: DefaultValue::Value("8"),
        levels: &[Level::Table],
        since: "0.1.0",
        description: "Inline compaction merges a size tier of a partition once it has this many files.",
        check: count,
    },
    Setting {
        name: "compact-cold-after",
        kind: "duration or off",
        default: DefaultValue::Value("1h"),
        levels: &[Level::Table, Level::Call],
        since: "0.5.0",
        description: "Inline maintenance rewrites a partition whole (sorted, duplicates removed) once the table holds \
                      data newer than the partition's end by this much and the partition has two small files, \
                      whether the append touched it or not. `off` leaves it to the size tiers.",
        check: duration_or_off,
    },
    Setting {
        name: "read-dedup",
        kind: "boolean",
        default: DefaultValue::Derived("as dedup"),
        levels: &[Level::Table, Level::Session],
        since: "0.5.0",
        description: "The Timelake reader returns only the latest write of each (series, time), before compaction \
                      has removed the older copies. Has no effect on tables with dedup off.",
        check: boolean,
    },
    Setting {
        name: "read-dedup-strategy",
        kind: "merge or keys-first",
        default: DefaultValue::Value("merge"),
        levels: &[Level::Table, Level::Session],
        since: "0.5.0",
        description: "How the reader removes copies: `merge` merges the files that may share keys in key order; \
                      `keys-first` lists the older copies from the key columns, then reads the files like any scan \
                      and leaves them out (slower on the industrial IoT benchmark).",
        check: read_dedup_strategy,
    },
    Setting {
        name: "filter-pushdown",
        kind: "auto, true or false",
        default: DefaultValue::Value("auto"),
        levels: &[Level::Table, Level::Session],
        since: "0.7.0",
        description: "The Timelake reader applies filters while decoding Parquet pages, cheapest first, and decodes \
                      the other columns only for the rows that pass. Less decoding when filters keep few rows, but \
                      each row group's filter columns and other columns are fetched one after the other. `auto` is \
                      on for local files and off on object storage, where first queries wait for the extra round \
                      trip; `true` suits repeat queries served from the cache.",
        check: auto_or_boolean,
    },
    Setting {
        name: "target-file-size-bytes",
        kind: "bytes",
        default: DefaultValue::Value("268435456"),
        levels: &[Level::Table],
        since: "0.1.0",
        description: "Size at which compaction output and writes roll to a new file (256 MiB).",
        check: count,
    },
    Setting {
        name: "row-group-rows",
        kind: "count",
        default: DefaultValue::Value("1048576"),
        levels: &[Level::Table],
        since: "0.1.0",
        description: "Parquet row group size. Page indexes keep single-series reads selective inside large row \
                      groups.",
        check: count,
    },
    Setting {
        name: "snapshot-retention",
        kind: "duration",
        default: DefaultValue::Value("1h"),
        levels: &[Level::Table],
        since: "0.1.0",
        description: "Expire snapshots older than this, keeping at least `min-snapshots` and at most \
                      `max-snapshots`.",
        check: duration,
    },
    Setting {
        name: "min-snapshots",
        kind: "count",
        default: DefaultValue::Value("5"),
        levels: &[Level::Table],
        since: "0.1.0",
        description: "Snapshots always kept, whatever their age.",
        check: count,
    },
    Setting {
        name: "max-snapshots",
        kind: "count",
        default: DefaultValue::Value("100"),
        levels: &[Level::Table],
        since: "0.1.0",
        description: "Snapshots kept at most: older ones expire even if younger than `snapshot-retention`.",
        check: count,
    },
    Setting {
        name: "head-format",
        kind: "codec name or none",
        default: DefaultValue::Value("arrow"),
        levels: &[Level::Table],
        since: "0.3.0",
        description: "Format of the head object (`metadata/timelake-head`): `arrow`, `json`, a registered codec, or \
                      `none` for no head.",
        check: name,
    },
    Setting {
        name: "head-inline-bytes",
        kind: "bytes",
        default: DefaultValue::Value("1048576"),
        levels: &[Level::Table],
        since: "0.3.0",
        description: "Room in the head for the last-value index, then the Parquet footers of the newest files \
                      (1 MiB). What doesn't fit is read separately.",
        check: count,
    },
    Setting {
        name: "keep-snapshots-until",
        kind: "milliseconds since the epoch",
        default: DefaultValue::Unset("none"),
        levels: &[Level::Table],
        since: "0.3.0",
        description: "No snapshot expires before this time. `timelake upgrade` sets it so the upgrade can be rolled \
                      back.",
        check: count,
    },
    Setting {
        name: "rollup-routing",
        kind: "boolean",
        default: DefaultValue::Value("true"),
        levels: &[Level::Table, Level::Session],
        since: "0.5.0",
        description: "The Timelake reader answers a `GROUP BY time_bucket(…)` query on this table from a rollup \
                      that matches it. Buckets changed since the rollup's last refresh, and partial buckets at the \
                      edges of the time range, still come from this table, so the answer is the same either way.",
        check: boolean,
    },
    Setting {
        name: "rollup-refresh-interval",
        kind: "duration or off",
        default: DefaultValue::Value("1m"),
        levels: &[Level::Table],
        since: "0.5.0",
        description: "On a rollup: inline maintenance after an append to its raw table refreshes it once its last \
                      refresh is this old. `off` leaves it to `timelake maintain` and `REFRESH MATERIALIZED ROLLUP`.",
        check: duration_or_off,
    },
    Setting {
        name: "data-page-rows",
        kind: "count",
        default: DefaultValue::Derived("20,000; 8,192 when written from WebAssembly"),
        levels: &[Level::Table],
        since: "0.6.0",
        description: "Rows per Parquet data page in the files written. Smaller pages make compaction from a Worker \
                      use less memory (it holds a page per column of each file it merges: 8,192 rows peaked at 77 MiB \
                      merging 24 TSBS files, 20,000 at 123 MiB) and page pruning finer, at a few percent more bytes \
                      (TSBS: +3.6%) and slower scans below about 8,000 rows.",
        check: count,
    },
    Setting {
        name: "version-check-interval",
        kind: "duration",
        default: DefaultValue::Value("1s"),
        levels: &[Level::Table, Level::Session],
        since: "0.6.0",
        description: "The Timelake reader uses a table version it loaded or checked this long before checking for a \
                      newer one, so queries in that time send no request for it and can miss commits by other \
                      processes. `0s` checks on every query. This process's own commits are seen at once, and \
                      writers always check.",
        check: duration,
    },
    Setting {
        name: "rollups",
        kind: "table names",
        default: DefaultValue::Unset("none"),
        levels: &[],
        since: "0.5.0",
        description: "On a raw table: its rollups, comma-separated. `CREATE` and `DROP MATERIALIZED ROLLUP` keep it.",
        check: columns,
    },
    Setting {
        name: "rollup-source",
        kind: "table name",
        default: DefaultValue::Unset("none"),
        levels: &[],
        since: "0.5.0",
        description: "On a rollup: the raw table it summarises.",
        check: name,
    },
    Setting {
        name: "rollup-bucket",
        kind: "duration",
        default: DefaultValue::Unset("none"),
        levels: &[],
        since: "0.5.0",
        description: "On a rollup: the width of its time buckets.",
        check: duration,
    },
    Setting {
        name: "rollup-aggregates",
        kind: "JSON",
        default: DefaultValue::Unset("none"),
        levels: &[],
        since: "0.5.0",
        description: "On a rollup: its aggregates, each an output column, a function, and the column it reads.",
        check: name,
    },
    Setting {
        name: "format-version",
        kind: "semantic version",
        default: DefaultValue::Derived("the current format for new tables; 0.1.0 if absent"),
        levels: &[],
        since: "0.3.0",
        description: "The table format. Timelake refuses tables of a newer format it can't handle; `timelake upgrade` \
                      brings a table to the current format.",
        check: format_version,
    },
];

fn boolean(s: &str) -> Result<()> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "false" => Ok(()),
        _ => Err(FormatError::new(format!("must be true or false, got {s:?}"))),
    }
}

fn auto_or_boolean(s: &str) -> Result<()> {
    match s.to_ascii_lowercase().as_str() {
        "auto" | "true" | "false" => Ok(()),
        _ => Err(FormatError::new(format!("must be auto, true or false, got {s:?}"))),
    }
}

fn count(s: &str) -> Result<()> {
    s.parse::<u64>().map(drop).map_err(|_| FormatError::new(format!("must be a non-negative integer, got {s:?}")))
}

fn duration(s: &str) -> Result<()> {
    parse_duration(s).map(drop)
}

fn duration_or_off(s: &str) -> Result<()> {
    if s.eq_ignore_ascii_case("off") { Ok(()) } else { duration(s) }
}

fn chunk(s: &str) -> Result<()> {
    s.parse::<Chunk>().map(drop)
}

fn read_dedup_strategy(s: &str) -> Result<()> {
    match s.to_ascii_lowercase().as_str() {
        "merge" | "keys-first" | "keys_first" => Ok(()),
        _ => Err(FormatError::new(format!("must be merge or keys-first, got {s:?}"))),
    }
}

fn format_version(s: &str) -> Result<()> {
    s.parse::<FormatVersion>().map(drop)
}

fn name(s: &str) -> Result<()> {
    if s.is_empty() { Err(FormatError::new("must not be empty")) } else { Ok(()) }
}

fn column(s: &str) -> Result<()> {
    name(s)
}

fn columns(s: &str) -> Result<()> {
    if s.split(',').any(|c| !c.trim().is_empty()) { Ok(()) } else { Err(FormatError::new("must name a column")) }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{Contract, property};

    #[test]
    fn names_are_unique_kebab_case_and_found_in_both_spellings() {
        for (i, s) in SETTINGS.iter().enumerate() {
            assert!(s.name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'), "{}", s.name);
            assert!(SETTINGS[..i].iter().all(|t| t.name != s.name), "{} twice", s.name);
            for spelling in [s.property(), s.sql_name(), s.name.to_string(), s.sql_name().to_ascii_uppercase()] {
                assert_eq!(find(&spelling).map(|f| f.name), Some(s.name), "{spelling}");
            }
        }
        assert!(find("timelake.no-such-setting").is_none());
    }

    /// Every property the contract reads is a setting.
    #[test]
    fn every_property_is_registered() {
        for key in [
            property::TIME_COLUMN,
            property::SERIES_KEY,
            property::CHUNK,
            property::DEDUP,
            property::RETENTION,
            property::TARGET_FILE_SIZE,
            property::ROW_GROUP_ROWS,
            property::AUTO_MAINTAIN,
            property::SNAPSHOT_RETENTION,
            property::MIN_SNAPSHOTS,
            property::MAX_SNAPSHOTS,
            property::COMPACT_MIN_FILES,
            property::COMPACT_COLD_AFTER,
            property::INGEST_TIME_COLUMN,
            property::READ_DEDUP,
            property::READ_DEDUP_STRATEGY,
            property::FILTER_PUSHDOWN,
            property::HEAD_FORMAT,
            property::HEAD_INLINE_BYTES,
            property::KEEP_SNAPSHOTS_UNTIL,
            property::FORMAT_VERSION,
            property::ROLLUPS,
            property::ROLLUP_ROUTING,
            property::ROLLUP_SOURCE,
            property::ROLLUP_BUCKET,
            property::ROLLUP_AGGREGATES,
            property::ROLLUP_REFRESH_INTERVAL,
            property::VERSION_CHECK_INTERVAL,
            property::DATA_PAGE_ROWS,
        ] {
            assert_eq!(find(key).map(Setting::property).as_deref(), Some(key));
        }
        assert_eq!(SETTINGS.len(), 29, "a new setting needs a property constant and an entry above");
    }

    /// A stated default is what the contract uses when the property is unset.
    #[test]
    fn defaults_match_the_contract() {
        let unset = format!("{:?}", Contract::from_properties(&HashMap::new()).unwrap());
        for s in SETTINGS {
            if let DefaultValue::Value(v) = s.default {
                s.check(v).unwrap();
                let set = HashMap::from([(s.property(), v.to_string())]);
                assert_eq!(format!("{:?}", Contract::from_properties(&set).unwrap()), unset, "{}", s.name);
            }
        }
    }

    #[test]
    fn values_are_checked() {
        let check = |name: &str, v: &str| find(name).unwrap().check(v);
        assert!(check("read-dedup", "TRUE").is_ok() && check("read-dedup", "yes").is_err());
        assert!(check("compact-cold-after", "off").is_ok() && check("compact-cold-after", "soon").is_err());
        assert!(check("read-dedup-strategy", "keys-first").is_ok() && check("read-dedup-strategy", "hash").is_err());
        assert!(check("chunk-interval", "1h").is_ok() && check("chunk-interval", "week").is_err());
        assert!(check("min-snapshots", "-1").is_err() && check("series-key", " , ").is_err());
        let err = check("read-dedup", "yes").unwrap_err().to_string();
        assert!(err.starts_with("timelake.read-dedup: must be true or false"), "{err}");
    }

    #[test]
    fn properties_are_checked_for_their_level() {
        let props = |kv: &[(&str, &str)]| -> HashMap<String, String> {
            kv.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
        };
        let ok = check_properties(
            &props(&[("timelake.read_dedup", " false "), ("write.parquet.compression-codec", "zstd")]),
            Level::Table,
        )
        .unwrap();
        assert_eq!(
            ok.into_iter().collect::<HashMap<_, _>>(),
            props(&[("timelake.read-dedup", "false"), ("write.parquet.compression-codec", "zstd")])
        );
        for (kv, msg) in [
            (("timelake.chunk-interval", "hour"), "fixed when the table is created"),
            (("timelake.format-version", "0.3.0"), "set by Timelake itself"),
            (("timelake.no-such", "1"), "unknown setting"),
            (("timelake.retention", "forever"), "bad duration"),
        ] {
            let err = check_properties(&props(&[kv]), Level::Table).unwrap_err().to_string();
            assert!(err.contains(msg), "{kv:?}: {err}");
        }
        let err = check_properties(&props(&[("timelake.min-snapshots", "1")]), Level::Session).unwrap_err();
        assert!(err.to_string().contains("only at: table"), "{err}");
    }
}
