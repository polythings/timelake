//! The registry of Timelake's settings (spec 9): every switch is declared once here, with its
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
use std::str::FromStr;

use crate::name::TableName;
use crate::value::{
    AutoBool, Choice, Compression, CompressionError, FloatEncoding, ReadDedupStrategy, SeriesOrder, SettingValue,
    describe,
};
use crate::{Chunk, FormatError, FormatVersion, Result, parse_duration, parse_duration_or_off};

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

/// The kind of value a setting takes: what the docs call it, and which values are valid.
#[derive(Clone, Copy, Debug)]
pub enum Kind {
    /// A column name.
    Column,
    /// Comma-separated column names, at least one.
    Columns,
    /// A table name, `[namespace.]table`.
    TableName,
    /// Comma-separated table names, at least one.
    TableNames,
    /// `true` or `false`, in any case.
    Boolean,
    /// A non-negative integer.
    Count,
    /// A positive integer: 0 would mean nothing (a row group of no rows).
    PositiveCount,
    /// A non-negative number of bytes.
    Bytes,
    /// A positive number of bytes.
    PositiveBytes,
    /// Milliseconds since the epoch.
    EpochMillis,
    /// A duration such as `90d` or `1 hour` ([`parse_duration`]).
    Duration,
    /// A duration, or `off`.
    DurationOrOff,
    /// One of a fixed set of names ([`Choice`]): how the docs list them, and the type's parser.
    OneOf {
        /// The names, as the docs list them.
        names: fn() -> String,
        /// Checks a value with the type's parser, which takes its other spellings too.
        check: fn(&str) -> Result<()>,
    },
    /// A head codec's name, or `none`.
    HeadFormat,
    /// A semantic version, `MAJOR.MINOR.PATCH`.
    Version,
    /// A rollup's aggregates, as JSON ([`crate::rollup::Aggregate`]).
    RollupAggregates,
}

impl Kind {
    /// One of the names of `T`.
    pub const fn one_of<T: Choice + FromStr<Err = FormatError>>() -> Self {
        Self::OneOf { names: describe::<T>, check: parses::<T> }
    }

    /// Errors when `value` (trimmed) isn't a valid value of this kind.
    pub fn check(self, value: &str) -> Result<()> {
        let s = value.trim();
        match self {
            Self::Column | Self::HeadFormat => non_empty(s),
            Self::RollupAggregates => crate::rollup::parse_aggregates(s).map(drop),
            Self::Columns => list(s, non_empty, "must name a column"),
            Self::TableName => parses::<TableName>(s),
            Self::TableNames => list(s, parses::<TableName>, "must name a table"),
            Self::Boolean => bool::parse_setting(s).map(drop),
            Self::Count | Self::Bytes | Self::EpochMillis => s
                .parse::<u64>()
                .map(drop)
                .map_err(|_| FormatError::new(format!("must be a non-negative integer, got {s:?}"))),
            Self::PositiveCount | Self::PositiveBytes => match s.parse::<u64>() {
                Ok(n) if n > 0 => Ok(()),
                _ => Err(FormatError::new(format!("must be a positive integer, got {s:?}"))),
            },
            Self::Duration => parse_duration(s).map(drop),
            Self::DurationOrOff => parse_duration_or_off(s).map(drop),
            Self::OneOf { check, .. } => check(s),
            Self::Version => parses::<FormatVersion>(s),
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Column => "column",
            Self::Columns => "columns",
            Self::TableName => "table name",
            Self::TableNames => "table names",
            Self::Boolean => "boolean",
            Self::Count | Self::PositiveCount => "count",
            Self::Bytes | Self::PositiveBytes => "bytes",
            Self::EpochMillis => "milliseconds since the epoch",
            Self::Duration => "duration",
            Self::DurationOrOff => "duration or off",
            Self::OneOf { names, .. } => return f.write_str(&names()),
            Self::HeadFormat => "codec name or none",
            Self::Version => "semantic version",
            Self::RollupAggregates => "JSON",
        })
    }
}

fn parses<T: FromStr<Err = FormatError>>(s: &str) -> Result<()> {
    s.parse::<T>().map(drop)
}

fn non_empty(s: &str) -> Result<()> {
    if s.is_empty() { Err(FormatError::new("must not be empty")) } else { Ok(()) }
}

/// A comma-separated list of at least one item, each checked by `item`; empty items are skipped.
fn list(s: &str, item: fn(&str) -> Result<()>, empty: &str) -> Result<()> {
    let items: Vec<&str> = s.split(',').map(str::trim).filter(|i| !i.is_empty()).collect();
    if items.is_empty() {
        return Err(FormatError::new(empty));
    }
    items.into_iter().try_for_each(item)
}

/// One setting.
#[derive(Debug)]
pub struct Setting {
    /// The name in kebab-case, without the `timelake.` prefix.
    pub name: &'static str,
    /// The kind of value: what the docs call it, and which values are valid.
    pub kind: Kind,
    /// The value when nothing sets it.
    pub default: DefaultValue,
    /// Where it can be set. Empty for properties Timelake manages itself.
    pub levels: &'static [Level],
    /// The Timelake version that added it.
    pub since: &'static str,
    /// What it does, for the docs.
    pub description: &'static str,
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
        self.kind.check(value).map_err(|e| FormatError::new(format!("{}: {e}", self.property())))
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
/// outside the `timelake.` prefix (Iceberg's own) pass through. One setting given in two
/// spellings is an error, as either could win.
pub fn check_properties<'a>(
    properties: impl IntoIterator<Item = (&'a String, &'a String)>,
    level: Level,
) -> Result<Vec<(String, String)>> {
    let mut out = vec![];
    let mut given: Vec<(&'static str, &'a String)> = vec![];
    let (mut codec, mut codec_level) = (None, None);
    for (key, value) in properties {
        if !key.trim().to_ascii_lowercase().starts_with(PREFIX) {
            match key.trim() {
                crate::property::COMPRESSION_CODEC => codec = Some(value.as_str()),
                crate::property::COMPRESSION_LEVEL => codec_level = Some(value.as_str()),
                _ => {}
            }
            out.push((key.clone(), value.clone()));
            continue;
        }
        let setting = find(key).ok_or_else(|| unknown(key))?;
        if let Some((_, first)) = given.iter().find(|(name, _)| *name == setting.name) {
            return Err(FormatError::new(format!("{key:?} and {first:?} are the same setting: give it once")));
        }
        given.push((setting.name, key));
        if !setting.allows(level) {
            return Err(not_settable(setting, level));
        }
        setting.check(value)?;
        out.push((setting.property(), value.trim().to_string()));
    }
    // The codec and level are checked together; a level alone is checked against the table's codec
    // by the caller, an integer at least here.
    match (codec, codec_level) {
        (Some(_), _) => drop(Compression::parse(codec, codec_level)?),
        (None, Some(l)) if l.trim().parse::<i32>().is_err() => {
            return Err(CompressionError::LevelNotInteger(l.to_string()).into());
        }
        _ => {}
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
        kind: Kind::Column,
        default: DefaultValue::Unset("none"),
        levels: &[Level::Create],
        since: "0.1.0",
        description: "The time column. Its presence makes the table a time-series table.",
    },
    Setting {
        name: "series-key",
        kind: Kind::Columns,
        default: DefaultValue::Unset("none"),
        levels: &[Level::Create],
        since: "0.1.0",
        description: "Comma-separated columns that identify a series.",
    },
    Setting {
        name: "chunk-interval",
        kind: Kind::one_of::<Chunk>(),
        default: DefaultValue::Derived("day for time-series tables"),
        levels: &[Level::Create],
        since: "0.1.0",
        description: "Partition size: how much time one partition covers.",
    },
    Setting {
        name: "dedup",
        kind: Kind::Boolean,
        default: DefaultValue::Derived("true with a series key, else false"),
        levels: &[Level::Create],
        since: "0.1.0",
        description: "Rows with the same (series, time) count once; the latest write wins. Applied within an append, \
                      by the Timelake reader, and by compaction. Fixed at creation: files written with it off may \
                      hold copies, which the reader would then miss.",
    },
    Setting {
        name: "ingest-time-column",
        kind: Kind::Column,
        default: DefaultValue::Derived("_ingest_ts for time-series tables"),
        levels: &[Level::Create],
        since: "0.5.0",
        description: "The column holding each row's insertion time (`ingest_time=False` or `--no-ingest-time` leaves \
                      it out). Each append fills it with the time the append started, unless the input brings its \
                      own values. Among rows with the same (series, time), the latest insertion wins, then the \
                      later commit.",
    },
    Setting {
        name: "retention",
        kind: Kind::Duration,
        default: DefaultValue::Unset("forever"),
        levels: &[Level::Create, Level::Table],
        since: "0.1.0",
        description: "Maintenance drops partitions that end before now minus this duration, e.g. 90d.",
    },
    Setting {
        name: "auto-maintain",
        kind: Kind::Boolean,
        default: DefaultValue::Value("true"),
        levels: &[Level::Table, Level::Call],
        since: "0.1.0",
        description: "Run size-tiered compaction, retention, and snapshot expiry inline after appends. One append \
                      can override it (`maintain=` in Python, `--no-maintain` on the CLI).",
    },
    Setting {
        name: "compact-min-files",
        kind: Kind::Count,
        default: DefaultValue::Value("8"),
        levels: &[Level::Table],
        since: "0.1.0",
        description: "Inline compaction merges a size tier of a partition once it has this many files.",
    },
    Setting {
        name: "compact-cold-after",
        kind: Kind::DurationOrOff,
        default: DefaultValue::Value("1h"),
        levels: &[Level::Table, Level::Call],
        since: "0.5.0",
        description: "Inline maintenance rewrites a partition whole (sorted, duplicates removed) once the table holds \
                      data newer than the partition's end by this much and the partition has two small files, \
                      whether the append touched it or not. `off` leaves it to the size tiers.",
    },
    Setting {
        name: "read-dedup",
        kind: Kind::Boolean,
        default: DefaultValue::Derived("as dedup"),
        levels: &[Level::Table, Level::Session],
        since: "0.5.0",
        description: "The Timelake reader returns only the latest write of each (series, time), before compaction \
                      has removed the older copies. Has no effect on tables with dedup off.",
    },
    Setting {
        name: "read-dedup-strategy",
        kind: Kind::one_of::<ReadDedupStrategy>(),
        default: DefaultValue::Value("merge"),
        levels: &[Level::Table, Level::Session],
        since: "0.5.0",
        description: "How the reader removes copies: `merge` merges the files that may share keys in key order; \
                      `keys-first` lists the older copies from the key columns, then reads the files like any scan \
                      and leaves them out (slower on the industrial IoT benchmark).",
    },
    Setting {
        name: "filter-pushdown",
        kind: Kind::one_of::<AutoBool>(),
        default: DefaultValue::Value("auto"),
        levels: &[Level::Table, Level::Session],
        since: "0.7.0",
        description: "The Timelake reader applies filters while decoding Parquet pages, cheapest first, and decodes \
                      the other columns only for the rows that pass. Less decoding when filters keep few rows, but \
                      each row group's filter columns and other columns are fetched one after the other. `auto` is \
                      on for local files and off on object storage, where first queries wait for the extra round \
                      trip; `true` suits repeat queries served from the cache.",
    },
    Setting {
        name: "series-order",
        kind: Kind::one_of::<SeriesOrder>(),
        default: DefaultValue::Value("auto"),
        levels: &[Level::Table, Level::Session],
        since: "0.8.0",
        description: "How the signal aggregates (`time_weighted_mean`, `integral`, `counter_diff`, `counter_rate`) \
                      get each series' readings in time order. They sum runs of readings in order and join them; \
                      runs that overlap in time can't be joined. Files are read in time order, each partition a \
                      stretch of time. `auto`: files that overlap another on the series and time, or aren't \
                      sorted, are merged in order first (as dedup on read does), with the other files of the time \
                      they span. `segments`: only copies are merged (with dedup on read); overlaps are an error. \
                      `merge`: every file is merged.",
    },
    Setting {
        name: "window-series-order",
        kind: Kind::one_of::<AutoBool>(),
        default: DefaultValue::Value("auto"),
        levels: &[Level::Table, Level::Session],
        since: "0.8.0",
        description: "A window over each series in time order (`lead(…) OVER (PARTITION BY <series key> ORDER BY \
                      time)`) reads the series merged in order, already split by series, so the reader neither \
                      repartitions nor sorts the rows. Faster on one large file (a day of readings: 18% less), \
                      slower over many (a week: 50% more), with less memory either way. `auto`: when the files read \
                      are in one time partition.",
    },
    Setting {
        name: "shared-signal-state",
        kind: Kind::Boolean,
        default: DefaultValue::Value("true"),
        levels: &[Level::Session],
        since: "0.8.0",
        description: "Signal aggregates of the same readings in one query (`time_weighted_mean`, `integral`, \
                      `counter_diff`, `counter_rate` of one column, by one method) compute their runs of readings \
                      once and each result from them, instead of each computing its own.",
    },
    Setting {
        name: "newest-first",
        kind: Kind::Boolean,
        default: DefaultValue::Value("true"),
        levels: &[Level::Table, Level::Session],
        since: "0.7.0",
        description: "A query sorted by the time column, newest first, with a limit (`ORDER BY time DESC LIMIT k`) \
                      reads the newest time partition first and older ones only while they can still hold one of \
                      the k newest rows. The same for the k newest buckets of a `GROUP BY time_bucket(…)` sorted by \
                      the bucket, newest first.",
    },
    Setting {
        name: "in-list-pruning",
        kind: Kind::Boolean,
        default: DefaultValue::Value("true"),
        levels: &[Level::Table, Level::Session],
        since: "0.7.0",
        description: "The Timelake reader skips row groups and pages whose range of a column holds none of the \
                      values of a `column IN (…)` filter with more than 20 values, which DataFusion doesn't prune \
                      with: lists in the query, and, on scans that don't filter rows while decoding (object \
                      storage by default), the key lists of a join's runtime filter.",
    },
    Setting {
        name: "target-file-size-bytes",
        kind: Kind::PositiveBytes,
        default: DefaultValue::Value("268435456"),
        levels: &[Level::Table],
        since: "0.1.0",
        description: "Size at which compaction output and writes roll to a new file (256 MiB).",
    },
    Setting {
        name: "row-group-rows",
        kind: Kind::PositiveCount,
        default: DefaultValue::Value("1048576"),
        levels: &[Level::Table],
        since: "0.1.0",
        description: "Parquet row group size. Page indexes keep single-series reads selective inside large row \
                      groups.",
    },
    Setting {
        name: "snapshot-retention",
        kind: Kind::Duration,
        default: DefaultValue::Value("1h"),
        levels: &[Level::Table],
        since: "0.1.0",
        description: "Expire snapshots older than this, keeping at least `min-snapshots` and at most \
                      `max-snapshots`.",
    },
    Setting {
        name: "min-snapshots",
        kind: Kind::Count,
        default: DefaultValue::Value("5"),
        levels: &[Level::Table],
        since: "0.1.0",
        description: "Snapshots always kept, whatever their age.",
    },
    Setting {
        name: "max-snapshots",
        kind: Kind::Count,
        default: DefaultValue::Value("100"),
        levels: &[Level::Table],
        since: "0.1.0",
        description: "Snapshots kept at most: older ones expire even if younger than `snapshot-retention`.",
    },
    Setting {
        name: "head-format",
        kind: Kind::HeadFormat,
        default: DefaultValue::Value("arrow"),
        levels: &[Level::Table],
        since: "0.3.0",
        description: "Format of the head object (`metadata/timelake-head`): `arrow`, `json`, a registered codec, or \
                      `none` for no head.",
    },
    Setting {
        name: "head-inline-bytes",
        kind: Kind::Bytes,
        default: DefaultValue::Value("1048576"),
        levels: &[Level::Table],
        since: "0.3.0",
        description: "Room in the head for the last-value index, then the Parquet footers of the newest files \
                      (1 MiB). What doesn't fit is read separately.",
    },
    Setting {
        name: "keep-snapshots-until",
        kind: Kind::EpochMillis,
        default: DefaultValue::Unset("none"),
        levels: &[Level::Table],
        since: "0.3.0",
        description: "No snapshot expires before this time. `timelake upgrade` sets it so the upgrade can be rolled \
                      back.",
    },
    Setting {
        name: "rollup-routing",
        kind: Kind::Boolean,
        default: DefaultValue::Value("true"),
        levels: &[Level::Table, Level::Session],
        since: "0.5.0",
        description: "The Timelake reader answers a `GROUP BY time_bucket(…)` query on this table from a rollup \
                      that matches it. Buckets changed since the rollup's last refresh, and partial buckets at the \
                      edges of the time range, still come from this table, so the answer is the same either way.",
    },
    Setting {
        name: "rollup-refresh-interval",
        kind: Kind::DurationOrOff,
        default: DefaultValue::Value("1m"),
        levels: &[Level::Table],
        since: "0.5.0",
        description: "On a rollup: inline maintenance after an append to its raw table refreshes it once its last \
                      refresh is this old. `off` leaves it to `timelake maintain` and `REFRESH MATERIALIZED ROLLUP`.",
    },
    Setting {
        name: "float-encoding",
        kind: Kind::one_of::<FloatEncoding>(),
        default: DefaultValue::Value("dictionary"),
        levels: &[Level::Table],
        since: "0.8.0",
        description: "How data files encode float columns. `dictionary`: a dictionary of each column chunk's values, \
                      falling back to byte-stream-split once it outgrows its page. Sensor readings repeat values: \
                      a third of the size on the IIoT benchmark and up to twice as fast to scan on TSBS DevOps; \
                      random doubles cost 5% more space and 6% more scan time. `byte-stream-split`: byte-stream-split \
                      only. Applies to files written from then on.",
    },
    Setting {
        name: "data-page-rows",
        kind: Kind::PositiveCount,
        default: DefaultValue::Derived("20,000; 8,192 when written from WebAssembly"),
        levels: &[Level::Table],
        since: "0.6.0",
        description: "Rows per Parquet data page in the files written. Smaller pages make compaction from a Worker \
                      use less memory (it holds a page per column of each file it merges: 8,192 rows peaked at 77 MiB \
                      merging 24 TSBS files, 20,000 at 123 MiB) and page pruning finer, at a few percent more bytes \
                      (TSBS: +3.6%) and slower scans below about 8,000 rows.",
    },
    Setting {
        name: "version-check-interval",
        kind: Kind::Duration,
        default: DefaultValue::Value("1s"),
        levels: &[Level::Table, Level::Session],
        since: "0.6.0",
        description: "The Timelake reader uses a table version it loaded or checked this long before checking for a \
                      newer one, so queries in that time send no request for it and can miss commits by other \
                      processes. `0s` checks on every query. This process's own commits are seen at once, and \
                      writers always check.",
    },
    Setting {
        name: "rollups",
        kind: Kind::TableNames,
        default: DefaultValue::Unset("none"),
        levels: &[],
        since: "0.5.0",
        description: "On a raw table: its rollups, comma-separated. `CREATE` and `DROP MATERIALIZED ROLLUP` keep it.",
    },
    Setting {
        name: "rollup-source",
        kind: Kind::TableName,
        default: DefaultValue::Unset("none"),
        levels: &[],
        since: "0.5.0",
        description: "On a rollup: the raw table it summarises.",
    },
    Setting {
        name: "rollup-bucket",
        kind: Kind::Duration,
        default: DefaultValue::Unset("none"),
        levels: &[],
        since: "0.5.0",
        description: "On a rollup: the width of its time buckets.",
    },
    Setting {
        name: "rollup-aggregates",
        kind: Kind::RollupAggregates,
        default: DefaultValue::Unset("none"),
        levels: &[],
        since: "0.5.0",
        description: "On a rollup: its aggregates, each an output column, a function, and the column it reads.",
    },
    Setting {
        name: "format-version",
        kind: Kind::Version,
        default: DefaultValue::Derived("the current format for new tables; 0.1.0 if absent"),
        levels: &[],
        since: "0.3.0",
        description: "The table format. Timelake refuses tables of a newer format it can't handle; `timelake upgrade` \
                      brings a table to the current format.",
    },
];

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{Contract, property};

    /// The default the docs give for `data-page-rows` is the writers' constants.
    #[cfg(feature = "write")]
    #[test]
    fn data_page_rows_default_is_the_writers() {
        use crate::file::{DATA_PAGE_ROWS, WASM_DATA_PAGE_ROWS};

        let thousands = |n: usize| {
            let digits = n.to_string();
            let mut out = String::new();
            for (i, c) in digits.chars().enumerate() {
                if i > 0 && (digits.len() - i).is_multiple_of(3) {
                    out.push(',');
                }
                out.push(c);
            }
            out
        };
        let expected =
            format!("{}; {} when written from WebAssembly", thousands(DATA_PAGE_ROWS), thousands(WASM_DATA_PAGE_ROWS));
        assert_eq!(find("data-page-rows").unwrap().default.to_string(), expected);
    }

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
            property::IN_LIST_PRUNING,
            property::NEWEST_FIRST,
            property::SERIES_ORDER,
            property::SHARED_SIGNAL_STATE,
            property::WINDOW_SERIES_ORDER,
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
            property::FLOAT_ENCODING,
        ] {
            assert_eq!(find(key).map(Setting::property).as_deref(), Some(key));
        }
        assert_eq!(SETTINGS.len(), 35, "a new setting needs a property constant and an entry above");
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
        assert!(check("min-snapshots", "0").is_ok() && check("row-group-rows", "0").is_err());
        assert!(check("target-file-size-bytes", "0").is_err() && check("data-page-rows", "0").is_err());
        let err = check("read-dedup", "yes").unwrap_err().to_string();
        assert!(err.starts_with("timelake.read-dedup: must be true or false"), "{err}");
        assert!(check("series-order", "Segments").is_ok() && check("series-order", "sorted").is_err());
        assert!(check("filter-pushdown", "AUTO").is_ok() && check("filter-pushdown", "yes").is_err());
        assert!(check("float-encoding", "byte-stream-split").is_ok() && check("float-encoding", "plain").is_err());
        assert!(check("rollups", "iot.r_1m, iot.r_1h").is_ok() && check("rollups", "iot/r").is_err());
        assert!(check("rollup-source", "iot.r").is_ok() && check("rollup-source", "a.b.c").is_err());
        let aggregates = r#"[{"name": "n", "function": "gap_count:300s"}]"#;
        assert!(check("rollup-aggregates", aggregates).is_ok() && check("rollup-aggregates", "avg").is_err());
        let err = check("window-series-order", "yes").unwrap_err().to_string();
        assert_eq!(err, "timelake.window-series-order: must be auto, true, or false, got \"yes\"");
    }

    /// The docs name each kind of value; a fixed set of names is listed from its type.
    #[test]
    fn kinds_name_their_values() {
        let kind = |name: &str| find(name).unwrap().kind.to_string();
        assert_eq!(kind("chunk-interval"), "hour, day, or month");
        assert_eq!(kind("read-dedup-strategy"), "merge or keys-first");
        assert_eq!(kind("series-order"), "auto, segments, or merge");
        assert_eq!(kind("compact-cold-after"), "duration or off");
        assert_eq!(kind("target-file-size-bytes"), "bytes");
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
            (("write.parquet.compression-level", "high"), "is not an integer"),
            (("write.parquet.compression-codec", "lzo"), "use zstd, lz4"),
        ] {
            let err = check_properties(&props(&[kv]), Level::Table).unwrap_err().to_string();
            assert!(err.contains(msg), "{kv:?}: {err}");
        }
        let twice = props(&[("timelake.read_dedup", "false"), ("timelake.read-dedup", "true")]);
        let err = check_properties(&twice, Level::Table).unwrap_err().to_string();
        assert!(err.contains("the same setting"), "{err}");
        let err = check_properties(&props(&[("timelake.min-snapshots", "1")]), Level::Session).unwrap_err();
        assert!(err.to_string().contains("only at: table"), "{err}");
    }
}
