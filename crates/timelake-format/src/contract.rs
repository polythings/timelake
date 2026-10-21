//! The Timelake table contract (spec 4): which column is time, which columns identify a series,
//! and the layout/maintenance policy derived from them. Stored as `timelake.*` Iceberg table
//! properties (see [`property`]), so the contract travels with the table and other
//! engines simply ignore it.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use chrono::Datelike;

use crate::value::{AutoBool, Choice, Compression, CompressionError, FloatEncoding, ReadDedupStrategy, SeriesOrder};
use crate::{FORMAT_VERSION, FormatError, FormatVersion, Result, property};

const HOUR_MICROS: i64 = 3_600_000_000;
const DAY_MICROS: i64 = 24 * HOUR_MICROS;
const DAY_SECS: u64 = 86_400;

/// `timelake.head-format` value that turns the head off.
pub const NO_HEAD: &str = "none";

/// Time partition granularity: how much time one partition covers.
///
/// Parses from `hour`, `day` or `month` (and spellings such as `1h`, `1 day`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Chunk {
    /// One partition per hour.
    Hour,
    /// One partition per day (the default).
    Day,
    /// One partition per calendar month.
    Month,
}

impl Chunk {
    /// The canonical name, as stored in the table properties.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Month => "month",
        }
    }

    /// The Iceberg partition transform that partitions by this chunk (`hour`, `day` or `month`).
    pub fn transform_name(self) -> &'static str {
        match self {
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Month => "month",
        }
    }

    /// The chunk an Iceberg partition transform partitions by; `None` for transforms other than
    /// `hour`, `day` and `month`.
    pub fn from_transform(transform: &str) -> Option<Self> {
        [Self::Hour, Self::Day, Self::Month].into_iter().find(|c| c.transform_name() == transform)
    }

    /// Iceberg transform value (hours/days/months since the epoch) of a timestamp in microseconds.
    pub fn value(self, micros: i64) -> Result<i32> {
        let out_of_range = || FormatError::new(format!("timestamp {micros}µs is out of range for {self} partitions"));
        let t = chrono::DateTime::from_timestamp_micros(micros).ok_or_else(out_of_range)?;
        let v = match self {
            Self::Hour => micros.div_euclid(HOUR_MICROS),
            Self::Day => micros.div_euclid(DAY_MICROS),
            Self::Month => (i64::from(t.year()) - 1970) * 12 + i64::from(t.month0()),
        };
        i32::try_from(v).map_err(|_| out_of_range())
    }

    /// Start of a partition, in microseconds since the epoch.
    pub fn start_micros(self, value: i32) -> Result<i64> {
        let start = match self {
            Self::Hour => Some(i64::from(value) * HOUR_MICROS),
            Self::Day => i64::from(value).checked_mul(DAY_MICROS),
            Self::Month => {
                let (year, month0) = (1970 + value.div_euclid(12), value.rem_euclid(12).unsigned_abs());
                chrono::NaiveDate::from_ymd_opt(year, month0 + 1, 1)
                    .and_then(|d| d.and_hms_opt(0, 0, 0))
                    .map(|t| t.and_utc().timestamp_micros())
            }
        };
        start.ok_or_else(|| self.out_of_range(value))
    }

    /// End (exclusive) of a partition, in microseconds since the epoch.
    pub fn end_micros(self, value: i32) -> Result<i64> {
        self.start_micros(value.checked_add(1).ok_or_else(|| self.out_of_range(value))?)
    }

    /// Human-readable partition value, used in data file paths (`time_day=2026-01-01`). Values
    /// outside the supported time range fall back to the plain number.
    pub fn label(self, value: i32) -> String {
        let Some(t) = self.start_micros(value).ok().and_then(chrono::DateTime::from_timestamp_micros) else {
            return value.to_string();
        };
        let format = match self {
            Self::Hour => "%Y-%m-%d-%H",
            Self::Day => "%Y-%m-%d",
            Self::Month => "%Y-%m",
        };
        t.format(format).to_string()
    }

    fn out_of_range(self, value: i32) -> FormatError {
        FormatError::new(format!("{self} partition {value} is out of range"))
    }
}

impl Choice for Chunk {
    const ALL: &'static [Self] = &[Self::Hour, Self::Day, Self::Month];

    fn name(self) -> &'static str {
        self.as_str()
    }

    fn aliases(self) -> &'static [&'static str] {
        match self {
            Self::Hour => &["1h", "1 hour"],
            Self::Day => &["1d", "1 day", "24h"],
            Self::Month => &["1 month"],
        }
    }
}

impl FromStr for Chunk {
    type Err = FormatError;

    fn from_str(s: &str) -> Result<Self> {
        crate::value::find(s).ok_or_else(|| {
            let names = crate::value::describe::<Self>();
            FormatError::new(format!("chunk interval must be {names}, got {:?}", s.trim().to_ascii_lowercase()))
        })
    }
}

impl fmt::Display for Chunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// As its name (`day`).
impl serde::Serialize for Chunk {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// Options for creating a table. Only `time_column` is required for a time-series table; leave it
/// unset for a plain (dimension) table such as device metadata.
#[derive(Clone, Debug, Default)]
pub struct TableOptions {
    /// The timestamp column; makes the table a time-series table.
    pub time_column: Option<String>,
    /// Columns that identify a series (e.g. `device_id`); requires a time column.
    pub series_key: Vec<String>,
    /// Time partition size (default: [`Chunk::Day`]).
    pub chunk: Option<Chunk>,
    /// Remove duplicate `(series_key, time)` rows during compaction (default: on with a series key).
    pub dedup: Option<bool>,
    /// Drop partitions entirely older than this.
    pub retention: Option<Duration>,
    /// Add an insertion-time column (`_ingest_ts`) to a time-series table (default: on).
    pub ingest_time: Option<bool>,
    /// Extra Iceberg/Timelake table properties.
    pub properties: HashMap<String, String>,
}

/// The parsed contract of an existing table.
#[derive(Clone, Debug)]
pub struct Contract {
    /// The timestamp column, if this is a time-series table.
    pub time_column: Option<String>,
    /// Columns that identify a series.
    pub series_key: Vec<String>,
    /// Time partition size.
    pub chunk: Option<Chunk>,
    /// Remove rows with the same `(series_key, time)` during compaction; the latest write wins.
    pub dedup: bool,
    /// The column holding each row's insertion time, if the table has one.
    pub ingest_time_column: Option<String>,
    /// Partitions entirely older than this are dropped by maintenance.
    pub retention: Option<Duration>,
    /// Compaction writes files of about this size.
    pub target_file_size: u64,
    /// Rows per Parquet row group.
    pub row_group_rows: usize,
    /// Appends run inline maintenance when thresholds are crossed.
    pub auto_maintain: bool,
    /// Snapshots younger than this are kept.
    pub snapshot_retention: Duration,
    /// The newest snapshots kept regardless of age.
    pub min_snapshots: usize,
    /// Snapshots beyond the newest `max_snapshots` are expired even if younger than the retention.
    pub max_snapshots: usize,
    /// Inline auto-maintenance merges a size tier of a partition once it has this many files.
    pub compact_min_files: usize,
    /// Inline auto-maintenance rewrites a partition whole once the table holds data newer than the
    /// partition's end by this much (`None`: never).
    pub compact_cold_after: Option<Duration>,
    /// The Timelake reader removes copies of a `(series…, time)` when reading.
    pub read_dedup: bool,
    /// How the Timelake reader removes copies.
    pub read_dedup_strategy: ReadDedupStrategy,
    /// Whether the Timelake reader filters rows while decoding Parquet.
    pub filter_pushdown: AutoBool,
    /// The Timelake reader prunes row groups and pages with `IN` lists of more than 20 values.
    pub in_list_pruning: bool,
    /// `ORDER BY time DESC LIMIT k` reads time partitions newest first and stops early.
    pub newest_first: bool,
    /// How the signal aggregates get readings in time order.
    pub series_order: SeriesOrder,
    /// Whether windows over each series in time order read it merged in order.
    pub window_series_order: AutoBool,
    /// The codec of the head object, lowercase; `None` (`none`) to not write one.
    pub head_format: Option<String>,
    /// Bytes the head may spend on optional sections (the last-value index).
    pub head_inline_bytes: u64,
    /// The table format version (see [`crate::FORMAT_VERSION`]).
    pub format_version: FormatVersion,
    /// No snapshot expires before this time (milliseconds since the epoch).
    pub keep_snapshots_until: Option<i64>,
    /// The table's rollups (table names).
    pub rollups: Vec<String>,
    /// The Timelake reader may answer queries on this table from its rollups.
    pub rollup_routing: bool,
    /// For a rollup: inline maintenance of its raw table refreshes it once its last refresh is this
    /// old (`None`: never).
    pub rollup_refresh_interval: Option<Duration>,
    /// The Timelake reader uses a table version it loaded or checked for this long before
    /// checking for a newer one.
    pub version_check_interval: Duration,
    /// Rows per Parquet data page in the files written (`None`: the writer's default).
    pub data_page_rows: Option<usize>,
    /// How data files encode float columns.
    pub float_encoding: FloatEncoding,
    /// Iceberg's `write.parquet.compression-codec` and `-level` (ZSTD 3 when unset). Iceberg's own
    /// properties, which other engines set too: an invalid one fails writes, never reads.
    pub compression: std::result::Result<Compression, CompressionError>,
}

impl Contract {
    /// Defaults for properties a table doesn't set.
    const DEFAULT_TARGET_FILE_SIZE: u64 = 256 << 20;
    const DEFAULT_ROW_GROUP_ROWS: usize = 1 << 20;
    const DEFAULT_SNAPSHOT_RETENTION: Duration = Duration::from_secs(3600);
    const DEFAULT_MIN_SNAPSHOTS: usize = 5;
    const DEFAULT_MAX_SNAPSHOTS: usize = 100;
    const DEFAULT_COMPACT_MIN_FILES: usize = 8;
    const DEFAULT_COMPACT_COLD_AFTER: Duration = Duration::from_secs(3600);
    const DEFAULT_HEAD_FORMAT: &str = "arrow";
    const DEFAULT_HEAD_INLINE_BYTES: u64 = 1 << 20;
    const DEFAULT_ROLLUP_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
    const DEFAULT_VERSION_CHECK_INTERVAL: Duration = Duration::from_secs(1);

    /// Reads the contract from Iceberg table properties, with defaults for what isn't set.
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self> {
        let p = Properties(properties);
        Ok(Self {
            time_column: p.get(property::TIME_COLUMN).map(str::to_string),
            series_key: p.list(property::SERIES_KEY),
            chunk: p.get(property::CHUNK).map(str::parse).transpose()?,
            dedup: p.flag(property::DEDUP, false)?,
            ingest_time_column: p.get(property::INGEST_TIME_COLUMN).map(str::to_string),
            retention: p.duration(property::RETENTION)?,
            target_file_size: p.positive(property::TARGET_FILE_SIZE, Self::DEFAULT_TARGET_FILE_SIZE)?,
            row_group_rows: p.positive(property::ROW_GROUP_ROWS, Self::DEFAULT_ROW_GROUP_ROWS)?,
            auto_maintain: p.flag(property::AUTO_MAINTAIN, true)?,
            snapshot_retention: p.duration(property::SNAPSHOT_RETENTION)?.unwrap_or(Self::DEFAULT_SNAPSHOT_RETENTION),
            min_snapshots: p.number(property::MIN_SNAPSHOTS, Self::DEFAULT_MIN_SNAPSHOTS)?,
            max_snapshots: p.number(property::MAX_SNAPSHOTS, Self::DEFAULT_MAX_SNAPSHOTS)?,
            compact_min_files: p.number(property::COMPACT_MIN_FILES, Self::DEFAULT_COMPACT_MIN_FILES)?,
            compact_cold_after: p.duration_or_off(property::COMPACT_COLD_AFTER, Self::DEFAULT_COMPACT_COLD_AFTER)?,
            read_dedup: p.flag(property::READ_DEDUP, p.flag(property::DEDUP, false)?)?,
            read_dedup_strategy: p.choice(property::READ_DEDUP_STRATEGY, ReadDedupStrategy::Merge)?,
            filter_pushdown: p.choice(property::FILTER_PUSHDOWN, AutoBool::Auto)?,
            in_list_pruning: p.flag(property::IN_LIST_PRUNING, true)?,
            newest_first: p.flag(property::NEWEST_FIRST, true)?,
            series_order: p.choice(property::SERIES_ORDER, SeriesOrder::Auto)?,
            window_series_order: p.choice(property::WINDOW_SERIES_ORDER, AutoBool::Auto)?,
            head_format: match p.get(property::HEAD_FORMAT).unwrap_or(Self::DEFAULT_HEAD_FORMAT) {
                s if s.eq_ignore_ascii_case(NO_HEAD) => None,
                s => Some(s.to_ascii_lowercase()),
            },
            head_inline_bytes: p.number(property::HEAD_INLINE_BYTES, Self::DEFAULT_HEAD_INLINE_BYTES)?,
            format_version: p.get(property::FORMAT_VERSION).map_or(Ok(FormatVersion::UNVERSIONED), str::parse)?,
            keep_snapshots_until: p
                .get(property::KEEP_SNAPSHOTS_UNTIL)
                .map(|_| p.number(property::KEEP_SNAPSHOTS_UNTIL, 0))
                .transpose()?,
            rollups: p.list(property::ROLLUPS),
            rollup_routing: p.flag(property::ROLLUP_ROUTING, true)?,
            rollup_refresh_interval: p
                .duration_or_off(property::ROLLUP_REFRESH_INTERVAL, Self::DEFAULT_ROLLUP_REFRESH_INTERVAL)?,
            version_check_interval: p
                .duration(property::VERSION_CHECK_INTERVAL)?
                .unwrap_or(Self::DEFAULT_VERSION_CHECK_INTERVAL),
            data_page_rows: p
                .get(property::DATA_PAGE_ROWS)
                .map(|_| p.positive(property::DATA_PAGE_ROWS, 1))
                .transpose()?,
            float_encoding: p.choice(property::FLOAT_ENCODING, FloatEncoding::Dictionary)?,
            compression: Compression::parse(p.get(property::COMPRESSION_CODEC), p.get(property::COMPRESSION_LEVEL)),
        })
    }

    /// Errors when this Timelake can't read the table correctly: its format is newer than
    /// [`FORMAT_VERSION`] in a way that breaks older readers.
    pub fn check_readable(&self) -> Result<()> {
        if self.format_version.readable_by(FORMAT_VERSION) {
            return Ok(());
        }
        Err(FormatError::new(format!(
            "table format {} is newer than this Timelake reads ({FORMAT_VERSION}): upgrade Timelake",
            self.format_version
        )))
    }

    /// Errors when this Timelake must not write to the table: it wouldn't keep what the table's
    /// newer format relies on.
    pub fn check_writable(&self) -> Result<()> {
        if self.format_version.writable_by(FORMAT_VERSION) {
            return Ok(());
        }
        Err(FormatError::new(format!(
            "table format {} is newer than this Timelake writes ({FORMAT_VERSION}): upgrade Timelake",
            self.format_version
        )))
    }

    /// Rows older than this (µs since the epoch) are past the table's retention at `now_micros`:
    /// partitions ending by then are dropped. `None` without a retention.
    pub fn retention_cutoff(&self, now_micros: i64) -> Option<i64> {
        self.retention.map(|r| now_micros.saturating_sub(duration_micros(r)))
    }

    /// Snapshots made before this (ms since the epoch) are past the snapshot retention at
    /// `now_ms`, and may expire.
    pub fn snapshot_cutoff_ms(&self, now_ms: i64) -> i64 {
        now_ms.saturating_sub(duration_millis(self.snapshot_retention))
    }

    /// Whether the table has a time column.
    pub fn is_time_series(&self) -> bool {
        self.time_column.is_some()
    }

    /// Sort key: series key columns, then time.
    pub fn sort_columns(&self) -> Vec<String> {
        let mut v = self.series_key.clone();
        v.extend(self.time_column.clone());
        v
    }

    /// The sort key, then the insertion time: in this order (ties kept in commit order) the last
    /// row of each `(series…, time)` is its latest write.
    pub fn write_order(&self) -> Vec<String> {
        let mut v = self.sort_columns();
        v.extend(self.ingest_time_column.clone());
        v
    }
}

/// Typed access to table properties; blank values count as unset.
struct Properties<'a>(&'a HashMap<String, String>);

impl Properties<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|s| s.trim()).filter(|s| !s.is_empty())
    }

    fn number<T: FromStr>(&self, key: &str, default: T) -> Result<T> {
        self.get(key).map_or(Ok(default), |s| {
            s.parse().map_err(|_| FormatError::new(format!("{key} must be a non-negative integer, got {s:?}")))
        })
    }

    /// A count or size where 0 would mean nothing (a row group of no rows).
    fn positive<T: FromStr + Default + PartialEq>(&self, key: &str, default: T) -> Result<T> {
        let n = self.number(key, default)?;
        if n == T::default() {
            return Err(FormatError::new(format!("{key} must be a positive integer, got 0")));
        }
        Ok(n)
    }

    fn flag(&self, key: &str, default: bool) -> Result<bool> {
        self.get(key).map_or(Ok(default), |s| {
            s.to_ascii_lowercase()
                .parse()
                .map_err(|_| FormatError::new(format!("{key} must be true or false, got {s:?}")))
        })
    }

    fn duration(&self, key: &str) -> Result<Option<Duration>> {
        self.get(key).map(parse_duration).transpose()
    }

    /// A duration, or `None` for `off`.
    fn duration_or_off(&self, key: &str, default: Duration) -> Result<Option<Duration>> {
        self.get(key).map_or(Ok(Some(default)), parse_duration_or_off)
    }

    /// One of a fixed set of names.
    fn choice<T: FromStr<Err = FormatError>>(&self, key: &str, default: T) -> Result<T> {
        self.get(key).map_or(Ok(default), |s| s.parse().map_err(|e| FormatError::new(format!("{key} {e}"))))
    }

    /// A comma-separated list (empty items dropped).
    fn list(&self, key: &str) -> Vec<String> {
        self.get(key)
            .map(|s| s.split(',').map(str::trim).filter(|c| !c.is_empty()).map(str::to_string).collect())
            .unwrap_or_default()
    }
}

/// Parses durations like `90d`, `12h`, `30m`, `45s`, `2w`, `1y`, or spelled out (`1 day`, `3 hours`).
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (n, unit) = s.split_at(split);
    let n: u64 = n.parse().map_err(|_| FormatError::new(format!("bad duration {s:?}")))?;
    let unit_secs = match unit.trim().to_ascii_lowercase().as_str() {
        "s" | "sec" | "secs" | "second" | "seconds" => 1,
        "m" | "min" | "mins" | "minute" | "minutes" => 60,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3600,
        "d" | "day" | "days" => DAY_SECS,
        "w" | "week" | "weeks" => 7 * DAY_SECS,
        "y" | "year" | "years" => 365 * DAY_SECS,
        _ => return Err(FormatError::new(format!("bad duration unit in {s:?} (use s, m, h, d, w, y)"))),
    };
    let secs = n.checked_mul(unit_secs).ok_or_else(|| FormatError::new(format!("duration {s:?} is too long")))?;
    Ok(Duration::from_secs(secs))
}

/// Parses a duration as [`parse_duration`] does, or `off` (in any case) as `None`.
pub fn parse_duration_or_off(s: &str) -> Result<Option<Duration>> {
    if s.trim().eq_ignore_ascii_case("off") { Ok(None) } else { parse_duration(s).map(Some) }
}

/// Whole microseconds of `d`, at most `i64::MAX` (settings can name durations of centuries).
pub fn duration_micros(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

/// Whole milliseconds of `d`, at most `i64::MAX`.
pub fn duration_millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// Formats whole seconds of a duration in the largest exact unit (`90d`, `36h`, `45s`), the
/// inverse of [`parse_duration`].
pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    match [(DAY_SECS, 'd'), (3600, 'h'), (60, 'm')]
        .into_iter()
        .find(|(unit, _)| secs != 0 && secs.is_multiple_of(*unit))
    {
        Some((unit, suffix)) => format!("{}{suffix}", secs / unit),
        None => format!("{secs}s"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::property::*;

    const T: i64 = 1_767_225_600_000_000; // 2026-01-01T00:00:00Z

    #[test]
    fn cutoffs_follow_the_retentions() {
        let c = Contract::from_properties(&HashMap::from([
            (RETENTION.to_string(), "1d".to_string()),
            (SNAPSHOT_RETENTION.to_string(), "2h".to_string()),
        ]))
        .unwrap();
        assert_eq!(c.retention_cutoff(T), Some(T - 86_400_000_000));
        assert_eq!(c.snapshot_cutoff_ms(T / 1000), T / 1000 - 7_200_000);
        assert_eq!(Contract::from_properties(&HashMap::new()).unwrap().retention_cutoff(T), None);
        // Durations past what i64 holds saturate rather than wrap.
        let forever = Duration::from_secs(u64::MAX);
        assert_eq!((duration_micros(forever), duration_millis(forever)), (i64::MAX, i64::MAX));
        assert_eq!(duration_micros(Duration::from_millis(1500)), 1_500_000);
    }

    #[test]
    fn chunk_values_round_trip() {
        for c in [Chunk::Hour, Chunk::Day, Chunk::Month] {
            let v = c.value(T + 1).unwrap();
            assert_eq!(c.start_micros(v).unwrap(), T, "{c:?}");
            assert!(c.end_micros(v).unwrap() > T);
        }
        for c in [Chunk::Hour, Chunk::Day, Chunk::Month] {
            assert_eq!(Chunk::from_transform(c.transform_name()), Some(c));
        }
        assert_eq!(Chunk::from_transform("bucket[16]"), None);
        assert_eq!(Chunk::Day.label(Chunk::Day.value(T).unwrap()), "2026-01-01");
        assert_eq!(Chunk::Hour.label(Chunk::Hour.value(T).unwrap()), "2026-01-01-00");
        assert_eq!(Chunk::Month.label(Chunk::Month.value(T).unwrap()), "2026-01");
        assert_eq!(Chunk::Month.value(T).unwrap(), (2026 - 1970) * 12);
        // Before the epoch, values are negative and still round down.
        assert_eq!(Chunk::Day.value(-1).unwrap(), -1);
        assert_eq!(Chunk::Month.value(-1).unwrap(), -1);
        assert_eq!(Chunk::Month.start_micros(-1).unwrap(), -31 * DAY_MICROS);
    }

    #[test]
    fn out_of_range_times_are_errors() {
        for c in [Chunk::Hour, Chunk::Day, Chunk::Month] {
            assert!(c.value(i64::MAX).is_err(), "{c:?}");
            assert!(c.value(i64::MIN).is_err(), "{c:?}");
        }
        assert!(Chunk::Day.start_micros(i32::MAX).is_err());
        assert!(Chunk::Month.start_micros(i32::MAX).is_err());
        assert!(Chunk::Hour.end_micros(i32::MAX).is_err());
        assert_eq!(Chunk::Month.label(i32::MAX), i32::MAX.to_string());
    }

    #[test]
    fn chunk_parses_and_displays() {
        for (s, c) in [("hour", Chunk::Hour), (" 1 Day ", Chunk::Day), ("24h", Chunk::Day), ("month", Chunk::Month)] {
            assert_eq!(s.parse::<Chunk>().unwrap(), c, "{s}");
        }
        assert!("week".parse::<Chunk>().is_err());
        for c in [Chunk::Hour, Chunk::Day, Chunk::Month] {
            assert_eq!(c.to_string().parse::<Chunk>().unwrap(), c);
        }
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("90d").unwrap().as_secs(), 90 * 86_400);
        assert_eq!(parse_duration("1h").unwrap().as_secs(), 3600);
        assert_eq!(parse_duration(" 0s ").unwrap(), Duration::ZERO);
        assert!(parse_duration("1 parsec").is_err());
        assert!(parse_duration("d").is_err());
        assert!(parse_duration("-1d").is_err());
    }

    #[test]
    fn durations_accept_singular_and_plural_units() {
        assert_eq!(parse_duration("1 day").unwrap().as_secs(), 86_400);
        assert_eq!(parse_duration("1 hour").unwrap().as_secs(), 3600);
        assert_eq!(parse_duration("1 week").unwrap().as_secs(), 7 * 86_400);
        assert_eq!(parse_duration("2 Weeks").unwrap().as_secs(), 14 * 86_400);
        assert_eq!(parse_duration("1 minute").unwrap().as_secs(), 60);
        assert_eq!(parse_duration("1 year").unwrap().as_secs(), 365 * 86_400);
    }

    #[test]
    fn duration_overflow_is_an_error() {
        assert!(parse_duration("99999999999999999y").is_err());
        assert!(parse_duration("99999999999999999999s").is_err());
    }

    #[test]
    fn durations_format_in_the_largest_exact_unit() {
        for (secs, s) in [(90 * 86_400, "90d"), (36 * 3600, "36h"), (90, "90s"), (120, "2m"), (0, "0s")] {
            assert_eq!(format_duration(Duration::from_secs(secs)), s);
            assert_eq!(parse_duration(s).unwrap().as_secs(), secs);
        }
    }

    fn props(kv: &[(&str, &str)]) -> HashMap<String, String> {
        kv.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
    }

    #[test]
    fn contract_defaults() {
        let c = Contract::from_properties(&HashMap::new()).unwrap();
        assert!(!c.is_time_series() && !c.dedup && c.auto_maintain);
        assert_eq!(c.retention, None);
        assert_eq!(c.snapshot_retention, Duration::from_secs(3600));
        assert_eq!((c.min_snapshots, c.max_snapshots, c.compact_min_files), (5, 100, 8));
        assert_eq!(
            (c.head_format.as_deref(), c.head_inline_bytes, c.format_version),
            (Some("arrow"), 1 << 20, FormatVersion::UNVERSIONED)
        );
        assert_eq!((c.compact_cold_after, c.read_dedup), (Some(Duration::from_hours(1)), false));
        let dedup = Contract::from_properties(&props(&[(property::DEDUP, "true")])).unwrap();
        assert!(dedup.read_dedup, "read dedup follows dedup");
        let off =
            props(&[(property::DEDUP, "true"), (property::READ_DEDUP, "false"), (property::COMPACT_COLD_AFTER, "off")]);
        let off = Contract::from_properties(&off).unwrap();
        assert_eq!((off.compact_cold_after, off.read_dedup), (None, false));
        let cold = Contract::from_properties(&props(&[(property::COMPACT_COLD_AFTER, "6h")])).unwrap();
        assert_eq!(cold.compact_cold_after, Some(Duration::from_hours(6)));
        assert_eq!(c.sort_columns(), [] as [String; 0]);
    }

    #[test]
    fn contract_parses_properties() {
        let c = Contract::from_properties(&props(&[
            (TIME_COLUMN, "time"),
            (SERIES_KEY, "site, device,"),
            (CHUNK, "hour"),
            (DEDUP, "True"),
            (AUTO_MAINTAIN, "FALSE"),
            (RETENTION, "1 day"),
            (MIN_SNAPSHOTS, "3"),
            (TARGET_FILE_SIZE, " 1024 "),
            (HEAD_FORMAT, "JSON"),
            (HEAD_INLINE_BYTES, "0"),
        ]))
        .unwrap();
        assert_eq!((c.head_format.as_deref(), c.head_inline_bytes), (Some("json"), 0));
        assert_eq!(c.sort_columns(), ["site", "device", "time"]);
        assert_eq!(c.chunk, Some(Chunk::Hour));
        assert!(c.dedup && !c.auto_maintain);
        assert_eq!(c.retention, Some(Duration::from_hours(24)));
        assert_eq!((c.min_snapshots, c.target_file_size), (3, 1024));
    }

    #[test]
    fn contract_rejects_bad_values() {
        for kv in [
            (DEDUP, "yes"),
            (AUTO_MAINTAIN, "0"),
            (CHUNK, "week"),
            (RETENTION, "forever"),
            (MAX_SNAPSHOTS, "-1"),
            (ROW_GROUP_ROWS, "0"),
            (TARGET_FILE_SIZE, "0"),
            (DATA_PAGE_ROWS, "0"),
            (READ_DEDUP_STRATEGY, "hash"),
            (FILTER_PUSHDOWN, "sometimes"),
            (SERIES_ORDER, "sorted"),
            (WINDOW_SERIES_ORDER, "yes"),
            (FLOAT_ENCODING, "plain"),
            (COMPACT_COLD_AFTER, "never"),
        ] {
            assert!(Contract::from_properties(&props(&[kv])).is_err(), "{kv:?}");
        }
        let err = Contract::from_properties(&props(&[(SERIES_ORDER, "sorted")])).unwrap_err().to_string();
        assert_eq!(err, "timelake.series-order must be auto, segments, or merge, got \"sorted\"");
    }

    #[test]
    fn contract_parses_choices_in_any_case() {
        let c = Contract::from_properties(&props(&[
            (READ_DEDUP_STRATEGY, "Keys_First"),
            (FILTER_PUSHDOWN, "TRUE"),
            (SERIES_ORDER, " segments "),
            (WINDOW_SERIES_ORDER, "false"),
            (FLOAT_ENCODING, "Byte-Stream-Split"),
            (HEAD_FORMAT, "None"),
            (ROLLUP_REFRESH_INTERVAL, "OFF"),
        ]))
        .unwrap();
        assert_eq!(c.read_dedup_strategy, ReadDedupStrategy::KeysFirst);
        assert_eq!((c.filter_pushdown, c.window_series_order), (AutoBool::True, AutoBool::False));
        assert_eq!((c.series_order, c.float_encoding), (SeriesOrder::Segments, FloatEncoding::ByteStreamSplit));
        assert_eq!((c.head_format, c.rollup_refresh_interval), (None, None));
    }

    /// Iceberg's compression properties are checked when a file is written, so a codec Timelake
    /// lacks doesn't stop reads.
    #[test]
    fn bad_compression_opens_and_fails_only_writes() {
        let c = Contract::from_properties(&props(&[(COMPRESSION_CODEC, "lzo")])).unwrap();
        assert!(c.compression.is_err());
        let c = Contract::from_properties(&props(&[(COMPRESSION_CODEC, "GZIP"), (COMPRESSION_LEVEL, "9")])).unwrap();
        assert_eq!(c.compression.map(|c| (c.codec(), c.level())), Ok((crate::value::CompressionCodec::Gzip, Some(9))));
    }
}
