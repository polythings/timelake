//! Materialized rollups (spec 27): tables of aggregates per time bucket that Timelake keeps up
//! to date from a raw table, and that the Timelake reader answers matching queries from.
//!
//! A rollup is an ordinary Timelake table (time column `bucket`, series key the grouping columns,
//! dedup on) whose properties hold its definition. Each of its snapshots records the raw table's
//! snapshot it reflects. A refresh recomputes the buckets covered by raw data files committed since
//! then and appends them; dedup on read keeps the newest copy of each bucket.

use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use timelake_format::duration_micros;

use crate::TableName;
use crate::alter::update_properties;
use crate::append::AppendOptions;
use crate::commit::{Change, commit};
use crate::error::{Error, Result};
use crate::lake::Lake;
use crate::storage::Uploads;
use crate::table::{LiveEntry, Table};
use crate::{format_duration, parse_duration, property, summary};

/// Rows a refresh appends per commit, at most about.
const ROWS_PER_COMMIT: usize = 1 << 20;
/// A refresh recomputes at least this much time per query (a window of whole buckets).
const MIN_WINDOW_MICROS: i64 = crate::query::time::DAY_MICROS;

/// Half-open ranges of time, in microseconds since the epoch.
pub(crate) type Ranges = Vec<Range<i64>>;

pub(crate) use timelake_format::rollup::{Aggregate, RollupFunction};

/// What a rollup computes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Definition {
    /// The raw table.
    pub source: TableName,
    /// Bucket width in microseconds (whole seconds).
    pub width: i64,
    /// The rollup's bucket column.
    pub bucket: String,
    /// The grouping columns: series-key columns of the raw table, with the same names.
    pub keys: Vec<String>,
    pub aggregates: Vec<Aggregate>,
}

impl Definition {
    /// The definition of `rollup`, from its properties.
    pub(crate) fn of(rollup: &Table) -> Result<Self> {
        let props = rollup.metadata().properties();
        let get =
            |key: &str| props.get(key).ok_or_else(|| Error::invalid(format!("{} is not a rollup", rollup.name())));
        let source: TableName = get(property::ROLLUP_SOURCE)?.parse()?;
        let width = duration_micros(parse_duration(get(property::ROLLUP_BUCKET)?)?);
        let aggregates = timelake_format::rollup::parse_aggregates(get(property::ROLLUP_AGGREGATES)?)?;
        let contract = rollup.contract();
        let bucket = contract.time_column.clone().ok_or_else(|| Error::invalid("a rollup needs a bucket column"))?;
        Ok(Self { source, width, bucket, keys: contract.series_key.clone(), aggregates })
    }

    /// The properties that hold the definition (managed by Timelake).
    pub(crate) fn properties(&self) -> Result<HashMap<String, String>> {
        let width = Duration::from_micros(u64::try_from(self.width).unwrap_or_default());
        Ok(HashMap::from([
            (property::ROLLUP_SOURCE.into(), self.source.to_string()),
            (property::ROLLUP_BUCKET.into(), format_duration(width)),
            (property::ROLLUP_AGGREGATES.into(), timelake_format::rollup::aggregates_json(&self.aggregates)?),
        ]))
    }
}

/// What of its raw table a refreshed rollup reflects, as its snapshot summary records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reflects {
    /// Nothing: the raw table went back past what the rollup held (a rollback), so it is
    /// computed again from every raw file.
    Nothing,
    /// The raw table's snapshot with this id and sequence number.
    Snapshot { id: i64, sequence: i64 },
}

impl Reflects {
    /// How the summary records [`Reflects::Nothing`]: what every Timelake version reads as a
    /// snapshot older than any raw file.
    const NOTHING: (&str, &str) = ("none", "-1");

    /// What `rollup` reflects; `None` if it was never refreshed.
    pub(crate) fn of(rollup: &Table) -> Option<Self> {
        let sequence: i64 = rollup.summary_value(summary::ROLLUP_SOURCE_SEQUENCE)?.parse().ok()?;
        let id = rollup.summary_value(summary::ROLLUP_SOURCE_SNAPSHOT).and_then(|s| s.parse().ok());
        match id {
            Some(id) if sequence >= 0 => Some(Self::Snapshot { id, sequence }),
            _ => Some(Self::Nothing),
        }
    }

    /// The raw table's data files committed after this sequence number are new to the rollup;
    /// `None` when all are.
    pub(crate) fn sequence(self) -> Option<i64> {
        match self {
            Self::Nothing => None,
            Self::Snapshot { sequence, .. } => Some(sequence),
        }
    }

    /// The snapshot summary properties that record this.
    fn record(self) -> HashMap<String, String> {
        let (id, sequence) = match self {
            Self::Nothing => (Self::NOTHING.0.to_string(), Self::NOTHING.1.to_string()),
            Self::Snapshot { id, sequence } => (id.to_string(), sequence.to_string()),
        };
        HashMap::from([
            (summary::ROLLUP_SOURCE_SNAPSHOT.to_string(), id),
            (summary::ROLLUP_SOURCE_SEQUENCE.to_string(), sequence),
        ])
    }
}

/// Whether the raw table snapshot `rollup` reflects is in the history of `raw`'s current snapshot.
/// `false` after a rollback past it: the rollup may hold rows the raw table no longer has. When
/// expiry has removed the snapshots in between, it can't tell and says `true`.
pub(crate) fn in_history(raw: &Table, rollup: &Table) -> bool {
    let Some(Reflects::Snapshot { id, sequence }) = Reflects::of(rollup) else { return true };
    let metadata = raw.metadata();
    let mut snapshot = metadata.current_snapshot();
    while let Some(s) = snapshot {
        if s.snapshot_id() == id {
            return true;
        }
        if s.sequence_number() <= sequence {
            return false;
        }
        let Some(parent) = s.parent_snapshot_id() else { return false };
        snapshot = metadata.snapshot_by_id(parent);
    }
    true
}

/// The start of the bucket holding `t`.
pub(crate) fn floor(t: i64, width: i64) -> i64 {
    t.div_euclid(width).saturating_mul(width)
}

/// The buckets holding rows of the raw table's data files committed after sequence number
/// `after` (all files for `None`), as merged bucket-aligned ranges. `None` if such a file has no
/// time bounds, so any bucket may have changed.
pub(crate) fn changed(raw: &Table, files: &[LiveEntry], after: Option<i64>, width: i64) -> Option<Ranges> {
    let id = raw.time_field_id()?;
    let mut ranges = vec![];
    for f in files.iter().filter(|f| after.is_none_or(|s| f.sequence_number > s)) {
        let (Some(lo), Some(hi)) = f.time_bounds(id) else { return None };
        ranges.push(floor(lo, width)..floor(hi, width).saturating_add(width));
    }
    Some(merge(ranges))
}

/// Sorts ranges and merges those that overlap or touch.
pub(crate) fn merge(mut ranges: Ranges) -> Ranges {
    ranges.sort_unstable_by_key(|r| (r.start, r.end));
    let mut out: Ranges = vec![];
    for r in ranges {
        match out.last_mut() {
            Some(last) if r.start <= last.end => last.end = last.end.max(r.end),
            _ => out.push(r),
        }
    }
    out
}

/// `ranges` cut into windows of whole buckets, at least a day long where the range is.
fn windows(ranges: &[Range<i64>], width: i64) -> Ranges {
    let window = (MIN_WINDOW_MICROS.saturating_add(width - 1) / width).max(1).saturating_mul(width);
    let mut out = vec![];
    for r in ranges {
        let mut start = r.start;
        while start < r.end {
            let end = start.saturating_add(window).min(r.end);
            out.push(start..end);
            start = end;
        }
    }
    out
}

pub use timelake_format::maintain::RollupRefresh;

/// What refreshing a table's rollups did.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Refreshed {
    /// Rollups that took new rows.
    pub rollups: usize,
    /// Rows appended to them.
    pub rows: u64,
}

/// Refreshes the rollups of `raw` (`mode` picks which). A rollup that no longer exists is skipped.
pub(crate) async fn refresh_all(raw: &Table, mode: RollupRefresh) -> Result<Refreshed> {
    let mut done = Refreshed::default();
    if mode == RollupRefresh::Never {
        return Ok(done);
    }
    // Narrowest buckets first: a wider rollup is then computed from an up-to-date narrower one.
    let names = raw.contract().rollups.iter().filter_map(|n| n.parse::<TableName>().ok());
    let widths = names.map(|name| async move {
        let width = match raw.lake().table(&name).await {
            Ok(t) => Definition::of(&t).map_or(i64::MAX, |d| d.width),
            Err(_) => i64::MAX,
        };
        (width, name)
    });
    let mut rollups = futures::future::join_all(widths).await;
    rollups.sort();
    for (_, name) in rollups {
        if mode == RollupRefresh::Due && !due(raw, &name) {
            continue;
        }
        match refresh(raw.lake(), &name, Some(raw), mode).await {
            Ok(rows) if rows > 0 => {
                done.rollups += 1;
                done.rows += rows;
            }
            Ok(_) | Err(Error::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(done)
}

/// Whether a rollup may need a refresh, judged from the version this process last saw (no
/// requests); `true` when it saw none.
fn due(raw: &Table, name: &TableName) -> bool {
    let Some((_, rollup)) = raw.lake().caches().tables.get(name) else { return true };
    let current = raw.current_snapshot().map(|s| s.sequence_number());
    if Reflects::of(&rollup).and_then(Reflects::sequence).is_some_and(|r| current.is_some_and(|c| r >= c)) {
        return false;
    }
    is_due(&rollup)
}

/// Whether `rollup`'s last commit is older than its refresh interval.
fn is_due(rollup: &Table) -> bool {
    let Some(interval) = rollup.contract().rollup_refresh_interval else { return false };
    let last = rollup.current_snapshot().map_or(0, |s| s.timestamp_ms());
    let age = chrono::Utc::now().timestamp_millis().saturating_sub(last);
    u128::try_from(age).unwrap_or(0) >= interval.as_millis()
}

/// Brings rollup `name` up to date with its raw table (`raw`, when the caller has it loaded):
/// recomputes the buckets the raw table's data files committed since the last refresh cover, and
/// appends them. Returns the rows appended.
pub(crate) fn refresh<'a>(
    lake: &'a Lake,
    name: &'a TableName,
    raw: Option<&'a Table>,
    mode: RollupRefresh,
) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + 'a>> {
    // Boxed: a refresh appends to the rollup, whose inline maintenance refreshes rollups.
    Box::pin(refresh_now(lake, name, raw, mode))
}

async fn refresh_now(lake: &Lake, name: &TableName, raw: Option<&Table>, mode: RollupRefresh) -> Result<u64> {
    let mut rollup = lake.load(name).await?;
    if mode == RollupRefresh::Due && !is_due(&rollup) {
        return Ok(0);
    }
    let def = Definition::of(&rollup)?;
    let raw = match raw {
        Some(t) if *t.name() == def.source => t.clone(),
        _ => lake.load(&def.source).await?,
    };
    let Some(snapshot) = raw.current_snapshot() else { return Ok(0) };
    let (snapshot_id, sequence) = (snapshot.snapshot_id(), snapshot.sequence_number());
    // Raw files committed after this sequence number are new to the rollup (all for `None`).
    let mut after = Reflects::of(&rollup).and_then(Reflects::sequence);
    if !in_history(&raw, &rollup) {
        // The raw table went back (a rollback): start over, recording that nothing is reflected
        // yet, so routing reads the raw table until the rollup is computed again.
        let removed = rollup.files().await?.to_vec();
        let change = Change::Delete { removed, lvi_cutoff: Some(i64::MAX) };
        rollup = commit(&rollup, change, Reflects::Nothing.record(), Uploads::default()).await?.0;
        after = None;
    } else if after.is_some_and(|r| r >= sequence) {
        return Ok(0);
    }
    let files = raw.files().await?;
    // No time bounds on a changed file: recompute everything, in one query.
    let windows: Vec<Option<Range<i64>>> = match changed(&raw, &files, after, def.width) {
        Some(ranges) => windows(&ranges, def.width).into_iter().map(Some).collect(),
        None => vec![None],
    };
    let record = Reflects::Snapshot { id: snapshot_id, sequence }.record();
    let session = crate::query::refresh_session(lake, raw.name(), name).await?;
    let (mut pending, mut rows): (Vec<RecordBatch>, u64) = (vec![], 0);
    for (i, window) in windows.iter().enumerate() {
        let ranges = window.clone().map(|w| vec![w]);
        let frame = crate::query::rollup_frame(&session, &raw, &def, ranges.as_deref())?;
        if i == 0 {
            same_states(&rollup, frame.schema().as_arrow())?;
        }
        let batches = frame.collect().await?;
        pending.extend(batches.into_iter().filter(|b| b.num_rows() > 0));
        let last = i + 1 == windows.len();
        let pending_rows: usize = pending.iter().map(RecordBatch::num_rows).sum();
        if pending_rows == 0 || !(last || pending_rows >= ROWS_PER_COMMIT) {
            continue;
        }
        // Only the last commit records the raw snapshot: until then, the rollup's record still
        // names the older snapshot, so the buckets written so far count as changed.
        let properties = if last { record.clone() } else { HashMap::new() };
        let (next, result) =
            rollup.append(&std::mem::take(&mut pending), AppendOptions { properties, ..Default::default() }).await?;
        rollup = next;
        rows += u64::try_from(result.rows).unwrap_or(u64::MAX);
    }
    Ok(rows)
}

/// Checks that `rollup` stores its states (`m4`, the signal aggregates) as `computed` gives them: a
/// state's layout can change between versions, and a rollup with an older one can't take new rows.
fn same_states(rollup: &Table, computed: &arrow::datatypes::Schema) -> Result<()> {
    let stored = rollup.schema();
    for field in computed.fields().iter().filter(|f| matches!(f.data_type(), DataType::List(_))) {
        if stored.field_with_name(field.name()).is_ok_and(|s| layout(s.data_type()) != layout(field.data_type())) {
            return Err(Error::invalid(format!(
                "rollup {} stores {} in an older layout: drop the rollup and create it again",
                rollup.name(),
                field.name()
            )));
        }
    }
    Ok(())
}

/// The struct field names in a state's type, nested: what tells its layouts apart. (Whole types
/// differ after Iceberg's round trip, which renames list items and changes nullability.)
pub(crate) fn layout(t: &DataType) -> Vec<&str> {
    match t {
        DataType::List(item) | DataType::LargeList(item) => layout(item.data_type()),
        DataType::Struct(fields) => {
            fields.iter().flat_map(|f| [vec![f.name().as_str()], layout(f.data_type())]).flatten().collect()
        }
        _ => vec![],
    }
}

/// Creates rollup `name` of `raw` with `def` (`opts` for the rollup table), computes it, and adds
/// it to the raw table's rollups. Returns the rows written.
pub(crate) async fn create(
    raw: &Table,
    name: &TableName,
    def: &Definition,
    schema: &arrow::datatypes::Schema,
    opts: crate::TableOptions,
) -> Result<u64> {
    let lake = raw.lake();
    lake.create_table_with(name, schema, opts, def.properties()?).await?;
    // Compute before registering, so appends to the raw table meanwhile don't start a second full
    // refresh; what they add counts as changed until the next refresh.
    let rows = refresh(lake, name, Some(raw), RollupRefresh::Always).await?;
    let raw = lake.load(raw.name()).await?;
    let name = name.to_string();
    update_properties(&raw, |raw| {
        let mut names = raw.contract().rollups.clone();
        if !names.contains(&name) {
            names.push(name.clone());
        }
        (vec![(property::ROLLUPS.into(), names.join(","))], vec![])
    })
    .await?;
    Ok(rows)
}

/// Drops rollup `name`: takes it out of its raw table's rollups, then deletes it.
pub(crate) async fn drop(lake: &Lake, name: &TableName) -> Result<()> {
    let rollup = lake.load(name).await?;
    let def = Definition::of(&rollup)?;
    match lake.load(&def.source).await {
        Ok(raw) if raw.contract().rollups.contains(&name.to_string()) => {
            let name = name.to_string();
            update_properties(&raw, |raw| {
                let names: Vec<&str> =
                    raw.contract().rollups.iter().map(String::as_str).filter(|n| *n != name).collect();
                if names.is_empty() {
                    (vec![], vec![property::ROLLUPS.to_string()])
                } else {
                    (vec![(property::ROLLUPS.to_string(), names.join(","))], vec![])
                }
            })
            .await?;
        }
        Ok(_) | Err(Error::NotFound(_)) => {}
        Err(e) => return Err(e),
    }
    lake.drop_table(name).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_merge_and_split_into_windows() {
        assert_eq!(merge(vec![5..7, 0..2, 2..3, 6..9]), vec![0..3, 5..9]);
        let day = MIN_WINDOW_MICROS;
        let hour = day / 24;
        assert_eq!(
            windows(std::slice::from_ref(&(0..2 * day + hour)), hour),
            vec![0..day, day..2 * day, 2 * day..2 * day + hour]
        );
        // A bucket wider than a day is one window.
        assert_eq!(windows(std::slice::from_ref(&(0..4 * day)), 2 * day), vec![0..2 * day, 2 * day..4 * day]);
        assert_eq!(floor(-1, 60), -60);
    }

    /// A state's layout is its field names: Iceberg's round trip (list item names, nullability,
    /// field ids) doesn't change it, a field more or less does.
    #[test]
    fn state_layouts() {
        use std::collections::HashMap;
        use std::sync::Arc;

        use arrow::datatypes::Field;
        let list = |item: &str, names: &[&str], nullable: bool| {
            let fields: Vec<Field> = names
                .iter()
                .map(|n| {
                    Field::new(*n, DataType::Float64, nullable)
                        .with_metadata(HashMap::from([("id".into(), "1".into())]))
                })
                .collect();
            DataType::List(Arc::new(Field::new(item, DataType::Struct(fields.into()), true)))
        };
        let names = ["first_time", "area", "growth"];
        assert_eq!(layout(&list("item", &names, false)), layout(&list("element", &names, true)));
        assert_ne!(layout(&list("item", &names, false)), layout(&list("item", &names[..2], false)));
    }
}
