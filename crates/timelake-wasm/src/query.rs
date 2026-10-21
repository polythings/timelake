//! A query's result, collected file by file.

use arrow_array::RecordBatch;

use crate::aggregate::Aggregator;
use crate::error::{Error, Result};
use crate::file::FileReader;
use crate::filter::filter;
use crate::latest::LatestRows;
use crate::output::{first_rows, rows_json, top_rows};
use crate::spec::QuerySpec;

/// Collects a query's result across files: rows, time-bucket aggregates (`spec.bucket`), or the
/// newest row per series (`spec.latest_by`).
#[derive(Debug)]
pub struct Collector {
    spec: QuerySpec,
    state: State,
}

#[derive(Debug)]
enum State {
    Rows(Rows),
    Buckets(Aggregator),
    Latest(LatestRows),
}

/// The rows of a query so far. With a limit, only rows that can still be in the result are kept: the first `limit`
/// when rows aren't ordered, the best `limit` by time when they are, so memory stays near `limit` rows (Workers have
/// 128 MB).
#[derive(Debug, Default)]
struct Rows {
    batches: Vec<RecordBatch>,
    rows: usize,
}

impl State {
    fn add(&mut self, batch: RecordBatch, spec: &QuerySpec) -> Result<()> {
        match self {
            Self::Rows(rows) => rows.push(batch, spec),
            Self::Buckets(agg) => agg.push(&batch),
            Self::Latest(latest) => latest.push(&batch),
        }
    }
}

impl Rows {
    fn push(&mut self, batch: RecordBatch, spec: &QuerySpec) -> Result<()> {
        if batch.num_rows() == 0 || self.is_full(spec) {
            return Ok(());
        }
        self.rows += batch.num_rows();
        self.batches.push(batch);
        let Some(limit) = spec.limit else { return Ok(()) };
        if spec.time_column.is_none() {
            self.batches = first_rows(std::mem::take(&mut self.batches), Some(limit));
            self.rows = self.rows.min(limit);
        } else if self.rows >= limit.saturating_mul(2).max(1) {
            // Twice the limit before trimming, so the sort's cost is spread over many rows.
            self.batches = top_rows(std::mem::take(&mut self.batches), spec)?;
            self.rows = self.batches.iter().map(RecordBatch::num_rows).sum();
        }
        Ok(())
    }

    /// Whether no later row can be in the result: the first `limit` rows are in, and rows aren't ordered by time.
    fn is_full(&self, spec: &QuerySpec) -> bool {
        spec.time_column.is_none() && spec.limit.is_some_and(|limit| self.rows >= limit)
    }
}

impl Collector {
    pub fn new(spec: QuerySpec) -> Result<Self> {
        let time_column =
            || spec.time_column.clone().ok_or_else(|| Error::invalid("bucket and latest queries need a time column"));
        let state = match (&spec.bucket, &spec.latest_by) {
            (Some(_), Some(_)) => return Err(Error::invalid("a query can't both bucket and pick the latest rows")),
            (Some(bucket), None) => State::Buckets(Aggregator::new(bucket.clone(), time_column()?)?),
            (None, Some(key)) => State::Latest(LatestRows::new(time_column()?, key.clone())),
            (None, None) => State::Rows(Rows::default()),
        };
        Ok(Self { spec, state })
    }

    pub fn spec(&self) -> &QuerySpec {
        &self.spec
    }

    /// Decodes the planned ranges of `file` and adds its matching rows, a batch at a time.
    pub fn push(&mut self, file: &FileReader) -> Result<()> {
        let Self { spec, state } = self;
        for batch in file.batches(spec)? {
            if let State::Rows(rows) = state
                && rows.is_full(spec)
            {
                break;
            }
            state.add(batch?, spec)?;
        }
        Ok(())
    }

    /// Adds the matching rows of a batch already in memory (the last-value index from a head).
    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.state.add(filter(batch, &self.spec)?, &self.spec)
    }

    /// The result as a JSON array of objects. Rows are handed over, so a second call has none.
    pub fn finish(&mut self) -> Result<String> {
        match &mut self.state {
            State::Rows(rows) => rows_json(std::mem::take(&mut rows.batches), &self.spec),
            State::Buckets(agg) => agg.finish(),
            State::Latest(latest) => rows_json(latest.rows().to_vec(), &self.spec),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Float64Array, TimestampMicrosecondArray};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use timelake_format::rows::UTC;

    use super::*;
    use crate::counting::thread::peak_of;

    /// Rows of `(time, value)`, with times in UTC microseconds.
    fn batch(rows: impl IntoIterator<Item = (i64, f64)>) -> RecordBatch {
        let (times, values): (Vec<i64>, Vec<f64>) = rows.into_iter().unzip();
        RecordBatch::try_from_iter([
            ("time", Arc::new(TimestampMicrosecondArray::from(times).with_timezone(UTC)) as ArrayRef),
            ("value", Arc::new(Float64Array::from(values))),
        ])
        .unwrap()
    }

    fn spec(time_column: bool, limit: Option<usize>, descending: bool) -> QuerySpec {
        QuerySpec { time_column: time_column.then(|| "time".into()), limit, descending, ..QuerySpec::default() }
    }

    /// Kept to the best `limit` rows as they arrive, the result is the one of sorting every row at the end: in time
    /// order, or the first rows as they came when there is no time column.
    #[test]
    fn a_limit_keeps_the_rows_sorting_everything_keeps() {
        // 40 batches of 50 rows, distinct times in a scrambled order.
        let batches: Vec<RecordBatch> = (0..40_i64)
            .map(|b| batch((0..50_i64).map(|r| ((b * 50 + r) * 7_919 % 2_000, (b * 50 + r) as f64))))
            .collect();
        for limit in [None, Some(0), Some(1), Some(7), Some(120), Some(1_999), Some(5_000)] {
            for (time_column, descending) in [(true, false), (true, true), (false, false)] {
                let spec = spec(time_column, limit, descending);
                let mut collector = Collector::new(spec.clone()).unwrap();
                for b in &batches {
                    collector.push_batch(b).unwrap();
                }
                let expected = rows_json(batches.clone(), &spec).unwrap();
                assert_eq!(collector.finish().unwrap(), expected, "{limit:?} {time_column} {descending}");
            }
        }
    }

    /// One Parquet file of `rows` rows, a row group per 64 Ki rows, opened with every byte fetched.
    fn parquet_file(rows: i64) -> FileReader {
        let mut bytes = vec![];
        let props = WriterProperties::builder().set_max_row_group_row_count(Some(1 << 16)).build();
        let first = batch([(0, 0.0)]);
        let mut writer = ArrowWriter::try_new(&mut bytes, first.schema(), Some(props)).unwrap();
        for start in (0..rows).step_by(1 << 16) {
            writer.write(&batch((start..rows.min(start + (1 << 16))).map(|t| (t, t as f64)))).unwrap();
        }
        writer.close().unwrap();
        let mut file = FileReader::new(bytes.len() as u64, &bytes).unwrap();
        file.plan(&spec(true, None, false));
        file.add_range(0, bytes);
        file
    }

    /// A query with a limit holds about one decoded batch at a time, not the file: 1 Mi rows decode to 16 MiB.
    #[test]
    fn a_limit_keeps_memory_near_the_limit() {
        let file = parquet_file(1 << 20);
        let run = |spec: QuerySpec| {
            peak_of(|| {
                let mut collector = Collector::new(spec).unwrap();
                collector.push(&file).unwrap();
                collector.finish().unwrap().len()
            })
        };
        let (_, newest) = run(spec(true, Some(10), true));
        let (_, first) = run(spec(false, Some(10), false));
        let decoded = (1 << 20) * 16;
        assert!(newest < decoded / 2, "the newest 10 rows took {newest} bytes at most");
        assert!(first < decoded / 2, "the first 10 rows took {first} bytes at most");
    }
}
