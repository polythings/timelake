//! The newest row per series by scanning, for tables without a last-value index.

use std::collections::HashMap;

use arrow_array::{Array, RecordBatch};
use arrow_select::interleave::interleave_record_batch;

use crate::error::{Error, Result};
use crate::filter::micros;
use crate::keys::KeyEncoder;
use crate::output::unify;

/// Keeps the newest row per series key across batches. Only the current winners are kept, so
/// memory grows with the number of series, not rows. On equal times the later row wins, like the
/// engine's last-value index.
#[derive(Debug)]
pub(crate) struct LatestRows {
    time_column: String,
    keys: KeyEncoder,
    /// Encoded series key → its row in `winners` and that row's time.
    slots: HashMap<Box<[u8]>, (usize, i64)>,
    winners: Option<RecordBatch>,
}

impl LatestRows {
    pub(crate) fn new(time_column: String, series_key: Vec<String>) -> Self {
        Self { time_column, keys: KeyEncoder::new(series_key), slots: HashMap::new(), winners: None }
    }

    pub(crate) fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        let time = batch
            .column_by_name(&self.time_column)
            .ok_or_else(|| Error::invalid(format!("unknown time column {}", self.time_column)))?;
        let time = micros(time)?;
        let keys = self.keys.encode(batch)?;
        let winners = self.winners.take().unwrap_or_else(|| batch.slice(0, 0));
        // Files may differ in columns or types: the winners and the batch must share a schema.
        let (_, both) = unify(vec![winners, batch.clone()])?;
        let [winners, batch] = <[RecordBatch; 2]>::try_from(both).map_err(|_| Error::invalid("two batches"))?;
        let batch = &batch;
        // Rows of the new winners batch, as (0 = old winners | 1 = this batch, row).
        let mut picks: Vec<(usize, usize)> = (0..winners.num_rows()).map(|row| (0, row)).collect();
        for row in (0..batch.num_rows()).filter(|&r| time.is_valid(r)) {
            let t = time.value(row);
            let key = keys.as_ref().map_or(&[][..], |k| k.row(row).data());
            if let Some((slot, newest)) = self.slots.get_mut(key) {
                if t >= *newest {
                    *newest = t;
                    picks[*slot] = (1, row);
                }
            } else {
                self.slots.insert(key.into(), (picks.len(), t));
                picks.push((1, row));
            }
        }
        self.winners = Some(interleave_record_batch(&[&winners, batch], &picks)?);
        Ok(())
    }

    /// The newest row of each series (no batch before the first push).
    pub(crate) fn rows(&self) -> &[RecordBatch] {
        self.winners.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, Int64Array};

    use super::*;

    fn batch(rows: &[(i64, i32, i64)]) -> RecordBatch {
        RecordBatch::try_from_iter([
            ("time", Arc::new(rows.iter().map(|r| r.0).collect::<Int64Array>()) as ArrayRef),
            ("tag", Arc::new(rows.iter().map(|r| r.1).collect::<Int32Array>())),
            ("v", Arc::new(rows.iter().map(|r| r.2).collect::<Int64Array>())),
        ])
        .unwrap()
    }

    fn values(latest: &LatestRows) -> Vec<(i32, i64)> {
        let rows = &latest.rows()[0];
        let tags = rows.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let v = rows.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let mut out: Vec<_> = tags.values().iter().copied().zip(v.values().iter().copied()).collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn keeps_the_newest_row_per_series() {
        let mut latest = LatestRows::new("time".into(), vec!["tag".into()]);
        latest.push(&batch(&[(1, 1, 10), (5, 2, 20), (3, 1, 11)])).unwrap();
        // Older rows don't replace newer ones; an equal time replaces (later write wins).
        latest.push(&batch(&[(2, 1, 12), (5, 2, 21), (9, 3, 30)])).unwrap();
        assert_eq!(values(&latest), [(1, 11), (2, 21), (3, 30)]);
    }

    /// Files with different columns: the winners take every column, null where a file lacks it.
    #[test]
    fn batches_with_different_columns() {
        let mut latest = LatestRows::new("time".into(), vec!["tag".into()]);
        let old = RecordBatch::try_from_iter([
            ("time", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("tag", Arc::new(Int32Array::from(vec![1]))),
        ])
        .unwrap();
        latest.push(&old).unwrap();
        latest.push(&batch(&[(5, 2, 20)])).unwrap();
        let rows = &latest.rows()[0];
        assert_eq!(rows.num_columns(), 3);
        assert_eq!(rows.column(2).null_count(), 1, "the old file has no v");
    }

    #[test]
    fn without_a_series_key_keeps_one_row() {
        let mut latest = LatestRows::new("time".into(), vec![]);
        latest.push(&batch(&[(1, 1, 10), (7, 2, 20)])).unwrap();
        latest.push(&batch(&[(3, 3, 30)])).unwrap();
        assert_eq!(values(&latest), [(2, 20)]);
    }
}
