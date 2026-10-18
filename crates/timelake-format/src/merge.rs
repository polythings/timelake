//! A k-way merge of sorted inputs, with dedup, for compaction (SPEC §10.1). It does no I/O: it
//! says which input needs its next batch, and the caller (the engine, reading an object store, or
//! the WASM writer, reading through JavaScript) feeds batches as they come. Memory is one batch per
//! input plus the output batch being built, however large the inputs are.
//!
//! Each input must be sorted by the key columns, as Timelake writes its files. Inputs are given in
//! commit order: among rows with equal keys, the earlier input's rows come first. With dedup, only
//! the last row of each run of rows equal in the first `unique` key columns is kept, so with the
//! key `(series…, time, insertion time)` and `unique` covering `(series…, time)` the latest write
//! wins, exactly as sorting everything and keeping the last row would.
//!
//! Rows are compared in Arrow's row format. Where the current batch of one input sorts entirely
//! before the next row of every other input, the whole run is taken as one slice (found by binary
//! search), so merging files that cover different series or times touches few rows one by one.
//!
//! ```
//! use std::sync::Arc;
//! use arrow_array::{Int64Array, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//! use timelake_format::merge::{Merge, Step};
//!
//! let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Int64, false)]));
//! let batch = |v: Vec<i64>| RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(v))]).unwrap();
//! let mut merge = Merge::new(schema.clone(), &["t"], 1, 2, 1024).unwrap();
//! let mut inputs = vec![vec![batch(vec![1, 3])].into_iter(), vec![batch(vec![2, 3])].into_iter()];
//! let mut rows: Vec<i64> = vec![];
//! loop {
//!     match merge.step().unwrap() {
//!         Step::Need(i) => merge.push(i, inputs[i].next()).unwrap(),
//!         Step::Output(b) => rows.extend(b.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values()),
//!         Step::Done => break,
//!     }
//! }
//! assert_eq!(rows, [1, 2, 3]);
//! ```

use std::cmp::Ordering;

use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_ord::partition::partition;
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::{ArrowError, SchemaRef, SortOptions};
use arrow_select::interleave::interleave_record_batch;
use arrow_select::take::take_record_batch;

/// Keys sort ascending with nulls first, the order Timelake writes.
const ASCENDING: SortOptions = SortOptions { descending: false, nulls_first: true };

/// What the merge needs or has next.
#[derive(Debug)]
pub enum Step {
    /// Push the next batch of this input (or `None` when it has no more).
    Need(usize),
    /// Merged rows, in key order.
    Output(RecordBatch),
    /// Every input is exhausted and every row was output.
    Done,
}

/// One input: its current batch, the batch's keys in row format, and the next row to take.
#[derive(Debug, Default)]
struct Input {
    batch: Option<RecordBatch>,
    rows: Option<Rows>,
    next: usize,
    /// No more batches.
    done: bool,
    /// The batch's index among the merge's sources, once rows of it were taken.
    source: Option<usize>,
}

impl Input {
    fn len(&self) -> usize {
        self.batch.as_ref().map_or(0, RecordBatch::num_rows)
    }

    fn exhausted(&self) -> bool {
        self.next >= self.len()
    }
}

/// The merge's state (see the module documentation).
#[derive(Debug)]
pub struct Merge {
    schema: SchemaRef,
    /// Indices of the key columns in the schema.
    keys: Vec<usize>,
    /// Dedup on the first this many keys (0: no dedup).
    unique: usize,
    converter: RowConverter,
    inputs: Vec<Input>,
    /// The batches the next output batch takes rows from, and those rows, in order: (source, row).
    sources: Vec<RecordBatch>,
    taken: Vec<(usize, usize)>,
    batch_rows: usize,
    /// With dedup: the last row output so far is held back, as a later row may replace it.
    held: Option<RecordBatch>,
    /// Statistics.
    rows_in: u64,
    rows_out: u64,
    runs: u64,
}

impl Merge {
    /// A merge of `inputs` inputs with columns `schema`, sorted by the columns `keys`, keeping the
    /// last row of each run equal in the first `unique` keys (0 for no dedup), and outputting
    /// batches of about `batch_rows` rows.
    pub fn new(
        schema: SchemaRef,
        keys: &[&str],
        unique: usize,
        inputs: usize,
        batch_rows: usize,
    ) -> Result<Self, ArrowError> {
        let keys: Vec<usize> = keys.iter().map(|k| schema.index_of(k)).collect::<Result<_, _>>()?;
        if unique > keys.len() {
            return Err(ArrowError::InvalidArgumentError(format!("dedup on {unique} of {} keys", keys.len())));
        }
        let fields =
            keys.iter().map(|&i| SortField::new_with_options(schema.field(i).data_type().clone(), ASCENDING)).collect();
        Ok(Self {
            converter: RowConverter::new(fields)?,
            schema,
            keys,
            unique,
            inputs: (0..inputs).map(|_| Input::default()).collect(),
            sources: vec![],
            taken: vec![],
            batch_rows: batch_rows.max(1),
            held: None,
            rows_in: 0,
            rows_out: 0,
            runs: 0,
        })
    }

    /// Gives input `i` its next batch, or `None` when it has no more. The batch must have the
    /// merge's schema and be sorted by its keys.
    pub fn push(&mut self, i: usize, batch: Option<RecordBatch>) -> Result<(), ArrowError> {
        let input = &mut self.inputs[i];
        match batch {
            None => {
                input.done = true;
                input.batch = None;
                input.rows = None;
            }
            Some(batch) => {
                if batch.schema() != self.schema {
                    return Err(ArrowError::SchemaError(format!("input {i} has a different schema")));
                }
                self.rows_in += batch.num_rows() as u64;
                let keys: Vec<ArrayRef> = self.keys.iter().map(|&k| batch.column(k).clone()).collect();
                // Without keys, inputs follow one another in order.
                input.rows = if keys.is_empty() { None } else { Some(self.converter.convert_columns(&keys)?) };
                input.batch = Some(batch);
                input.next = 0;
                input.source = None;
            }
        }
        Ok(())
    }

    /// The next step: an input to feed, a merged batch, or the end.
    pub fn step(&mut self) -> Result<Step, ArrowError> {
        loop {
            if let Some(i) = self.inputs.iter().position(|s| s.exhausted() && !s.done) {
                return Ok(Step::Need(i));
            }
            if self.taken.len() >= self.batch_rows {
                return Ok(Step::Output(self.flush(false)?));
            }
            let Some(first) = self.smallest() else {
                if !self.taken.is_empty() || self.held.is_some() {
                    return Ok(Step::Output(self.flush(true)?));
                }
                return Ok(Step::Done);
            };
            self.take_run(first);
        }
    }

    /// Rows read, rows output, and runs taken so far.
    pub fn stats(&self) -> (u64, u64, u64) {
        (self.rows_in, self.rows_out, self.runs)
    }

    /// The input whose next row sorts first (the earlier input among equals).
    fn smallest(&self) -> Option<usize> {
        let mut best: Option<usize> = None;
        for (i, input) in self.inputs.iter().enumerate() {
            if input.exhausted() {
                continue;
            }
            if best.is_none_or(|b| self.compare(i, b) == Ordering::Less) {
                best = Some(i);
            }
        }
        best
    }

    /// Compares the next rows of inputs `a` and `b`, the earlier input first among equals.
    fn compare(&self, a: usize, b: usize) -> Ordering {
        let row = |i: usize| self.inputs[i].rows.as_ref().map(|r| r.row(self.inputs[i].next));
        row(a).cmp(&row(b)).then(a.cmp(&b))
    }

    /// Takes the rows of input `i` that sort before the next row of every other input.
    fn take_run(&mut self, i: usize) {
        let input = &self.inputs[i];
        if input.batch.is_none() {
            return;
        }
        let (start, len) = (input.next, input.len());
        // The next row of all the others, and whether input `i` comes first among equals.
        let bound = (0..self.inputs.len())
            .filter(|&j| j != i && !self.inputs[j].exhausted())
            .min_by(|&a, &b| self.compare(a, b))
            .and_then(|j| self.inputs[j].rows.as_ref().map(|r| (r.row(self.inputs[j].next), i < j)));
        let end = match (bound, &input.rows) {
            (None, _) | (_, None) => len,
            (Some((other, first_on_ties)), Some(rows)) => {
                // Rows `start..end` sort before `other` (or equal to it, when `i` goes first).
                let before = |r: usize| {
                    let ord = rows.row(r).cmp(&other);
                    ord == Ordering::Less || (ord == Ordering::Equal && first_on_ties)
                };
                // Galloping: runs are often a row or a few (inputs that alternate key by key), so
                // probe 1, 2, 4… rows ahead, then search between the last two probes. Rows before
                // `lo` are in the run; row `hi` (or the end) is not.
                let (mut lo, mut probe, mut step) = (start + 1, start + 1, 1);
                let mut hi = len;
                while probe < len {
                    if !before(probe) {
                        hi = probe;
                        break;
                    }
                    lo = probe + 1;
                    probe += step;
                    step *= 2;
                }
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    if before(mid) {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                lo
            }
        };
        let end = end.min(start + (self.batch_rows - self.taken.len().min(self.batch_rows)).max(1));
        let input = &mut self.inputs[i];
        let source = *input.source.get_or_insert_with(|| {
            self.sources.push(input.batch.clone().expect("checked above"));
            self.sources.len() - 1
        });
        self.taken.extend((start..end).map(|row| (source, row)));
        input.next = end;
        self.runs += 1;
    }

    /// The rows taken so far as one batch, deduplicated; with dedup, the last row is held back
    /// unless this is the end.
    fn flush(&mut self, end: bool) -> Result<RecordBatch, ArrowError> {
        let batch = self.take_rows()?;
        let batch = if self.unique == 0 || batch.num_rows() == 0 {
            batch
        } else {
            let mut kept = self.last_per_group(&batch)?;
            if !end && kept.num_rows() > 0 {
                let n = kept.num_rows();
                self.held = Some(kept.slice(n - 1, 1));
                kept = kept.slice(0, n - 1);
            }
            kept
        };
        self.rows_out += batch.num_rows() as u64;
        Ok(batch)
    }

    /// The held row, then the rows taken, as one batch: gathered in one pass, since with inputs
    /// that alternate key by key most runs are a single row. The sources are then released.
    fn take_rows(&mut self) -> Result<RecordBatch, ArrowError> {
        let mut sources = std::mem::take(&mut self.sources);
        let mut rows = std::mem::take(&mut self.taken);
        for input in &mut self.inputs {
            input.source = None;
        }
        if let Some(held) = self.held.take() {
            sources.push(held);
            rows.insert(0, (sources.len() - 1, 0));
        }
        match sources.as_slice() {
            [] => Ok(RecordBatch::new_empty(self.schema.clone())),
            // One source in order (a long run, or the end of one input): a slice, no copy.
            [only] if rows.windows(2).all(|w| w[1].1 == w[0].1 + 1) => Ok(only.slice(rows[0].1, rows.len())),
            _ => interleave_record_batch(&sources.iter().collect::<Vec<_>>(), &rows),
        }
    }

    /// The last row of each run of rows equal in the first `unique` keys.
    fn last_per_group(&self, batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        let columns: Vec<ArrayRef> = self.keys[..self.unique].iter().map(|&k| batch.column(k).clone()).collect();
        let groups = partition(&columns)?.ranges();
        if groups.len() == batch.num_rows() {
            return Ok(batch.clone());
        }
        let last: UInt32Array = groups.iter().map(|g| u32::try_from(g.end - 1).unwrap_or(u32::MAX)).collect();
        take_record_batch(batch, &last)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("t", DataType::Int64, false),
            Field::new("tag", DataType::Utf8, false),
        ]))
    }

    fn batch(rows: &[(&str, i64, &str)]) -> RecordBatch {
        let s: StringArray = rows.iter().map(|r| Some(r.0)).collect();
        let t: Int64Array = rows.iter().map(|r| r.1).collect();
        let tag: StringArray = rows.iter().map(|r| Some(r.2)).collect();
        RecordBatch::try_new(schema(), vec![Arc::new(s), Arc::new(t), Arc::new(tag)]).unwrap()
    }

    /// Runs a merge over inputs given as lists of batches; returns the output tags.
    fn run(inputs: Vec<Vec<RecordBatch>>, unique: usize, batch_rows: usize) -> Vec<String> {
        let mut merge = Merge::new(schema(), &["s", "t"], unique, inputs.len(), batch_rows).unwrap();
        let mut feeds: Vec<_> = inputs.into_iter().map(Vec::into_iter).collect();
        let mut out = vec![];
        loop {
            match merge.step().unwrap() {
                Step::Need(i) => merge.push(i, feeds[i].next()).unwrap(),
                Step::Output(b) => {
                    assert!(b.num_rows() <= batch_rows + 1);
                    let tags = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
                    out.extend(tags.iter().map(|t| t.unwrap().to_string()));
                }
                Step::Done => return out,
            }
        }
    }

    #[test]
    fn interleaves_inputs_in_key_order() {
        let a = vec![batch(&[("a", 1, "a1"), ("a", 3, "a3")]), batch(&[("b", 1, "b1")])];
        let b = vec![batch(&[("a", 2, "a2")]), batch(&[("b", 0, "b0"), ("c", 0, "c0")])];
        assert_eq!(run(vec![a, b], 0, 1024), ["a1", "a2", "a3", "b0", "b1", "c0"]);
    }

    #[test]
    fn later_inputs_win_on_equal_keys() {
        let old = vec![batch(&[("a", 1, "old"), ("a", 2, "old2")])];
        let new = vec![batch(&[("a", 1, "new")])];
        assert_eq!(run(vec![old.clone(), new.clone()], 2, 1024), ["new", "old2"]);
        // Without dedup both stay, older first.
        assert_eq!(run(vec![old, new], 0, 1024), ["old", "new", "old2"]);
    }

    #[test]
    fn dedup_spans_output_batches() {
        // Small output batches: a duplicate pair may straddle two of them.
        let a = vec![batch(&[("a", 1, "x"), ("a", 2, "x"), ("a", 3, "x")])];
        let b = vec![batch(&[("a", 1, "y"), ("a", 2, "y"), ("a", 3, "y")])];
        for rows in [1, 2, 3, 1024] {
            assert_eq!(run(vec![a.clone(), b.clone()], 2, rows), ["y", "y", "y"], "batch_rows {rows}");
        }
    }

    #[test]
    fn duplicates_within_one_input_keep_the_last() {
        let a = vec![batch(&[("a", 1, "first")]), batch(&[("a", 1, "second"), ("a", 2, "z")])];
        assert_eq!(run(vec![a], 2, 1024), ["second", "z"]);
    }

    #[test]
    fn disjoint_inputs_are_taken_as_runs() {
        let a = vec![batch(&[("a", 1, "1"), ("a", 2, "2"), ("a", 3, "3")])];
        let b = vec![batch(&[("b", 1, "4"), ("b", 2, "5")])];
        let mut merge = Merge::new(schema(), &["s", "t"], 2, 2, 1024).unwrap();
        let mut feeds = [a.into_iter(), b.into_iter()];
        loop {
            match merge.step().unwrap() {
                Step::Need(i) => merge.push(i, feeds[i].next()).unwrap(),
                Step::Output(_) => {}
                Step::Done => break,
            }
        }
        assert_eq!(merge.stats(), (5, 5, 2));
    }

    #[test]
    fn without_keys_inputs_follow_one_another() {
        let mut merge = Merge::new(schema(), &[], 0, 2, 1024).unwrap();
        let mut feeds = [vec![batch(&[("b", 1, "1")])].into_iter(), vec![batch(&[("a", 1, "2")])].into_iter()];
        let mut out = vec![];
        loop {
            match merge.step().unwrap() {
                Step::Need(i) => merge.push(i, feeds[i].next()).unwrap(),
                Step::Output(b) => out.push(b.num_rows()),
                Step::Done => break,
            }
        }
        assert_eq!(out.iter().sum::<usize>(), 2);
    }

    /// Random inputs give what sorting everything and keeping the last row of each key gives.
    #[test]
    fn matches_sort_then_keep_last() {
        let mut seed = 42u64;
        let mut rand = |n: usize| {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            usize::try_from(seed >> 33).unwrap() % n
        };
        let series = ["a", "b", "c", "d"];
        for case in 0..200 {
            let k = 1 + rand(5);
            let mut inputs = vec![];
            let mut all = vec![];
            for i in 0..k {
                let mut rows: Vec<(&str, i64, String)> = (0..rand(200))
                    .map(|r| (series[rand(4)], i64::try_from(rand(20)).unwrap(), format!("{i}.{r}")))
                    .collect();
                rows.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
                all.extend(rows.iter().cloned());
                let mut batches = vec![];
                let mut rest = rows.as_slice();
                while !rest.is_empty() {
                    let n = 1 + rand(rest.len());
                    let part: Vec<_> = rest[..n].iter().map(|r| (r.0, r.1, r.2.as_str())).collect();
                    batches.push(batch(&part));
                    rest = &rest[n..];
                }
                inputs.push(batches);
            }
            // Stable: input order, then row order, among equal keys.
            all.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
            let mut expected: Vec<(&str, i64, String)> = vec![];
            for row in all {
                if expected.last().is_some_and(|l| (l.0, l.1) == (row.0, row.1)) {
                    expected.pop();
                }
                expected.push(row);
            }
            let expected: Vec<String> = expected.into_iter().map(|r| r.2).collect();
            let batch_rows = 1 + rand(8);
            assert_eq!(run(inputs, 2, batch_rows), expected, "case {case}");
        }
    }

    #[test]
    fn empty_inputs() {
        assert_eq!(run(vec![vec![], vec![batch(&[])]], 2, 8), Vec::<String>::new());
        assert_eq!(run(vec![vec![], vec![batch(&[("a", 1, "x")])]], 2, 8), ["x"]);
    }
}
