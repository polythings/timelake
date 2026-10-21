//! Aggregates over each group's readings in time order, computed as runs: a run summarizes
//! readings that follow each other in time, so that runs that follow each other join exactly and
//! partial results from different partitions, and a rollup's buckets, merge into the result over
//! all readings. Runs that overlap in time don't join (their readings would have to be
//! interleaved): the result is an error, and scans under these aggregates read each series in time
//! order to avoid it ([`super::series_order`]).
//!
//! The signal aggregates ([`super::signal`]), the gap aggregates ([`super::gaps`]) and the
//! flatline aggregates ([`super::flatline`]) are run aggregates; each gives its run, how it reads
//! its input, and how it writes its state and result.

use std::fmt::Debug;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::common::{DFSchema, exec_err};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::ExprSchemable;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::prelude::Expr;
use timelake_format::rollup::FunctionFamily;

use super::groups::{Fold, GroupState};
use super::rollup::unalias;

/// A summary of readings in time order, from its first reading's time to its last's.
pub(super) trait Run: Copy + Debug + Send + Sync + 'static {
    /// One reading.
    type Reading: Copy;
    /// What the aggregate was given besides the readings, which joining needs (the signal's
    /// method, the gap threshold).
    type How: Copy + Debug + Send + Sync + 'static;

    fn new(reading: Self::Reading) -> Self;
    fn time(reading: &Self::Reading) -> i64;
    fn first_time(&self) -> i64;
    fn last_time(&self) -> i64;
    /// Adds `reading`, at or after the last.
    fn push(&mut self, reading: Self::Reading, how: Self::How);
    /// `self` followed by `next`, which starts at or after `self`'s last reading.
    #[must_use]
    fn join(self, next: Self, how: Self::How) -> Self;
}

/// A group's runs so far, in arrival order: the run being extended is the last. (Keeping that run
/// in place of the vector's pointer made the groups twice as large, and hourly means 40% slower.)
#[derive(Clone, Debug)]
pub(super) struct Runs<R>(pub(super) Vec<R>);

impl<R> Default for Runs<R> {
    fn default() -> Self {
        Self(vec![])
    }
}

/// One row's input: a reading, or a run from a state.
#[derive(Clone, Copy)]
pub(super) enum Input<R: Run> {
    Reading(R::Reading),
    Run(R),
}

impl<R: Run> Runs<R> {
    fn take(&mut self, input: Input<R>, how: R::How) {
        match input {
            Input::Reading(p) => match self.0.last_mut() {
                Some(run) if R::time(&p) >= run.last_time() => run.push(p, how),
                _ => self.0.push(R::new(p)),
            },
            Input::Run(r) => self.0.push(r),
        }
    }

    /// All runs joined in time order, or an error if two overlap.
    pub(super) fn joined(&self, how: R::How) -> DfResult<Option<R>> {
        let mut runs = self.0.clone();
        runs.sort_by(|a, b| a.first_time().cmp(&b.first_time()).then(a.last_time().cmp(&b.last_time())));
        let mut out: Option<R> = None;
        for run in runs {
            out = Some(match out {
                None => run,
                Some(prev) if run.first_time() < prev.last_time() => {
                    return exec_err!(
                        "a group's readings came in runs that overlap in time: group by the series key (each series \
                         is one signal). With timelake.series_order = 'segments', files that overlap do this too"
                    );
                }
                Some(prev) => prev.join(run, how),
            });
        }
        Ok(out)
    }
}

/// A run aggregate: its run, how it reads its input columns and writes its state and result.
pub(super) trait RunAggregate: Debug + Send + Sync + 'static {
    type Run: Run;

    fn how(&self) -> <Self::Run as Run>::How;
    /// Calls `add(row, input)` for each row's reading or each run of a row's state.
    fn inputs(&self, columns: &[ArrayRef], add: impl FnMut(usize, Input<Self::Run>)) -> DfResult<()>;
    /// The groups' runs as a state column, each group's runs as they came (joining them here
    /// could hide a run of another partition between them).
    fn states<'a>(&self, groups: impl Iterator<Item = &'a Runs<Self::Run>>) -> ArrayRef;
    /// The result column for `groups`.
    fn results<'a>(&self, groups: impl Iterator<Item = &'a Runs<Self::Run>>) -> DfResult<ArrayRef>;
}

impl<R: Run> GroupState for Runs<R> {
    fn heap_size(&self) -> usize {
        self.0.capacity() * size_of::<R>()
    }
}

/// A run aggregate folds each group's readings and states into its runs.
impl<A: RunAggregate> Fold for A {
    type Group = Runs<A::Run>;
    type Item = Input<A::Run>;

    fn items(&self, columns: &[ArrayRef], add: impl FnMut(usize, Input<A::Run>)) -> DfResult<()> {
        self.inputs(columns, add)
    }

    fn fold(&self, runs: &mut Runs<A::Run>, input: Input<A::Run>) {
        runs.take(input, self.how());
    }

    fn states<'a>(&self, groups: impl Iterator<Item = &'a Runs<A::Run>>) -> ArrayRef {
        RunAggregate::states(self, groups)
    }

    fn results<'a>(&self, groups: impl Iterator<Item = &'a Runs<A::Run>>) -> DfResult<ArrayRef> {
        RunAggregate::results(self, groups)
    }
}

/// Whether `e` is a run aggregate over readings (its first argument a time), not over states
/// (whose runs say where they are in time), in a plan whose input has `schema`.
pub(crate) fn over_readings(e: &Expr, schema: &DFSchema) -> bool {
    let Expr::AggregateFunction(AggregateFunction { func, params }) = unalias(e) else { return false };
    let name = func.name();
    let runs = super::signal::Output::of(name).is_some()
        || super::gaps::Output::of(name).is_some()
        || super::flatline::Output::of(name).is_some();
    runs && params.args.first().is_some_and(|t| matches!(t.get_type(schema), Ok(DataType::Timestamp(..))))
}
