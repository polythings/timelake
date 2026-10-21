//! Accumulators for aggregates that fold each group's rows into a state of the group's own: one
//! group at a time (DataFusion's fallback, e.g. without `GROUP BY`), or all groups at once.

use std::fmt::Debug;

use arrow::array::{Array, ArrayRef, BooleanArray};
use datafusion::common::ScalarValue;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{Accumulator, EmitTo, GroupsAccumulator};

/// One group's state.
pub(super) trait GroupState: Debug + Default + Send + Sync + 'static {
    /// Memory the state holds besides itself.
    fn heap_size(&self) -> usize {
        0
    }
}

/// An aggregate as a fold: items read from its input columns, each added to its row's group.
pub(super) trait Fold: Debug + Send + Sync + 'static {
    type Group: GroupState;
    type Item;

    /// Calls `add(row, item)` for each item of the input `columns` (rows, or the parts of states).
    fn items(&self, columns: &[ArrayRef], add: impl FnMut(usize, Self::Item)) -> DfResult<()>;
    /// Adds `item` to `group`.
    fn fold(&self, group: &mut Self::Group, item: Self::Item);
    /// The groups' states as a column.
    fn states<'a>(&self, groups: impl Iterator<Item = &'a Self::Group>) -> ArrayRef;
    /// The groups' results as a column.
    fn results<'a>(&self, groups: impl Iterator<Item = &'a Self::Group>) -> DfResult<ArrayRef>;
}

/// One group.
#[derive(Debug)]
pub(super) struct OneGroup<F: Fold> {
    group: F::Group,
    fold: F,
}

impl<F: Fold> OneGroup<F> {
    pub(super) fn new(fold: F) -> Self {
        Self { group: F::Group::default(), fold }
    }
}

impl<F: Fold> Accumulator for OneGroup<F> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let (group, fold) = (&mut self.group, &self.fold);
        fold.items(values, |_, item| fold.fold(group, item))
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::try_from_array(&self.fold.states(std::iter::once(&self.group)), 0)?])
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        ScalarValue::try_from_array(&self.fold.results(std::iter::once(&self.group))?, 0)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.group.heap_size()
    }
}

/// All groups at once, as DataFusion's grouped aggregation uses.
#[derive(Debug)]
pub(super) struct AllGroups<F: Fold> {
    groups: Vec<F::Group>,
    fold: F,
}

impl<F: Fold> AllGroups<F> {
    pub(super) fn new(fold: F) -> Self {
        Self { groups: vec![], fold }
    }

    fn add(&mut self, columns: &[ArrayRef], groups: &[usize], filter: Option<&BooleanArray>, n: usize) -> DfResult<()> {
        self.groups.resize_with(n, F::Group::default);
        let (all, fold) = (&mut self.groups, &self.fold);
        fold.items(columns, |row, item| {
            if filter.is_none_or(|f| f.is_valid(row) && f.value(row)) {
                fold.fold(&mut all[groups[row]], item);
            }
        })
    }
}

impl<F: Fold> GroupsAccumulator for AllGroups<F> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DfResult<()> {
        self.add(values, group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DfResult<()> {
        self.add(values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DfResult<ArrayRef> {
        let taken = emit_to.take_needed(&mut self.groups);
        self.fold.results(taken.iter())
    }

    fn state(&mut self, emit_to: EmitTo) -> DfResult<Vec<ArrayRef>> {
        let taken = emit_to.take_needed(&mut self.groups);
        Ok(vec![self.fold.states(taken.iter())])
    }

    fn size(&self) -> usize {
        self.groups.iter().map(GroupState::heap_size).sum::<usize>() + self.groups.capacity() * size_of::<F::Group>()
    }
}
