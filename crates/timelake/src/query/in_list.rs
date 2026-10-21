//! Row-group and page pruning for `column IN (…)` lists longer than DataFusion prunes with (#103).
//!
//! DataFusion's pruning predicate rewrites an `IN` list into an OR of equalities only up to 20
//! values and prunes nothing for longer ones, such as `tag IN (<50 machines>)`. Here a list's
//! values are sorted once, and a row group or page is kept when one of them lies within its
//! `[min, max]`: a binary search per page. The result is handed to the scan as each file's access
//! plan; DataFusion's own pruning still applies on top of it. A join's runtime filter, an `IN` list
//! of the build side's keys known only once the scan runs, is split into lists DataFusion prunes
//! with as each file is opened ([`SplitInLists`]).

use std::cmp::Ordering;

use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::Operator;
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::physical_expr::expressions::{BinaryExpr, DynamicFilterPhysicalExpr, InListExpr, Literal, in_list};
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion::physical_plan::PhysicalExpr;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::ParquetMetaData;

/// Lists up to this long are left to DataFusion's pruning.
const DATAFUSION_MAX_LIST: usize = 20;

/// A filter `column IN (values)`: the values sorted, without nulls or repeats.
#[derive(Debug)]
pub(super) struct InList {
    column: String,
    values: Vec<ScalarValue>,
}

/// The `IN` lists among `filters` (each one of the scan's conjuncts) that DataFusion won't prune
/// with: on a column, with literal values only, and longer than it handles.
pub(super) fn long_in_lists(filters: &[Expr]) -> Vec<InList> {
    let mut out = vec![];
    for expr in filters.iter().flat_map(split_conjunction) {
        let Expr::InList(list) = expr else { continue };
        let Expr::Column(column) = list.expr.as_ref() else { continue };
        if list.negated || list.list.len() <= DATAFUSION_MAX_LIST {
            continue;
        }
        let values: Option<Vec<ScalarValue>> = list
            .list
            .iter()
            .map(|e| match e {
                Expr::Literal(v, _) => Some(v.clone()),
                _ => None,
            })
            .collect();
        let Some(mut values) = values else { continue };
        values.retain(|v| !v.is_null());
        let Some(mut values) = sorted(values) else { continue };
        values.dedup();
        out.push(InList { column: column.name.clone(), values });
    }
    out
}

/// The row groups and pages of a file that can hold a row matching every list, or `None` when
/// the lists prune nothing (or the file's statistics can't tell).
pub(super) fn access_plan(metadata: &ParquetMetaData, schema: &Schema, lists: &[InList]) -> Option<ParquetAccessPlan> {
    let n = metadata.num_row_groups();
    let mut keep: Vec<Option<RowSelection>> = vec![None; n]; // None: the whole row group
    let mut skip = vec![false; n];
    let mut pruned = false;
    for list in lists {
        let Ok(stats) = StatisticsConverter::try_new(&list.column, schema, metadata.file_metadata().schema_descr())
        else {
            continue;
        };
        let groups = metadata.row_groups();
        let (Ok(mins), Ok(maxes)) = (stats.row_group_mins(groups), stats.row_group_maxes(groups)) else { continue };
        let Some(values) = values_as(&list.values, mins.data_type()) else { continue };
        let indexes = metadata.column_index().zip(metadata.offset_index());
        for rg in 0..n {
            if skip[rg] {
                continue;
            }
            if !overlaps(&values, &mins, &maxes, rg) {
                skip[rg] = true;
                pruned = true;
                continue;
            }
            let Some((column_index, offset_index)) = indexes else { continue };
            let Some(pages) = page_selection(&stats, &values, column_index, offset_index, groups, rg) else { continue };
            pruned = true;
            keep[rg] = Some(match keep[rg].take() {
                Some(previous) => previous.intersection(&pages),
                None => pages,
            });
        }
    }
    if !pruned {
        return None;
    }
    let mut plan = ParquetAccessPlan::new_all(n);
    for (rg, (skip, selection)) in skip.into_iter().zip(keep).enumerate() {
        if skip {
            plan.skip(rg);
        } else if let Some(selection) = selection {
            if selection.selects_any() {
                plan.scan_selection(rg, selection);
            } else {
                plan.skip(rg);
            }
        }
    }
    Some(plan)
}

/// The pages of row group `rg` whose `[min, max]` holds one of `values`, as a row selection; `None`
/// if the page index can't tell or every page is kept.
fn page_selection(
    stats: &StatisticsConverter<'_>,
    values: &[ScalarValue],
    column_index: &parquet::file::metadata::ParquetColumnIndex,
    offset_index: &parquet::file::metadata::ParquetOffsetIndex,
    groups: &[parquet::file::metadata::RowGroupMetaData],
    rg: usize,
) -> Option<RowSelection> {
    let mins = stats.data_page_mins(column_index, offset_index, [&rg]).ok()?;
    let maxes = stats.data_page_maxes(column_index, offset_index, [&rg]).ok()?;
    let counts = stats.data_page_row_counts(offset_index, groups, [&rg]).ok()??;
    let mut selectors = Vec::with_capacity(counts.len());
    let mut all = true;
    for page in 0..counts.len() {
        let rows = usize::try_from(counts.value(page)).ok()?;
        if overlaps(values, &mins, &maxes, page) {
            selectors.push(RowSelector::select(rows));
        } else {
            selectors.push(RowSelector::skip(rows));
            all = false;
        }
    }
    (!all).then(|| RowSelection::from(selectors))
}

/// Whether one of the sorted `values` lies within `[mins[i], maxes[i]]`; true when either bound is
/// unknown.
fn overlaps(values: &[ScalarValue], mins: &ArrayRef, maxes: &ArrayRef, i: usize) -> bool {
    let (Ok(min), Ok(max)) = (ScalarValue::try_from_array(mins, i), ScalarValue::try_from_array(maxes, i)) else {
        return true;
    };
    if min.is_null() || max.is_null() {
        return true;
    }
    let first = values.partition_point(|v| v.partial_cmp(&min) == Some(Ordering::Less));
    values.get(first).is_some_and(|v| v.partial_cmp(&max) != Some(Ordering::Greater))
}

/// `values` cast to the statistics' type (a list of `Utf8` literals against `Utf8View` statistics,
/// say), still sorted; `None` if a value doesn't cast or the values can't be sorted.
fn values_as(values: &[ScalarValue], data_type: &arrow::datatypes::DataType) -> Option<Vec<ScalarValue>> {
    let cast: Option<Vec<ScalarValue>> = values
        .iter()
        .map(|v| if v.data_type() == *data_type { Some(v.clone()) } else { v.cast_to(data_type).ok() })
        .collect();
    sorted(cast?)
}

/// `values` sorted, or `None` (prune nothing) if two of them don't compare or one is NaN, which
/// Parquet statistics leave out of a page's bounds.
fn sorted(mut values: Vec<ScalarValue>) -> Option<Vec<ScalarValue>> {
    let nan = |v: &ScalarValue| match v {
        ScalarValue::Float16(Some(f)) => f.is_nan(),
        ScalarValue::Float32(Some(f)) => f.is_nan(),
        ScalarValue::Float64(Some(f)) => f.is_nan(),
        _ => false,
    };
    if values.iter().any(nan) {
        return None;
    }
    let mut comparable = true;
    values.sort_by(|a, b| {
        a.partial_cmp(b).unwrap_or_else(|| {
            comparable = false;
            Ordering::Equal
        })
    });
    comparable.then_some(values)
}

/// Adapts each file's scan predicate as DataFusion's default adapter does, and adds to each long
/// `IN` list inside a join's runtime filter an OR of lists short enough for DataFusion to prune
/// with. (Lists written in the query are pruned with by [`access_plan`] before the scan; a runtime
/// filter exists only once the scan runs.) The runtime filter is taken as it stands when the file
/// is opened: by then a hash join has built its side, and a runtime filter only ever narrows, so
/// that value stays correct for the file.
#[derive(Debug)]
pub(super) struct SplitInLists;

impl PhysicalExprAdapterFactory for SplitInLists {
    fn create(&self, logical: SchemaRef, physical: SchemaRef) -> DfResult<Arc<dyn PhysicalExprAdapter>> {
        let inner = DefaultPhysicalExprAdapterFactory.create(logical, physical.clone())?;
        Ok(Arc::new(SplitAdapter { inner, schema: physical }))
    }
}

#[derive(Debug)]
struct SplitAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
    schema: SchemaRef,
}

impl PhysicalExprAdapter for SplitAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> DfResult<Arc<dyn PhysicalExpr>> {
        let expr = self.inner.rewrite(expr)?;
        expr.transform_down(|e| {
            let Some(dynamic) = e.downcast_ref::<DynamicFilterPhysicalExpr>() else { return Ok(Transformed::no(e)) };
            let split = dynamic.current()?.transform_up(|e| self.split(e))?;
            // Runtime filters stay live unless taking them as they stand helps pruning.
            Ok(if split.transformed {
                Transformed::new(split.data, true, TreeNodeRecursion::Jump)
            } else {
                Transformed::no(e)
            })
        })
        .map(|t| t.data)
    }
}

impl SplitAdapter {
    fn split(&self, e: Arc<dyn PhysicalExpr>) -> DfResult<Transformed<Arc<dyn PhysicalExpr>>> {
        let Some(list) = e.downcast_ref::<InListExpr>() else { return Ok(Transformed::no(e)) };
        let literals = list.list().iter().all(|v| v.downcast_ref::<Literal>().is_some());
        if list.negated() || list.list().len() <= DATAFUSION_MAX_LIST || !literals {
            return Ok(Transformed::no(e));
        }
        let mut parts = list
            .list()
            .chunks(DATAFUSION_MAX_LIST)
            .map(|chunk| in_list(Arc::clone(list.expr()), chunk.to_vec(), &false, &self.schema));
        let first = parts.next().expect("a list longer than a chunk")?;
        let or = parts.try_fold(first, |acc, part| -> DfResult<Arc<dyn PhysicalExpr>> {
            Ok(Arc::new(BinaryExpr::new(acc, Operator::Or, part?)))
        })?;
        // The list itself stays, for filtering rows (one lookup per row); the parts are for pruning.
        Ok(Transformed::yes(Arc::new(BinaryExpr::new(Arc::clone(&e), Operator::And, or))))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field};
    use bytes::Bytes;
    use datafusion::logical_expr::{col, lit};
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::testing::{T0, answer, batch, faulty_lake, opts};
    use crate::{AppendOptions, Session, property};

    /// Devices 0..100, 100 rows each, one page per device and 50 devices per row group.
    fn file() -> (Schema, ParquetMetaData) {
        let schema = Schema::new(vec![Field::new("device", DataType::Int32, false)]);
        let devices = Int32Array::from_iter_values((0..100).flat_map(|d| std::iter::repeat_n(d, 100)));
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(devices)]).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(5000))
            .set_data_page_row_count_limit(100)
            .set_write_batch_size(100)
            .build();
        let mut out = vec![];
        let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&Bytes::from(out))
            .unwrap();
        (schema, metadata)
    }

    fn device_in(values: impl IntoIterator<Item = i32>) -> Expr {
        col("device").in_list(values.into_iter().map(lit).collect(), false)
    }

    #[test]
    fn only_long_literal_lists_are_taken() {
        let lists = long_in_lists(&[
            device_in(0..21).and(col("value").gt(lit(1))),
            device_in(0..20),
            col("device").in_list((0..30).map(lit).collect(), true),
            col("device").in_list((0..30).map(|i| col("value") + lit(i)).collect(), false),
        ]);
        assert_eq!(lists.len(), 1);
        assert_eq!((lists[0].column.as_str(), lists[0].values.len()), ("device", 21));
    }

    #[test]
    fn row_groups_and_pages_without_a_value_are_skipped() {
        let (schema, metadata) = file();
        // Devices 3 and 7 in the first row group, 60..83 in the second: 25 pages of 100 rows.
        let lists = long_in_lists(&[device_in([3, 7].into_iter().chain(60..83))]);
        let plan = access_plan(&metadata, &schema, &lists).unwrap();
        let rows: Vec<usize> = (0..2)
            .map(|rg| match &plan.inner()[rg] {
                datafusion::datasource::physical_plan::parquet::RowGroupAccess::Selection(s) => s.row_count(),
                other => panic!("row group {rg}: {other:?}"),
            })
            .collect();
        assert_eq!(rows, [200, 2300]);

        // Only devices of the second row group: the first is skipped.
        let plan = access_plan(&metadata, &schema, &long_in_lists(&[device_in(60..90)])).unwrap();
        assert!(!plan.should_scan(0) && plan.should_scan(1));
        // Values in every page prune nothing.
        assert!(access_plan(&metadata, &schema, &long_in_lists(&[device_in(0..100)])).is_none());
    }

    /// Parquet statistics leave NaN out, so a list with NaN must keep a row group that holds one.
    #[test]
    fn a_list_with_nan_keeps_row_groups() {
        let schema = Schema::new(vec![Field::new("x", DataType::Float64, false)]);
        let x = arrow::array::Float64Array::from(vec![1.0, f64::NAN, 3.0]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(x)]).unwrap();
        let mut out = vec![];
        let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .parse_and_finish(&Bytes::from(out))
            .unwrap();
        let values = [f64::NAN].into_iter().chain((10..40).map(f64::from)).map(lit).collect();
        let lists = long_in_lists(&[col("x").in_list(values, false)]);
        assert!(access_plan(&metadata, &schema, &lists).is_none_or(|plan| plan.should_scan(0)));
    }

    #[test]
    fn runtime_filters_get_lists_to_prune_with() {
        use datafusion::physical_expr::expressions::{Column, lit as plit};
        let schema = Arc::new(Schema::new(vec![Field::new("device", DataType::Int32, false)]));
        let device: Arc<dyn PhysicalExpr> = Arc::new(Column::new("device", 0));
        let list = |n: i32| in_list(Arc::clone(&device), (0..n).map(plit).collect(), &false, &schema).unwrap();
        let adapter = SplitInLists.create(Arc::clone(&schema), Arc::clone(&schema)).unwrap();

        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(vec![Arc::clone(&device)], plit(true)));
        dynamic.update(list(45)).unwrap();
        let rewritten = adapter.rewrite(dynamic).unwrap().to_string();
        // The list, and three lists of at most 20 values.
        assert_eq!(rewritten.matches(" IN (").count(), 4, "{rewritten}");
        assert_eq!(rewritten.matches(" OR ").count(), 2, "{rewritten}");

        // A list written in the query is left to the access plan; a short runtime list to DataFusion.
        assert_eq!(adapter.rewrite(list(45)).unwrap().to_string(), list(45).to_string());
        let short = Arc::new(DynamicFilterPhysicalExpr::new(vec![Arc::clone(&device)], plit(true)));
        short.update(list(20)).unwrap();
        assert!(adapter.rewrite(short).unwrap().downcast_ref::<DynamicFilterPhysicalExpr>().is_some());
    }

    /// The same rows with the setting on and off, on a table whose pages hold 10 rows: with a list
    /// in the query, and with a join whose runtime filter lists 25 devices.
    #[tokio::test]
    async fn queries_give_the_same_rows_with_in_list_pruning() {
        let (lake, _) = faulty_lake();
        let mut o = opts();
        o.properties.insert(property::DATA_PAGE_ROWS.into(), "10".into());
        let t = lake.create_table("iot.r", &crate::testing::schema(), o).await.unwrap();
        let rows: Vec<(i64, i32, f64)> =
            (0..60).flat_map(|d| (0..30).map(move |i| (T0 + i, d, f64::from(d)))).collect();
        let no_maintenance = AppendOptions { maintain: Some(false), ..Default::default() };
        t.append(&[batch(&rows)], no_maintenance.clone()).await.unwrap();
        let dims =
            Schema::new(vec![Field::new("device", DataType::Int32, false), Field::new("grp", DataType::Int32, false)]);
        let d = lake.create_table("iot.d", &dims, crate::TableOptions::default()).await.unwrap();
        let grp = Int32Array::from_iter_values((0..60).map(|d| i32::from((10..35).contains(&d))));
        let devices =
            RecordBatch::try_new(Arc::new(dims), vec![Arc::new(Int32Array::from_iter_values(0..60)), Arc::new(grp)])
                .unwrap();
        d.append(&[devices], no_maintenance).await.unwrap();

        let list: Vec<String> = (10..35).map(|d| d.to_string()).collect();
        let queries = [
            format!("SELECT count(*) AS n, sum(value) AS s FROM iot.r WHERE device IN ({})", list.join(", ")),
            "SELECT count(*) AS n, sum(r.value) AS s FROM iot.r r JOIN iot.d d ON r.device = d.device WHERE d.grp = 1"
                .to_string(),
        ];
        for sql in queries {
            let mut answers = vec![];
            for on in ["true", "false"] {
                let session = Session::new(lake.clone());
                session.sql(&format!("SET timelake.in_list_pruning = {on}")).await.unwrap();
                answers.push(answer(&session, &sql).await.unwrap());
            }
            assert_eq!(answers[0], answers[1], "{sql}");
            assert!(answers[0].contains("| 750 |"), "{sql}: {}", answers[0]);
        }
    }
}
