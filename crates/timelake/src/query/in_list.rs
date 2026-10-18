//! Row-group and page pruning for `column IN (…)` lists longer than DataFusion prunes with (#103).
//!
//! DataFusion's pruning predicate rewrites an `IN` list into an OR of equalities only up to 20
//! values and prunes nothing for longer ones, such as `tag IN (<50 machines>)`. Here a list's
//! values are sorted once, and a row group or page is kept when one of them lies within its
//! `[min, max]`: a binary search per page. The result is handed to the scan as each file's access
//! plan; DataFusion's own pruning still applies on top of it.

use std::cmp::Ordering;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::Schema;
use datafusion::common::ScalarValue;
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::utils::split_conjunction;
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
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
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
/// say), still sorted; `None` if a value doesn't cast.
fn values_as(values: &[ScalarValue], data_type: &arrow::datatypes::DataType) -> Option<Vec<ScalarValue>> {
    let cast: Option<Vec<ScalarValue>> = values
        .iter()
        .map(|v| if v.data_type() == *data_type { Some(v.clone()) } else { v.cast_to(data_type).ok() })
        .collect();
    let mut cast = cast?;
    cast.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    Some(cast)
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
    use crate::testing::{T0, batch, faulty_lake, opts};
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

    fn in_list(values: impl IntoIterator<Item = i32>) -> Expr {
        col("device").in_list(values.into_iter().map(lit).collect(), false)
    }

    #[test]
    fn only_long_literal_lists_are_taken() {
        let lists = long_in_lists(&[
            in_list(0..21).and(col("value").gt(lit(1))),
            in_list(0..20),
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
        let lists = long_in_lists(&[in_list([3, 7].into_iter().chain(60..83))]);
        let plan = access_plan(&metadata, &schema, &lists).unwrap();
        let rows: Vec<usize> = (0..2)
            .map(|rg| match &plan.inner()[rg] {
                datafusion::datasource::physical_plan::parquet::RowGroupAccess::Selection(s) => s.row_count(),
                other => panic!("row group {rg}: {other:?}"),
            })
            .collect();
        assert_eq!(rows, [200, 2300]);

        // Only devices of the second row group: the first is skipped.
        let plan = access_plan(&metadata, &schema, &long_in_lists(&[in_list(60..90)])).unwrap();
        assert!(!plan.should_scan(0) && plan.should_scan(1));
        // Values in every page prune nothing.
        assert!(access_plan(&metadata, &schema, &long_in_lists(&[in_list(0..100)])).is_none());
    }

    /// The same rows with the setting on and off, on a table whose pages hold 10 rows.
    #[tokio::test]
    async fn queries_give_the_same_rows_with_in_list_pruning() {
        let (lake, _) = faulty_lake();
        let mut o = opts();
        o.properties.insert(property::DATA_PAGE_ROWS.into(), "10".into());
        let t = lake.create_table("iot.r", &crate::testing::schema(), o).await.unwrap();
        let rows: Vec<(i64, i32, f64)> =
            (0..60).flat_map(|d| (0..30).map(move |i| (T0 + i, d, f64::from(d)))).collect();
        t.append(&[batch(&rows)], AppendOptions { maintain: Some(false), ..Default::default() }).await.unwrap();
        let list: Vec<String> = (10..35).map(|d| d.to_string()).collect();
        let sql = format!("SELECT count(*) AS n, sum(value) AS s FROM iot.r WHERE device IN ({})", list.join(", "));
        let mut answers = vec![];
        for on in ["true", "false"] {
            let session = Session::new(lake.clone());
            session.sql(&format!("SET timelake.in_list_pruning = {on}")).await.unwrap();
            let batches = session.sql(&sql).await.unwrap().collect().await.unwrap();
            answers.push(arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string());
        }
        assert_eq!(answers[0], answers[1]);
        assert!(answers[0].contains("| 750 |"), "{}", answers[0]);
    }
}
