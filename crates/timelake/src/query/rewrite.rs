//! Answers latest-row queries from the last-value index: the newest row per series
//! (`ROW_NUMBER() OVER (PARTITION BY <series key> ORDER BY <time> DESC) = 1`) and the newest row
//! (`ORDER BY <time> DESC LIMIT 1`, filtered on the series key).

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::provider_as_source;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::expr::{Sort, WindowFunction};
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{
    BinaryExpr, Expr, Filter, LogicalPlan, Operator, Projection, TableScan, WindowFunctionDefinition,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

use super::provider::{Kind, TimelakeProvider, scan_under};
use crate::table::Table;

/// The optimizer rule. Runs after DataFusion's own rules, so filters are already pushed into scans.
#[derive(Debug)]
pub(super) struct LatestRewrite;

impl OptimizerRule for LatestRewrite {
    fn name(&self) -> &'static str {
        "timelake_latest_rewrite"
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> DfResult<Transformed<LogicalPlan>> {
        plan.transform_down(|node| {
            let rewritten = match rewrite_row_number(&node)? {
                Some(new) => Some(new),
                None => rewrite_top1(&node)?,
            };
            Ok(rewritten.map_or(Transformed::no(node), Transformed::yes))
        })
    }
}

/// The Timelake data table behind a scan, if it has a last-value index.
fn indexed_table(scan: &TableScan) -> Option<(Table, String)> {
    let p = TimelakeProvider::of_scan(scan)?;
    let table = p.table();
    let time_column = table.contract().time_column.clone()?;
    (p.kind() == Kind::Data && table.last_value_index().is_some()).then(|| (table.clone(), time_column))
}

/// `scan` reading the last-value index instead of the data files.
fn latest_scan(scan: &TableScan, table: Table) -> LogicalPlan {
    let mut s = scan.clone();
    s.source = provider_as_source(Arc::new(TimelakeProvider::latest(table)));
    s.filters = vec![];
    LogicalPlan::TableScan(s)
}

/// An integer literal 1 (`rn = 1`). Other types, such as the string `'1'`, are not rewritten.
fn is_one(e: &Expr) -> bool {
    use ScalarValue as S;
    matches!(
        e,
        Expr::Literal(
            S::Int8(Some(1))
                | S::Int16(Some(1))
                | S::Int32(Some(1))
                | S::Int64(Some(1))
                | S::UInt8(Some(1))
                | S::UInt16(Some(1))
                | S::UInt32(Some(1))
                | S::UInt64(Some(1)),
            _
        )
    )
}

/// Splits `rn = 1` (either side), where `is_rn` says which column is `rn`, from the other
/// conjuncts of a filter predicate.
fn split_rn_equals_one(
    predicate: &Expr,
    is_rn: impl Fn(&datafusion::common::Column) -> bool,
) -> (Option<&datafusion::common::Column>, Vec<Expr>) {
    let mut rn = None;
    let mut others = vec![];
    for c in split_conjunction(predicate) {
        let hit = match c {
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) if rn.is_none() => {
                match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(col), one) | (one, Expr::Column(col)) if is_one(one) && is_rn(col) => Some(col),
                    _ => None,
                }
            }
            _ => None,
        };
        match hit {
            Some(col) => rn = Some(col),
            None => others.push(c.clone()),
        }
    }
    (rn, others)
}

/// `Filter(rn = 1) ← [Projection] ← Window(row_number() PARTITION BY series ORDER BY time DESC) ← Scan`
fn rewrite_row_number(node: &LogicalPlan) -> DfResult<Option<LogicalPlan>> {
    let LogicalPlan::Filter(filter) = node else { return Ok(None) };
    // Find the window node, allowing column-only/alias projections in between.
    let mut current = filter.input.as_ref();
    let mut projections = vec![];
    let window = loop {
        match current {
            LogicalPlan::Projection(p) => {
                projections.push(p);
                current = &p.input;
            }
            LogicalPlan::Window(w) => break w,
            _ => return Ok(None),
        }
    };
    // `rn = 1` on the window's output (directly or via an alias in a projection), possibly AND-ed
    // with other predicates, which are kept in a filter above.
    let window_output = window.schema.fields().len() - 1;
    let output_name = window.schema.field(window_output).name();
    let is_rn = |rn: &datafusion::common::Column| {
        rn.name == *output_name
            || projections.iter().any(|p| {
                p.expr.iter().any(|e| {
                    matches!(e, Expr::Alias(a) if a.name == rn.name
                        && matches!(a.expr.as_ref(), Expr::Column(c) if c.name == *output_name))
                })
            })
    };
    let (Some(rn), others) = split_rn_equals_one(&filter.predicate, is_rn) else { return Ok(None) };
    if others.iter().any(|e| e.column_refs().iter().any(|c| c.name == rn.name)) {
        return Ok(None);
    }
    let [window_expr] = window.window_expr.as_slice() else { return Ok(None) };
    let LogicalPlan::TableScan(scan) = window.input.as_ref() else { return Ok(None) };
    if !scan.filters.is_empty() || scan.fetch.is_some() {
        return Ok(None);
    }
    let Some((table, time_column)) = indexed_table(scan) else { return Ok(None) };
    let Expr::WindowFunction(wf) = window_expr.clone().unalias() else { return Ok(None) };
    let WindowFunction { fun, params } = wf.as_ref();
    if !matches!(fun, WindowFunctionDefinition::WindowUDF(u) if u.name() == "row_number") {
        return Ok(None);
    }
    let partition_by: Option<Vec<&str>> = params
        .partition_by
        .iter()
        .map(|e| match e {
            Expr::Column(c) => Some(c.name.as_str()),
            _ => None,
        })
        .collect();
    if partition_by != Some(table.contract().series_key.iter().map(String::as_str).collect()) {
        return Ok(None);
    }
    match params.order_by.as_slice() {
        [Sort { expr: Expr::Column(c), asc: false, .. }] if c.name == time_column => {}
        _ => return Ok(None),
    }
    // Window → Projection(input columns…, 1 AS <window column>) over the index scan.
    let index_scan = latest_scan(scan, table);
    let mut exprs: Vec<Expr> = scan.projected_schema.columns().into_iter().map(Expr::Column).collect();
    let (qualifier, field) = window.schema.qualified_field(window_output);
    exprs.push(Expr::Literal(ScalarValue::UInt64(Some(1)), None).alias_qualified(qualifier.cloned(), field.name()));
    let mut rebuilt =
        LogicalPlan::Projection(Projection::try_new_with_schema(exprs, Arc::new(index_scan), window.schema.clone())?);
    // Rebuild the projections between filter and window on top, keeping the other predicates.
    for p in projections.iter().rev() {
        rebuilt = LogicalPlan::Projection(Projection::try_new_with_schema(
            p.expr.clone(),
            Arc::new(rebuilt),
            p.schema.clone(),
        )?);
    }
    match conjunction(others) {
        Some(predicate) => Ok(Some(LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(rebuilt))?))),
        None => Ok(Some(rebuilt)),
    }
}

/// `Limit(1) ← Sort(time DESC) ← [Filter on series key] ← Scan(filters on series key)`
fn rewrite_top1(node: &LogicalPlan) -> DfResult<Option<LogicalPlan>> {
    let LogicalPlan::Sort(sort) = node else { return Ok(None) };
    if sort.fetch != Some(1) {
        return Ok(None);
    }
    let mut filters: Vec<&Expr> = vec![];
    let scan = scan_under(&sort.input, |node| match node {
        LogicalPlan::Filter(f) if filters.is_empty() => {
            filters.push(&f.predicate);
            Some(&f.input)
        }
        _ => None,
    });
    let Some(scan) = scan else { return Ok(None) };
    if scan.fetch.is_some() {
        return Ok(None);
    }
    let Some((table, time_column)) = indexed_table(scan) else { return Ok(None) };
    match sort.expr.as_slice() {
        [Sort { expr: Expr::Column(c), asc: false, .. }] if c.name == time_column => {}
        _ => return Ok(None),
    }
    // Every filter must only reference series key columns: then the newest matching row is the
    // newest among those series' newest rows.
    let key: HashSet<&str> = table.contract().series_key.iter().map(String::as_str).collect();
    let only_keys = |e: &Expr| e.column_refs().iter().all(|c| key.contains(c.name.as_str()));
    if key.is_empty() || !filters.iter().all(|e| only_keys(e)) || !scan.filters.iter().all(only_keys) {
        return Ok(None);
    }
    let scan_filters = conjunction(scan.filters.iter().cloned());
    let index_scan = latest_scan(scan, table);
    // The index scan doesn't take pushed-down filters: re-apply them above it.
    let input = match scan_filters {
        Some(predicate) if filters.is_empty() => LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(index_scan))?),
        _ => index_scan,
    };
    let replaced = replace_scan(&sort.input, input)?;
    Ok(Some(node.with_new_exprs(node.expressions(), vec![replaced])?))
}

/// Replaces the single table scan under `plan` with `new`.
fn replace_scan(plan: &LogicalPlan, new: LogicalPlan) -> DfResult<LogicalPlan> {
    match plan {
        LogicalPlan::TableScan(_) => Ok(new),
        other => {
            let [input] = other.inputs()[..] else {
                return Err(DataFusionError::Internal("unexpected plan shape".into()));
            };
            let child = replace_scan(input, new)?;
            other.with_new_exprs(other.expressions(), vec![child])
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::lit;

    use super::*;
    use crate::query::Session;
    use crate::testing::{T0, answer, batch, faulty_lake, opts, schema};
    use crate::{AppendOptions, Lake};

    #[test]
    fn only_integer_one_is_one() {
        for one in [lit(1i32), lit(1i64), lit(1u64), lit(1u8)] {
            assert!(is_one(&one), "{one}");
        }
        for other in [lit("1"), lit(1.0f64), lit(2i64), lit(ScalarValue::Int64(None)), lit(true)] {
            assert!(!is_one(&other), "{other}");
        }
    }

    /// Whether the optimized plan reads the last-value index.
    async fn reads_index(s: &Session, sql: &str) -> bool {
        let plan = s.sql(sql).await.unwrap().into_optimized_plan().unwrap();
        let mut found = false;
        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node
                && let Some(p) = TimelakeProvider::of_scan(scan)
            {
                found |= p.kind() == Kind::Latest;
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })
        .unwrap();
        found
    }

    async fn lake_with_data() -> Lake {
        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let rows: Vec<_> =
            (0..3).flat_map(|d| (0..4).map(move |i| (T0 + i, d, f64::from(d * 10) + i as f64))).collect();
        t.append(&[batch(&rows)], AppendOptions::default()).await.unwrap();
        lake
    }

    const ROW_NUMBER: &str = "(SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY time DESC) AS rn FROM iot.r)";

    #[tokio::test]
    async fn rewrites_latest_row_shapes() {
        let s = Session::new(lake_with_data().await);
        let cases = [
            (format!("SELECT * FROM {ROW_NUMBER} WHERE rn = 1"), true),
            (format!("SELECT * FROM {ROW_NUMBER} WHERE 1 = rn"), true),
            (format!("SELECT device, value FROM {ROW_NUMBER} WHERE rn = 1 AND value > 5"), true),
            (format!("SELECT device FROM {ROW_NUMBER} t WHERE t.rn = 1"), true),
            ("SELECT * FROM iot.r WHERE device = 1 ORDER BY time DESC LIMIT 1".into(), true),
            ("SELECT value FROM iot.r WHERE device IN (1, 2) ORDER BY time DESC LIMIT 1".into(), true),
            // Not the latest row per series: rn = 2, other conditions on rn, other windows.
            (format!("SELECT * FROM {ROW_NUMBER} WHERE rn = 2"), false),
            (format!("SELECT * FROM {ROW_NUMBER} WHERE rn <= 1"), false),
            (format!("SELECT * FROM {ROW_NUMBER} WHERE rn = 1 OR value > 5"), false),
            (format!("SELECT * FROM {ROW_NUMBER} WHERE rn = 1 AND rn < 5"), false),
            ("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY time ASC) AS rn FROM iot.r) WHERE rn = 1".into(), false),
            ("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY value ORDER BY time DESC) AS rn FROM iot.r) WHERE rn = 1".into(), false),
            ("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY time DESC) AS rn FROM iot.r) WHERE rn = 1".into(), false),
            ("SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY device ORDER BY time DESC) AS rn FROM iot.r) WHERE rn = 1".into(), false),
            // A filter below the window changes which row is newest.
            ("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY time DESC) AS rn FROM iot.r WHERE value < 5) WHERE rn = 1".into(), false),
            // Top-1 filtered on a non-key column, ascending, or more than one row.
            ("SELECT * FROM iot.r WHERE value < 5 ORDER BY time DESC LIMIT 1".into(), false),
            ("SELECT * FROM iot.r WHERE device = 1 ORDER BY time ASC LIMIT 1".into(), false),
            ("SELECT * FROM iot.r WHERE device = 1 ORDER BY time DESC LIMIT 2".into(), false),
            ("SELECT * FROM iot.r ORDER BY value DESC LIMIT 1".into(), false),
        ];
        for (sql, rewritten) in cases {
            assert_eq!(reads_index(&s, &sql).await, rewritten, "{sql}");
        }
    }

    /// `rn = 1` is found among other conjuncts that compare a column with 1.
    #[tokio::test]
    async fn finds_rn_among_other_equalities() {
        use arrow::array::{Int32Array, RecordBatch, TimestampMicrosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        let (lake, _store) = faulty_lake();
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("device", DataType::Int32, true),
            Field::new("code", DataType::Int32, true),
        ]));
        let t = lake.create_table("iot.c", &schema, opts()).await.unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![T0, T0 + 1, T0, T0 + 1]).with_timezone("UTC")),
                Arc::new(Int32Array::from(vec![1, 1, 2, 2])),
                Arc::new(Int32Array::from(vec![0, 1, 1, 0])),
            ],
        )
        .unwrap();
        t.append(&[batch], AppendOptions::default()).await.unwrap();
        let s = Session::new(lake);
        let inner = "(SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY time DESC) AS rn FROM iot.c)";
        for sql in [
            format!("SELECT device FROM {inner} WHERE code = 1 AND rn = 1"),
            format!("SELECT device FROM {inner} WHERE rn = 1 AND code = 1"),
        ] {
            assert!(reads_index(&s, &sql).await, "{sql}");
            let rows = answer(&s, &sql).await.unwrap();
            assert!(rows.contains("| 1      |") && !rows.contains("| 2      |"), "{sql}\n{rows}");
        }
    }

    /// `rn = '1'` compares as a number after type coercion, so rewriting it is correct; the
    /// rewrite must give the same rows either way.
    #[tokio::test]
    async fn string_one_gives_the_same_rows() {
        let s = Session::new(lake_with_data().await);
        let count = async |sql: String| answer(&s, &format!("SELECT count(*) FROM ({sql})")).await.unwrap();
        let by_int = count(format!("SELECT * FROM {ROW_NUMBER} WHERE rn = 1")).await;
        assert_eq!(count(format!("SELECT * FROM {ROW_NUMBER} WHERE rn = '1'")).await, by_int);
        assert!(by_int.contains("| 3 "), "{by_int}");
    }
}
