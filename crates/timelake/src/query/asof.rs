//! ASOF JOIN (spec 28): each left row with the right row of the same keys whose time is the
//! nearest at or before its own (or after, by the match condition), as Snowflake writes it:
//!
//! ```sql
//! SELECT p.time, p.value AS pressure, s.value AS state
//! FROM readings p ASOF JOIN states s MATCH_CONDITION (p.time >= s.time) ON p.machine = s.machine
//! ```
//!
//! Every left row is kept, with nulls where no right row matches. The match condition compares a
//! left time with a right time by `>=`, `>`, `<=` or `<`; the `ON` keys are equalities.
//!
//! DataFusion has no ASOF JOIN: [`rewrite_statement`] turns one into a left join whose condition
//! carries the match condition inside a marker function, [`AsofRule`] replaces that join with an
//! [`AsofJoin`] node, and [`AsofJoinExec`] runs it: the right side is collected and each key's rows
//! sorted by time, then each left row finds its match by binary search. On a day of the industrial
//! benchmark's one-second readings (8.6 M rows) matched with their machines' states: 0.23 s, against
//! 0.36 s for the same answer from a union and `forward_fill`, and 78 s as a range join.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::ops::ControlFlow;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, UInt32Array};
use arrow::compute::{cast, concat_batches, take};
use arrow::datatypes::{DataType, Int64Type, SchemaRef, TimeUnit};
use arrow::row::{RowConverter, SortField};
use async_trait::async_trait;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchemaRef, internal_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::utils::split_conjunction_owned;
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, Extension, JoinType, LogicalPlan, Operator, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, UserDefinedLogicalNode, UserDefinedLogicalNodeCore, Volatility,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties, PlanProperties, collect,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::sql::sqlparser::ast::{
    self, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, JoinConstraint,
    JoinOperator, ObjectName, Query, SetExpr, TableFactor, TableWithJoins, VisitMut, VisitorMut,
};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::OnceCell;

/// The marker the rewritten join condition wraps the match condition in.
const MARKER: &str = "timelake_asof";

/// Rewrites each `ASOF JOIN … MATCH_CONDITION (c) ON k` in `statement` into
/// `LEFT JOIN … ON k AND timelake_asof(c)`, which [`AsofRule`] turns back into an ASOF join.
pub(super) fn rewrite_statement(statement: &mut datafusion::sql::parser::Statement) {
    struct Rewrite;
    impl VisitorMut for Rewrite {
        type Break = ();
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
            rewrite_set_expr(&mut query.body);
            ControlFlow::Continue(())
        }
    }
    match statement {
        datafusion::sql::parser::Statement::Statement(s) => {
            let _ = s.visit(&mut Rewrite);
        }
        datafusion::sql::parser::Statement::Explain(e) => rewrite_statement(&mut e.statement),
        _ => {}
    }
}

fn rewrite_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => select.from.iter_mut().for_each(rewrite_joins),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(left);
            rewrite_set_expr(right);
        }
        _ => {}
    }
}

fn rewrite_joins(from: &mut TableWithJoins) {
    if let TableFactor::NestedJoin { table_with_joins, .. } = &mut from.relation {
        rewrite_joins(table_with_joins);
    }
    for join in &mut from.joins {
        if let TableFactor::NestedJoin { table_with_joins, .. } = &mut join.relation {
            rewrite_joins(table_with_joins);
        }
        let JoinOperator::AsOf { match_condition, constraint } = &join.join_operator else { continue };
        let marker = ast::Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new(MARKER)]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(match_condition.clone()))],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        });
        let on = match constraint {
            JoinConstraint::On(keys) => ast::Expr::BinaryOp {
                left: Box::new(ast::Expr::Nested(Box::new(keys.clone()))),
                op: ast::BinaryOperator::And,
                right: Box::new(marker),
            },
            _ => marker,
        };
        join.join_operator = JoinOperator::LeftOuter(JoinConstraint::On(on));
    }
}

/// The marker function: planned away by [`AsofRule`], an error if anything is left to run.
pub(super) fn marker_udf() -> ScalarUDF {
    ScalarUDF::from(Marker { signature: Signature::exact(vec![DataType::Boolean], Volatility::Immutable) })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Marker {
    signature: Signature,
}

impl ScalarUDFImpl for Marker {
    fn name(&self) -> &str {
        MARKER
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        plan_err!("ASOF JOIN takes equalities in ON and one comparison of times in MATCH_CONDITION")
    }
}

/// How a left row's time relates to its match's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd)]
enum Direction {
    /// The latest right time at or before the left time (`>=`), or strictly before (`>`).
    Before { inclusive: bool },
    /// The earliest right time at or after the left time (`<=`), or strictly after (`<`).
    After { inclusive: bool },
}

/// An ASOF join: the left input's rows, each with its matching right row's columns.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct AsofJoin {
    left: LogicalPlan,
    right: LogicalPlan,
    /// The equal keys, left and right.
    on: Vec<(Expr, Expr)>,
    left_time: Expr,
    right_time: Expr,
    direction: Direction,
    /// Left rows without a match are kept (with nulls), or left out where the join became an inner one.
    keep_unmatched: bool,
    schema: DFSchemaRef,
}

impl PartialOrd for AsofJoin {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mine = (&self.left, &self.right, &self.on, &self.left_time, &self.right_time);
        let theirs = (&other.left, &other.right, &other.on, &other.left_time, &other.right_time);
        match mine.partial_cmp(&theirs)? {
            Ordering::Equal => {
                (self.direction, self.keep_unmatched).partial_cmp(&(other.direction, other.keep_unmatched))
            }
            unequal => Some(unequal),
        }
    }
}

impl UserDefinedLogicalNodeCore for AsofJoin {
    fn name(&self) -> &'static str {
        "AsofJoin"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.left, &self.right]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let keys = self.on.iter().flat_map(|(l, r)| [l.clone(), r.clone()]);
        keys.chain([self.left_time.clone(), self.right_time.clone()]).collect()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let on: Vec<String> = self.on.iter().map(|(l, r)| format!("{l} = {r}")).collect();
        let op = match self.direction {
            Direction::Before { inclusive: true } => ">=",
            Direction::Before { inclusive: false } => ">",
            Direction::After { inclusive: true } => "<=",
            Direction::After { inclusive: false } => "<",
        };
        write!(f, "AsofJoin: {} {op} {} on [{}]", self.left_time, self.right_time, on.join(", "))
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let [left, right] = <[LogicalPlan; 2]>::try_from(inputs)
            .map_err(|_| datafusion::error::DataFusionError::Internal("an ASOF join has two inputs".into()))?;
        let n = self.on.len();
        if exprs.len() != 2 * n + 2 {
            return internal_err!("an ASOF join's expressions are its keys and two times");
        }
        let on = (0..n).map(|i| (exprs[2 * i].clone(), exprs[2 * i + 1].clone())).collect();
        Ok(Self {
            left,
            right,
            on,
            left_time: exprs[2 * n].clone(),
            right_time: exprs[2 * n + 1].clone(),
            direction: self.direction,
            keep_unmatched: self.keep_unmatched,
            schema: Arc::clone(&self.schema),
        })
    }
}

/// Replaces a left join whose condition carries the marker with an [`AsofJoin`].
#[derive(Debug)]
pub(super) struct AsofRule;

impl OptimizerRule for AsofRule {
    fn name(&self) -> &'static str {
        "timelake_asof_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> DfResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(join) = &plan else { return Ok(Transformed::no(plan)) };
        let Some(filter) = &join.filter else { return Ok(Transformed::no(plan)) };
        let parts = split_conjunction_owned(filter.clone());
        let (markers, rest): (Vec<Expr>, Vec<Expr>) = parts.into_iter().partition(is_marker);
        let [marker] = markers.as_slice() else {
            return if markers.is_empty() {
                Ok(Transformed::no(plan))
            } else {
                plan_err!("one MATCH_CONDITION per ASOF JOIN")
            };
        };
        // A filter above that rejects nulls on the right makes the left join an inner one.
        let keep_unmatched = match join.join_type {
            JoinType::Left => true,
            JoinType::Inner => false,
            _ => return plan_err!("ASOF JOIN keeps every left row"),
        };
        if !rest.is_empty() {
            return plan_err!("ASOF JOIN takes equalities in ON and one comparison of times in MATCH_CONDITION");
        }
        let Expr::ScalarFunction(f) = marker else { unreachable!("a marker is a call") };
        let Some(Expr::BinaryExpr(BinaryExpr { left, op, right })) = f.args.first() else {
            return plan_err!("MATCH_CONDITION compares a left time with a right time");
        };
        let refs_left = |e: &Expr| e.column_refs().iter().all(|c| join.left.schema().has_column(c));
        let refs_right = |e: &Expr| e.column_refs().iter().all(|c| join.right.schema().has_column(c));
        // Written as `left_time op right_time`, or the other way round.
        let (left_time, right_time, op) = if refs_left(left) && refs_right(right) {
            ((**left).clone(), (**right).clone(), *op)
        } else if refs_left(right) && refs_right(left) {
            let Some(swapped) = op.swap() else { return plan_err!("MATCH_CONDITION takes >=, >, <= or <") };
            ((**right).clone(), (**left).clone(), swapped)
        } else {
            return plan_err!("MATCH_CONDITION compares a left time with a right time");
        };
        let direction = match op {
            Operator::GtEq => Direction::Before { inclusive: true },
            Operator::Gt => Direction::Before { inclusive: false },
            Operator::LtEq => Direction::After { inclusive: true },
            Operator::Lt => Direction::After { inclusive: false },
            _ => return plan_err!("MATCH_CONDITION takes >=, >, <= or <"),
        };
        let node = AsofJoin {
            left: join.left.as_ref().clone(),
            right: join.right.as_ref().clone(),
            on: join.on.clone(),
            left_time,
            right_time,
            direction,
            keep_unmatched,
            schema: Arc::clone(&join.schema),
        };
        Ok(Transformed::yes(LogicalPlan::Extension(Extension { node: Arc::new(node) })))
    }
}

fn is_marker(e: &Expr) -> bool {
    matches!(e, Expr::ScalarFunction(f) if f.name() == MARKER)
}

/// Plans an [`AsofJoin`] as an [`AsofJoinExec`].
#[derive(Debug)]
pub(super) struct AsofPlanner;

#[async_trait]
impl ExtensionPlanner for AsofPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(join) = node.as_any().downcast_ref::<AsofJoin>() else { return Ok(None) };
        let ([left_logical, right_logical], [left, right]) = (logical_inputs, physical_inputs) else {
            return internal_err!("an ASOF join has two inputs");
        };
        let on_left = |e: &Expr| planner.create_physical_expr(e, left_logical.schema(), session_state);
        let on_right = |e: &Expr| planner.create_physical_expr(e, right_logical.schema(), session_state);
        let left_keys = join.on.iter().map(|(l, _)| on_left(l)).collect::<DfResult<_>>()?;
        let right_keys = join.on.iter().map(|(_, r)| on_right(r)).collect::<DfResult<_>>()?;
        let schema: SchemaRef = Arc::new(join.schema.as_arrow().clone());
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            left.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Some(Arc::new(AsofJoinExec {
            left: Arc::clone(left),
            right: Arc::clone(right),
            left_keys,
            right_keys,
            left_time: on_left(&join.left_time)?,
            right_time: on_right(&join.right_time)?,
            direction: join.direction,
            keep_unmatched: join.keep_unmatched,
            schema,
            properties: Arc::new(properties),
            built: Arc::new(OnceCell::new()),
        })))
    }
}

/// The right side, collected: its rows, and each key's rows sorted by time.
#[derive(Debug)]
struct Built {
    rows: RecordBatch,
    by_key: HashMap<Box<[u8]>, Vec<(i64, u32)>>,
    converter: RowConverter,
}

/// Runs an ASOF join: collects the right input once, then streams the left input's partitions.
#[derive(Debug)]
pub(super) struct AsofJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    left_keys: Vec<Arc<dyn PhysicalExpr>>,
    right_keys: Vec<Arc<dyn PhysicalExpr>>,
    left_time: Arc<dyn PhysicalExpr>,
    right_time: Arc<dyn PhysicalExpr>,
    direction: Direction,
    keep_unmatched: bool,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    built: Arc<OnceCell<Arc<Built>>>,
}

impl DisplayAs for AsofJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AsofJoinExec: {} keys, {:?}", self.left_keys.len(), self.direction)
    }
}

/// A time column as microseconds (timestamps) or as whole numbers.
fn times(a: &ArrayRef) -> DfResult<ArrayRef> {
    let a = match a.data_type() {
        DataType::Timestamp(..) | DataType::Date32 | DataType::Date64 => {
            cast(a, &DataType::Timestamp(TimeUnit::Microsecond, None))?
        }
        t if t.is_integer() => Arc::clone(a),
        t => return plan_err!("ASOF JOIN compares times or whole numbers, not {t}"),
    };
    Ok(cast(&a, &DataType::Int64)?)
}

fn evaluate(exprs: &[Arc<dyn PhysicalExpr>], batch: &RecordBatch) -> DfResult<Vec<ArrayRef>> {
    exprs.iter().map(|e| e.evaluate(batch)?.into_array(batch.num_rows())).collect()
}

impl AsofJoinExec {
    async fn build(&self, context: Arc<TaskContext>) -> DfResult<Arc<Built>> {
        let batches = collect(Arc::clone(&self.right), context).await?;
        let rows = concat_batches(&self.right.schema(), &batches)?;
        let keys = evaluate(&self.right_keys, &rows)?;
        let fields = keys.iter().map(|k| SortField::new(k.data_type().clone())).collect();
        let converter = RowConverter::new(fields)?;
        let encoded = converter.convert_columns(&keys)?;
        let time = times(&self.right_time.evaluate(&rows)?.into_array(rows.num_rows())?)?;
        let time = time.as_primitive::<Int64Type>();
        let mut by_key: HashMap<Box<[u8]>, Vec<(i64, u32)>> = HashMap::new();
        for i in 0..rows.num_rows() {
            if time.is_null(i) || keys.iter().any(|k| k.is_null(i)) {
                continue;
            }
            let at = u32::try_from(i).map_err(|_| datafusion::error::DataFusionError::Internal("rows".into()))?;
            by_key.entry(encoded.row(i).as_ref().into()).or_default().push((time.value(i), at));
        }
        for list in by_key.values_mut() {
            list.sort_unstable();
        }
        Ok(Arc::new(Built { rows, by_key, converter }))
    }

    /// The left batch with its matches' right columns.
    fn probe(&self, built: &Built, batch: &RecordBatch) -> DfResult<RecordBatch> {
        let keys = evaluate(&self.left_keys, batch)?;
        let encoded = built.converter.convert_columns(&keys)?;
        let time = times(&self.left_time.evaluate(batch)?.into_array(batch.num_rows())?)?;
        let time = time.as_primitive::<Int64Type>();
        let matches: UInt32Array = (0..batch.num_rows())
            .map(|i| {
                if time.is_null(i) {
                    return None;
                }
                let list = built.by_key.get(encoded.row(i).as_ref())?;
                let t = time.value(i);
                let at = match self.direction {
                    Direction::Before { inclusive } => {
                        let end = list.partition_point(|&(r, _)| if inclusive { r <= t } else { r < t });
                        end.checked_sub(1)?
                    }
                    Direction::After { inclusive } => {
                        let start = list.partition_point(|&(r, _)| if inclusive { r < t } else { r <= t });
                        (start < list.len()).then_some(start)?
                    }
                };
                Some(list[at].1)
            })
            .collect();
        let right = built.rows.columns().iter().map(|c| take(c, &matches, None)).collect::<Result<Vec<_>, _>>()?;
        let columns = batch.columns().iter().cloned().chain(right).collect();
        let joined = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        if self.keep_unmatched {
            return Ok(joined);
        }
        let matched = arrow::compute::is_not_null(&matches)?;
        Ok(arrow::compute::filter_record_batch(&joined, &matched)?)
    }
}

impl ExecutionPlan for AsofJoinExec {
    fn name(&self) -> &'static str {
        "AsofJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution, Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> DfResult<Arc<dyn ExecutionPlan>> {
        let [left, right] = <[Arc<dyn ExecutionPlan>; 2]>::try_from(children)
            .map_err(|_| datafusion::error::DataFusionError::Internal("an ASOF join has two inputs".into()))?;
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&self.schema)),
            left.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Arc::new(Self {
            left,
            right,
            left_keys: self.left_keys.clone(),
            right_keys: self.right_keys.clone(),
            left_time: Arc::clone(&self.left_time),
            right_time: Arc::clone(&self.right_time),
            direction: self.direction,
            keep_unmatched: self.keep_unmatched,
            schema: Arc::clone(&self.schema),
            properties: Arc::new(properties),
            built: Arc::new(OnceCell::new()),
        }))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DfResult<SendableRecordBatchStream> {
        let left = self.left.execute(partition, Arc::clone(&context))?;
        let this = Self {
            left: Arc::clone(&self.left),
            right: Arc::clone(&self.right),
            left_keys: self.left_keys.clone(),
            right_keys: self.right_keys.clone(),
            left_time: Arc::clone(&self.left_time),
            right_time: Arc::clone(&self.right_time),
            direction: self.direction,
            keep_unmatched: self.keep_unmatched,
            schema: Arc::clone(&self.schema),
            properties: Arc::clone(&self.properties),
            built: Arc::clone(&self.built),
        };
        let this = Arc::new(this);
        let stream = futures::stream::once(async move {
            let built = Arc::clone(this.built.get_or_try_init(|| this.build(context)).await?);
            Ok::<_, datafusion::error::DataFusionError>(left.map(move |batch| this.probe(&built, &batch?)))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.schema), stream)))
    }
}

#[cfg(test)]
mod tests {
    use crate::Session;
    use crate::testing::faulty_lake;

    async fn answer(sql: &str) -> crate::Result<Vec<String>> {
        let s = Session::new(faulty_lake().0);
        let rows = s.sql(sql).await?.collect().await?;
        let text = arrow::util::pretty::pretty_format_batches(&rows).unwrap().to_string();
        Ok(text.lines().filter(|l| l.starts_with("| ")).skip(1).map(str::to_owned).collect())
    }

    const TABLES: &str = "
        WITH l AS (SELECT * FROM (VALUES ('a', 10), ('a', 20), ('a', 35), ('b', 5), ('c', 40)) AS v(k, t)),
             r AS (SELECT * FROM (VALUES ('a', 10, 'x'), ('a', 30, 'y'), ('b', 7, 'z')) AS v(k, t, s))";

    #[tokio::test]
    async fn nearest_match_by_key() {
        let at_or_before =
            answer(&format!("{TABLES} SELECT l.k, l.t, r.s FROM l ASOF JOIN r MATCH_CONDITION (l.t >= r.t) ON l.k = r.k ORDER BY l.k, l.t"))
                .await
                .unwrap();
        assert_eq!(
            at_or_before,
            ["| a | 10 | x |", "| a | 20 | x |", "| a | 35 | y |", "| b | 5  |   |", "| c | 40 |   |"]
        );
        let before =
            answer(&format!("{TABLES} SELECT l.t, r.s FROM l ASOF JOIN r MATCH_CONDITION (r.t < l.t) ON l.k = r.k WHERE l.k = 'a' ORDER BY l.t"))
                .await
                .unwrap();
        assert_eq!(before, ["| 10 |   |", "| 20 | x |", "| 35 | y |"]);
        let after =
            answer(&format!("{TABLES} SELECT l.k, l.t, r.s FROM l ASOF JOIN r MATCH_CONDITION (l.t <= r.t) ON l.k = r.k ORDER BY l.k, l.t"))
                .await
                .unwrap();
        assert_eq!(after, ["| a | 10 | x |", "| a | 20 | y |", "| a | 35 |   |", "| b | 5  | z |", "| c | 40 |   |"]);
        // A filter on the right side's columns keeps only the matched rows.
        let matched = answer(&format!(
            "{TABLES} SELECT l.k, l.t, r.s FROM l ASOF JOIN r MATCH_CONDITION (l.t > r.t) ON l.k = r.k WHERE r.s IS NOT NULL ORDER BY l.k, l.t"
        ))
        .await
        .unwrap();
        assert_eq!(matched, ["| a | 20 | x |", "| a | 35 | y |"]);
        let bad = format!("{TABLES} SELECT * FROM l ASOF JOIN r MATCH_CONDITION (l.t = r.t) ON l.k = r.k");
        assert!(answer(&bad).await.is_err());
    }
}
