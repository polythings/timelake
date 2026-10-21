//! ASOF JOIN (spec 28): each left row with the right row of the same keys whose time is the
//! nearest at or before its own (or after, by the match condition), as Snowflake writes it, or as
//! DuckDB does (`ASOF [LEFT] JOIN r ON p.machine = r.machine AND p.time >= r.time`):
//!
//! ```sql
//! SELECT p.time, p.value AS pressure, s.value AS state
//! FROM readings p ASOF JOIN states s MATCH_CONDITION (p.time >= s.time) ON p.machine = s.machine
//! ```
//!
//! Snowflake's form and DuckDB's `ASOF LEFT JOIN` keep every left row, with nulls where no right row
//! matches; DuckDB's `ASOF JOIN` leaves those rows out. The match condition compares a
//! left time with a right time by `>=`, `>`, `<=` or `<`; the `ON` keys are equalities.
//!
//! DataFusion has no ASOF JOIN: [`rewrite_statement`] turns one into a left join whose condition
//! carries the match condition inside a marker function, [`AsofRule`] replaces that join with an
//! [`AsofJoin`] node before the optimizer runs, and [`AsofJoinExec`] runs it: the right side is
//! collected and each key's rows sorted by time, then each left row finds its match by binary
//! search. On a day of the industrial benchmark's one-second readings (8.6 M rows) matched with
//! their machines' states: 0.23 s, against 0.36 s for the same answer from a union and
//! `forward_fill`, and 78 s as a range join.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::ops::ControlFlow;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, UInt32Array};
use arrow::compute::{cast, concat_batches, take};
use arrow::datatypes::{DataType, Int64Type, SchemaRef, UInt32Type};
use arrow::row::{RowConverter, SortField};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, internal_err, plan_err};
use datafusion::error::Result as DfResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::logical_plan::builder::build_join_schema;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, Extension, Filter, Join, JoinType, LogicalPlan, Operator, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, UserDefinedLogicalNode, UserDefinedLogicalNodeCore, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, apply_expression_roots};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties, PlanProperties, collect,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::sql::sqlparser::ast::{
    self, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, JoinConstraint,
    JoinOperator, ObjectName, Query, SetExpr, Spanned, TableFactor, TableWithJoins, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::tokenizer::Location;
use futures::{StreamExt, TryStreamExt};
use tokio::sync::OnceCell;

use super::time;

/// The marker the rewritten join condition wraps the match condition in.
const MARKER: &str = "timelake_asof";

/// Rewrites each ASOF join in `statement` into `[LEFT] JOIN … ON keys AND timelake_asof(c)`, which
/// [`AsofRule`] turns back into an ASOF join. Snowflake's form, `ASOF JOIN … MATCH_CONDITION (c)
/// ON keys`, keeps unmatched left rows. DuckDB's form, `ASOF [LEFT] JOIN r ON keys AND c` (with its
/// `ASOF` taken out before parsing), is found by where its right side starts (`duckdb`, each with
/// whether it is a `LEFT` join): the one inequality in its `ON` is the match condition.
pub(super) fn rewrite_statement(
    statement: &mut datafusion::sql::parser::Statement,
    duckdb: &[(Location, bool)],
) -> Result<(), String> {
    struct Rewrite<'a>(&'a [(Location, bool)]);
    impl VisitorMut for Rewrite<'_> {
        type Break = String;
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<String> {
            match rewrite_set_expr(&mut query.body, self.0) {
                Ok(()) => ControlFlow::Continue(()),
                Err(e) => ControlFlow::Break(e),
            }
        }
    }
    match statement {
        datafusion::sql::parser::Statement::Statement(s) => match s.visit(&mut Rewrite(duckdb)) {
            ControlFlow::Break(e) => Err(e),
            ControlFlow::Continue(()) => Ok(()),
        },
        datafusion::sql::parser::Statement::Explain(e) => rewrite_statement(&mut e.statement, duckdb),
        _ => Ok(()),
    }
}

fn rewrite_set_expr(body: &mut SetExpr, duckdb: &[(Location, bool)]) -> Result<(), String> {
    match body {
        SetExpr::Select(select) => select.from.iter_mut().try_for_each(|f| rewrite_joins(f, duckdb)),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(left, duckdb)?;
            rewrite_set_expr(right, duckdb)
        }
        _ => Ok(()),
    }
}

fn rewrite_joins(from: &mut TableWithJoins, duckdb: &[(Location, bool)]) -> Result<(), String> {
    if let TableFactor::NestedJoin { table_with_joins, .. } = &mut from.relation {
        rewrite_joins(table_with_joins, duckdb)?;
    }
    for join in &mut from.joins {
        if let TableFactor::NestedJoin { table_with_joins, .. } = &mut join.relation {
            rewrite_joins(table_with_joins, duckdb)?;
        }
        if let JoinOperator::AsOf { match_condition, constraint } = &join.join_operator {
            let keys = match constraint {
                JoinConstraint::On(keys) => Some(keys.clone()),
                _ => None,
            };
            join.join_operator = marked(keys, match_condition.clone(), true);
            continue;
        }
        let start = join.relation.span().start;
        let Some(&(_, left)) = duckdb.iter().find(|(at, _)| *at == start) else { continue };
        let on = match &join.join_operator {
            JoinOperator::Join(JoinConstraint::On(on))
            | JoinOperator::Inner(JoinConstraint::On(on))
            | JoinOperator::Left(JoinConstraint::On(on))
            | JoinOperator::LeftOuter(JoinConstraint::On(on)) => on.clone(),
            _ => return Err("ASOF JOIN takes ON with its keys and one comparison of times".into()),
        };
        let mut parts = vec![];
        conjuncts(on, &mut parts);
        let comparison = |e: &ast::Expr| {
            use ast::BinaryOperator as B;
            matches!(e, ast::Expr::BinaryOp { op: B::GtEq | B::Gt | B::LtEq | B::Lt, .. })
        };
        let (comparisons, keys): (Vec<_>, Vec<_>) = parts.into_iter().partition(comparison);
        let [time] = <[ast::Expr; 1]>::try_from(comparisons)
            .map_err(|_| "ASOF JOIN's ON takes one comparison of times (>=, >, <= or <)".to_owned())?;
        let keys = keys.into_iter().reduce(|a, b| ast::Expr::BinaryOp {
            left: Box::new(a),
            op: ast::BinaryOperator::And,
            right: Box::new(b),
        });
        join.join_operator = marked(keys, time, left);
    }
    Ok(())
}

/// `e`'s parts joined by `AND`, parentheses taken off.
fn conjuncts(e: ast::Expr, out: &mut Vec<ast::Expr>) {
    match e {
        ast::Expr::BinaryOp { left, op: ast::BinaryOperator::And, right } => {
            conjuncts(*left, out);
            conjuncts(*right, out);
        }
        ast::Expr::Nested(e) => conjuncts(*e, out),
        other => out.push(other),
    }
}

/// The join an ASOF join is planned as: on `keys` and the marked match condition, a left join if
/// unmatched left rows are kept, else an inner one.
fn marked(keys: Option<ast::Expr>, match_condition: ast::Expr, keep_unmatched: bool) -> JoinOperator {
    let marker = ast::Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(MARKER)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(match_condition))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    });
    let on = match keys {
        Some(keys) => ast::Expr::BinaryOp {
            left: Box::new(ast::Expr::Nested(Box::new(keys))),
            op: ast::BinaryOperator::And,
            right: Box::new(marker),
        },
        None => marker,
    };
    if keep_unmatched {
        JoinOperator::LeftOuter(JoinConstraint::On(on))
    } else {
        JoinOperator::Inner(JoinConstraint::On(on))
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
        // The inputs may have lost columns the query doesn't need.
        let join_type = if self.keep_unmatched { JoinType::Left } else { JoinType::Inner };
        let schema = Arc::new(build_join_schema(left.schema(), right.schema(), &join_type)?);
        Ok(Self {
            left,
            right,
            on,
            left_time: exprs[2 * n].clone(),
            right_time: exprs[2 * n + 1].clone(),
            direction: self.direction,
            keep_unmatched: self.keep_unmatched,
            schema,
        })
    }

    /// The left columns asked for from the left input, the right ones from the right input; the
    /// optimizer adds the keys and times.
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        let n = self.left.schema().fields().len();
        let (left, right): (Vec<usize>, Vec<usize>) = output_columns.iter().partition(|&&i| i < n);
        Some(vec![left, right.into_iter().map(|i| i - n).collect()])
    }
}

/// Replaces a join whose condition carries the marker with an [`AsofJoin`]. It is an analyzer rule,
/// so it runs before the optimizer: filter pushdown would otherwise move a `WHERE` on the right
/// side's columns below the join, and the nearest row would be chosen among the filtered rows.
#[derive(Debug)]
pub(super) struct AsofRule;

impl AnalyzerRule for AsofRule {
    fn name(&self) -> &'static str {
        "timelake_asof_join"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DfResult<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| match plan {
            LogicalPlan::Join(join) => asof_join(join),
            LogicalPlan::Filter(filter) => push_left(filter),
            plan => Ok(Transformed::no(plan)),
        })
        .data()
    }
}

/// `join` as an [`AsofJoin`] if its condition carries the marker.
fn asof_join(join: Join) -> DfResult<Transformed<LogicalPlan>> {
    let Some(filter) = &join.filter else { return Ok(Transformed::no(LogicalPlan::Join(join))) };
    let parts = split_conjunction_owned(filter.clone());
    let (markers, rest): (Vec<Expr>, Vec<Expr>) = parts.into_iter().partition(is_marker);
    let [marker] = markers.as_slice() else {
        return if markers.is_empty() {
            Ok(Transformed::no(LogicalPlan::Join(join)))
        } else {
            plan_err!("one MATCH_CONDITION per ASOF JOIN")
        };
    };
    let keep_unmatched = match join.join_type {
        JoinType::Left => true,
        JoinType::Inner => false,
        _ => return plan_err!("ASOF JOIN keeps every left row"),
    };
    let sides = Sides { left: &join.left, right: &join.right };
    let mut on = join.on.clone();
    for e in &rest {
        match e {
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => match sides.of(left, right) {
                Some((l, r, _)) => on.push((l, r)),
                None => return plan_err!("ASOF JOIN's keys compare a left column with a right one"),
            },
            _ => return plan_err!("ASOF JOIN takes equalities in ON and one comparison of times in MATCH_CONDITION"),
        }
    }
    let (left_time, right_time, direction) = match_condition(marker, &sides)?;
    let node = AsofJoin {
        left: join.left.as_ref().clone(),
        right: join.right.as_ref().clone(),
        on,
        left_time,
        right_time,
        direction,
        keep_unmatched,
        schema: join.schema,
    };
    Ok(Transformed::yes(LogicalPlan::Extension(Extension { node: Arc::new(node) })))
}

/// A join's two sides, to tell which side an expression reads.
struct Sides<'a> {
    left: &'a LogicalPlan,
    right: &'a LogicalPlan,
}

impl Sides<'_> {
    /// `a` and `b` as (left, right) expressions, and whether they were the other way round; `None`
    /// unless one reads only the left side's columns and the other only the right's.
    fn of(&self, a: &Expr, b: &Expr) -> Option<(Expr, Expr, bool)> {
        let reads = |e: &Expr, side: &LogicalPlan| {
            let columns = e.column_refs();
            !columns.is_empty() && columns.iter().all(|c| side.schema().has_column(c))
        };
        if reads(a, self.left) && reads(b, self.right) {
            Some((a.clone(), b.clone(), false))
        } else if reads(b, self.left) && reads(a, self.right) {
            Some((b.clone(), a.clone(), true))
        } else {
            None
        }
    }
}

/// The left time, the right time and the direction of a `MATCH_CONDITION` (its marker).
fn match_condition(marker: &Expr, sides: &Sides<'_>) -> DfResult<(Expr, Expr, Direction)> {
    let Expr::ScalarFunction(f) = marker else { return internal_err!("a MATCH_CONDITION marker is a call") };
    let Some(Expr::BinaryExpr(BinaryExpr { left, op, right })) = f.args.first() else {
        return plan_err!("MATCH_CONDITION compares a left time with a right time");
    };
    // Written as `left_time op right_time`, or the other way round.
    let (left_time, right_time, op) = match sides.of(left, right) {
        Some((l, r, false)) => (l, r, *op),
        Some((l, r, true)) => {
            let Some(swapped) = op.swap() else { return plan_err!("MATCH_CONDITION takes >=, >, <= or <") };
            (l, r, swapped)
        }
        None => return plan_err!("MATCH_CONDITION compares a left time with a right time"),
    };
    let direction = match op {
        Operator::GtEq => Direction::Before { inclusive: true },
        Operator::Gt => Direction::Before { inclusive: false },
        Operator::LtEq => Direction::After { inclusive: true },
        Operator::Lt => Direction::After { inclusive: false },
        _ => return plan_err!("MATCH_CONDITION takes >=, >, <= or <"),
    };
    Ok((left_time, right_time, direction))
}

/// Pushes the parts of a filter above an [`AsofJoin`] that read only left columns into its left
/// input: a left row's match doesn't depend on the other left rows. DataFusion's filter pushdown
/// can't, as it pushes a filter into every input of a node it doesn't know.
#[derive(Debug)]
pub(super) struct AsofFilterPushdown;

impl OptimizerRule for AsofFilterPushdown {
    fn name(&self) -> &'static str {
        "timelake_asof_filter_pushdown"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> DfResult<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) => push_left(filter),
            plan => Ok(Transformed::no(plan)),
        }
    }
}

/// `filter` with the parts that read only an [`AsofJoin`]'s left columns moved into its left input.
fn push_left(filter: Filter) -> DfResult<Transformed<LogicalPlan>> {
    let LogicalPlan::Extension(Extension { node }) = filter.input.as_ref() else {
        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
    };
    let Some(join) = node.as_any().downcast_ref::<AsofJoin>() else {
        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
    };
    let on_left = |e: &Expr| {
        let columns = e.column_refs();
        !e.is_volatile() && !columns.is_empty() && columns.iter().all(|c| join.left.schema().has_column(c))
    };
    let (left, kept): (Vec<Expr>, Vec<Expr>) =
        split_conjunction_owned(filter.predicate.clone()).into_iter().partition(on_left);
    let Some(left) = conjunction(left) else { return Ok(Transformed::no(LogicalPlan::Filter(filter))) };
    let node =
        AsofJoin { left: LogicalPlan::Filter(Filter::try_new(left, Arc::new(join.left.clone()))?), ..join.clone() };
    let joined = LogicalPlan::Extension(Extension { node: Arc::new(node) });
    Ok(Transformed::yes(match conjunction(kept) {
        Some(kept) => LogicalPlan::Filter(Filter::try_new(kept, Arc::new(joined))?),
        None => joined,
    }))
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
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(join) = node.as_any().downcast_ref::<AsofJoin>() else { return Ok(None) };
        let ([left_logical, right_logical], [left, right]) = (logical_inputs, physical_inputs) else {
            return internal_err!("an ASOF join has two inputs");
        };
        let on_left = |e: &Expr| planner.create_physical_expr(e, left_logical.schema(), session, planning_ctx);
        let on_right = |e: &Expr| planner.create_physical_expr(e, right_logical.schema(), session, planning_ctx);
        let left_keys = join.on.iter().map(|(l, _)| on_left(l)).collect::<DfResult<_>>()?;
        let right_keys = join.on.iter().map(|(_, r)| on_right(r)).collect::<DfResult<_>>()?;
        let matching = Matching {
            left_keys,
            right_keys,
            left_time: on_left(&join.left_time)?,
            right_time: on_right(&join.right_time)?,
            direction: join.direction,
            keep_unmatched: join.keep_unmatched,
        };
        let schema: SchemaRef = Arc::new(join.schema.as_arrow().clone());
        Ok(Some(Arc::new(AsofJoinExec::new(Arc::clone(left), Arc::clone(right), matching, schema))))
    }
}

/// The right side, collected: its rows, and each key's rows sorted by time.
#[derive(Debug)]
struct Built {
    rows: RecordBatch,
    by_key: HashMap<Box<[u8]>, Vec<(i64, u32)>>,
    converter: RowConverter,
}

/// What an ASOF join matches rows on, over its inputs' columns.
#[derive(Clone, Debug)]
struct Matching {
    left_keys: Vec<Arc<dyn PhysicalExpr>>,
    right_keys: Vec<Arc<dyn PhysicalExpr>>,
    left_time: Arc<dyn PhysicalExpr>,
    right_time: Arc<dyn PhysicalExpr>,
    direction: Direction,
    keep_unmatched: bool,
}

/// Runs an ASOF join: collects the right input once, then streams the left input's partitions.
/// Clones share the collected right input.
#[derive(Clone, Debug)]
pub(super) struct AsofJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    matching: Matching,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    built: Arc<OnceCell<Arc<Built>>>,
}

impl DisplayAs for AsofJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AsofJoinExec: {} keys, {:?}", self.matching.left_keys.len(), self.matching.direction)
    }
}

/// A time column as microseconds (timestamps) or as whole numbers.
fn times(a: &ArrayRef) -> DfResult<ArrayRef> {
    let a = match a.data_type() {
        DataType::Timestamp(..) | DataType::Date32 | DataType::Date64 => time::micros_array(a)?,
        t if t.is_integer() => cast(a, &DataType::Int64)?,
        t => return plan_err!("ASOF JOIN compares times or whole numbers, not {t}"),
    };
    Ok(a)
}

fn evaluate(exprs: &[Arc<dyn PhysicalExpr>], batch: &RecordBatch) -> DfResult<Vec<ArrayRef>> {
    exprs.iter().map(|e| e.evaluate(batch)?.into_array(batch.num_rows())).collect()
}

impl AsofJoinExec {
    fn new(left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>, matching: Matching, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            left.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self { left, right, matching, schema, properties: Arc::new(properties), built: Arc::new(OnceCell::new()) }
    }

    async fn build(&self, context: Arc<TaskContext>) -> DfResult<Arc<Built>> {
        let batches = collect(Arc::clone(&self.right), context).await?;
        let rows = concat_batches(&self.right.schema(), &batches)?;
        let keys = evaluate(&self.matching.right_keys, &rows)?;
        let fields = keys.iter().map(|k| SortField::new(k.data_type().clone())).collect();
        let converter = RowConverter::new(fields)?;
        let encoded = converter.convert_columns(&keys)?;
        let time = times(&self.matching.right_time.evaluate(&rows)?.into_array(rows.num_rows())?)?;
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
        let keys = evaluate(&self.matching.left_keys, batch)?;
        let encoded = built.converter.convert_columns(&keys)?;
        let time = times(&self.matching.left_time.evaluate(batch)?.into_array(batch.num_rows())?)?;
        let time = time.as_primitive::<Int64Type>();
        let matches: UInt32Array = (0..batch.num_rows())
            .map(|i| {
                if time.is_null(i) {
                    return None;
                }
                let list = built.by_key.get(encoded.row(i).as_ref())?;
                let t = time.value(i);
                let at = match self.matching.direction {
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
        // An inner join drops the unmatched left rows first: its right columns may not hold nulls.
        let (batch, matches) = if self.matching.keep_unmatched {
            (batch.clone(), matches)
        } else {
            let matched = arrow::compute::is_not_null(&matches)?;
            let kept = arrow::compute::filter(&matches, &matched)?;
            (arrow::compute::filter_record_batch(batch, &matched)?, kept.as_primitive::<UInt32Type>().clone())
        };
        let right = built.rows.columns().iter().map(|c| take(c, &matches, None)).collect::<Result<Vec<_>, _>>()?;
        let columns = batch.columns().iter().cloned().chain(right).collect();
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
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
        Ok(Arc::new(Self::new(left, right, self.matching.clone(), Arc::clone(&self.schema))))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        let m = &self.matching;
        apply_expression_roots(m.left_keys.iter().chain(&m.right_keys).chain([&m.left_time, &m.right_time]), f)
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DfResult<SendableRecordBatchStream> {
        let left = self.left.execute(partition, Arc::clone(&context))?;
        let this = Arc::new(self.clone());
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
    use crate::testing::answer_on_empty_lake;

    /// A query's rows, without the header.
    async fn answer(sql: &str) -> crate::Result<Vec<String>> {
        let text = answer_on_empty_lake(sql).await?;
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
        // A filter on the right side's columns applies to the matches, after the nearest row is found.
        let x = answer(&format!(
            "{TABLES} SELECT l.k, l.t, r.s FROM l ASOF JOIN r MATCH_CONDITION (l.t >= r.t) ON l.k = r.k WHERE r.s = 'x' ORDER BY l.t"
        ))
        .await
        .unwrap();
        assert_eq!(x, ["| a | 10 | x |", "| a | 20 | x |"]);
        let inner = answer(&format!(
            "{TABLES} SELECT l.t, r.s FROM l ASOF JOIN r ON l.k = r.k AND l.t >= r.t WHERE r.s = 'x' AND l.t > 10 ORDER BY l.t"
        ))
        .await
        .unwrap();
        assert_eq!(inner, ["| 20 | x |"]);
        let bad = format!("{TABLES} SELECT * FROM l ASOF JOIN r MATCH_CONDITION (l.t = r.t) ON l.k = r.k");
        assert!(answer(&bad).await.is_err());
    }

    #[tokio::test]
    async fn duckdb_form() {
        // ASOF LEFT JOIN keeps every left row; ASOF JOIN only the matched ones.
        let left = answer(&format!(
            "{TABLES} SELECT l.k, l.t, r.s FROM l ASOF LEFT JOIN r ON l.k = r.k AND l.t >= r.t ORDER BY l.k, l.t"
        ))
        .await
        .unwrap();
        assert_eq!(left, ["| a | 10 | x |", "| a | 20 | x |", "| a | 35 | y |", "| b | 5  |   |", "| c | 40 |   |"]);
        let inner = answer(&format!(
            "{TABLES} SELECT l.k, l.t, r.s FROM l asof join r ON r.t <= l.t AND l.k = r.k ORDER BY l.k, l.t"
        ))
        .await
        .unwrap();
        assert_eq!(inner, ["| a | 10 | x |", "| a | 20 | x |", "| a | 35 | y |"]);
        // Both forms in one query, the second ASOF JOIN with a derived table on its right.
        let both = answer(&format!(
            "{TABLES} SELECT l.t, r.s, r2.s AS next FROM l
             ASOF JOIN r MATCH_CONDITION (l.t >= r.t) ON l.k = r.k
             ASOF LEFT JOIN (SELECT k, t, s FROM r) r2 ON l.k = r2.k AND l.t < r2.t
             WHERE l.k = 'a' ORDER BY l.t"
        ))
        .await
        .unwrap();
        assert_eq!(both, ["| 10 | x | y    |", "| 20 | x | y    |", "| 35 | y |      |"]);
        let two = format!("{TABLES} SELECT * FROM l ASOF JOIN r ON l.k = r.k AND l.t >= r.t AND l.t < r.t + 100");
        assert!(answer(&two).await.is_err());
    }
}
