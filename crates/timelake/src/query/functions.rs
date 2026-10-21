//! What Timelake's SQL functions share: reading their arguments.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;

/// The value of argument `i` of a call, if it is a literal.
pub(super) fn literal_arg(exprs: &[Arc<dyn PhysicalExpr>], i: usize) -> Option<&ScalarValue> {
    exprs.get(i)?.downcast_ref::<Literal>().map(Literal::value)
}

/// The types of an aggregate call's arguments.
pub(super) fn arg_types(args: &AccumulatorArgs<'_>) -> Vec<DataType> {
    args.expr_fields.iter().map(|f| f.data_type().clone()).collect()
}
