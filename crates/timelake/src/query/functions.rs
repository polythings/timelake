//! What Timelake's SQL functions share: their literal arguments.

use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;

/// The value of argument `i` of a call, if it is a literal.
pub(super) fn literal_arg(exprs: &[Arc<dyn PhysicalExpr>], i: usize) -> Option<&ScalarValue> {
    exprs.get(i)?.downcast_ref::<Literal>().map(Literal::value)
}
