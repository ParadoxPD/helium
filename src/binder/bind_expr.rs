//! Expression binding for the binder.
//!
//! Converts SQL AST expressions into BoundExpr:
//! - resolves column names to ColumnId
//! - performs type checking
//!
//! This module MUST NOT depend on IR or execution.

use crate::binder::bound::BoundExpr;
use crate::binder::errors::BindError;
use crate::binder::scope::ColumnScope;
use crate::frontend::sql::ast::Expr as SqlExpr;
use crate::ir::expr::BinaryOp;
use crate::types::datatype::DataType;
use crate::types::schema::ColumnId;
use crate::types::value::Value;

/// Entry point: bind a SQL expression into a BoundExpr.
/// Returns the bound expression AND its inferred type.
pub fn bind_expr(expr: &SqlExpr, scope: &ColumnScope) -> Result<(BoundExpr, DataType), BindError> {
    match expr {
        // ---------- column reference ----------
        SqlExpr::Column { name, .. } => {
            let (column_id, ty) = scope.resolve(name)?;
            Ok((BoundExpr::Column { column_id }, ty))
        }

        // ---------- literal ----------
        SqlExpr::Literal(v) => Ok((BoundExpr::Literal(v.clone()), literal_type(v))),

        // ---------- NULL ----------
        SqlExpr::Null => Ok((BoundExpr::Null, DataType::Null)),

        // ---------- unary ----------
        SqlExpr::Unary { op, expr } => {
            let (inner, inner_ty) = bind_expr(expr, scope)?;
            let result_ty = infer_unary_type(*op, &inner_ty)?;
            Ok((
                BoundExpr::Unary {
                    op: *op,
                    expr: Box::new(inner),
                },
                result_ty,
            ))
        }

        // ---------- binary ----------
        SqlExpr::Binary { left, op, right } => {
            let (l, l_ty) = bind_expr(left, scope)?;
            let (r, r_ty) = bind_expr(right, scope)?;
            let result_ty = infer_binary_type(*op, &l_ty, &r_ty)?;

            Ok((
                BoundExpr::Binary {
                    left: Box::new(l),
                    op: *op,
                    right: Box::new(r),
                },
                result_ty,
            ))
        }
    }
}
fn literal_type(v: &Value) -> DataType {
    match v {
        Value::Int32(_) => DataType::Int32,
        Value::Int64(_) => DataType::Int64,
        Value::Float32(_) => DataType::Float32,
        Value::Float64(_) => DataType::Float64,
        Value::Boolean(_) => DataType::Boolean,
        Value::String(_) => DataType::Varchar { max_len: None },
        Value::Blob(_) => DataType::Blob,
        Value::Date(_) => DataType::Date,
        Value::Timestamp(_) => DataType::Timestamp,
        Value::Null => DataType::Null,
    }
}

fn infer_unary_type(op: UnaryOp, inner: &DataType) -> Result<DataType, BindError> {
    match op {
        UnaryOp::Minus => {
            if *inner == DataType::Int64 || *inner == DataType::Float64 {
                Ok(inner.clone())
            } else {
                Err(BindError::TypeMismatchUnary {
                    op: format!("{:?}", op),
                    found: inner.clone(),
                })
            }
        }

        UnaryOp::Not => {
            if *inner == DataType::Boolean {
                Ok(DataType::Boolean)
            } else {
                Err(BindError::TypeMismatchUnary {
                    op: format!("{:?}", op),
                    found: inner.clone(),
                })
            }
        }
    }
}

fn infer_binary_type(
    op: BinaryOp,
    left: &DataType,
    right: &DataType,
) -> Result<DataType, BindError> {
    match op {
        Add | Sub | Mul | Div => {
            if left == right && (*left == DataType::Int64 || *left == DataType::Float64) {
                Ok(left.clone())
            } else {
                Err(BindError::TypeMismatchBinary {
                    op: format!("{:?}", op),
                    left: left.clone(),
                    right: right.clone(),
                })
            }
        }

        Eq | Neq | Lt | Lte | Gt | Gte => {
            if left == right || *left == DataType::Null || *right == DataType::Null {
                Ok(DataType::Boolean)
            } else {
                Err(BindError::TypeMismatchBinary {
                    op: format!("{:?}", op),
                    left: left.clone(),
                    right: right.clone(),
                })
            }
        }

        And | Or => {
            if *left == DataType::Boolean && *right == DataType::Boolean {
                Ok(DataType::Boolean)
            } else {
                Err(BindError::TypeMismatchBinary {
                    op: format!("{:?}", op),
                    left: left.clone(),
                    right: right.clone(),
                })
            }
        }
    }
}
