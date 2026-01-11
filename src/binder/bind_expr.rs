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
use crate::frontend::sql::ast::{BinaryOp as AstBinaryOp, Expr as SqlExpr, UnaryOp as AstUnaryOp};
use crate::ir::expr::{BinaryOp as IrBinaryOp, UnaryOp as IrUnaryOp};
use crate::types::datatype::DataType;
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
        //SqlExpr::Null => Ok((BoundExpr::Null, DataType::Null)),

        // ---------- unary ----------
        SqlExpr::Unary { op, expr } => {
            let (inner, inner_ty) = bind_expr(expr, scope)?;
            let ir_op = lower_unary_op(*op);
            let result_ty = infer_unary_type(ir_op, &inner_ty)?;
            Ok((
                BoundExpr::Unary {
                    op: ir_op,
                    expr: Box::new(inner),
                },
                result_ty,
            ))
        }

        SqlExpr::Binary { left, op, right } => {
            let (l, l_ty) = bind_expr(left, scope)?;
            let (r, r_ty) = bind_expr(right, scope)?;
            let ir_op = lower_binary_op(*op);
            let result_ty = infer_binary_type(ir_op, &l_ty, &r_ty)?;

            Ok((
                BoundExpr::Binary {
                    left: Box::new(l),
                    op: ir_op,
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

fn infer_unary_type(op: IrUnaryOp, inner: &DataType) -> Result<DataType, BindError> {
    match op {
        IrUnaryOp::Neg => {
            if *inner == DataType::Int64 || *inner == DataType::Float64 {
                Ok(inner.clone())
            } else {
                Err(BindError::TypeMismatchUnary {
                    op: format!("{:?}", op),
                    found: inner.clone(),
                })
            }
        }

        IrUnaryOp::Not => {
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
    op: IrBinaryOp,
    left: &DataType,
    right: &DataType,
) -> Result<DataType, BindError> {
    match op {
        IrBinaryOp::Add | IrBinaryOp::Sub | IrBinaryOp::Mul | IrBinaryOp::Div => {
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

        IrBinaryOp::Eq
        | IrBinaryOp::Neq
        | IrBinaryOp::Lt
        | IrBinaryOp::Lte
        | IrBinaryOp::Gt
        | IrBinaryOp::Gte => {
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

        IrBinaryOp::And | IrBinaryOp::Or => {
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

fn lower_binary_op(op: AstBinaryOp) -> IrBinaryOp {
    match op {
        AstBinaryOp::Add => IrBinaryOp::Add,
        AstBinaryOp::Sub => IrBinaryOp::Sub,
        AstBinaryOp::Mul => IrBinaryOp::Mul,
        AstBinaryOp::Div => IrBinaryOp::Div,

        AstBinaryOp::Eq => IrBinaryOp::Eq,
        AstBinaryOp::Neq => IrBinaryOp::Neq,
        AstBinaryOp::Lt => IrBinaryOp::Lt,
        AstBinaryOp::Lte => IrBinaryOp::Lte,
        AstBinaryOp::Gt => IrBinaryOp::Gt,
        AstBinaryOp::Gte => IrBinaryOp::Gte,

        AstBinaryOp::And => IrBinaryOp::And,
        AstBinaryOp::Or => IrBinaryOp::Or,
    }
}

fn lower_unary_op(op: AstUnaryOp) -> IrUnaryOp {
    match op {
        AstUnaryOp::Minus => IrUnaryOp::Neg,
        AstUnaryOp::Not => IrUnaryOp::Not,
    }
}
