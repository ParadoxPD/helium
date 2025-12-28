use std::collections::HashMap;

use crate::common::value::Value;
use crate::frontend::sql::ast::{BinaryOp, Expr as SqlExpr};
use crate::frontend::sql::ast::{FromItem, SelectStmt};
use crate::ir::expr::{ColumnRef, Expr as IRExpr};

#[derive(Debug)]
pub enum BindError {
    UnknownTable(String),
    UnknownColumn(String),
    AmbiguousColumn(String),
}

pub struct BoundSelect {
    pub columns: Vec<IRExpr>,
    pub from: BoundFromItem,
    pub where_clause: Option<IRExpr>,
    pub order_by: Vec<(IRExpr, bool)>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum BoundFromItem {
    Table {
        name: String,
        alias: String,
    },
    Join {
        left: Box<BoundFromItem>,
        right: Box<BoundFromItem>,
        on: IRExpr,
    },
}

pub struct Binder {
    tables: HashMap<String, String>,
}

impl Binder {
    pub fn bind(stmt: SelectStmt) -> Result<BoundSelect, BindError> {
        let mut binder = Binder {
            tables: HashMap::new(),
        };

        binder.collect_tables(&stmt.from)?;
        let from = binder.bind_from(stmt.from)?;
        let columns = stmt.columns;
        let where_clause = stmt.where_clause.map(|e| binder.bind_expr(e)).transpose()?;
        let order_by = stmt.order_by;

        Ok(BoundSelect {
            columns: columns
                .into_iter()
                .map(|c| binder.bind_expr(SqlExpr::Column(c.into())))
                .collect::<Result<_, _>>()?,
            from,
            where_clause,
            order_by: order_by
                .into_iter()
                .map(|o| Ok((binder.bind_expr(SqlExpr::Column(o.column))?, o.asc)))
                .collect::<Result<_, _>>()?,
            limit: stmt.limit,
        })
    }

    fn collect_tables(&mut self, from: &FromItem) -> Result<(), BindError> {
        match from {
            FromItem::Table { name, alias } => {
                let key = alias.as_ref().unwrap_or(name);
                self.tables.insert(key.clone(), name.clone());
            }
            FromItem::Join { left, right, .. } => {
                self.collect_tables(left)?;
                self.collect_tables(right)?;
            }
        }
        Ok(())
    }

    fn bind_from(&mut self, from: FromItem) -> Result<BoundFromItem, BindError> {
        match from {
            FromItem::Table { name, alias } => {
                let alias = alias.unwrap_or_else(|| name.clone());
                Ok(BoundFromItem::Table { name, alias })
            }

            FromItem::Join { left, right, on } => Ok(BoundFromItem::Join {
                left: Box::new(self.bind_from(*left)?),
                right: Box::new(self.bind_from(*right)?),
                on: self.bind_expr(on)?,
            }),
        }
    }

    fn bind_expr(&self, expr: SqlExpr) -> Result<IRExpr, BindError> {
        match expr {
            SqlExpr::Column(c) => {
                if let Some((table, name)) = c.split_once('.') {
                    self.bind_column(ColumnRef {
                        table: Some(table.to_string()),
                        name: name.to_string(),
                        index: None,
                    })
                } else {
                    self.bind_column(ColumnRef {
                        table: None,
                        name: c,
                        index: None,
                    })
                }
            }

            SqlExpr::Binary { left, op, right } => {
                let ir_op = match op {
                    BinaryOp::Eq => crate::ir::expr::BinaryOp::Eq,
                    BinaryOp::Gt => crate::ir::expr::BinaryOp::Gt,
                    BinaryOp::Lt => crate::ir::expr::BinaryOp::Lt,
                    BinaryOp::And => crate::ir::expr::BinaryOp::And,
                };
                Ok(IRExpr::Binary {
                    left: Box::new(self.bind_expr(*left)?),
                    op: ir_op,
                    right: Box::new(self.bind_expr(*right)?),
                })
            }

            SqlExpr::LiteralInt(v) => Ok(IRExpr::lit(Value::Int64(v))),
        }
    }

    fn bind_column(&self, col: ColumnRef) -> Result<IRExpr, BindError> {
        match col.table {
            Some(alias) => {
                if !self.tables.contains_key(&alias) {
                    return Err(BindError::UnknownTable(alias));
                }
                Ok(IRExpr::BoundColumn {
                    table: alias,
                    name: col.name,
                })
            }

            None => {
                let matches: Vec<_> = self.tables.keys().collect();

                if matches.is_empty() {
                    return Err(BindError::UnknownColumn(col.name));
                }

                if matches.len() > 1 {
                    return Err(BindError::AmbiguousColumn(col.name));
                }

                Ok(IRExpr::BoundColumn {
                    table: matches[0].clone(),
                    name: col.name,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::sql::parser::parse;

    #[test]
    fn binds_simple_column() {
        let stmt = parse("SELECT id FROM users;");
        let bound = match stmt {
            crate::frontend::sql::ast::Statement::Select(s) => Binder::bind(s).unwrap(),
            _ => panic!(),
        };

        assert!(matches!(bound.where_clause, None));
    }

    #[test]
    fn errors_on_ambiguous_column() {
        let stmt = parse("SELECT id FROM users u JOIN orders o ON u.id = o.user_id;");

        let res = match stmt {
            crate::frontend::sql::ast::Statement::Select(s) => Binder::bind(s),
            _ => panic!(),
        };

        assert!(matches!(res, Err(BindError::AmbiguousColumn(_))));
    }

    #[test]
    fn binds_qualified_column() {
        let stmt = parse("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id;");

        let bound = match stmt {
            crate::frontend::sql::ast::Statement::Select(s) => Binder::bind(s).unwrap(),
            _ => panic!(),
        };

        // binding succeeded
        assert!(bound.columns.len() == 1);
    }
}
