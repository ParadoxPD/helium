use std::collections::HashMap;

use crate::common::value::Value;
use crate::exec::catalog::{self, Catalog};
use crate::frontend::sql::ast::{BinaryOp, Expr as SqlExpr, FromItem, SelectStmt};
use crate::ir::expr::{BinaryOp as IRBinaryOp, Expr as IRExpr};

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

pub struct Binder<'a> {
    // alias -> table name
    tables: HashMap<String, String>,
    catalog: &'a Catalog,
}

impl<'a> Binder<'a> {
    pub fn bind(stmt: SelectStmt, catalog: &Catalog) -> Result<BoundSelect, BindError> {
        let mut binder = Binder {
            tables: HashMap::new(),
            catalog,
        };

        binder.collect_tables(&stmt.from)?;
        let from = binder.bind_from(stmt.from)?;

        let mut bound_columns = Vec::new();

        for expr in stmt.columns {
            match expr {
                SqlExpr::Column { name, table } if name == "*" => {
                    let alias = table.unwrap_or_else(|| {
                        if binder.tables.len() != 1 {
                            return "AmbiguousColumn".to_string();
                            //return Err(BindError::AmbiguousColumn("*".into()));
                        }
                        binder.tables.keys().next().unwrap().clone()
                    });

                    let table_name = binder.tables.get(&alias).unwrap();
                    let schema = binder
                        .catalog
                        .get(table_name)
                        .ok_or_else(|| BindError::UnknownTable(table_name.clone()))?
                        .schema()
                        .clone();

                    for col in schema {
                        bound_columns.push(IRExpr::BoundColumn {
                            table: alias.clone(),
                            name: col.clone(),
                        });
                    }
                }
                other => bound_columns.push(binder.bind_expr(other)?),
            }
        }

        let where_clause = match stmt.where_clause {
            Some(e) => Some(binder.bind_expr(e)?),
            None => None,
        };

        let order_by = stmt
            .order_by
            .into_iter()
            .map(|o| Ok((binder.bind_expr(o.expr)?, o.asc)))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(BoundSelect {
            columns: bound_columns,
            from,
            where_clause,
            order_by,
            limit: stmt.limit,
        })
    }

    fn collect_tables(&mut self, from: &FromItem) -> Result<(), BindError> {
        match from {
            FromItem::Table { name, alias } => {
                let alias = alias.clone().unwrap_or_else(|| name.clone());
                self.tables.insert(alias, name.clone());
            }

            FromItem::Join { left, right, .. } => {
                self.collect_tables(left)?;
                self.collect_tables(right)?;
            }
        }
        Ok(())
    }

    fn bind_from(&self, from: FromItem) -> Result<BoundFromItem, BindError> {
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
            SqlExpr::Column { table, name } => self.bind_column(table, name),

            SqlExpr::Literal(v) => Ok(IRExpr::Literal(v)),

            SqlExpr::Binary { left, op, right } => {
                let ir_op = match op {
                    BinaryOp::Eq => IRBinaryOp::Eq,
                    BinaryOp::Neq => IRBinaryOp::Neq,
                    BinaryOp::Gt => IRBinaryOp::Gt,
                    BinaryOp::Gte => IRBinaryOp::Gte,
                    BinaryOp::Lt => IRBinaryOp::Lt,
                    BinaryOp::Lte => IRBinaryOp::Lte,
                    BinaryOp::And => IRBinaryOp::And,
                    BinaryOp::Or => IRBinaryOp::Or,
                    BinaryOp::Add => IRBinaryOp::Add,
                    BinaryOp::Sub => IRBinaryOp::Sub,
                    BinaryOp::Mul => IRBinaryOp::Mul,
                    BinaryOp::Div => IRBinaryOp::Div,
                };

                Ok(IRExpr::Binary {
                    left: Box::new(self.bind_expr(*left)?),
                    op: ir_op,
                    right: Box::new(self.bind_expr(*right)?),
                })
            }
        }
    }

    fn bind_column(&self, table: Option<String>, name: String) -> Result<IRExpr, BindError> {
        match table {
            Some(alias) => {
                if !self.tables.contains_key(&alias) {
                    return Err(BindError::UnknownTable(alias));
                }
                Ok(IRExpr::BoundColumn { table: alias, name })
            }

            None => {
                let matches: Vec<_> = self.tables.keys().collect();

                if matches.is_empty() {
                    return Err(BindError::UnknownColumn(name));
                }

                if matches.len() > 1 {
                    return Err(BindError::AmbiguousColumn(name));
                }

                Ok(IRExpr::BoundColumn {
                    table: matches[0].clone(),
                    name,
                })
            }
        }
    }
}

mod tests {
    use super::*;
    use crate::exec::catalog::Catalog;
    use crate::frontend::sql::parser::parse;
    use crate::storage::table::HeapTable;
    use crate::{buffer::buffer_pool::BufferPool, storage::page_manager::FilePageManager};
    use std::sync::{Arc, Mutex};

    fn test_catalog() -> Catalog {
        let mut catalog = Catalog::new();

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let user_schema = vec!["id".into(), "name".into()];
        let mut users_table = HeapTable::new("users".into(), user_schema.clone(), 4, bp.clone());

        catalog.insert("users".into(), Arc::new(users_table));

        let order_schema = vec!["user_id".into()];
        let mut orders_table = HeapTable::new("users".into(), order_schema.clone(), 4, bp.clone());
        catalog.insert("orders".into(), Arc::new(orders_table));

        catalog
    }

    #[test]
    fn binds_simple_column() {
        let stmt = parse("SELECT id FROM users;");
        let catalog = test_catalog();

        let bound = match stmt {
            crate::frontend::sql::ast::Statement::Select(s) => Binder::bind(s, &catalog).unwrap(),
            _ => panic!(),
        };

        assert!(bound.where_clause.is_none());
    }

    #[test]
    fn errors_on_ambiguous_column() {
        let stmt = parse("SELECT id FROM users u JOIN orders o ON u.id = o.user_id;");
        let catalog = test_catalog();

        let res = match stmt {
            crate::frontend::sql::ast::Statement::Select(s) => Binder::bind(s, &catalog),
            _ => panic!(),
        };

        assert!(matches!(res, Err(BindError::AmbiguousColumn(_))));
    }

    #[test]
    fn binds_qualified_column() {
        let stmt = parse("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id;");
        let catalog = test_catalog();

        let bound = match stmt {
            crate::frontend::sql::ast::Statement::Select(s) => Binder::bind(s, &catalog).unwrap(),
            _ => panic!(),
        };

        assert_eq!(bound.columns.len(), 1);
    }
}
