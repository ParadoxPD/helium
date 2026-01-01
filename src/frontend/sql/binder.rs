use std::collections::{HashMap, HashSet};

use anyhow::Result;
use anyhow::bail;

use crate::common::schema::Schema;
use crate::common::types::DataType;
use crate::common::value::Value;
use crate::exec::catalog::{self, Catalog};
use crate::frontend::sql::ast::SqlType;
use crate::frontend::sql::ast::{
    BinaryOp, CreateTableStmt, DeleteStmt, DropTableStmt, Expr as SqlExpr, FromItem, InsertStmt,
    SelectStmt, UpdateStmt,
};
use crate::ir::expr::{BinaryOp as IRBinaryOp, ColumnRef, Expr as IRExpr};

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

pub struct BoundCreateTable {
    pub table: String,
    pub schema: Schema, // Vec<Column>
}

pub struct BoundDropTable {
    pub table: String,
}

pub struct BoundUpdate {
    pub table: String,
    pub assignments: Vec<(ColumnRef, IRExpr)>,
    pub predicate: Option<IRExpr>,
}

pub struct BoundInsert {
    pub table: String,
    pub values: Vec<IRExpr>,
}

pub struct BoundDelete {
    pub table: String,
    pub predicate: Option<IRExpr>,
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

    fn bind_create_table(&self, stmt: CreateTableStmt) -> Result<BoundCreateTable> {
        let mut seen = HashSet::new();
        let mut schema = Vec::new();

        for col in stmt.columns {
            if !seen.insert(col.name.clone()) {
                bail!("duplicate column: {}", col.name);
            }
            let ty = match col.ty {
                SqlType::Int => DataType::Int64,
                SqlType::Bool => DataType::Bool,
                SqlType::Text => DataType::String,
            };
            schema.push(Column {
                name: col.name,
                ty, // SqlType → DataType
                nullable: col.nullable,
            });
        }

        if schema.is_empty() {
            bail!("table must have at least one column");
        }

        Ok(BoundCreateTable {
            table: stmt.table_name,
            schema,
        })
    }

    fn bind_drop_table(&self, stmt: DropTableStmt) -> Result<BoundDropTable> {
        if !self.catalog.table_exists(&stmt.table_name) {
            bail!("table does not exist");
        }

        Ok(BoundDropTable {
            table: stmt.table_name,
        })
    }

    fn bind_update(&self, stmt: UpdateStmt) -> Result<BoundUpdate> {
        let table = self.catalog.get_table(&stmt.table)?;

        let mut assigns = Vec::new();
        for (col, expr) in stmt.assignments {
            let column = table.schema.lookup(&col)?;
            let bound_expr = self.bind_expr(expr)?;

            self.typecheck_assign(&column, &bound_expr)?;

            assigns.push((column, bound_expr));
        }

        let predicate = stmt.where_clause.map(|e| self.bind_expr(e)).transpose()?;

        Ok(BoundUpdate {
            table: stmt.table,
            assignments: assigns,
            predicate,
        })
    }

    fn bind_insert(&self, stmt: InsertStmt) -> Result<BoundInsert> {
        let table = self.catalog.get_table(&stmt.table)?;

        if stmt.values.len() != table.schema.len() {
            bail!("column count mismatch");
        }

        let mut values = Vec::new();

        for (expr, col) in stmt.values.into_iter().zip(&table.schema) {
            let bound = self.bind_expr(expr)?;
            self.typecheck_assign(col, &bound)?;
            values.push(bound);
        }

        Ok(BoundInsert {
            table: stmt.table,
            values,
        })
    }

    fn bind_delete(&self, stmt: DeleteStmt) -> Result<BoundDelete> {
        self.catalog.get_table(&stmt.table)?;

        let pred = stmt.where_clause.map(|e| self.bind_expr(e)).transpose()?;

        Ok(BoundDelete {
            table: stmt.table,
            predicate: pred,
        })
    }
}
