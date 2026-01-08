use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::common::schema::Column;
use crate::common::schema::Schema;
use crate::common::types::DataType;
use crate::common::value::Value;
use crate::exec::catalog::Catalog;
use crate::frontend::sql::ast::SqlType;
use crate::frontend::sql::ast::Statement;
use crate::frontend::sql::ast::{
    BinaryOp, CreateTableStmt, DeleteStmt, DropTableStmt, Expr as SqlExpr, FromItem, InsertStmt,
    SelectStmt, UnaryOp, UpdateStmt,
};
use crate::ir::expr::{BinaryOp as IRBinaryOp, Expr as IRExpr, UnaryOp as IRUnaryOp};

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindError {
    UnknownTable(String),
    UnknownColumn(String),
    AmbiguousColumn(String),
    DuplicateColumn(String),
    ColumnCountMismatch,
    Unsupported,
    EmptyTable,
    TypeMismatch {
        column: String,
        expected: String,
        found: String,
    },
    TypeMismatchBinary {
        op: String,
        left: DataType,
        right: DataType,
    },
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindError::UnknownTable(t) => write!(f, "'{}' table does not exist", t),
            BindError::UnknownColumn(c) => write!(f, "'{}' column does not exist", c),
            BindError::AmbiguousColumn(c) => write!(f, "ambiguous column '{}'", c),
            BindError::ColumnCountMismatch => write!(f, "column count mismatch"),
            BindError::Unsupported => write!(f, "Unsupported Statement"),
            BindError::DuplicateColumn(c) => write!(f, "duplicate column '{}'", c),
            BindError::EmptyTable => write!(f, "table must have at least one column"),
            BindError::TypeMismatch {
                column,
                expected,
                found,
            } => write!(
                f,
                "Type mismatch on column : {}, expected : {}, got : {}",
                column, expected, found
            ),
            BindError::TypeMismatchBinary { op, left, right } => {
                write!(f, "{} {} {}", op, left, right)
            }
        }
    }
}

impl std::error::Error for BindError {}

#[derive(Debug)]
pub struct BoundSelect {
    pub columns: Vec<(IRExpr, String)>,
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
    pub tables: HashMap<String, String>,
    pub catalog: &'a Catalog,
}

#[derive(Debug)]
pub struct BoundCreateTable {
    pub table: String,
    pub schema: Schema, // Vec<Column>
}

#[derive(Debug)]
pub struct BoundDropTable {
    pub table: String,
}

#[derive(Debug)]
pub struct BoundUpdate {
    pub table: String,
    pub assignments: Vec<(Column, IRExpr)>,
    pub predicate: Option<IRExpr>,
}

#[derive(Debug)]
pub struct BoundInsert {
    pub table: String,
    pub rows: Vec<Vec<IRExpr>>,
}

#[derive(Debug)]
pub struct BoundDelete {
    pub table: String,
    pub predicate: Option<IRExpr>,
}

#[derive(Debug)]
pub struct BoundCreateIndex {
    pub name: String,
    pub table: String,
    pub column: String,
}

#[derive(Debug)]
pub struct BoundDropIndex {
    pub name: String,
}

#[derive(Debug)]
pub enum BoundStatement {
    Select(BoundSelect),
    Insert(BoundInsert),
    Update(BoundUpdate),
    Delete(BoundDelete),
    CreateTable(BoundCreateTable),
    DropTable(BoundDropTable),
    CreateIndex(BoundCreateIndex),
    DropIndex(BoundDropIndex),

    Explain {
        analyze: bool,
        stmt: Box<BoundStatement>,
    },
}

impl<'a> Binder<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self {
            catalog,
            tables: HashMap::new(),
        }
    }

    pub fn bind_statement(&mut self, stmt: Statement) -> Result<BoundStatement, BindError> {
        println!(
            "Catalog table keys in bind statement {:?}",
            self.catalog.tables.keys()
        );
        println!("table keys in bind statement {:?}", self.tables);
        match stmt {
            Statement::Select(s) => Ok(BoundStatement::Select(self.bind_select(s)?)),
            Statement::Insert(s) => Ok(BoundStatement::Insert(self.bind_insert(s)?)),
            Statement::Update(s) => Ok(BoundStatement::Update(self.bind_update(s)?)),
            Statement::Delete(s) => Ok(BoundStatement::Delete(self.bind_delete(s)?)),
            Statement::CreateTable(s) => {
                Ok(BoundStatement::CreateTable(self.bind_create_table(s)?))
            }
            Statement::DropTable(s) => Ok(BoundStatement::DropTable(self.bind_drop_table(s)?)),
            Statement::CreateIndex {
                name,
                table,
                column,
            } => self.bind_create_index(name, table, column),
            Statement::DropIndex { name } => self.bind_drop_index(name),
            Statement::Explain { analyze, stmt } => {
                let inner = self.bind_statement(*stmt)?;
                Ok(BoundStatement::Explain {
                    analyze,
                    stmt: Box::new(inner),
                })
            }
        }
    }

    pub fn bind_select(&mut self, stmt: SelectStmt) -> Result<BoundSelect, BindError> {
        // collect FROM tables first
        self.collect_tables(&stmt.from)?;
        let from = self.bind_from(stmt.from)?;

        let mut bound_columns = Vec::new();

        for item in stmt.columns {
            let expr = item.expr;
            let alias = item.alias; // <-- THIS was missing

            match expr {
                // SELECT *
                SqlExpr::Column { name, table } if name == "*" => {
                    match table {
                        Some(alias_tbl) => {
                            let table_name = self
                                .tables
                                .get(&alias_tbl)
                                .ok_or_else(|| BindError::UnknownTable(alias_tbl.clone()))?
                                .clone();

                            let table = self
                                .catalog
                                .get_table(&table_name)
                                .ok_or_else(|| BindError::UnknownTable(table_name.clone()))?;

                            for col in &table.schema.columns {
                                bound_columns.push((
                                    IRExpr::BoundColumn {
                                        table: alias_tbl.clone(),
                                        name: col.name.clone(),
                                    },
                                    col.name.clone(), // output name
                                ));
                            }
                        }

                        None => {
                            if self.tables.len() != 1 {
                                return Err(BindError::AmbiguousColumn("*".into()));
                            }

                            let (alias_tbl, table_name) = self.tables.iter().next().unwrap();
                            let table = self
                                .catalog
                                .get_table(table_name)
                                .ok_or_else(|| BindError::UnknownTable(table_name.clone()))?;

                            for col in &table.schema.columns {
                                bound_columns.push((
                                    IRExpr::BoundColumn {
                                        table: alias_tbl.clone(),
                                        name: col.name.clone(),
                                    },
                                    col.name.clone(),
                                ));
                            }
                        }
                    }
                }

                // normal expression
                other => {
                    let bound = self.bind_expr(other)?;
                    let name = match (&bound, alias) {
                        (IRExpr::BoundColumn { name, .. }, None) => name.clone(),
                        (_, Some(a)) => a,
                        _ => "expr".to_string(),
                    };

                    bound_columns.push((bound, name));
                }
            }
        }

        let where_clause = match stmt.where_clause {
            Some(e) => Some(self.bind_expr(e)?),
            None => None,
        };

        let order_by = stmt
            .order_by
            .into_iter()
            .map(|o| Ok((self.bind_expr(o.expr)?, o.asc)))
            .collect::<Result<Vec<_>, BindError>>()?;

        Ok(BoundSelect {
            columns: bound_columns,
            from,
            where_clause,
            order_by,
            limit: stmt.limit,
        })
    }

    pub fn collect_tables(&mut self, from: &FromItem) -> Result<(), BindError> {
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

    pub fn resolve_column(&self, table_alias: &str, column: &str) -> Result<(), BindError> {
        let table_name = self
            .tables
            .get(table_alias)
            .ok_or_else(|| BindError::UnknownTable(table_alias.into()))?;

        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| BindError::UnknownTable(table_name.clone()))?;

        if !table.schema.has_column(column) {
            return Err(BindError::UnknownColumn(column.into()));
        }

        Ok(())
    }

    pub fn bind_from(&self, from: FromItem) -> Result<BoundFromItem, BindError> {
        match from {
            FromItem::Table { name, alias } => {
                println!("ALIAS = {:?}", alias);
                let alias = match alias {
                    Some(a) => a,
                    None => name.clone(),
                };

                Ok(BoundFromItem::Table { name, alias })
            }

            FromItem::Join { left, right, on } => Ok(BoundFromItem::Join {
                left: Box::new(self.bind_from(*left)?),
                right: Box::new(self.bind_from(*right)?),
                on: self.bind_expr(on)?,
            }),
        }
    }

    pub fn bind_expr(&self, expr: SqlExpr) -> Result<IRExpr, BindError> {
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
            SqlExpr::Unary { op, expr } => {
                let ir_op = match op {
                    UnaryOp::Not => IRUnaryOp::Not,
                    UnaryOp::Minus => IRUnaryOp::Neg,
                };
                Ok(IRExpr::Unary {
                    op: ir_op,
                    expr: Box::new(self.bind_expr(*expr)?),
                })
            }
        }
    }

    pub fn bind_column(&self, table: Option<String>, name: String) -> Result<IRExpr, BindError> {
        println!("TABLE {:?}", table);
        match table {
            Some(alias) => {
                self.resolve_column(&alias, &name)?;
                Ok(IRExpr::BoundColumn { table: alias, name })
            }
            None => {
                let mut matches = Vec::new();

                for alias in self.tables.keys() {
                    if self.resolve_column(alias, &name).is_ok() {
                        matches.push(alias.clone());
                    }
                }

                match matches.len() {
                    0 => Err(BindError::UnknownColumn(name)),
                    1 => Ok(IRExpr::BoundColumn {
                        table: matches[0].clone(),
                        name,
                    }),
                    _ => Err(BindError::AmbiguousColumn(name)),
                }
            }
        }
    }

    pub fn bind_create_table(&self, stmt: CreateTableStmt) -> Result<BoundCreateTable, BindError> {
        let mut seen = HashSet::new();
        let mut columns = Vec::new();

        for col in stmt.columns {
            if !seen.insert(col.name.clone()) {
                return Err(BindError::DuplicateColumn(col.name));
            }

            let ty = match col.ty {
                SqlType::Int => DataType::Int64,
                SqlType::Bool => DataType::Bool,
                SqlType::Text => DataType::String,
            };

            columns.push(Column {
                name: col.name,
                ty,
                nullable: col.nullable,
            });
        }

        if columns.is_empty() {
            return Err(BindError::EmptyTable);
        }
        println!(
            "Catalog table keys in bind statement {:?}",
            self.catalog.tables.keys()
        );
        println!("table keys in bind statement {:?}", self.tables);

        Ok(BoundCreateTable {
            table: stmt.table_name,
            schema: Schema { columns },
        })
    }

    pub fn bind_drop_table(&self, stmt: DropTableStmt) -> Result<BoundDropTable, BindError> {
        if !self.catalog.table_exists(&stmt.table_name) {
            return Err(BindError::UnknownTable(stmt.table_name));
        }

        Ok(BoundDropTable {
            table: stmt.table_name,
        })
    }

    pub fn bind_update(&mut self, stmt: UpdateStmt) -> Result<BoundUpdate, BindError> {
        let table = self
            .catalog
            .get_table(&stmt.table)
            .ok_or_else(|| BindError::UnknownTable(stmt.table.clone()))?;

        self.tables.clear();
        self.tables.insert(stmt.table.clone(), stmt.table.clone());

        let mut assigns = Vec::new();

        for (col_name, sql_expr) in stmt.assignments {
            let column = table
                .schema
                .lookup(col_name.clone())
                .ok_or_else(|| BindError::UnknownColumn(col_name.clone()))?;

            let ir_expr = self.bind_expr(sql_expr)?;

            let expr_ty = self.infer_expr_type(&ir_expr, &col_name)?;

            if expr_ty == DataType::Null && column.nullable {
            } else if expr_ty != column.ty {
                return Err(BindError::TypeMismatch {
                    expected: column.ty.to_string(),
                    found: expr_ty.to_string(),
                    column: col_name,
                });
            }

            assigns.push((column.clone(), ir_expr));
        }

        let predicate = stmt.where_clause.map(|e| self.bind_expr(e)).transpose()?;

        Ok(BoundUpdate {
            table: stmt.table,
            assignments: assigns,
            predicate,
        })
    }

    pub fn bind_insert(&self, stmt: InsertStmt) -> Result<BoundInsert, BindError> {
        let table = self
            .catalog
            .get_table(&stmt.table)
            .ok_or_else(|| BindError::UnknownTable(stmt.table.clone()))?;

        let mut bound_rows = Vec::new();

        for row in stmt.rows {
            if row.len() != table.schema.columns.len() {
                return Err(BindError::ColumnCountMismatch);
            }

            let mut values = Vec::new();

            for (sql_expr, column) in row.into_iter().zip(&table.schema.columns) {
                let ir_expr = self.bind_expr(sql_expr)?;
                let ty = self.infer_expr_type(&ir_expr, &column.name)?;

                if ty == DataType::Null && column.nullable {
                    // allowed
                } else if ty != column.ty {
                    return Err(BindError::TypeMismatch {
                        expected: column.ty.to_string(),
                        found: ty.to_string(),
                        column: column.name.clone(),
                    });
                }

                values.push(ir_expr);
            }

            bound_rows.push(values);
        }

        Ok(BoundInsert {
            table: stmt.table,
            rows: bound_rows,
        })
    }

    pub fn bind_delete(&mut self, stmt: DeleteStmt) -> Result<BoundDelete, BindError> {
        if !self.catalog.table_exists(&stmt.table) {
            return Err(BindError::UnknownTable(stmt.table.clone()));
        }

        self.tables.clear();
        self.tables.insert(stmt.table.clone(), stmt.table.clone());

        let pred = stmt.where_clause.map(|e| self.bind_expr(e)).transpose()?;

        Ok(BoundDelete {
            table: stmt.table,
            predicate: pred,
        })
    }

    pub fn bind_create_index(
        &self,
        name: String,
        table: String,
        column: String,
    ) -> Result<BoundStatement, BindError> {
        let table_entry = self
            .catalog
            .get_table(&table)
            .ok_or_else(|| BindError::UnknownTable(table.clone()))?;

        if !table_entry.schema.has_column(&column) {
            return Err(BindError::UnknownColumn(column));
        }

        Ok(BoundStatement::CreateIndex(BoundCreateIndex {
            name,
            table,
            column,
        }))
    }

    pub fn bind_drop_index(&self, name: String) -> Result<BoundStatement, BindError> {
        // NOTE: index existence validation can be added later
        Ok(BoundStatement::DropIndex(BoundDropIndex { name }))
    }

    pub fn infer_expr_type(
        &self,
        expr: &IRExpr,
        column_name: &String,
    ) -> Result<DataType, BindError> {
        match expr {
            // ---------- literals ----------
            IRExpr::Literal(Value::Int64(_)) => Ok(DataType::Int64),
            IRExpr::Literal(Value::Float64(_)) => Ok(DataType::Float64),
            IRExpr::Literal(Value::Bool(_)) => Ok(DataType::Bool),
            IRExpr::Literal(Value::String(_)) => Ok(DataType::String),
            IRExpr::Literal(Value::Null) => Ok(DataType::Null),

            // ---------- bound columns ----------
            IRExpr::BoundColumn { table, name } => {
                let table = self
                    .catalog
                    .get_table(table)
                    .ok_or_else(|| BindError::UnknownTable(table.clone()))?;

                let col = table.schema.lookup(name.to_string());
                Ok(col.unwrap().ty)
            }

            // ---------- unary ----------
            IRExpr::Unary { op, expr } => {
                let inner = self.infer_expr_type(expr, column_name)?;

                match op {
                    IRUnaryOp::Neg => {
                        if inner == DataType::Int64 {
                            Ok(DataType::Int64)
                        } else {
                            Err(BindError::TypeMismatch {
                                expected: DataType::Int64.to_string(),
                                found: inner.to_string(),
                                column: column_name.to_string(),
                            })
                        }
                    }

                    IRUnaryOp::Not => {
                        if inner == DataType::Bool {
                            Ok(DataType::Bool)
                        } else {
                            Err(BindError::TypeMismatch {
                                expected: DataType::Bool.to_string(),
                                found: inner.to_string(),
                                column: column_name.to_string(),
                            })
                        }
                    }
                }
            }

            // ---------- binary ----------
            IRExpr::Binary { left, op, right } => {
                let l = self.infer_expr_type(left, column_name)?;
                let r = self.infer_expr_type(right, column_name)?;

                self.infer_binary_type(op, l, r)
            }

            IRExpr::Null => Ok(DataType::Null),

            IRExpr::Column(_) => {
                panic!("Unbound column reached type inference")
            }
        }
    }

    fn infer_binary_type(
        &self,
        op: &IRBinaryOp,
        l: DataType,
        r: DataType,
    ) -> Result<DataType, BindError> {
        use IRBinaryOp::*;

        match op {
            Add | Sub | Mul | Div => {
                if l == DataType::Int64 && r == DataType::Int64 {
                    Ok(DataType::Int64)
                } else {
                    Err(BindError::TypeMismatchBinary {
                        op: format!("{:?}", op),
                        left: l,
                        right: r,
                    })
                }
            }

            Eq | Neq | Gt | Gte | Lt | Lte => {
                if l == r || l == DataType::Null || r == DataType::Null {
                    Ok(DataType::Bool)
                } else {
                    Err(BindError::TypeMismatchBinary {
                        op: format!("{:?}", op),
                        left: l,
                        right: r,
                    })
                }
            }

            And | Or => {
                if l == DataType::Bool && r == DataType::Bool {
                    Ok(DataType::Bool)
                } else {
                    Err(BindError::TypeMismatchBinary {
                        op: format!("{:?}", op),
                        left: l,
                        right: r,
                    })
                }
            }
        }
    }
}
