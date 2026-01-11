//! Statement binding.
//!
//! Converts SQL AST statements into BoundStatement.
//! Owns table resolution, scope construction, and statement shape.

use std::collections::HashMap;

use crate::binder::bind_expr::bind_expr;
use crate::binder::bound::*;
use crate::binder::errors::BindError;
use crate::binder::scope::ColumnScope;
use crate::catalog::catalog::Catalog;
use crate::frontend::sql::ast::*;
use crate::ir::plan::JoinType;
use crate::types::datatype::DataType;

pub struct Binder<'a> {
    pub catalog: &'a Catalog,
}
impl<'a> Binder<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    pub fn bind_statement(&self, stmt: Statement) -> Result<BoundStatement, BindError> {
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
            } => Ok(BoundStatement::CreateIndex(
                self.bind_create_index(name, table, column)?,
            )),

            Statement::DropIndex { name } => {
                Ok(BoundStatement::DropIndex(self.bind_drop_index(name)?))
            }

            Statement::Explain { analyze, stmt } => {
                let inner = self.bind_statement(*stmt)?;
                Ok(BoundStatement::Explain {
                    analyze,
                    stmt: Box::new(inner),
                })
            }
        }
    }
}

impl<'a> Binder<'a> {
    fn bind_select(&self, stmt: SelectStmt) -> Result<BoundSelect, BindError> {
        // 1. Resolve FROM clause
        let (from, scope) = self.bind_from(stmt.from)?;

        // 2. Bind projection
        let mut projection = Vec::new();
        for item in stmt.columns {
            match item.expr {
                // SELECT *
                Expr::Column { name, table } if name == "*" => {
                    self.expand_star(&mut projection, table.as_deref(), &scope)?;
                }

                other => {
                    let (expr, _) = bind_expr(&other, &scope)?;
                    projection.push(expr);
                }
            }
        }

        if projection.is_empty() {
            return Err(BindError::EmptyProject);
        }

        // 3. WHERE
        let selection = stmt
            .where_clause
            .map(|e| bind_expr(&e, &scope).map(|(x, _)| x))
            .transpose()?;

        // 4. ORDER BY
        let order_by = stmt
            .order_by
            .into_iter()
            .map(|o| {
                let (expr, _) = bind_expr(&o.expr, &scope)?;
                Ok((expr, o.asc))
            })
            .collect::<Result<Vec<_>, BindError>>()?;

        // 5. LIMIT / OFFSET
        let limit = stmt.limit.map(|v| v as u64);
        let offset = stmt.offset.map(|v| v as u64);

        Ok(BoundSelect {
            projection,
            from,
            selection,
            order_by,
            limit,
            offset,
        })
    }
}

impl<'a> Binder<'a> {
    fn bind_from(&self, from: FromItem) -> Result<(BoundFrom, ColumnScope), BindError> {
        let mut columns_scope = ColumnScope::new();
        let from = self.bind_from_inner(from, &mut columns_scope)?;
        Ok((from, columns_scope))
    }

    fn bind_from_inner(
        &self,
        from: FromItem,
        scope: &mut ColumnScope,
    ) -> Result<BoundFrom, BindError> {
        match from {
            FromItem::Table { name, .. } => {
                let table = self
                    .catalog
                    .get_table_by_name(&name)
                    .ok_or_else(|| BindError::UnknownTable(name.clone()))?;

                for col in &table.schema.columns {
                    scope.add_column(col.name.clone(), col.id, col.data_type.clone())?;
                }

                Ok(BoundFrom::Table { table_id: table.id })
            }

            FromItem::Join { left, right, on } => {
                let left = self.bind_from_inner(*left, scope)?;
                let right = self.bind_from_inner(*right, scope)?;

                let (on_expr, ty) = bind_expr(&on, scope)?;
                if ty != DataType::Boolean {
                    return Err(BindError::TypeMismatchBinary {
                        op: "JOIN ON".into(),
                        left: ty,
                        right: DataType::Boolean,
                    });
                }

                Ok(BoundFrom::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    on: on_expr,
                    join_type: JoinType::Inner,
                })
            }
        }
    }

    fn bind_insert(&self, stmt: InsertStmt) -> Result<BoundInsert, BindError> {
        let mut rows = Vec::new();

        let table = self
            .catalog
            .get_table_by_name(&stmt.table)
            .ok_or_else(|| BindError::UnknownTable(stmt.table.clone()))?;

        let mut scope = ColumnScope::new();

        for col in table.schema.columns.iter() {
            scope.add_column(col.name.clone(), col.id, col.data_type.clone())?
        }
        for row in stmt.rows {
            if row.len() != table.schema.columns.len() {
                return Err(BindError::ColumnCountMismatch);
            }

            let mut bound = Vec::new();
            for expr in row {
                let (e, _) = bind_expr(&expr, &scope)?;
                bound.push(e);
            }
            rows.push(bound);
        }

        Ok(BoundInsert {
            table_id: table.id,
            rows,
        })
    }

    fn bind_update(&self, stmt: UpdateStmt) -> Result<BoundUpdate, BindError> {
        let table = self
            .catalog
            .get_table_by_name(&stmt.table)
            .ok_or_else(|| BindError::UnknownTable(stmt.table.clone()))?;

        let mut scope = ColumnScope::new();

        for col in table.schema.columns.iter() {
            scope.add_column(col.name.clone(), col.id, col.data_type.clone())?
        }
        let assignments = stmt
            .assignments
            .into_iter()
            .map(|(name, expr)| {
                let (e, _) = bind_expr(&expr, &scope)?;
                let col = table
                    .schema
                    .column_named(&name)
                    .ok_or_else(|| BindError::UnknownColumn(name.clone()))?;
                Ok((col.id, e))
            })
            .collect::<Result<Vec<_>, BindError>>()?;

        let predicate = stmt
            .where_clause
            .map(|e| bind_expr(&e, &scope).map(|(x, _)| x))
            .transpose()?;

        Ok(BoundUpdate {
            table_id: table.id,
            assignments,
            predicate,
        })
    }

    fn bind_delete(&self, stmt: DeleteStmt) -> Result<BoundDelete, BindError> {
        let table = self
            .catalog
            .get_table_by_name(&stmt.table)
            .ok_or_else(|| BindError::UnknownTable(stmt.table.clone()))?;

        let mut scope = ColumnScope::new();

        for col in table.schema.columns.iter() {
            scope.add_column(col.name.clone(), col.id, col.data_type.clone())?
        }

        let predicate = stmt
            .where_clause
            .map(|e| bind_expr(&e, &scope).map(|(x, _)| x))
            .transpose()?;

        Ok(BoundDelete {
            table_id: table.id,
            predicate,
        })
    }

    fn expand_star(
        &self,
        out: &mut Vec<BoundExpr>,
        table: Option<&str>,
        scope: &ColumnScope,
    ) -> Result<(), BindError> {
        match table {
            Some(_) => {
                return Err(BindError::NotImplemented("qualified *".into()));
            }

            None => {
                for (id, _) in scope.iter_columns() {
                    out.push(BoundExpr::Column { column_id: id });
                }
            }
        }
        Ok(())
    }
}
