use std::collections::HashMap;

use crate::{
    binder::{bind_stmt::Binder, errors::BindError},
    frontend::sql::ast::FromItem,
    types::{
        datatype::DataType,
        schema::{Column, ColumnId, Schema},
    },
};

impl Schema {
    pub fn has_column_named(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c.name == name)
    }

    pub fn column_named(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_index_named(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

/// Column resolution scope.
/// Maps visible column names to ColumnId + DataType.
#[derive(Debug)]
pub struct ColumnScope {
    pub(crate) columns: HashMap<String, (ColumnId, DataType)>,
}

impl ColumnScope {
    pub fn new(columns: HashMap<String, (ColumnId, DataType)>) -> Self {
        Self { columns }
    }

    pub fn resolve(&self, name: &str) -> Result<(ColumnId, DataType), BindError> {
        self.columns
            .get(name)
            .cloned()
            .ok_or_else(|| BindError::UnknownColumn(name.to_string()))
    }
}

impl<'a> Binder<'a> {
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
}
